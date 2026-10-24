# Protocol Buffers spooling encoding: design notes

Branch: `feature/protobuf-spooling-encoding`

## Summary

The spooling client protocol gains three query data encodings, `protobuf`,
`protobuf+zstd` and `protobuf+lz4`, next to the existing `json`, `json+zstd`
and `json+lz4`. A segment is one `Segment` message in the Protocol Buffers
wire format, holding a length-delimited `Row` message per result row, and a
typed `Value` per column. The schema is
`client/trino-client/src/main/proto/trino_spooled_segment.proto`.

Clients advertise the encodings they support in `X-Trino-Query-Data-Encoding`
and the server picks the first one it supports, so a protobuf-capable client
talking to an older server, or a `json`-only client talking to a new server,
keeps working through the protocol's normal capability selection. The new
encodings are enabled by default on the server
(`protocol.spooling.encoding.protobuf*.enabled`). The Java client still lists
the `json` encodings first, so protobuf is opt-in through the `encoding`
connection property or the CLI `--encoding` option until the benchmark
results justify changing the default order.

## Why a hand-written wire codec

`trino-client` is shaded into the JDBC driver and has a deliberately small
dependency footprint (Jackson, OkHttp, Guava, aircompressor). It did not depend
on `protobuf-java`, and adding a runtime dependency on it would grow the
driver and tie its protobuf version to every embedding application. The
`.proto` file is therefore the authoritative contract, while the Java client
and server use a small hand-written reader and writer of the wire format
(`io.trino.client.spooling.encoding.protobuf.ProtobufWireReader` and
`ProtobufWireWriter`, about 500 lines together). `protobuf-java` and `protoc`
are test-scoped only: the client tests compile the schema and check both
directions against the generated classes, so the hand-written codec cannot
drift from the schema unnoticed.

The wire format needs only four of protobuf's features: varints (with zig-zag
for signed fields), fixed 32/64-bit values, length-delimited fields, and tags.
Unknown fields are skipped, so a newer server can add fields to any message
without breaking an older client, as long as it keeps the existing
`Value.kind` fields.

## Segment layout

```
Segment  := (tag(1, LEN) length Row)*
Row      := (tag(1, LEN) length Value)*
Value    := tag(kind, wire type) payload
```

The server writes one `Row` at a time to the segment output stream through a
reusable buffer, so memory use is bounded by the largest row, not the segment.
The client reads one `Row` per `next()` call of the row iterator, so decoding is
streaming and does not materialize the segment. Nested messages (array, map,
row, decimal, ...) are written into child buffers to learn their length before
the parent is written.

The compressed variants wrap the `protobuf` encoder with the existing
`CompressedQueryDataEncoder` (Zstandard or LZ4), with the same compression
threshold and `uncompressedSize` attribute as the `json` variants.

## Type mapping

Every Trino type maps to exactly one `Value.kind` field. The "client value"
column is the Java value `ProtobufQueryDataDecoder` produces, which is the
value `JsonQueryDataDecoder` produces for the same row, so the JDBC driver,
the CLI, and every other consumer of `trino-client` work unchanged.

| Trino type | `Value` field | Wire representation | Client value |
|---|---|---|---|
| `NULL` of any type | `null` (1) | `NullValue.NULL_VALUE` varint | `null` |
| `BOOLEAN` | `boolean` (2) | varint 0/1 | `Boolean` |
| `TINYINT` | `tinyint` (3) | zig-zag varint | `Byte` |
| `SMALLINT` | `smallint` (4) | zig-zag varint | `Short` |
| `INTEGER` | `integer` (5) | zig-zag varint | `Integer` |
| `BIGINT` | `bigint` (6) | zig-zag varint | `Long` |
| `REAL` | `real` (7) | fixed32 IEEE 754 (NaN, ±Infinity preserved) | `Float` |
| `DOUBLE` | `double` (8) | fixed64 IEEE 754 (NaN, ±Infinity preserved) | `Double` |
| `DECIMAL(p, s)` | `decimal` (9) | `Decimal{unscaled_long` (zig-zag, when the unscaled value fits in 64 bits) or `unscaled_bytes` (big-endian two's complement, `BigInteger.toByteArray`), `scale` (zig-zag)`}` | `String`, plain notation with `s` fraction digits |
| `VARCHAR(n)` | `varchar` (10) | UTF-8 bytes | `String` |
| `CHAR(n)` | `char` (11) | UTF-8 bytes without pad spaces | `String` padded to `n` code points |
| `VARBINARY` | `varbinary` (12) | bytes | `byte[]` |
| `DATE` | `date` (13) | zig-zag varint, days since 1970-01-01 | `String` `uuuu-MM-dd` |
| `TIME(p)` | `time` (14) | `Time{picos` since midnight (varint), `precision}` | `String` `HH:mm:ss[.f{p}]` |
| `TIME(p) WITH TIME ZONE` | `time_with_time_zone` (15) | `TimeWithTimeZone{picos, precision, offset_minutes` (zig-zag)`}` | `String` `HH:mm:ss[.f{p}]±HH:mm` |
| `TIMESTAMP(p)` | `timestamp` (16) | `Timestamp{epoch_micros` (zig-zag), `picos_of_micro, precision}` | `String` `uuuu-MM-dd HH:mm:ss[.f{p}]` |
| `TIMESTAMP(p) WITH TIME ZONE` | `timestamp_with_time_zone` (17) | `TimestampWithTimeZone{epoch_millis` (zig-zag), `picos_of_milli, precision, zone_id, offset_seconds` (zig-zag, the zone's offset at that instant, resolved by the server)`}` | `String` `uuuu-MM-dd HH:mm:ss[.f{p}] <zone id>` |
| `INTERVAL YEAR TO MONTH` | `interval_year_to_month` (18) | zig-zag varint, total months | `String` `Y-M` |
| `INTERVAL DAY TO SECOND` | `interval_day_to_second` (19) | `IntervalDayToSecond{micros` (zig-zag), `picos_of_micro, fractional_precision}` | `String` `D HH:mm:ss[.f{p}]` |
| `JSON` | `json` (20) | UTF-8 JSON text | `String` |
| `UUID` | `uuid` (21) | 16 bytes, network byte order | `String` canonical form |
| `IPADDRESS` | `ipaddress` (22) | 16 bytes, IPv4 as IPv4-mapped IPv6 | `String` canonical form |
| `NUMBER` | `number` (23) | `NumberValue{decimal` or `special` (NaN, ±Infinity)`}` | `String` |
| `VARIANT` | `variant` (24) | `VariantValue{binary{metadata, value}` when the client supports `VARIANT_BINARY`, else `json` text`}` | `EncodedVariant`, or `Object` parsed from JSON |
| `ARRAY(T)` | `array` (25) | `ArrayValue{repeated Value elements}` | unmodifiable `List` |
| `MAP(K, V)` | `map` (26) | `MapValue{repeated Value keys, repeated Value values}`, `keys[i]` ↔ `values[i]` | unmodifiable `Map` |
| `ROW(...)` | `row` (27) | `Row{repeated Value values}` in field order | `io.trino.client.Row` |
| other types whose client value is a `byte[]` (HyperLogLog, P4HyperLogLog, SetDigest, qdigest, tdigest, ...) | `varbinary` (12) | bytes | `byte[]` |
| other types whose client value is text (Geometry, SphericalGeography, color, ...) | `text` (28) | UTF-8 bytes | `String` |
| any other type (BingTile, KdbTree, ...) | `json_object` (29) | UTF-8 JSON text of the type's object value | `Object` parsed from JSON |

The last three rows are the only place where a value's representation is
decided by the Java type of `Type.getObjectValue`, and the `json_object` field
exists only for types the client has no representation for anyway; the `json`
encoding sends the same JSON document for them.

The server resolves the zone's offset at each instant and sends it with the
zone id. The client renders `epoch_millis + offset_seconds`, so it shows the
local date-time the `json` encoding shows (which the server formats with its
own time zone rules) without a time zone database of its own: a client JVM
with older tzdata, or without a zone the server knows (for example
`America/Ciudad_Juarez` on an old JDK 11), still renders the right value.

Map keys keep their type on the wire. The `json` encoding has to turn every key
into a JSON object key, which loses the type of non-string keys; the client
restores the Java value from the column type. The protobuf decoder produces the
same Java values, but a map with structural keys (array, map or row keys) is
reproduced faithfully rather than through the JSON string form.

### Client capabilities

The server honors the same client capabilities as the `json` encoding:

| Capability absent | Effect on the protobuf segment |
|---|---|
| `PARAMETRIC_DATETIME` | time and timestamp values are rounded to precision 3 before encoding, and `precision` is 3; map keys are sent at full precision, as the json encoding renders them with `toString()` |
| `PARAMETRIC_INTERVAL` | `INTERVAL DAY TO SECOND` is rounded to milliseconds, `fractional_precision` is 3, so the client formats the legacy form |
| `NUMBER` | the column is declared `varchar` and the value is sent as `varchar` text |
| `VARIANT_BINARY` | `VariantValue.json` is sent instead of `VariantValue.binary` |
| `VARIANT` | the column is declared `json` and the value is sent as `json` text |

### Validation on the client

The decoder checks every segment it reads and fails with
`ProtobufFormatException` instead of producing a wrong value:

- a `Value` whose field does not match the column type ("Expected value field
  10 but got 6"), or that sets no field;
- a `Row` with more values than columns, or fewer;
- a map with a different number of keys and values (keys that become equal
  only as rendered, such as both values of a zone's fall-back hour, merge
  last-value-wins like in the json decoder);
- a precision outside 0 to 12, a UUID or IP address that is not 16 bytes;
- temporal fields outside their range: picoseconds of a day, of a
  microsecond or of a millisecond, a time zone offset beyond ±14:00 for
  `TIME WITH TIME ZONE` or ±18:00 for `TIMESTAMP WITH TIME ZONE`, a negative
  decimal scale or empty unscaled bytes;
- a segment field tag of field number 0 or wider than 32 bits;
- malformed varints (more than 10 bytes, or a 32-bit field that does not fit),
  truncated messages, lengths that exceed the enclosing message, and the
  deprecated group wire types.

Unknown fields in `Segment`, `Row` (other than `values`), and the nested
messages are skipped, so the schema can grow without breaking older clients.

The row buffer grows with the bytes actually read, so a corrupt row length
cannot make the client allocate up to 2 GB before it notices the segment is
truncated.

### Server encoder

One `QueryDataEncoder` writes the segments of every driver of a query
(`OutputSpoolingOperatorFactory` shares it), so `encodeTo` runs concurrently.
The protobuf encoder keeps no per call state: the row writer is created for
each segment and the zone cache is a `ConcurrentHashMap`.

### Differences from the json encoding

The decoded values equal the `json` encoding's, with these exceptions, each
pinned by a test in `TestProtobufEncodingUtils`:

- **Variant as JSON text** (client without `VARIANT_BINARY`): the client's
  json decoder copies the JSON value through a parser, which normalizes
  numbers (`1.50` becomes `1.5`, a decimal beyond double precision is
  rounded). The protobuf decoder returns the text the server wrote.
- **Malformed UTF-8 in a varchar**: the client's json decoder fails, the
  protobuf decoder replaces each malformed sequence with U+FFFD. Generated
  proto3 parsers in other languages reject the segment, as the json decoder
  does.
- **Legacy day to second intervals at the ends of the range** (client without
  `PARAMETRIC_INTERVAL`): the json encoding formats the value rounded to
  milliseconds as text, which can exceed the microsecond range; the protobuf
  encoding sends microseconds and fails with "Value out of range for an
  interval after rounding" for values within half a millisecond of the ends.
- **Types the client does not know**: the json decoder base64 decodes them,
  which fails for a type whose value is text or a JSON document; the protobuf
  decoder returns the text, bytes or parsed JSON document.

Map keys of day to second intervals that collide once rounded to
milliseconds for a legacy client fail with the json encoding's error, rather
than one entry being dropped by the client.

## Files

| Module | Path | Purpose |
|---|---|---|
| trino-client | `src/main/proto/trino_spooled_segment.proto` | the contract |
| trino-client | `io/trino/client/spooling/encoding/protobuf/ProtobufWireFormat` | field numbers and tag helpers shared by the server and the client |
| trino-client | `.../protobuf/ProtobufWireWriter`, `ProtobufWireReader` | wire format codec |
| trino-client | `.../protobuf/ProtobufValueDecoders`, `ProtobufRowIterator` | typed value decoders and streaming row iterator |
| trino-client | `io/trino/client/spooling/encoding/ProtobufQueryDataDecoder` | the `protobuf`, `protobuf+zstd` and `protobuf+lz4` decoders |
| trino-main | `io/trino/server/protocol/ProtobufEncodingUtils` | per-type encoders from `Block` positions |
| trino-main | `io/trino/server/protocol/spooling/encoding/ProtobufQueryDataEncoder` | the encoders and their factories |
| trino-main | `io/trino/server/protocol/spooling/QueryDataEncodingConfig`, `QueryDataEncodingModule` | configuration and bindings |

## Tests

- `TestProtobufWireFormat` (trino-client): the hand-written writer against
  `CodedOutputStream`, and the reader against malformed input (endless or
  truncated varints, 64-bit varints in 32-bit fields, lengths beyond the
  message, nested limits, group wire types, field number 0).
- `TestProtobufQueryDataDecoder` (trino-client): segments built with the
  protoc-generated classes for every type, including precision 0 to 12, zone
  ids, padding of `CHAR`, structural map keys, unknown fields, mismatched
  column types, truncated segments, and the compressed variants.
- `TestProtobufEncodingUtils` (trino-main): every Trino type encoded from
  `Page`s with all three protobuf encodings and decoded by the client, compared
  with the `json` encoding of the same pages, with and without each client
  capability; zone offsets across historical and daylight saving transitions;
  segments encoded concurrently by one encoder; and the differences listed
  above.
- `TestJdbcSpooledEncodings` (trino-jdbc): end to end through the JDBC driver
  with `encoding=protobuf`, `protobuf+zstd` and `protobuf+lz4`, including a
  60,175 row result that spans several segments and the server's encoding
  selection.
- `TestProtobuf*SpooledDistributedQueries` (trino-tests): the engine query
  suite over spooled protobuf segments on S3 compatible storage, like the
  existing `TestJson*SpooledDistributedQueries`.

## Results

See [protobuf-spooling-encoding-results.md](protobuf-spooling-encoding-results.md)
for the size and decode time comparison against `json+zstd` on TPC-H
`lineitem` at scale factor 1.
