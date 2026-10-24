# Draft pull request: Add protobuf spooled query data encodings

Branch: `feature/protobuf-spooling-encoding` (not opened as a pull request).

Title: **Add protobuf encodings for spooled query data**

## Description

Adds `protobuf`, `protobuf+zstd` and `protobuf+lz4` as query data encodings of
the spooling client protocol, next to `json`, `json+zstd` and `json+lz4`.

A protobuf segment is one `Segment` message holding a length-delimited `Row`
message per result row and a typed `Value` per column, as defined in
`client/trino-client/src/main/proto/trino_spooled_segment.proto`. Every Trino
type maps to one field of the `Value` oneof (decimals as unscaled value and
scale, timestamps as epoch micros, picos and precision, zone ids and the
offset the server resolved for `WITH TIME ZONE`, typed map keys, ...). Nothing is routed through JSON text,
and `NULL` is an explicit value.

- Server: `ProtobufQueryDataEncoder` writes rows straight from `Page`s with
  per-type encoders (`ProtobufEncodingUtils`), one row at a time. The
  encoder is shared by the drivers of a query and keeps no per call state. The compressed variants reuse `CompressedQueryDataEncoder`.
  The encodings are enabled by default and controlled by
  `protocol.spooling.encoding.protobuf.enabled`,
  `protocol.spooling.encoding.protobuf+zstd.enabled` and
  `protocol.spooling.encoding.protobuf+lz4.enabled`.
- Client: `ProtobufQueryDataDecoder` decodes a segment row by row with a
  streaming iterator and produces exactly the Java values
  `JsonQueryDataDecoder` produces, so the JDBC driver, the CLI and other users
  of `trino-client` work unchanged. The client honors the same capabilities
  (`PARAMETRIC_DATETIME`, `PARAMETRIC_INTERVAL`, `NUMBER`, `VARIANT`,
  `VARIANT_BINARY`) as the `json` encoding. Protobuf encodings are registered
  after the `json` ones in `QueryDataDecoders`, so clients keep requesting
  `json+zstd` first and opt in to protobuf through the `encoding` connection
  property (JDBC) or `--encoding` (CLI).
- `trino-client` does not gain a runtime dependency: the wire format is read
  and written by a small hand-written codec
  (`io.trino.client.spooling.encoding.protobuf`). `protobuf-java` and `protoc`
  are test-scoped; the client tests compile the schema and check the codec in
  both directions against the generated classes.
- Docs: new properties in `properties-client-protocol.md`, the new encodings
  in the CLI and JDBC docs, and a segment encodings section in
  `develop/client-protocol.md`.

Design notes with the complete type mapping table:
`docs/briefs/protobuf-spooling-encoding-design.md`.
Size and timing comparison on TPC-H `lineitem` sf1:
`docs/briefs/protobuf-spooling-encoding-results.md`.

### Tests

- `TestProtobufWireFormat`: writer bytes against `CodedOutputStream`, reader
  against malformed input (truncated and oversized varints, lengths beyond the
  message, nested limits, group wire types, field number 0).
- `TestProtobufQueryDataDecoder`: segments built with protoc-generated classes
  for every type, precision 0 to 12, zone ids, `CHAR` padding, structural map
  keys, unknown fields, mismatched column types, truncated segments, compressed
  variants.
- `TestProtobufEncodingUtils`: every Trino type encoded from `Page`s with all
  three protobuf encodings and decoded by the client, compared with the `json`
  encoding of the same pages, with and without each client capability.
- `TestJdbcSpooledEncodings`: end to end through the JDBC driver with
  `encoding=protobuf`, `protobuf+zstd` and `protobuf+lz4`, including a 60,175
  row result spanning several segments, and the server's encoding selection.
- `TestProtobuf{,Zstd,Lz4}SpooledDistributedQueries`: the engine query suite
  over spooled protobuf segments on S3 compatible storage (Floci container).
- `BenchmarkQueryDataEncoding`: JMH benchmark of encode and decode of
  `lineitem` sf1 and a generated type-rich table for all six encodings.

## Additional context and related issues

- Spooling protocol encodings and selection: `PreferredQueryDataEncoderSelector`
  picks the first encoding the client lists that the server has enabled. This
  is the protocol's normal capability negotiation; a `json`-only client or an
  older server keeps working.
- Related discussion of alternative segment encodings: trinodb/trino#24586
  (Arrow encoding), draft PR trinodb/trino#24587.
- Benchmark results (`BenchmarkQueryDataEncoding`, 3 forks, TPC-H `lineitem`
  sf1 and a generated type-rich table): the server encodes 2.2× to 4.3×
  faster uncompressed and up to 2.9× faster compressed. Client decoding is
  faster on `lineitem` but 1.5× to 1.9× slower on the type-rich table,
  because the client now formats decimals and timestamps as strings, which
  JSON receives preformatted. Size after Zstandard is 16% larger on
  `lineitem` and 14% smaller on the type-rich table. Details in
  `docs/briefs/protobuf-spooling-encoding-results.md`.
- Open questions for reviewers:
  1. Default client preference order: this PR keeps `json+zstd` first.
  2. The row layout with a `oneof` per value costs three bytes per value (tag
     and length of the nested `Value`, and the `oneof` tag), which is why the
     uncompressed protobuf segment is larger than JSON for `lineitem`. A columnar layout per segment
     (packed repeated fields per column with a null bitmap) would remove this
     overhead; it is out of scope here and would be a new encoding name.
  3. Whether the hand-written codec is acceptable versus a test-only
     `protobuf-java` dependency promoted to runtime.

## Release notes

(x) Release notes are required, with the following suggested text:

```markdown
## General
* Add `protobuf`, `protobuf+zstd`, and `protobuf+lz4` encodings for spooled
  query results, controlled by the `protocol.spooling.encoding.protobuf.enabled`,
  `protocol.spooling.encoding.protobuf+zstd.enabled`, and
  `protocol.spooling.encoding.protobuf+lz4.enabled` configuration properties.

## Client
* Add support for the `protobuf`, `protobuf+zstd`, and `protobuf+lz4` spooled
  query data encodings to the Java client, the JDBC driver, and the CLI.
```
