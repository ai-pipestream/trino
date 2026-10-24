# Report: protobuf spooling encoding (Thread 3)

**Branch:** `feature/protobuf-spooling-encoding` on `ai-pipestream/trino`, pushed at `40dd2374`
(6 commits on top of master `e76d788b`). No pull request opened.

## Commits

| sha | title |
|---|---|
| `7009cfa0` | Add protobuf wire format codec to trino-client |
| `d8404283` | Add protobuf spooled query data decoder |
| `f36935e3` | Add protobuf spooled query data encoder |
| `d4a34dd8` | Test protobuf spooling encodings end to end |
| `ac6fb9fe` | Document protobuf spooled encodings |
| `40dd2374` | Add protobuf spooling encoding briefs |

Authored as Kristian <kristian@apache.org>, no Co-authored-by or Assisted-by trailers,
titles 35 to 46 characters, bodies wrapped at 72. Each commit builds on its own.

## What changed

- `client/trino-client/src/main/proto/trino_spooled_segment.proto`: the contract. A `Segment`
  of length-delimited `Row`s, one typed `Value` per column with a `oneof` per Trino type family
  (29 kinds, explicit `NullValue`). Nothing goes through JSON text.
- `trino-client`: hand-written wire codec (`ProtobufWireWriter`, `ProtobufWireReader`, about
  470 lines), `ProtobufValueDecoders` (one decoder per type, same Java values as the JSON decoder),
  `ProtobufRowIterator` (streams one row at a time), `ProtobufQueryDataDecoder` with zstd and lz4
  variants registered in `QueryDataDecoders` after the json ones. **No runtime dependency added**:
  `protobuf-java` and `protoc` (protoc-jar-maven-plugin 3.11.4, same as trino-kafka) are test scope.
- `trino-main`: `ProtobufEncodingUtils` (per-type encoders from `Block` positions, honoring
  PARAMETRIC_DATETIME, PARAMETRIC_INTERVAL, NUMBER, VARIANT, VARIANT_BINARY exactly like the JSON
  encoder), `ProtobufQueryDataEncoder` + `ZstdFactory`/`Lz4Factory`, config properties
  `protocol.spooling.encoding.protobuf.enabled`, `protobuf+zstd.enabled`, `protobuf+lz4.enabled`
  (default true, `@ConfigDescription`), bindings in `QueryDataEncodingModule`.
- Docs: `admin/properties-client-protocol.md`, `client/cli.md`, `client/jdbc.md`,
  `develop/client-protocol.md` (new "Segment encodings" section).
- Briefs on the branch: `docs/briefs/protobuf-spooling-encoding-design.md` (full type mapping
  table), `docs/briefs/protobuf-spooling-encoding-results.md` (benchmark),
  `docs/briefs/feature-protobuf-spooling-encoding-pr.md` (draft PR description following the
  template: Description / Additional context and related issues / Release notes).

## Tests run (real output, final run after all style fixes)

| Test | Result |
|---|---|
| `TestProtobufWireFormat` (trino-client) | Tests run: 14, Failures: 0, Errors: 0 |
| `TestProtobufQueryDataDecoder` (trino-client) | Tests run: 21, Failures: 0, Errors: 0 |
| `TestProtobufEncodingUtils` (trino-main, every type × protobuf/zstd/lz4 vs json) | Tests run: 27, Failures: 0, Errors: 0 |
| `TestQueryDataEncodingConfig` (trino-main) | Tests run: 2, Failures: 0, Errors: 0 |
| `TestJdbcSpooledEncodings` (trino-jdbc, JDBC forced to each protobuf encoding, 60,175 row lineitem result, encoding negotiation) | Tests run: 3, Failures: 0, Errors: 0 |
| `TestProtobuf{,Zstd,Lz4}SpooledDistributedQueries` (trino-tests) | **Not run.** They need Docker (Floci S3 container), which is not running in this container, and trino-tests needs ~8 uninstalled modules (iceberg, base-jdbc, ...) to compile. They are three-line subclasses identical to the json ones. |

Static checks: `airstyle:format`, `sortpom:sort`, `license:format` (the .proto needed the header),
`./mvnw verify -DskipTests -P ci -pl client/trino-client,core/trino-main,client/trino-jdbc` → BUILD SUCCESS,
`./mvnw clean test-compile -Dair.check.skip-all=true -P errorprone-compiler` on the same modules → exit 0,
no warnings in the new files. `trino-tests` and `docs` were not part of the verify run.
Pre-existing failures unrelated to this work: `TestSSL` in trino-client fails in this container
(network "Connection reset"), so the client install ran with `-Dtest='TestProtobuf*'`.

## Benchmark (TPC-H lineitem sf1, 6,001,215 rows, 46 segments, JMH, 4 vCPU, single thread)

| Encoding | Size | vs json+zstd | Encode ms | Decode ms |
|---|---:|---:|---:|---:|
| json | 870.5 MB | 3.57× | 6,255 | 5,488 |
| json+zstd | 243.6 MB | 1.00× | 12,102 | 7,681 |
| json+lz4 | 395.8 MB | 1.62× | 9,559 | 6,242 |
| protobuf | 909.7 MB | 3.73× | 2,184 | 3,720 |
| protobuf+zstd | 282.6 MB | 1.16× | 9,132 | 6,336 |
| protobuf+lz4 | 409.1 MB | 1.68× | 6,035 | 4,969 |

Protobuf is 2.9× faster to encode and 1.5× faster to decode than JSON, but **larger**: 4.5%
uncompressed, 16% after zstd. Cause: the row-of-`oneof` layout costs 3 bytes per value (Value tag,
length, kind tag) = 48 of 152 bytes per lineitem row, and lineitem's doubles are short decimals in
JSON text but 8 bytes in protobuf. Full analysis in the results brief.

## Open questions for Kristian

1. **Size vs CPU.** With the layout the spec asked for (rows of typed values), protobuf+zstd trades
   16% more bytes for ~20% less CPU on both ends. If size matters more, a columnar segment layout
   (packed repeated field per column + null bitmap) would remove the per-value overhead and should
   beat JSON before compression. That is a different encoding; want it as a follow-up?
2. **Client preference order.** Kept `json+zstd` first; protobuf is opt-in via `encoding=` (JDBC)
   or `--encoding` (CLI). Flip it only if the size trade is acceptable.
3. **Hand-written codec vs protobuf-java.** I went hand-written to keep trino-client and the shaded
   JDBC driver dependency-free (decision card still open). The .proto is authoritative and tests
   pin the codec to protoc output, so switching to protobuf-java later is mechanical.
4. **Variant null quirk.** Without VARIANT_BINARY, the json encoding sends the JSON text `null` for
   a null variant and the client returns Java null; the protobuf decoder mirrors that for parity.

## Review readiness

Everything Kristian asked for is on the branch. Ready for the cross-model adversarial review
(Opus 5.5 reviews and fixes, then Fable reviews). Suggested review focus: `ProtobufValueDecoders`
formatting parity (timestamps, intervals, decimals), `ProtobufWireReader` bounds handling,
`ProtobufEncodingUtils.TypeObjectValueEncoder` fallback, and the `.proto` field numbering.
