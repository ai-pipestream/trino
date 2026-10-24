# Review: protobuf spooling encoding (`feature/protobuf-spooling-encoding`)

Adversarial review of Fable's branch (6 commits, head `40dd2374`, fork issue #7; review head `318c37eb`). The Opus 5.5 review thread did it on 2026-10-22. Fixes are new commits on top of the branch, authored as Kristian, with no AI trailers, and pushed to `claude/project-thread-b0n7dd`, a fast-forward of `feature/protobuf-spooling-encoding`. That branch is untouched. Issues: #29 to #33 under #7. Fable reviews these commits next.

## Verdict

**Not accepted as pushed. Accepted with the review commits, pending Fable's review of them.**

The design is sound. The hand-written codec is careful: bounded lengths, nested limits, 10-byte varint cap, group wire types rejected, and every direction is pinned against protoc output. The type mapping matches the json decoder for every type I checked against `SqlTimestamp`, `SqlTimestampWithTimeZone`, `SqlTime(WithTimeZone)`, `Decimals.toString`, `SqlIntervalDayTime`, `TrinoNumber` and `JsonType.getSlice`. Capability handling (`PARAMETRIC_DATETIME`, `PARAMETRIC_INTERVAL`, `NUMBER`, `VARIANT`, `VARIANT_BINARY`) mirrors `JsonEncodingUtils`.

But the encoder was not safe for the way Trino uses it, and the Docker suites that would have shown that had never been run. The report called them "three-line subclasses identical to the json ones". On Fable's head they fail **101 of 1,044** tests with corrupt segments.

## Findings

| # | Severity | Finding | Status |
|---|----------|---------|--------|
| 1 | Critical | `OutputSpoolingOperatorFactory` shares one encoder between all drivers, so `encodeTo` runs concurrently. The protobuf encoder kept its row writer and a zone `HashMap` in shared fields, so concurrent segments overwrote each other's rows. Docker suites: 101 of 1,044 failed (`Row has more than 2 values`, ...). A unit probe corrupted 64 of 160 segments. | Fixed `f0bb35eb` (#29) |
| 2 | High | Timestamp with time zone was rendered with the **client's** tzdata (`ZoneId.of`). A client with older tzdata shows a different local time than json. A zone the client JVM lacks (e.g. `America/Ciudad_Juarez` on an old JDK 11; trino-client targets 11) fails the result with `ZoneRulesException`. | Fixed `74629592` (#30): server sends `offset_seconds` |
| 3 | Medium | Legacy (no `PARAMETRIC_INTERVAL`) interval map keys that collide after millisecond rounding were silently merged by the client. The json encoding fails with `NOT_SUPPORTED`. | Fixed `c2ba08f7` (#31) |
| 4 | Medium | The row iterator allocated up to 2 GB from an untrusted row length before reading. Segment tags wider than 32 bits were truncated, and field number 0 was accepted. | Fixed `50716fc1` (#32) |
| 5 | Low | Out-of-range fields were formatted instead of rejected: time picos ≥ 24h, picos of micro or milli out of range, offsets beyond ±14:00, a negative decimal scale. Empty unscaled bytes threw `NumberFormatException`. | Fixed `50716fc1` (#32) |
| 6 | Low | JSON documents of types without a dedicated encoding used `new JsonMapper()`, not the server's injected mapper that the json encoding uses. | Fixed `c2ba08f7` (#33) |
| 7 | Info | "Identical to json" does not hold in four corner cases. Variant JSON text is kept verbatim, where the json decoder loses precision (`1.50` → `1.5`). Malformed UTF-8 varchar decodes with U+FFFD, where json fails. Legacy intervals within 0.5 ms of the range ends fail to encode. Unknown text or JSON types decode correctly where json base64-garbles them. | Documented and pinned by tests (#33) |

## Assessments (no code change)

### Config and `Test*Config`
Fine. Three booleans, default `true`, with `@ConfigDescription`, mirroring the json ones, and `TestQueryDataEncodingConfig` covers defaults and explicit mapping. Enabling them by default on the server is safe because the client picks, and the Java client lists json first.

### Trino standards
- Commit messages: capitalized imperative titles, ≤ 50 chars, bodies wrapped at 72, no AI trailers, authored as Kristian; the branch never mentions the source project.
- AssertJ, no mocks, specific error codes (`NOT_SUPPORTED`, `SERIALIZATION_ERROR`). `trino-client` stays Java 11 compatible (old-style switches, no records) and gains no runtime dependency.
- For upstream, fold the review commits into the commits they fix. `docs/briefs/` must not go upstream.

### Per-commit builds
`test-compile` of `trino-client`, `trino-main` and `trino-jdbc` passes on all 10 commits, Fable's 6 and the review's 4. `verify -DskipTests -P ci` on head: see "What ran".

### Benchmark fairness
Structurally fair. Both encodings start from the same pages and go through the same server encoder and client decoder path, with the same compressors and threshold. Decode includes producing the Java values. But:
- **One table, and it favors JSON on size.** `lineitem` has no nulls, no timestamps, no decimals (TPC-H doubles with two digits), and short strings. The 16% size penalty is not a general result. Add at least one type-rich table (decimals, timestamps with time zone, nulls, arrays) before using the numbers to argue the default order.
- **The compressed CPU claim is within noise.** One fork, 4 shared vCPUs: `protobuf+zstd` encode is 9,132 ± 2,660 ms against `json+zstd` 12,102 ± 1,847 ms, so the confidence intervals overlap. The "25% faster to produce" claim is not established. Decode (6,336 ± 790 vs 7,681 ± 800) is marginal. The uncompressed results (2.9× encode, 1.5× decode) are solid.
- The benchmark segments (131,072 rows, about 19 MB of JSON) are larger than the server default `protocol.spooling.max-segment-size` of 16 MB. That is harmless for the comparison, but should be stated.

### Row-of-oneof layout (Kristian's call, not changed)
- The cost is fixed: 3 bytes per value (`Value` tag, length, `kind` tag), 48 of 152 bytes per `lineitem` row. Protobuf has no `repeated oneof`, so a row-oriented protobuf layout cannot do much better.
- The main benefit claimed, row-by-row streaming, mostly does not materialize. `CompressedQueryDataDecoder` reads the whole compressed segment and decompresses it into a full byte array before decoding, so the recommended `+zstd`/`+lz4` variants are not streamed. Segments are capped at 16 MB by default anyway.
- The per-value `oneof` tag is redundant with the column types the client already has. It is useful only as a consistency check.
- A columnar segment (one packed field per column plus a null bitmap) would remove the per-value overhead and put similar values next to each other, which helps zstd most. But it overlaps with the upstream Arrow proposal (trinodb/trino#24586, draft #24587). Upstream reviewers will ask why not Arrow. My recommendation: if size or upstream acceptance matters, decide between columnar protobuf and backing Arrow before investing more in this layout. If CPU on the coordinator is the goal, the current layout delivers that.

## What ran (JDK 25 Temurin, Docker 29, 4 vCPU)

- Baseline at `40dd2374`: `TestProtobufSpooledDistributedQueries`, `TestProtobufZstdSpooledDistributedQueries`, `TestProtobufLz4SpooledDistributedQueries` (Floci S3 in Docker): **1,044 run, 99 failures, 2 errors**.
- After fixes, at `c2ba08f7`: the same three classes: **1,044 run, 0 failures**. `TestProtobufWireFormat` 14/14, `TestProtobufQueryDataDecoder` 23/23, `TestProtobufEncodingUtils` 33/33, `TestQueryDataEncodingConfig` 2/2.
- `TestJdbcSpooledEncodings` (JDBC driver forced to each protobuf encoding) at `c2ba08f7`: **3 run, 0 failures**.
- `verify -DskipTests -P ci` on `c2ba08f7` for `trino-client`, `trino-main`, `trino-jdbc`: **BUILD SUCCESS** (airstyle, checkstyle, modernizer, dependency analysis, sortpom, license). `test-compile` of the same modules passes on every commit.
- Probes, run once and then turned into tests: concurrency, legacy interval collision and extremes, variant number normalization, malformed UTF-8.

## Review commits (for Fable)

1. `f0bb35eb` Fix concurrent protobuf segment encoding
2. `74629592` Send resolved zone offsets in protobuf timestamps (adds `TimestampWithTimeZone.offset_seconds = 5` to the `.proto`)
3. `50716fc1` Harden protobuf segment decoding
4. `c2ba08f7` Match json encoding for protobuf edge cases
5. `318c37eb` Update protobuf spooling briefs after review (design notes, PR draft, benchmark review notes)

## Follow-up fixes, 2026-10-24

Kristian asked for the items left as "assessed only" or "documented" to be fixed. Five commits on
`claude/project-thread-b0n7dd` on top of the accepted head `1dd8dd39` (head `9e22a234`). The feature branch is untouched.

| Commit | Change |
|---|---|
| `968df371` | Server replaces malformed UTF-8 in varchar/char with U+FFFD (JDK semantics), so the segment is valid proto3 for every parser. Text map keys that collide after replacement fail with a clear error |
| `015ed6ac` | Legacy day-to-second intervals are sent as `legacy_millis` (field 4, `optional`), so the ends of the range match json instead of failing |
| `131c4132` | Client formats dates and 64-bit decimals by hand (format checked against the server pattern for years beyond 9999 and before 0) |
| `3fc2a1fa` | Benchmark gets a type-rich table, 3 forks, and 65,536-row segments (under 16 MB) |
| `9e22a234` | Briefs: design notes, re-measured results, PR draft |

New finding from the benchmark: on the type-rich table the protobuf client decodes 1.5x to 1.9x slower than json,
because decimals and timestamps reach the client as strings that json gets preformatted. Server encode is 2x to 4x
faster, and size after zstd is +16% on lineitem and -14% on the type-rich table. Not changed: the row-of-oneof
layout (columnar backlog #34) and the client API returning strings.

Ran on head: Docker suites 1,044/0 failures, JDBC 3/3, client 38/38, server 81/81, `install -P ci` + Error Prone pass,
every commit test-compiles. Commits for Fable to cross-review: the five above.

## Fable cross-review of the follow-up fixes, 2026-10-24

Reviewed `1dd8dd39..9e22a234` on `claude/project-thread-b0n7dd` (968df371 UTF-8, 015ed6ac legacy_millis,
131c4132 formatting, 3fc2a1fa benchmark, 9e22a234 briefs). Authored by Kristian, no AI trailers, no forbidden
mentions.

**Verdict: the five commits are correct and accepted.** No defect found:

- UTF-8 replacement: the server check uses airlift `tryGetCodePointAt`, which rejects encoded surrogates,
  overlong 2/3/4 byte forms, code points above U+10FFFF and 5/6 byte start bytes, so every `string` field the
  server writes is valid for a strict parser. Replacement goes through `new String(bytes, UTF_8)`, so the
  client sees the same text whether or not the server replaced. The map-key collision check is only paid
  for a map that holds a malformed key. The existing test did not prove the surrogate case at the byte level
  (the client renders one U+FFFD either way); a test now checks the whole segment is well-formed UTF-8 and
  covers a code point above U+10FFFF, a five byte start and a lone continuation byte.
- `legacy_millis`: explicit presence, both ends of the range round-trip, client rejects a message carrying
  both forms, nested legacy intervals go through the same object-value path, map keys still collide on
  the encoded bytes.
- Formatting commit: correct, but superseded (below).
- Benchmark: parameters and generated data are sound (30-digit decimal(38,10), five zones, 20%/30% nulls).

**Client decode regression.** Baseline in this container, `types` table, decode only: json 1,864 ms,
protobuf 3,198 ms (1.7x). Profile: `Long.toString`/`StringBuilder` digits 14%, BigInteger 8%, LocalDateTime
and String construction the rest. It can be reduced without any JSON fallback; the wire format is unchanged.
Three commits on `claude/project-thread-z4ytaf` (on top of 9e22a234, local, NOT pushed):

| Commit | Change |
|---|---|
| Format protobuf client values without java.time and BigInteger | `ProtobufValueFormatter`: each date/time/timestamp/decimal written into a byte array of exact length and made a Latin-1 string; civil date from the epoch day; 128-bit decimals converted by dividing 32-bit words by 10^9 (BigInteger only beyond 128 bits); per-column zone id cache for `TIMESTAMP WITH TIME ZONE`; formats pinned to `LocalDate`, `DateTimeFormatter`, `BigDecimal.toPlainString` by `TestProtobufValueFormatter` over range ends and random values |
| Read protobuf segments in chunks | `ProtobufRowIterator` reads the segment in 64 kB chunks and decodes rows in place (no per-byte `InputStream.read`, no per-row copy); buffer still grows only as bytes arrive; varint fast path in `ProtobufWireReader`; test with 1-byte and random chunk deliveries and 100 kB rows |
| Check more malformed UTF-8 forms in protobuf text values | server test above |

Result (same machine, decode only, 1 fork, 5 iterations, all six encodings):

| Encoding | `lineitem` decode ms | `types` decode ms |
|---|---:|---:|
| json / protobuf | 5,901 / 3,578 (1.65x faster) | 1,883 / 2,078 (10% slower) |
| json+zstd / protobuf+zstd | 7,871 / 5,430 (1.45x faster) | 3,046 / 3,025 (equal) |
| json+lz4 / protobuf+lz4 | 6,234 / 4,349 (1.43x faster) | 2,616 / 2,307 (12% faster) |

What remains on `types` uncompressed is inherent to the client API returning strings for decimals and
timestamps (json gets them preformatted by the coordinator) and to the 3 bytes of framing per value
(columnar layout, fork #34). Typed client values would remove the first; neither needs JSON anywhere.
A fourth commit updates the three briefs (design, results, PR draft) accordingly.

Suites on the final tree (commits 34a977e3, 3e38e7eb, 748abf65 + briefs): `TestProtobuf{,Zstd,Lz4}SpooledDistributedQueries`
1,044/0 (Docker), `TestJdbcSpooledEncodings` 3/3, trino-client protobuf tests 32/32 (26 decoder + 6 formatter),
`TestProtobufEncodingUtils` 34/34, `install -P ci` (checkstyle, modernizer, dependency analysis) and Error Prone
green on trino-client and trino-main. Branch `claude/project-thread-z4ytaf` local only until Kristian OKs the push;
next step per the review loop: Opus reviews the four Fable commits.
