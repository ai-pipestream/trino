# Protocol Buffers spooling encoding: size and time comparison

Branch: `feature/protobuf-spooling-encoding`. Design notes:
[protobuf-spooling-encoding-design.md](protobuf-spooling-encoding-design.md).

## Setup

- Data, generated in memory:
  - `lineitem`: TPC-H `lineitem` at scale factor 1 from the `io.trino.tpch`
    generator, 6,001,215 rows, 16 columns with the types the TPC-H connector
    exposes (3 `bigint`, 1 `integer`, 4 `double`, 3 `date`, 5 `varchar`). No
    nulls, short strings, doubles with two fraction digits: a table that
    favors JSON on size.
  - `types`: 2,000,000 generated rows (fixed seed) of `bigint`,
    `decimal(12, 2)`, `decimal(38, 10)` with 30 digit values,
    `timestamp(6)`, `timestamp(3) with time zone` over five zones,
    full precision `double`, `boolean`, `varchar` (20% null), `integer`
    (30% null), `uuid` and `array(varchar)` of 0 to 4 elements.
- Segments of 65,536 rows: about 9.5 MB of JSON for `lineitem` (92
  segments) and 15 MB for `types` (31 segments), below the default
  `protocol.spooling.max-segment-size` of 16 MB.
- Benchmark: `BenchmarkQueryDataEncoding` (JMH, trino-main tests). One
  operation encodes, or decodes, all segments of a table. Encoding goes from
  `Page`s to a discarding output stream through the server
  `QueryDataEncoder`; decoding reads the encoded bytes from memory through the
  client `QueryDataDecoder` and iterates every row, so it includes producing
  the Java values the JDBC driver consumes. Compression is included in the
  compressed variants, with the default 8 kB threshold, so every segment is
  compressed.
- Machine: 4 vCPU Intel Xeon 2.80 GHz container (shared), JDK 25.0.5,
  `-Xmx8g`, single thread, average time. Encode: **3 forks**, 3 warmup and 5
  measurement iterations of 3 s each; error is the 99.9% confidence interval
  over the 15 measurements. Decode: 1 fork, 2 warmup and 5 measurement
  iterations of 3 s each, in a second container after the client formatting
  changes (`ProtobufValueFormatter`, chunked segment reads), so the two
  columns are not from the same run. Compare encode times only with encode
  times, and decode times only with decode times.

Run with:

```
./mvnw test-compile exec:exec -Dair.check.skip-all=true -Dmaven.compiler.proc=full -pl core/trino-main \
    -Dexec.classpathScope=test -Dexec.executable=java \
    -Dexec.args="-cp %classpath io.trino.server.protocol.spooling.encoding.BenchmarkQueryDataEncoding"
```

## Size

| Encoding | `lineitem` bytes | per row | vs `json+zstd` | `types` bytes | per row | vs `json+zstd` |
|---|---:|---:|---:|---:|---:|---:|
| `json` | 870,511,777 | 145.1 | 3.57× | 455,036,886 | 227.5 | 2.11× |
| `json+zstd` | 243,582,699 | 40.6 | 1.00× | 216,092,822 | 108.0 | 1.00× |
| `json+lz4` | 395,875,643 | 66.0 | 1.63× | 356,279,048 | 178.1 | 1.65× |
| `protobuf` | 909,745,035 | 151.6 | 3.73× | 337,186,093 | 168.6 | 1.56× |
| `protobuf+zstd` | 282,973,724 | 47.2 | 1.16× | 186,396,901 | 93.2 | **0.86×** |
| `protobuf+lz4` | 409,123,750 | 68.2 | 1.68× | 223,739,674 | 111.9 | 1.04× |

## Time (ms for the whole table)

| Encoding | `lineitem` encode | `lineitem` decode | `types` encode | `types` decode |
|---|---:|---:|---:|---:|
| `json` | 6,383 ± 353 | 5,901 ± 1,144 | 4,757 ± 175 | 1,883 ± 257 |
| `json+zstd` | 12,595 ± 364 | 7,871 ± 2,677 | 8,980 ± 327 | 3,046 ± 575 |
| `json+lz4` | 10,640 ± 617 | 6,234 ± 724 | 6,414 ± 497 | 2,616 ± 270 |
| `protobuf` | 2,844 ± 607 | 3,578 ± 554 | 1,118 ± 133 | 2,078 ± 215 |
| `protobuf+zstd` | 11,374 ± 3,022 | 5,430 ± 1,029 | 3,846 ± 563 | 3,025 ± 236 |
| `protobuf+lz4` | 7,557 ± 2,332 | 4,349 ± 336 | 2,229 ± 515 | 2,307 ± 288 |

Before the client formatting changes, in the same container and setup, the
`types` decode was 1,864 ms for `json` and 3,198 ms for `protobuf`.

## Reading the numbers

- **Server encode.** Protobuf is faster everywhere it can be measured:
  2.2× (`lineitem`) and 4.3× (`types`) uncompressed, 2.3× with Zstandard
  and 2.9× with LZ4 on `types`, 1.4× with LZ4 on `lineitem`. With Zstandard
  on `lineitem` the compressor dominates, and the difference is within the
  noise (11,374 ± 3,022 against 12,595 ± 364). The JSON encoder formats every
  decimal, timestamp and double as text, which is what protobuf saves.
- **Client decode.** Here the type mix decides. On `lineitem` protobuf decodes
  1.65× faster uncompressed and 1.4× faster compressed. On `types` it is
  10% slower uncompressed, equal with Zstandard and 12% faster with LZ4. The
  client values of decimals and timestamps are strings, which the JSON decoder
  receives already formatted by the server, while the protobuf decoder must
  format them itself. Before the client formatted them by hand (byte arrays
  turned into Latin-1 strings, 128-bit decimals converted without
  `BigInteger`, the zone id of a column decoded once) and read the segment in
  chunks, the `types` decode was 1.7× slower than json; what remains is the
  cost of producing the strings at all, which typed client values would
  remove, but that is a client API change outside this encoding.
- **Size.** It depends on the data. On `lineitem`, protobuf is 4.5% larger
  before and 16% larger after Zstandard: each value costs 3 bytes of framing
  (48 of the 152 bytes of a row), and the two-digit doubles are shorter as
  text than as 8 bytes. On `types`, protobuf is 26% smaller before and 14%
  smaller after Zstandard: decimals, timestamps, zone ids, uuids and nulls are
  much shorter in binary.
- **Noise.** The compressed protobuf encodes of `lineitem` are bimodal
  (single operations between 4.8 s and 15.8 s with LZ4); a GC profile shows
  under 0.1% of the time in GC, so this is the shared machine, not the
  encoder. The uncompressed and the `types` results are stable across forks.

## Conclusions

- Protobuf reliably saves coordinator CPU, by 2× to 4× before compression.
  That is the case for it: a coordinator that encodes many large results.
- For the client it is a win on string and integer heavy results and a
  wash on type-rich ones, where it formats what the server used to: 10%
  slower uncompressed, equal or faster with compression.
- Size is a wash that depends on the column types: worse on string and
  "nice" double heavy tables, better on decimal, timestamp and nullable
  tables.
- The Java client therefore keeps requesting `json+zstd` first; protobuf is
  opt-in through the `encoding` property. A columnar layout (fork issue #34)
  would remove the 3 bytes of framing per value and should help the
  `lineitem` case most.
- The compressed decoders read and decompress the whole segment before
  decoding it (`CompressedQueryDataDecoder`), so row by row streaming only
  applies to uncompressed `protobuf` segments.
