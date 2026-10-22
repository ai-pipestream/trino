# Protocol Buffers spooling encoding: size and time comparison

Branch: `feature/protobuf-spooling-encoding`. Design notes:
[protobuf-spooling-encoding-design.md](protobuf-spooling-encoding-design.md).

## Setup

- Data: TPC-H `lineitem` at scale factor 1, generated in memory with the
  `io.trino.tpch` generator: 6,001,215 rows, 16 columns with the types the
  TPC-H connector exposes (3 `bigint`, 1 `integer`, 4 `double`, 3 `date`,
  5 `varchar`). Rows are grouped into 46 segments of 131,072 rows each
  (about 19 MB of JSON per segment).
- Benchmark: `BenchmarkQueryDataEncoding` (JMH, trino-main tests). One
  operation encodes, or decodes, all 46 segments. Encoding goes from `Page`s
  to a discarding output stream through the server `QueryDataEncoder`;
  decoding reads the encoded bytes from memory through the client
  `QueryDataDecoder` and iterates every row, so it includes producing the Java
  values the JDBC driver consumes. Compression and decompression are included
  in the compressed variants. The compression threshold is the default 8 kB,
  so every segment is compressed.
- Machine: 4 vCPU Intel Xeon 2.80 GHz container, 15 GB RAM, JDK 25.0.5,
  `-Xmx8g`, single thread, average time mode, 1 fork, 3 warmup iterations of
  5 s, 5 measurement iterations of 5 s. Error is the 99.9% confidence
  interval reported by JMH.

Run with:

```
./mvnw test-compile exec:exec -Dair.check.skip-all=true -Dmaven.compiler.proc=full -pl core/trino-main \
    -Dexec.classpathScope=test -Dexec.executable=java \
    -Dexec.args="-cp %classpath io.trino.server.protocol.spooling.encoding.BenchmarkQueryDataEncoding"
```

## Results

| Encoding | Encoded size | Bytes per row | Relative to `json+zstd` | Encode (ms, all rows) | Decode (ms, all rows) |
|---|---:|---:|---:|---:|---:|
| `json` | 870,511,731 | 145.1 | 3.57× | 6,255 ± 171 | 5,488 ± 744 |
| `json+zstd` | 243,602,071 | 40.6 | 1.00× | 12,102 ± 1,847 | 7,681 ± 800 |
| `json+lz4` | 395,836,713 | 66.0 | 1.62× | 9,559 ± 758 | 6,242 ± 1,051 |
| `protobuf` | 909,745,035 | 151.6 | 3.73× | 2,184 ± 380 | 3,720 ± 194 |
| `protobuf+zstd` | 282,639,046 | 47.1 | 1.16× | 9,132 ± 2,660 | 6,336 ± 790 |
| `protobuf+lz4` | 409,095,007 | 68.2 | 1.68× | 6,035 ± 2,736 | 4,969 ± 1,192 |

Throughput for the uncompressed encodings, from the averages above:

| | `json` | `protobuf` |
|---|---:|---:|
| Encode, rows per second | 0.96 M | 2.75 M |
| Decode, rows per second | 1.09 M | 1.61 M |

## Reading the numbers

- **Time.** Encoding a protobuf segment is 2.9 times faster than encoding a
  JSON segment, and decoding it on the client is 1.5 times faster, because
  nothing is formatted or parsed as text: numbers are written as varints and
  IEEE 754 bit patterns, and dates as day counts. With compression the
  compressor dominates, so `protobuf+zstd` is 25% faster to produce and 18%
  faster to consume than `json+zstd`, and `protobuf+lz4` is 37% and 20% faster
  than `json+lz4`.
- **Size.** The protobuf segment is 4.5% larger than the JSON segment before
  compression and 16% larger after Zstandard compression. Two things work
  against protobuf on this table:
  - Every value is a nested `Value` message, which costs a tag byte, a length
    byte and the `oneof` tag byte: 3 bytes for each of the 16 columns, or
    48 of the 152 bytes of a row. JSON spends about 17 bytes per row on
    brackets and commas. This overhead is the price of the row-of-typed-values
    layout and of the self-describing `oneof`.
  - The four `double` columns of `lineitem` hold prices and percentages with
    two decimals, which JSON prints in 4 to 9 characters while protobuf always
    writes 8 bytes. `date` columns go the other way (3 to 4 bytes instead of
    13) and `bigint` columns are about even.
  Tables with wide integer, decimal, timestamp or binary columns, or with
  nulls, lean further toward protobuf; tables of short strings and "nice"
  doubles lean toward JSON.
- **Variance.** The compressed encodings have wide confidence intervals
  because the container's 4 vCPUs are shared; the uncompressed `protobuf`
  results are the most stable. The relative ordering was the same in every
  iteration.

## Conclusions

- `protobuf+zstd` trades about 16% more storage and transfer for about 20%
  less CPU on both the coordinator and the client on this workload. Whether
  that is a good trade depends on whether a deployment is bound by object
  storage and network or by CPU. The Java client therefore keeps requesting
  `json+zstd` first; protobuf is opt-in through the `encoding` property.
- Uncompressed `protobuf` is the fastest option end to end when the storage
  is local or the network is fast, which also makes it the natural choice for
  the `coordinator_proxy` and `worker_proxy` retrieval modes within a data
  center.
- If size matters more, a columnar segment layout (one packed repeated field
  per column with a null bitmap, so that values carry no per-value tags or
  lengths) would remove the 48 bytes of overhead per row and is expected to
  land well below JSON before compression. It is a different encoding and is
  left as follow-up work; see the open questions in the pull request draft.
