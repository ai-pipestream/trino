# Results: protobuf control plane (phases 1–3 slice)

Branch `experiment/protobuf-control-plane-impl` on the `ai-pipestream/trino`
fork, based on the brief commit `824ea904f2` (master `e76d788b`).

## Scope actually delivered

The brief's phases 1–6 in full (45 plan nodes, hive handles, a 4-node SF100
benchmark) were explicitly descoped with the requester to phases 1–3 plus a
local microbenchmark. Delivered, each as its own commit:

1. Inventory of every class reachable from the five control-plane payloads:
   `protobuf-control-plane-inventory.md`.
2. `internal-communication.control-plane.encoding` (`JSON` default,
   `PROTOBUF`) in `InternalCommunicationConfig`, protobuf schemas in
   `core/trino-main/src/main/proto/io/trino/server/protocol/v1/task.proto`,
   explicit mappers in `io.trino.server.protobuf`, and end-to-end wiring for
   `GET /{taskId}/status`, `GET /{taskId}/dynamicfilters` and
   `POST /{taskId}/fail` (request body; its `TaskInfo` response stays JSON
   until phase 5). No JSON is carried inside protobuf; `Domain` values travel
   as serialized blocks in a `bytes` field.
3. Handle codec SPI in `trino-spi` (`ConnectorHandleCodec`,
   `ConnectorHandleCodecProvider` with one accessor per family,
   `Connector.getHandleCodecProvider()`), the engine wrapper
   (`ConnectorHandle` proto + `io.trino.metadata.ConnectorHandleProtobufCodec`,
   which fails with `NOT_SUPPORTED` naming catalog, family and handle type
   when a codec is absent), and real protobuf codecs for every handle family
   tpch (table incl. `TupleDomain` constraint, column, split, partitioning,
   transaction) and memory (table, column, split, output, insert,
   transaction) implement.

End-to-end evidence: `io.trino.tests.tpch.TestProtobufControlPlane` runs a
multi-node `DistributedQueryRunner` with the switch set to `PROTOBUF` and
asserts correct results for a scan, a join with dynamic filtering, and an
aggregation. Task status polls, dynamic-filter fetches and fail requests
exercise protobuf on real HTTP between coordinator and workers.

## Microbenchmark (local, indicative only)

Method: scratch JUnit harness (not committed; rerun instructions below),
20,000 iterations after 2,000 warm-up, single-threaded, on the development
machine (32 cores, Temurin 25). This is not JMH and not the brief's phase-6
cluster benchmark; treat the ratios as directional. Two consecutive runs
agreed within noise; the second run is shown.

| Payload | Encoding | Bytes | Serialize | Deserialize |
| --- | --- | ---: | ---: | ---: |
| `TaskStatus` (1 failure attached) | JSON | 1024 | 2782 ns/op | 3893 ns/op |
| `TaskStatus` (1 failure attached) | protobuf | 224 | 1962 ns/op | 2161 ns/op |
| `VersionedDynamicFilterDomains` (100-value bigint domain + 1 range) | protobuf | 2783 | 13262 ns/op | 11448 ns/op |

TaskStatus on the wire shrinks by about 78 % and (de)serialization is roughly
1.4–1.8× faster in this harness. The domains payload has no JSON comparison
row: a plain `JsonCodec` cannot serialize `Domain` without the server's
Block/Type Jackson modules, so inventing a JSON baseline outside the server
stack would not have been honest.

To rerun: recreate `BenchmarkProtoCodecs` in
`core/trino-main/src/test/java/io/trino/server/protobuf/` (serialize/parse
loops over `TaskStatusProtoMapper` / `JsonCodec<TaskStatus>` and
`DynamicFilterDomainsProtoMapper`), then
`mvn test -pl core/trino-main -Dtest=BenchmarkProtoCodecs`.

## Tests run for this branch

- `TestTaskProtoMappers` (4), `TestProtobufResponseHandler` (2, includes the
  JSON-content-type-rejected test), `TestInternalCommunicationConfig` (2).
- `TestConnectorHandleProtobufCodec` (3, includes both negative tests).
- `TestTpchHandleCodecProvider` (7), `TestMemoryHandleCodecProvider` (7).
- Existing touched suites: `TestHttpRemoteTask` (11),
  `TestTaskInfoFetcherInstanceMismatch` (3), `TestTpchMetadata` (9),
  `TestMemoryMetadata` (14).
- `TestProtobufControlPlane` distributed queries (4) under `PROTOBUF`.
- `mvn verify -DskipTests` passes for `core/trino-main`, `core/trino-spi`,
  `plugin/trino-tpch`, `plugin/trino-memory`, `testing/trino-tests`
  (checkstyle, license, dependency analysis, airstyle, sortpom).

Not run: full `trino-main` test suite, tpch/memory connector test suites
under the switch (they would still use JSON for `TaskUpdateRequest`/
`TaskInfo`, which is expected at this phase), Error Prone profile beyond the
default compile, and any multi-node/SF100 benchmark.

## Remaining work (brief phases 4–6)

- Plan and expressions: `PlanFragment`, 45 `PlanNode` subtypes, 16 IR
  `Expression` subtypes, `Symbol`, `Type` signatures, partitioning schemes.
- `TaskUpdateRequest` and `TaskInfo`: `SessionRepresentation`,
  `SplitAssignment` (this is where the phase-3 handle codecs get wired in
  through `ConnectorHandleProtobufCodec`), `OutputBuffers`, W3C trace context
  for `Span`, operator stats and the polymorphic `Metric`/`OperatorInfo`
  families. Until then those endpoints stay JSON even with the switch on,
  so "zero JSON on /v1/task" is not yet claimed.
- Handle codecs for all other connectors, including hive (all 14 families it
  implements). Note the brief said 13 families; `HandleJsonModule` at this
  base registers 14 (table credentials) and the SPI covers all 14.
- Known simplification: the tpch table-handle codec encodes `Domain` values
  as native scalars and rejects non-scalar (array/row/map) domain values with
  `NOT_SUPPORTED`. The engine `DomainProtoMapper` has no such limitation (it
  uses block bytes), but connectors have no `BlockEncodingSerde` in
  `ConnectorContext`; giving them one is a phase-4 decision.
- Low-volume internal endpoints still on JSON: `/v1/announce`, `/v1/info`,
  `/v1/status`, `/v1/memory`, `/v1/node`.
- Phase-6 benchmark on 1 coordinator + at least 3 workers, TPC-H/TPC-DS
  SF100, with JFR data, per the brief.
