# Results: protobuf control plane (phases 1–3)

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

4. Phase 3 completion (review follow-up, fork issues #15, #16, #17):
   - `lib/trino-protobuf-handles`, a small library shared by the engine and
     connectors: the `Domain` schema and `DomainProtoCodec` (values travel as
     blocks through `BlockEncodingSerde`; the sorted-range-set
     `DiscreteSetMarker` is carried so single-value and IN-list domains come
     back exactly as built) and the `protobufHandleCodec` adapter that gives
     every connector the same parsing, class checks and categorized errors.
     The library's test type manager and block serde ship in its test jar
     for connectors that cannot depend on `trino-main`.
   - `ConnectorContext.getBlockEncodingSerde()` so connectors can encode
     blocks; `ConnectorContextModule` binds it lazily.
   - The engine resolves codecs through the real catalog manager
     (`ConnectorServices.getHandleCodecProvider()`,
     `io.trino.metadata.HandleFamily`, `ConnectorHandleProtobufCodec`) and
     has its first production caller, `SplitProtoMapper`
     (`Split`/`ScheduledSplit`/`SplitAssignment` schemas), ready for
     `TaskUpdateRequest` in phase 5. Unknown catalogs, catalogs not loaded
     on the node, absent codecs, corrupt payloads and misbehaving codecs each
     raise a distinct categorized error; there is no JSON fallback path.
   - Engine-owned connectors have real codecs: `information_schema`, the
     per-catalog `system` tables (whose transaction handle nests the served
     catalog's transaction handle through that catalog's own codec) and the
     global `system` connector (transaction, split and the `exclude_columns`
     and `sequence` table function handles).
   - Hive: `HiveHandleCodecProvider` covers all nine families hive
     implements (table, column, split, output, insert, table execute, merge,
     transaction, partitioning incl. `HiveUpdateHandle`), mirroring the JSON
     constructors field for field, metastore `Table`/`Partition` inside
     `HivePageSinkMetadata` included. Codecs run under the plugin class
     loader through new `ClassLoaderSafe*` wrappers in the plugin toolkit.
   - tpch and memory now use the shared domain codec and adapter; the tpch
     scalar-only domain encoding and its `NOT_SUPPORTED` path are gone, so an
     array/row/map domain value round-trips like any other.
   - `/v1/task` no longer negotiates: under `PROTOBUF` a request that cannot
     accept `application/x-protobuf` gets 406 and a JSON `/fail` body gets
     415; under `JSON` the protobuf forms are rejected the same way. The
     fetchers send `Accept` explicitly.

End-to-end evidence: `io.trino.tests.tpch.TestProtobufControlPlane` runs a
multi-node `DistributedQueryRunner` with the switch set to `PROTOBUF` and
asserts correct results for a scan, a join with dynamic filtering (and that a
domain really was collected), a worker failure and an aggregation.
`io.trino.server.TestTaskResourceEncoding` runs a query on a
`TestingTrinoServer` under each encoding and asserts the response
`Content-Type` of `/status` and `/dynamicfilters`, the 406 and 415 rejections,
and that `/fail` accepts the configured encoding only.

The brief's negative e2e test ("a connector without codecs fails loudly under
PROTOBUF") still cannot exist on this branch: no handle crosses the wire until
`TaskUpdateRequest` is protobuf (phase 5). Its unit form is
`TestConnectorHandleProtobufCodec.testMissingCodecIsNotSupported`, which
resolves through the real catalog manager and names catalog, family and
class in the error.

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

- `lib/trino-protobuf-handles`: `TestDomainProtoCodec` (12, incl. a lying
  discrete marker, duplicate discrete values, corrupt blocks, mismatched
  inclusive flags, null in equatable values, unknown types),
  `TestProtobufHandleCodec` (4).
- `core/trino-main`: `TestConnectorHandleProtobufCodec` (12),
  `TestSplitProtoMapper` (11), `TestSystemHandleCodecProvider` (9),
  `TestInformationSchemaHandleCodecProvider` (7),
  `TestGlobalSystemHandleCodecProvider` (5), `TestTaskProtoMappers` (15),
  `TestTaskResourceEncoding` (6), `TestHttpRemoteTask`,
  `TestProtobufResponseHandler`, `TestInternalCommunicationConfig`.
- `plugin/trino-tpch`: `TestTpchHandleCodecProvider` (15).
- `plugin/trino-memory`: `TestMemoryHandleCodecProvider` (12) and the full
  `TestMemoryConnectorTest` (306, 81 skipped as on master).
- `plugin/trino-hive`: `TestHiveHandleCodecProvider` (15), `TestHivePlugin`
  (11), `TestHiveConnectorFactory` (1), `TestHiveSplit`, `TestHiveTableHandle`,
  `TestHiveColumnHandle`.
- `testing/trino-tests`: `TestProtobufControlPlane` (6) under `PROTOBUF`.
- `mvn install -DskipTests` (license, dependency analysis, modernizer,
  checkstyle, revapi on `trino-spi`) passes for `core/trino-spi`,
  `lib/trino-protobuf-handles`, `lib/trino-plugin-toolkit`, `core/trino-main`,
  `plugin/trino-tpch`, `plugin/trino-memory`, `plugin/trino-hive`;
  `airstyle:check` and `sortpom:verify` pass for every touched module and the
  root pom; `test-compile -P errorprone-compiler` passes for the same
  modules.

Not run: full `trino-main` and hive test suites, any multi-node/SF100
benchmark.

## Remaining work (brief phases 4–6)

- Plan and expressions: `PlanFragment`, 45 `PlanNode` subtypes, 16 IR
  `Expression` subtypes, `Symbol`, `Type` signatures, partitioning schemes.
- `TaskUpdateRequest` and `TaskInfo`: `SessionRepresentation`,
  `SplitAssignment` (the protobuf form and mapper exist; the request message
  that carries it does not yet), `OutputBuffers`, W3C trace context for
  `Span`, operator stats and the polymorphic `Metric`/`OperatorInfo`
  families. Until then those endpoints stay JSON even with the switch on,
  so "zero JSON on /v1/task" is not yet claimed. `POST /{taskId}/fail` is
  exercised end to end only by `TestTaskResourceEncoding`; no query path on
  this branch triggers it (it is driven by `SqlQueryExecution.failTask`,
  e.g. a memory kill).
- Exchange handles: `SpoolingExchangeInput` and `ExchangeSourceHandle` are
  owned by the `ExchangeManager` plugin, not a connector, so they need their
  own codec hook on the exchange SPI; `SplitProtoMapper` rejects them with
  `NOT_SUPPORTED` today, which means fault-tolerant execution requires the
  JSON control plane.
- `JsonTableFunctionHandle` (global system `json_table`) carries a plan of
  IR expressions and waits for the phase-4 expression schema.
  `RemoteTransactionHandle` has no connector either.
- Handle codecs for every other connector; hive, tpch, memory and the
  engine-owned connectors are done.
- `SortedRangeSet` decoded from protobuf is rebuilt through the SPI
  constructors, so a set the sender marked `NON_DISCRETE` comes back as
  `UNKNOWN` and is classified again on first use. Preserving it would need a
  public SPI constructor that takes the marker.
- Low-volume internal endpoints still on JSON: `/v1/announce`, `/v1/info`,
  `/v1/status`, `/v1/memory`, `/v1/node`.
- Phase-6 benchmark on 1 coordinator + at least 3 workers, TPC-H/TPC-DS
  SF100, with JFR data, per the brief.
