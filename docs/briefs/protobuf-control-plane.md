# Brief: protobuf control plane between coordinator and workers

Status: experiment on the `ai-pipestream/trino` fork only. Not for upstream
yet. Base: `master` at `e76d788b`.

## Goal

Replace JSON on the coordinator-to-worker control plane with protobuf, end to
end. Data pages between workers are already binary
(`application/x-trino-pages`) and are out of scope.

Success means a query on a multi-node cluster runs with zero JSON on the
`/v1/task` control endpoints, and a benchmark shows the effect on
coordinator CPU, request bytes and query latency.

## Non-negotiable rules

1. **No JSON inside protobuf.** Never put a Jackson-serialized string or byte
   array into a protobuf field. If something cannot be expressed in protobuf
   yet, the experiment fails loudly for that query, it does not fall back.
2. **No format negotiation and no fallback.** Trino does not run mixed
   versions: `io.trino.node.RemoteNodeState` marks a node `INVALID` on
   "Node version mismatch". Every node runs the same build, so both ends
   always agree on the format.
3. **One switch, for benchmarking only.** Add
   `internal-communication.control-plane.encoding` with values `JSON`
   (default) and `PROTOBUF` in `InternalCommunicationConfig`, so the same
   build can be measured both ways. It is a cluster-wide setting, not a
   per-request negotiation. Follow `.github/DEVELOPMENT.md` for config rules
   (`@ConfigDescription`, `TestInternalCommunicationConfig`).
4. **Unsupported means an error.** A connector without a protobuf handle
   codec, used with `PROTOBUF`, fails the query with a clear
   `TrinoException` naming the connector and handle type.
5. Follow Trino style: no mocking libraries, AssertJ, Guava immutable
   collections, `mvnd airstyle:format`, `mvnd sortpom:sort`. Commit titles
   are imperative, capitalized, no trailing period, at most 50 characters.
   No `Co-authored-by` or `Assisted-by` trailers that credit AI tools: the
   upstream commit check rejects them.

## What is JSON today

All in `core/trino-main`.

| Endpoint (`TaskResource`, `/v1/task`) | Payload | Sender | Codec today |
| --- | --- | --- | --- |
| `POST /{taskId}` | `TaskUpdateRequest` request, `TaskInfo` response | `HttpRemoteTask.sendUpdate` | `JsonCodec<TaskUpdateRequest>`, `JsonCodec<TaskInfo>` |
| `GET /{taskId}/status` | `TaskStatus` (long poll) | `ContinuousTaskStatusFetcher` | `JsonCodec<TaskStatus>` |
| `GET /{taskId}` | `TaskInfo` | `TaskInfoFetcher` | `JsonCodec<TaskInfo>` |
| `GET /{taskId}/dynamicfilters` | `VersionedDynamicFilterDomains` | `DynamicFiltersFetcher` | `JsonCodec<VersionedDynamicFilterDomains>` |
| `POST /{taskId}/fail` | `FailTaskRequest` | `HttpRemoteTask` | `JsonCodec<FailTaskRequest>` |
| `DELETE /{taskId}` | `TaskInfo` | `HttpRemoteTask` | `JsonCodec<TaskInfo>` |

Codecs are wired in `HttpRemoteTaskFactory` and the server side in
`TaskResource` (`@Consumes`/`@Produces(APPLICATION_JSON)`).

Other internal JSON endpoints (`/v1/announce`, `/v1/info`, `/v1/status`,
`/v1/memory`, `/v1/node`) are low-volume. Leave them for a later phase and
list them as remaining work.

## The hard part: polymorphic Jackson types

`TaskUpdateRequest` (`io.trino.server.TaskUpdateRequest`) carries:
`SessionRepresentation`, extra credentials, an OpenTelemetry `Span`,
`Optional<PlanFragment>`, table credentials, `List<SplitAssignment>`,
`OutputBuffers`, dynamic filter `Domain`s, an exchange encryption key and a
speculative flag.

Three polymorphism layers need real protobuf schemas:

1. **Plan nodes.** `io.trino.sql.planner.plan.PlanNode` has 45
   `@JsonSubTypes` entries. Model as a `oneof` per node type in a `PlanNode`
   message.
2. **IR expressions.** `io.trino.sql.ir.Expression` has 16 subtypes. Same
   approach. Constants carry a Trino `Type` and a value: encode values with
   the existing binary block encoding (`BlockSerdeUtil` / `BlockEncodingSerde`),
   which is binary already. `BlockJsonSerde` only base64-wraps those bytes for
   JSON, so in protobuf they become a plain `bytes` field. That is binary
   data, not JSON, so rule 1 holds.
3. **Connector handles.** `io.trino.metadata.HandleJsonModule` registers 13
   handle families (table, column, split, output, insert, table execute,
   merge, index, transaction, partitioning, exchange sink, exchange source,
   table function). `HandleResolver` encodes each as
   `"<pluginClassLoaderId>:<className>"` plus the handle's own Jackson
   fields. There is no SPI for a non-JSON handle codec today.

Also polymorphic: `io.trino.spi.metrics.Metric`
(`@JsonTypeInfo(use = CLASS)`) inside operator stats in `TaskInfo`. 14 files
in `trino-main` use `@JsonTypeInfo`; inventory them all.

## Design

### New SPI: connector handle codecs

Add to `trino-spi`:

```java
public interface ConnectorHandleCodec<T>
{
    byte[] serialize(T handle);

    T deserialize(byte[] bytes);
}

public interface ConnectorHandleCodecProvider
{
    default Optional<ConnectorHandleCodec<ConnectorSplit>> getSplitCodec() { return Optional.empty(); }
    default Optional<ConnectorHandleCodec<ConnectorTableHandle>> getTableHandleCodec() { return Optional.empty(); }
    default Optional<ConnectorHandleCodec<ColumnHandle>> getColumnHandleCodec() { return Optional.empty(); }
    // one accessor per handle family registered in HandleJsonModule
}
```

and `Connector.getHandleCodecProvider()` returning
`Optional<ConnectorHandleCodecProvider>`, default empty. The bytes a
connector produces are its own protobuf message. The engine wraps them as:

```proto
message ConnectorHandle {
  string catalog_handle = 1;  // which catalog/connector owns the bytes
  bytes payload = 2;          // connector-defined protobuf message
}
```

Resolve the codec from the catalog, not from a class name, so handles no
longer depend on plugin class loader ids. Engine-owned handles (system
connector, `RemoteTransactionHandle`, the `io.trino.metadata.*Handle`
wrappers) get engine `.proto` messages directly.

Keep `trino-spi` free of a protobuf dependency: the SPI is `byte[]` in and
out. Only connectors that opt in depend on protobuf.

### Protobuf schemas

- Put engine messages under `core/trino-main/src/main/proto/io/trino/server/protocol/v1/`
  (`task.proto`, `plan.proto`, `expression.proto`, `session.proto`,
  `stats.proto`).
- Generate Java with `com.github.os72:protoc-jar-maven-plugin`, which the
  root `pom.xml` already manages (`plugin/trino-kafka` uses it for test
  sources), but bound to `generate-sources` for main code.
- Write explicit mappers `TaskUpdateRequest <-> TaskUpdateRequestProto` and so
  on. Do not reflect over Jackson annotations to derive schemas.
- Preserve every field. Write a round-trip test per message that compares the
  Java record before and after (`assertThat(decoded).isEqualTo(original)` or
  field-by-field for types without `equals`).

### Transport

Keep HTTP and the existing endpoints and airlift HTTP client. Only the body
changes: content type `application/x-protobuf` when the switch is
`PROTOBUF`. Use `@Consumes`/`@Produces` for both types on `TaskResource`
and pick the codec from the config switch on the client side. gRPC is
explicitly out of scope for this experiment; changing the transport and the
format at the same time would hide which one moved the numbers.

## Phases

Do them in order, each one a separate commit that builds and passes tests.

1. **Inventory.** Generate a list of every class reachable from
   `TaskUpdateRequest`, `TaskInfo`, `TaskStatus`,
   `VersionedDynamicFilterDomains`, `FailTaskRequest`, marking which are
   polymorphic. Commit it to `docs/briefs/protobuf-control-plane-inventory.md`.
2. **Simple, high-frequency messages.** `TaskStatus`,
   `VersionedDynamicFilterDomains` (including `Domain`/`ValueSet` via block
   bytes), `FailTaskRequest`. These have no plan or handles. Wire the switch
   end to end for these endpoints.
3. **Handle codec SPI.** Add the SPI, the engine `ConnectorHandle` wrapper,
   and codecs for `trino-tpch`, `trino-memory` and `trino-hive` (all 13
   families each one implements).
4. **Plan and expressions.** `PlanFragment`, `PlanNode`, `Expression`,
   `Symbol`, `Type` signatures, partitioning schemes.
5. **`TaskUpdateRequest` and `TaskInfo`.** Including `SessionRepresentation`,
   `SplitAssignment`, `OutputBuffers`, `Span` (encode trace context
   explicitly, W3C traceparent fields), stats and `Metric` subtypes.
6. **Benchmark.** See below.

## Testing

- Unit round-trip tests per message (phase 2 to 5).
- Run distributed query tests with the switch set
  to `PROTOBUF`. `DistributedQueryRunner` is the base; run the tpch and
  memory connector test suites, and `trino-hive`'s
  `TestHiveConnectorTest` subset, with the switch on.
- A negative test: a connector with no codec under `PROTOBUF` fails the query
  with the expected error.
- Run `mvnd verify -DskipTests -P ci -pl core/trino-main,core/trino-spi,...`
  and the Error Prone profile before declaring done.

## Benchmark

`HttpRemoteTask` already records `stats.updateWithPlanBytes(...)` and
`stats.updateWithDynamicFilterBytes(...)`, and adapts split batch size to
request size (`adjustSplitBatchSize`). Use those plus JFR.

Measure with JSON vs PROTOBUF on the same build, 1 coordinator and at least
3 workers, TPC-H and TPC-DS at scale factor 100 via the tpch/tpcds
connectors:

- Coordinator CPU time in serialization (JFR allocation and CPU samples).
- Bytes per task update and per status poll.
- Query wall time p50/p95 for the full suite.
- Splits per task update after adaptive batching.

Report the numbers in `docs/briefs/protobuf-control-plane-results.md`, raw
data included. A neutral or negative result is a valid outcome; report it.

## Exit criteria

- Phases 1 to 5 merged on the experiment branch with green tests for tpch,
  memory and hive under `PROTOBUF`.
- No JSON bytes on `/v1/task` control endpoints under `PROTOBUF`: verify
  with a test that asserts request and response content types.
- Results document committed.
- A remaining-work list: other connectors without codecs, the low-volume
  endpoints, and anything simplified.
