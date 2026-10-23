# Inventory: classes reachable from the task control-plane payloads

Phase 1 of the [protobuf control-plane brief](protobuf-control-plane.md).
Base: `master` at `e76d788b` plus the brief commit `824ea904f2`.

Method: starting from the five payload roots, record components and getters
were followed transitively through `core/trino-main`, `core/trino-spi` and
`client/trino-client`. A class is marked **polymorphic** when Jackson needs a
type id for it today (`@JsonTypeInfo`, `@JsonSubTypes`, or the
`AbstractTypedJacksonModule` handle resolver in
`io.trino.metadata.HandleJsonModule`). Polymorphic classes are the ones that
need a `oneof` (or the handle-codec wrapper) in protobuf; everything else maps
field by field.

Status column: **done** = protobuf message and mapper exist on this branch,
**phase 4/5** = not yet done, fails loudly under `PROTOBUF` until then.

## Root 1: `io.trino.execution.TaskStatus` — done (phase 2)

| Class | Polymorphic | Notes |
| --- | --- | --- |
| `io.trino.execution.TaskStatus` (record, 23 components) | no | `task.proto: TaskStatusProto` |
| `io.trino.execution.TaskId` | no | dotted string form (`TaskId.valueOf` / `toString`) |
| `io.trino.execution.TaskState` | no | enum: PLANNED, RUNNING, FLUSHING, FINISHED, CANCELING, CANCELED, ABORTING, ABORTED, FAILING, FAILED |
| `io.trino.execution.ExecutionFailureInfo` (recursive) | no | shared with `FailTaskRequest` |
| `io.trino.client.ErrorLocation` | no | line/column |
| `io.trino.spi.ErrorCode` | no | code, name, `ErrorType`, fatal |
| `io.trino.spi.ErrorType` | no | enum |
| `io.trino.spi.HostAddress` | no | `toString` / `fromString` |
| `io.trino.execution.buffer.OutputBufferStatus` | no | `OptionalLong` version + 2 booleans |
| `io.airlift.units.DataSize`, `io.airlift.units.Duration` | no | encoded as bytes / nanos (`int64`) |
| `java.net.URI`, `java.util.OptionalInt` | no | string / optional field presence |

## Root 2: `io.trino.server.FailTaskRequest` — done (phase 2)

Single field: `ExecutionFailureInfo` (see root 1). Request body is protobuf
under the switch; the response is `TaskInfo`, which stays JSON until phase 5.

## Root 3: `io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains` — done (phase 2)

| Class | Polymorphic | Notes |
| --- | --- | --- |
| `VersionedDynamicFilterDomains` | no | `int64 version` + map entries |
| `io.trino.sql.planner.plan.DynamicFilterId` | no | string id |
| `io.trino.spi.predicate.Domain` | no (wraps polymorphic `ValueSet`) | `nullAllowed` + values |
| `io.trino.spi.predicate.ValueSet` | **yes** (`@type`) | `allOrNone` → `AllOrNoneValueSet`, `equatable` → `EquatableValueSet`, `sortable` → `SortedRangeSet`; modelled as `oneof` over all-or-none / discrete / ranges via `ValuesProcessor` |
| `io.trino.spi.predicate.Range` | no | low/high bounds + inclusivity; bound values as block bytes |
| `io.trino.spi.type.Type` | **yes** (open set of implementations) | encoded as `TypeId` string (`Type.getTypeId()`), resolved with `TypeManager.getType(TypeId)` |
| scalar values inside `Domain` | n/a | one `Block` per value list, serialized with `BlockEncodingSerde` into a `bytes` field (binary already; see brief rule 1) |

## Root 4: `io.trino.execution.TaskInfo` — phase 5

| Class | Polymorphic | Notes |
| --- | --- | --- |
| `io.trino.execution.TaskInfo` (record) | no | contains `TaskStatus` (done) |
| `io.trino.execution.buffer.OutputBufferInfo` | no | includes `BufferState` enum, `PipelinedBufferInfo`, spooling stats |
| `io.trino.execution.buffer.PipelinedBufferInfo` | no | |
| `io.trino.sql.planner.plan.PlanNodeId` | no | string id |
| `io.trino.operator.TaskStats` | no | large flat record of counters/timestamps |
| `io.trino.operator.PipelineStats` | no | contains `DriverStats` |
| `io.trino.operator.DriverStats` | no | contains `OperatorStats` |
| `io.trino.operator.OperatorStats` | no | contains the two polymorphic fields below |
| `io.trino.operator.OperatorInfo` | **yes** (`@JsonSubTypes`, 8) | DirectExchangeClientStatus, JoinOperatorInfo, LocalExchangeBufferInfo, OutputSpoolingInfo, PartitionedOutputInfo, TableFinishInfo, TableWriterInfo, WindowInfo |
| `io.trino.spi.metrics.Metrics` / `io.trino.spi.metrics.Metric` | **yes** (`@JsonTypeInfo(use = CLASS)`) | subtypes live in SPI and connectors (e.g. `Count`, `LongCount` in plugin toolkit); open set — needs a closed `oneof` for SPI metrics and a loud failure for unknown connector metrics |
| `io.trino.operator.BlockedReason` | no | enum |
| `io.trino.execution.buffer.BufferState` | no | enum |

## Root 5: `io.trino.server.TaskUpdateRequest` — phases 4 and 5

| Class | Polymorphic | Notes |
| --- | --- | --- |
| `io.trino.server.TaskUpdateRequest` (record) | no | 10 components |
| `io.trino.SessionRepresentation` | no | identity, session properties, catalog properties, prepared statements, transaction id, client info |
| `io.trino.spi.security.Identity`, `io.trino.transaction.TransactionId`, `io.trino.execution.QueryIdGenerator` ids | no | scalar/strings |
| `io.opentelemetry.api.trace.Span` | n/a (not a data class) | encode W3C trace context explicitly (trace id, span id, sampled flag, tracestate) |
| `io.trino.sql.planner.PlanFragment` | no | root `PlanNode`, symbols/types, partitioning, stage execution descriptor |
| `io.trino.sql.planner.plan.PlanNode` | **yes** (`@JsonSubTypes`, 45) | adaptivePlanNode, aggregation, apply, assignUniqueId, correlatedJoin, distinctLimit, dynamicFilterSource, scalar, exchange, explainAnalyze, filter, groupId, indexJoin, indexSource, intersect, join, limit, markDistinct, mergeProcessor, mergeWriter, output, patternRecognition, project, remoteSource, rowNumber, sample, semiJoin, simpleTableExecuteNode, sort, spatialJoin, statisticsWriterNode, tableDelete, tableExecute, tableCommit, tableFunction, tableFunctionProcessor, tableScan, tableCommit, tableUpdate, tableWriter, topN, topNRanking, union, unnest, values, window |
| `io.trino.sql.ir.Expression` | **yes** (`@JsonSubTypes`, 16, sealed) | array, bind, call, case, cast, coalesce, constant, field, in, isnull, lambda, let, logical, reference, row, match; `Constant` values as block bytes |
| `io.trino.sql.planner.Symbol`, `io.trino.sql.planner.plan.PlanNodeId` | no | name + `Type` / string id |
| `io.trino.metadata.TableHandle` and other `io.trino.metadata.*Handle` wrappers | no (engine wrappers) | wrap a `CatalogHandle` (string) plus a polymorphic connector handle |
| connector handles (see below) | **yes** (handle resolver) | phase 3 SPI |
| `io.trino.execution.SplitAssignment` | no | `PlanNodeId` + `ScheduledSplit`s |
| `io.trino.execution.ScheduledSplit` | no | sequence id + `io.trino.metadata.Split` |
| `io.trino.metadata.Split` | no | catalog handle + polymorphic `ConnectorSplit` + split weight |
| `io.trino.execution.buffer.OutputBuffers` | **yes** (`@JsonSubTypes`, 2) | `pipelined` → PipelinedOutputBuffers, `spool` → SpoolingOutputBuffers; spooling variant carries `ExchangeSinkInstanceHandle` (polymorphic, connector-owned) |
| `io.trino.spi.connector.ConnectorTableCredentials` | **yes** (handle resolver) | 14th family in `HandleJsonModule` |
| `io.trino.exchange.ExchangeInput` | **yes** (`@JsonSubTypes`, 2) | reachable via `RemoteSourceNode` / spooling buffers |
| `io.trino.sql.planner.PartitioningScheme`, `Partitioning` | no | contains `ConnectorPartitioningHandle` (polymorphic) |
| `io.trino.spi.predicate.TupleDomain<ColumnHandle>` | no | map of polymorphic column handles to `Domain` (done) |
| `io.trino.spi.connector.LocalProperty` implementations | **yes** (`@JsonTypeInfo`) | reachable via table layouts in plan nodes |
| `io.trino.spi.expression.ConnectorExpression` | **yes** (`@JsonTypeInfo`) | reachable via table scan constraints pushed into handles |
| `io.trino.sql.planner.rowpattern.ir.IrRowPattern`, `io.trino.jsonpath.ir.IrPathNode`, `io.trino.sql.routine.ir.IrStatement` | **yes** | reachable only inside specific plan nodes (pattern recognition, json table, routines) |
| `io.airlift.slice.Slice` (exchange encryption key) | no | `bytes` field |

## Connector handle families (`io.trino.metadata.HandleJsonModule`)

All **polymorphic** today via `AbstractTypedJacksonModule` with ids of the
form `"<pluginClassLoaderId>:<className>"`. Phase 3 gives each family an
accessor on `ConnectorHandleCodecProvider`:

1. `ConnectorTableHandle`
2. `ColumnHandle`
3. `ConnectorSplit`
4. `ConnectorOutputTableHandle`
5. `ConnectorInsertTableHandle`
6. `ConnectorTableExecuteHandle`
7. `ConnectorMergeTableHandle`
8. `ConnectorIndexHandle`
9. `ConnectorTransactionHandle`
10. `ConnectorPartitioningHandle`
11. `ExchangeSinkInstanceHandle`
12. `ExchangeSourceHandle`
13. `ConnectorTableFunctionHandle`
14. `ConnectorTableCredentials`

(The brief says 13; `HandleJsonModule` at this base registers 14 — table
credentials were added as their own family. The SPI covers all 14.)

Engine-owned handles that also flow here and get engine `.proto` messages in
phase 4/5: system connector handles (`io.trino.connector.system.*`),
`io.trino.transaction.RemoteTransactionHandle`-style wrappers, and the
`io.trino.metadata.*Handle` catalog wrappers.

## All `@JsonTypeInfo` files in scope

`trino-main`: `ExchangeInput`, `OutputBuffers`, `IrPathNode`, `OperatorInfo`,
`JsonTableColumn`, `JsonTablePlanNode`, `Expression`, `PlanNode`,
`StatisticsWriterNode` (nested), `TableWriterNode` (nested), `IrRowPattern`,
`ValuePointer`, `IrStatement` (plus `UiQueryResource`, not on this control
plane).
`trino-spi`: `LocalProperty`, `TableReferenceInfo` (event listener, not on
this control plane), `ConnectorExpression`, `Argument` (table functions),
`Metric`, `ValueSet`.
`trino-client`: `spooling/Segment` (client protocol, out of scope).

## Remaining after this branch (phases 4–6)

- `PlanFragment` / `PlanNode` (45) / `Expression` (16) protobuf schemas.
- `TaskUpdateRequest`, `TaskInfo`, session, span, stats and `Metric`.
- Handle codecs for every connector other than tpch and memory (including
  hive); under `PROTOBUF` they fail the query by design.
- Low-volume internal endpoints left on JSON: `/v1/announce`, `/v1/info`,
  `/v1/status`, `/v1/memory`, `/v1/node`.
- Multi-node benchmark at SF100 (phase 6); this branch has local
  microbenchmarks only (see `protobuf-control-plane-results.md`).
