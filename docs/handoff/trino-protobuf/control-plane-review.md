# Review: protobuf control plane, phases 1–3

Branch reviewed: `experiment/protobuf-control-plane-impl` on ai-pipestream/trino (ce572c30, five commits on top of the brief commit 824ea904).
Review fixes: `experiment/protobuf-control-plane-review` (same base, plus one commit).
Tracking: fork issues #9 (work), #10 (this review, checklist ticked), findings #15 (phase 3 wiring), #16 (commit structure), #17 (content-type test and server negotiation), #18 (benchmark), #19 (review fixes to pick up), #20 (design notes), #21 (phase 4–6 blockers).

## Verdict

Phases 1 and 2 are accepted. Phase 3 is **not** accepted as "done": the SPI and the tpch and memory codecs are real and tested, but the engine never calls them, hive is missing, and the brief's phase-3 negative test (a query against a codec-less connector fails under `PROTOBUF`) cannot exist yet because nothing resolves a codec per catalog. Call phase 3 "SPI landed, engine wiring pending" and keep the box open.

The branch also needs its commits restructured before anyone treats it as upstream-shaped: the fifth commit is a formatting fixup, and the per-commit CI check would fail on commits 2–4.

## What I verified myself

Environment: JDK 25 (Temurin, installed in the cloud container), Maven 3.9.16 via `./mvnw`, offline after the first build. All runs were against the untouched impl commit in a separate worktree, then repeated on the review branch.

| Suite (impl commit ce572c30) | Result |
| --- | --- |
| `TestTaskProtoMappers` | 4 pass |
| `TestProtobufResponseHandler` | 2 pass |
| `TestInternalCommunicationConfig` | 2 pass |
| `TestConnectorHandleProtobufCodec` | 3 pass |
| `TestHttpRemoteTask` | 11 pass |
| `TestTaskInfoFetcherInstanceMismatch` | 3 pass |
| `TestTpchHandleCodecProvider` / `TestTpchMetadata` | 7 / 9 pass |
| `TestMemoryHandleCodecProvider` / `TestMemoryMetadata` | 7 / 14 pass |
| `TestProtobufControlPlane` (distributed, `PROTOBUF`) | 4 pass, 22 s |
| `install -DskipTests` for trino-main, tpch, memory, trino-tests and dependencies | pass |

Muse's claimed test runs reproduce. The `mvn verify -DskipTests` claim I did not repeat with the full check set (`-Dair.check.skip-all` was on for speed), but airstyle was run separately, see finding 2.

Benchmark reproduction (scratch JUnit loop, 20,000 iterations, three rounds, JDK 25 in the container; not JMH, directional only):

| Payload | JSON compact bytes | JSON pretty bytes | protobuf bytes |
| --- | ---: | ---: | ---: |
| `TaskStatus`, FAILED, one failure with cause and suppressed (the `TestTaskProtoMappers` fixture) | 1341 | 1699 | 501 |
| `TaskStatus.initialTaskStatus` | 679 | 808 | 119 |

Timing (ns/op, 20,000 iterations per round, JSON via the airlift `ObjectMapper` the server uses, which writes compact JSON):

| Payload | Round | serialize JSON | serialize proto | deserialize JSON | deserialize proto |
| --- | ---: | ---: | ---: | ---: | ---: |
| with failure | 0 | 25418 | 13379 | 30288 | 14069 |
| with failure | 1 | 5853 | 13051 | 11945 | 15374 |
| with failure | 2 | 14032 | 20419 | 51164 | 39453 |
| initial | 0 | 15049 | 6954 | 26323 | 15075 |
| initial | 1 | 14101 | 5440 | 23280 | 14580 |
| initial | 2 | 13712 | 5284 | 25101 | 15164 |

The byte reduction reproduces in direction (63 % and 82 % smaller). The exact 1024/224 figures do not reproduce because the fixture Muse used is not committed; mine is the committed test fixture, which carries a cause, a suppressed entry and three stack frames. The timings above were taken while another Maven build ran in the same container and swing by 4× between rounds, so they neither confirm nor refute the 1.4–1.8× claim. Protobuf was faster in 9 of 12 cells, which is consistent with the claim but not evidence for the ratio.

Muse's results doc reports 1024 vs 224 bytes and 1.4–1.8× faster codec time. The doc does not include raw data and the harness is not committed, so the brief's "raw data included" criterion is not met and the specific numbers cannot be checked.

## Findings

Severity: **blocker** (must change before phases 1–3 are accepted), **fix** (applied on the review branch), **note** (design feedback, no change made).

1. **Blocker. Phase 3 engine wrapper is dead code.** `core/trino-main/src/main/java/io/trino/metadata/ConnectorHandleProtobufCodec.java` has no production caller; nothing in the engine calls `Connector.getHandleCodecProvider()`; there is no `CatalogHandle -> ConnectorHandleCodecProvider` lookup in `CatalogManager`/`ConnectorServices`. The "fails loudly" behaviour exists only in a unit test with a lambda provider. The brief's phase-3 test ("a connector with no codec under `PROTOBUF` fails the query with the expected error") is not possible on this branch. Hive codecs are absent (acknowledged in the results doc).

2. **Blocker. Commit structure fails the per-commit CI check.** `.github/actions/compile-commit` runs airstyle on every commit of a PR. At a469e3b8 (before "Format code and add experiment results"), `./mvnw -pl core/trino-main airstyle:check` reports 7 files that need formatting, so commits 2–4 would each fail `check-commit`. Formatting belongs in the commits that introduced the code; the results doc should be its own commit. Commit titles are fine (36–46 chars, imperative, no period) and there are no AI trailers, but every body is empty; upstream expects a short why for commits of this size.

3. **Blocker. "Zero JSON on /v1/task" is not asserted, and the e2e test would pass if JSON were used.** `TestProtobufControlPlane` only checks query results. Nothing asserts a request or response content type, and the server still serves JSON on the same endpoints under `PROTOBUF` (`TaskResource` keeps `@Produces(JSON, X_PROTOBUF)` and the JSON `failTask` method). On the review branch the join test now asserts that a dynamic filter domain was actually collected, which can only arrive through the protobuf-only fetcher. A content-type assertion is still missing.

4. **Blocker (process). Benchmark not reproducible from the branch.** The harness is described but not committed and no raw per-run data is included. See reproduction above.

5. **Fix. `ProtobufResponseHandler` accepted a 200 with no `Content-Type`.** `FullJsonResponseHandler` treats a missing content type as "no value"; the protobuf handler parsed the body anyway. Now rejected, with tests for missing content type, non-200, empty body and corrupt body.

6. **Fix. Enum and oneof decoding threw uncategorized exceptions.** `TaskState.valueOf(proto.getState().name())`, the same for `ErrorType` and memory `InsertMode`, and `DomainProtoMapper`'s `VALUES_NOT_SET` all threw `IllegalArgumentException` on `*_UNSPECIFIED`/`UNRECOGNIZED`. They now throw `TrinoException(GENERIC_INTERNAL_ERROR)` with a message naming the field, per DEVELOPMENT.md "categorize errors". Tests added.

7. **Fix. Redundant config binding.** `InternalCommunicationModule` added `configBinder(binder).bindConfig(InternalCommunicationConfig.class)` right after `buildConfigObject(InternalCommunicationConfig.class)`, which already binds it (checked against airlift configuration 452). Removed.

8. **Fix. `DynamicFilterDomainsProtoMapper.fromProto` built a mutable `HashMap`.** Now `toImmutableMap`, per the Guava immutable collections rule.

9. **Fix. Test gaps.** The `df_discrete` case in `TestTaskProtoMappers` used `array(bigint)`, which is orderable, so it exercised `SortedRangeSet`, not the `EquatableValueSet` branch; that branch had no coverage at all. Added: equatable inclusive, complement and none (via `color`, comparable but not orderable), `Range.all` as a sorted range set, unbounded ranges, `notNull`, a 10,000-value domain, exact `fullGcTime` precision (the old assertion compared milliseconds only, and the fixture used a whole-millisecond value so sub-millisecond loss was invisible), corrupt-bytes and unspecified-enum negatives for all three messages. tpch: multi-range and unbounded constraints, `onlyNull`, `TupleDomain.none()`, non-scalar value rejection (`NOT_SUPPORTED`), corrupt bytes. memory: `APPEND`, unspecified insert mode, split without limit, corrupt bytes. Totals: trino-main protobuf tests 4+2 → 7+6, tpch 7 → 11, memory 7 → 11.

10. **Note. `SimpleProtobufHttpResponseHandler` is a 97-line copy of `SimpleHttpResponseHandler`.** Only the response type differs. Generalise over a small response interface (or adapt `JsonResponse`) instead of copying.

11. **Note. Server-side format negotiation remains.** Under `PROTOBUF` the worker still accepts a JSON `FailTaskRequest` and still produces JSON for `/status` when asked. The brief forbids negotiation. Either make the JSON methods 415/406 under `PROTOBUF` or document that the server side is intentionally dual until phase 5.

12. **Note. `DomainProtoMapper` rebuilds `SortedRangeSet` from `Range` objects.** `getOrderedRanges()` materialises one `Range` per range and the mapper rebuilds a block; `SortedRangeSet` already holds a sorted-ranges block plus inclusive flags, which is what the JSON codec ships. For large dynamic filters this is extra CPU in exactly the place the experiment is measuring. `DomainRange` positions are `int32` with a `-1` sentinel (a 10-byte varint); `sint32` or `optional` would be tighter.

13. **Note. `ConnectorHandle` wrapper returns `byte[]`.** `ConnectorHandleProtobufCodec.encode` serialises the wrapper message to bytes, so a phase-5 `Split` message would carry bytes-in-bytes. Return the `ConnectorHandle` message and embed it.

14. **Note. Build plumbing.** The protoc plugin block is copied into three poms with a hard-coded `3.11.4`; root `pluginManagement` is the Trino convention (kafka is the existing exception). `trino-tpch` now ships protobuf-java to every module that depends on it for tests.

15. **Note. Lone surrogates.** Jackson escapes unpaired surrogates in failure messages; protobuf-java replaces them with `?`. Edge case only.

## Exit criteria (brief) as of this branch

| Criterion | Status |
| --- | --- |
| Phase 1 inventory | done |
| Phase 2 TaskStatus, dynamic filter domains, FailTaskRequest on protobuf | done, verified e2e for status and dynamic filters; `/fail` request verified by unit round-trip only |
| Phase 3 SPI with tpch, memory, hive codecs | SPI and two connectors done; engine wiring and hive missing |
| Phases 4–5 | not started (declared) |
| No JSON on `/v1/task` asserted by a test | not done |
| Benchmark with raw data | local microbenchmark only, no raw data, harness not committed |
| Remaining-work list | present in results doc, and accurate |

## Gaps that will block phases 4–6

- Engine-side resolution of a codec provider per `CatalogHandle` (the `Function<CatalogHandle, Optional<ConnectorHandleCodecProvider>>` nobody constructs), including the `INFORMATION_SCHEMA` and `SYSTEM` catalog handle types and engine-owned handles (`GlobalSystemConnector`, `RemoteTransactionHandle`, `io.trino.metadata.*Handle` wrappers).
- `ConnectorHandle` as an embedded message, not bytes (finding 13).
- Connectors have no `BlockEncodingSerde`; the tpch codec invented a scalar-only `ScalarValue` schema and rejects non-scalar domains. Either give connectors a serde through `ConnectorContext` or standardise a shared domain message.
- Hive: 14 families, `HiveSplit` alone has dozens of fields and nested `HiveColumnHandle`/`HiveTypeName`; plan time for it.
- `TaskInfo` carries `OperatorInfo` (8 subtypes) and `Metric` (open set, connector-defined). A closed `oneof` covers SPI metrics; connector metrics need either a codec SPI or a loud failure.
- `TaskUpdateRequest`: `Span` as W3C trace context, `SessionRepresentation`, `OutputBuffers` with connector-owned `ExchangeSinkInstanceHandle`.
- Removing the server-side JSON methods under `PROTOBUF` so the "zero JSON" test can be written.
- A committed benchmark harness (JMH module or a `Benchmark*` class under `testing/trino-benchmark`) and the multi-node SF100 run.

## Review branch contents

One commit on `experiment/protobuf-control-plane-review`, based on ce572c30. Production changes: findings 5–8. Test changes: finding 9 and the dynamic-filter assertion in `TestProtobufControlPlane`. Formatted with `airstyle:format`. All suites listed above pass on it; see the per-suite table in the thread reply.
