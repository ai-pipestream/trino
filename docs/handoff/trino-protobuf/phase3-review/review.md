# Review: protobuf control plane phase 3

Branch reviewed: `claude/project-thread-tsljpr` on ai-pipestream/trino, eight commits `f2b3a9ef..86fcef3a` on top of `598dd300`.
Review fixes: seven commits `a7c76052..877310d1` on top of `86fcef3a`, pushed 2026-10-23 to fork branch `claude/project-thread-mjwdto` (no PR). The same commits are in `phase3-review-fixes.patch` next to this file.

Tracking: fork issues #38 (this review), #35 (build blocker), #36 (domain decoding), #37 (phase 5 design note).

## Verdict

Not accepted as delivered: the root pom change breaks the build of `trino-hive-formats` (and through it everything that depends on hive), so the branch does not build from a clean local repository. The design is sound and the work is otherwise careful, so with the review fixes below phase 3 is **accepted**, subject to the cross-review of the fixes.

Phase 3 as the brief defines it is done with the fixes: the codec SPI, the per-catalog engine wiring, the engine-owned codecs, hive, tpch and memory. Nothing in production calls the handle codecs until `TaskUpdateRequest` moves to protobuf in phase 5; that is expected, and the results doc says so.

No JSON fallback was found. Handles, domains and splits are encoded as real protobuf messages; domain values travel as Trino blocks inside a `bytes` field, which is the block wire format, not JSON.

## Findings

| # | Severity | Finding | Status |
| --- | --- | --- | --- |
| 1 | Blocker | Root `pluginManagement` gave every protoc module `src/main/proto` as source and include directory. `trino-hive-formats` has none, so protoc fails; `trino-kafka` also inherited it. Missed because hive-formats came from a stale `~/.m2`. | Fixed: directories moved into each module's execution |
| 2 | Major | `DomainProtoCodec` let a block of the wrong class through as a raw `ClassCastException` (later, in the engine), turned an invalid type id into a raw `IllegalArgumentException`, and ignored bytes after the block. | Fixed, all `TrinoException` naming the type |
| 3 | Major (perf) | Decoding large discrete sets was slower than JSON (every value rebuilt as a `Range`): 10k values 5.0 ms vs JSON 1.7 ms, 1M values 221 ms vs 153 ms. | Fixed: a verified normalized discrete set goes straight to `SortedRangeSet.fromJson(..., DISCRETE)`; now 1.0 ms and 81 ms |
| 4 | Minor | `ProtobufHandleCodec` and `ConnectorHandleProtobufCodec` did not categorize NPE, CCE, IOOBE etc. from handle constructors or connector codecs. | Fixed |
| 5 | Minor | `/v1/task` Accept handling: `q=0.0`, `q=0.000` were accepted; `*/*` overrode a specific `q=0`; unparsable q was not handled. | Fixed per RFC 9110 12.5.1 |
| 6 | Minor | `TestClassLoaderSafeWrappers` did not cover the two new wrappers. | Fixed |
| 7 | Test gap | No test round-tripped the handles of real planned queries. | New `TestHiveHandleCodecsForPlannedQueries` (passes; no codec bug found) |
| 8 | Doc | Results doc said keeping `NON_DISCRETE` "would need a public SPI constructor"; `fromJson` exists. Its verification also missed hive-formats. | Fixed in the doc |
| 9 | Design note | Protobuf handle decoding needs the catalog loaded, so phase 5 must decode `TaskUpdateRequest` after `ensureCatalogsLoaded`, not in a JAX-RS reader as the JSON path does. | Added to remaining work |
| 10 | Note | The lakehouse connector uses hive handles without a codec provider. | Added to remaining work |
| nit | | `ClassLoaderSafeConnectorHandleCodecProvider` allocates a wrapper per lookup. | Not changed |

## Commits

1. Scope protoc configuration to each module
2. Harden protobuf domain decoding
3. Wrap failures of protobuf handle codecs
4. Honor Accept quality on task endpoints
5. Test class loader safe handle codecs
6. Round-trip handles of planned hive queries
7. Document phase 3 review fixes

Author Kristian Rickert, no AI trailers.

## Verification

JDK 25 (Temurin), Maven 3.9.16, 4-core cloud container. All on the fixed tree unless noted.

| Suite | Result |
| --- | --- |
| `TestDomainProtoCodec` / `TestProtobufHandleCodec` | 18 / 5 pass (5 domain tests and 1 handle test fail on `86fcef3a`) |
| `TestConnectorHandleProtobufCodec`, `TestTaskResourceEncoding`, `TestSplitProtoMapper`, `TestTaskProtoMappers`, system / information_schema / global system codec tests, `TestProtobufResponseHandler`, `TestHttpRemoteTask`, `TestInternalCommunicationConfig`, `TestTaskInfoFetcherInstanceMismatch` | 90 pass |
| `TestHiveHandleCodecProvider` / `TestHiveHandleCodecsForPlannedQueries` / `TestHivePlugin` | 15 / 4 / 11 pass |
| `TestTpchHandleCodecProvider` / `TestMemoryHandleCodecProvider` / `TestClassLoaderSafeWrappers` | 15 / 12 / 1 pass |
| `TestProtobufControlPlane` (multi-node, `PROTOBUF`) | 6 pass |
| `install -DskipTests` with all checks: protobuf-handles, plugin-toolkit, trino-main, tpch, memory, hive-formats, hive | pass |
| `test-compile -P errorprone-compiler` on the six changed Java modules; root `sortpom:verify` | pass |
| `trino-kafka` | not built: packages.confluent.io did not resolve from the container today. Its effective pom no longer carries `src/main/proto`. |

Not run: full trino-main and hive suites, `TestMemoryConnectorTest`.

## For the cross-review

Look hardest at `DomainProtoCodec.isNormalizedDiscreteSet`: it is the only place that trusts input enough to skip the `Range` rebuild. It requires both ends of every pair inclusive, low equal to high, no null, no NaN (double and real), and strictly ascending under the type's unordered-last comparison. Anything else takes the old checked path. Also check the planned-query test's normalisation: it compares range sets by the values in their block, because the serde may write a dictionary block on one side and a flat block on the other.
