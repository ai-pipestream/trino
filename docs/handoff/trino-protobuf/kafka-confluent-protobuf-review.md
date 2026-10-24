# Review: Confluent protobuf for Kafka (`feature/kafka-confluent-protobuf`)

Adversarial review of Fable's branch (8 commits, head `e798d395`, fork issue #6), done 2026-10-22 by the Opus 5.5 review thread. Fixes are new commits on top of the branch, authored as Kristian, with no AI trailers, and pushed to `claude/project-thread-rsk4u1`. Issues: #22 to #27 under #6. Fable reviews these commits next.

## Verdict

**Not accepted as pushed. Accepted with the review commits, pending Fable's review of them.**

The design holds up. Decoding uses the writer's schema id, registry failures are not cached, references resolve transitively with cycle, dangling and conflict checks, and no Confluent protobuf class is referenced. None of the rule-3 defects (R5 to R8, silent skipping) were ported. But the Docker schema-registry tests had never been run, and running them exposed a real interoperability bug. Schemas registered by Confluent's own serializer read map columns as `array(row(key, value))`. The same root cause decoded nested record types with the wrong message.

## Findings

| # | Severity | Finding | Status |
|---|----------|---------|--------|
| 1 | High | The text a Confluent serializer registers declares map entries as explicit messages with `option map_entry = true`. The builder dropped the option, so map columns became `array(row)`. The upstream test `testSchemaWithImportDataTypes` failed. | Fixed `456a1d92` |
| 2 | High | The serializer (and Confluent's deserializer) count those explicit entry messages in the message index path. `findMessage` skipped them, so a record of a nested message declared after a map field decoded as the wrong type. Verified against Confluent's `ProtobufSchema.toMessageIndexes`/`toMessageName` and `KafkaProtobufSerializer`. | Fixed `456a1d92` |
| 3 | High | Confluent serializers do not register references for "known types" (default `skip.known.types=true`). That covers `google/type/*.proto` and `confluent/meta.proto` / `confluent/type/decimal.proto`, as well as `google/protobuf/*`. Only `google/protobuf/*` was bundled, so schemas using `google.type.Date`, `Money` or `confluent.type.Decimal` failed to compile. This is a regression from the copy-the-jars setup. | Fixed `c2b244d9` (google/type, from Apache-licensed `proto-google-common-protos`) and `53b41833` (Confluent types, **needs Kristian's call**) |
| 4 | Medium | proto3 `optional` fields became regular columns, as the PR intends, but an unset field read as `0`/`''`. The old JSON oneof column told unset apart from default. | Fixed `60680ff1`: unset reads as NULL |
| 5 | Medium | INSERT of an `array(varchar)` into a repeated enum threw `ClassCastException`. An unknown enum name threw an NPE. Both were latent upstream, and this PR's INSERT support makes them reachable. | Fixed `b51d9d13` (`CONSTRAINT_VIOLATION`) |
| 6 | Medium | INSERT into a Confluent table with only a key subject threw an NPE (`messageBytes.length`). Writing a null value would write tombstones. | Fixed `427bafa3`: rejected with `NOT_SUPPORTED` |
| 7 | Low | `testInsertWithoutKeySubject` could never pass (`buildOrThrow` on a duplicate `key.serializer`). The PR draft listed the new Docker cases as tests, but they had not been run. | Fixed `b50c4454` |
| 8 | Low | Docs said the well-known types are "resolved through the schema registry", and nothing documented proto3 `optional` or the key-only INSERT rule. | Fixed `052827e6` |

## Not fixed (follow-ups)

- **Columns map to fields by name, not field number.** The brief asked for field-number mapping (#20204). Name mapping fixes the reported failure (fields missing in older writers read as NULL). A field renamed between versions also reads as NULL, though protobuf treats a rename as wire compatible. Fixing that needs field numbers in the column mapping. I'd do it as a follow-up, not in this PR.
- **Schema registry basic auth and TLS.** These are not configurable through connector properties. Avro has the same gap, and it is not caused by this branch. A small, separate PR could add `kafka.confluent-schema-registry-*` auth/TLS properties.
- **INSERT of NULL values** still fails with `GENERIC_INTERNAL_ERROR`. That is the existing upstream encoder behavior, and the product test `TestKafkaProtobufWritesSmokeTest` asserts it, so I left it alone.
- **Commit history.** For upstream, fold the review commits into the commits they fix. The branch is also still based on `master`, not on `fix/kafka-protobuf-message-index`, which has no commits. This branch carries its own index-path handling, so nothing is lost.

## Author's open points

- **proto3 optional:** I agree with making them regular columns. With finding 4 fixed, nothing is lost compared with the JSON column. Release notes should call it out, because the `_field` JSON columns disappear.
- **Basic auth:** this is a pre-existing gap, so it belongs in a follow-up (see above).
- **Null keys:** writing a null key for tables without a key subject is right. A null value (finding 6) is not, so it is rejected.

## What ran (JDK 25 Temurin, Docker 29)

`packages.confluent.io` is blocked here. The io.confluent 8.1.1 jars came from Confluent's official `cp-schema-registry:8.1.1` Docker image, with the parent POMs from GitHub, served to Maven as a local mirror.

- Baseline at `e798d395`: `lib/trino-record-decoder` 118 tests pass. Kafka protobuf unit tests pass (90). `TestKafkaProtobufWithSchemaRegistryMinimalFunctionality` (Docker): **13 run, 1 failure, 1 error** (findings 1 and 7).
- After fixes, at `fe31a5c7`: `lib/trino-record-decoder` **121 pass**. Full `plugin/trino-kafka` suite including every Docker test (`TestKafkaConnectorTest`, `TestKafkaDistributed`, `TestKafkaLatestConnectorSmokeTest`, both schema-registry minimal-functionality classes, ...): **514 run, 0 failures, 182 skipped**. The skips are the connector test's unsupported-feature skips. `TestKafkaProtobufWithSchemaRegistryMinimalFunctionality` passes 13 of 13.
- `verify -DskipTests -P ci -pl lib/trino-record-decoder,plugin/trino-kafka` passes on **all 17 commits**, Fable's 8 and the review's 9. This covers airstyle, checkstyle, modernizer, dependency analysis and sortpom. Caveat: the `org.apache.kafka:kafka-clients:8.1.1-ccs` POM used by dependency analysis was reconstructed from Apache Kafka 4.1.1's POM, because the Confluent repository is blocked.
- `clean test-compile -P errorprone-compiler` on head passes. The only warnings in new code are `BadImport` for the nested `FieldDescriptorProto.Type` import (in the builder and in one test) and `EffectivelyPrivate` in the wire format's varint reader, all of them non-fatal.
- `verify -P ci` for `plugin/trino-redis` passes. Redis bundles `trino-record-decoder` and so now also `proto-google-common-protos`.
- Not run: product tests (`testing/trino-product-tests`).

Branch: the review commits are pushed to `claude/project-thread-rsk4u1`, a fast-forward of `feature/kafka-confluent-protobuf`. That branch itself is untouched pending Kristian's OK.

## Review commits (for Fable)

1. `456a1d92` Honor map entries declared in protobuf schemas
2. `c2b244d9` Resolve Google common type imports in protobuf
3. `60680ff1` Decode unset proto3 optional fields as NULL
4. `b51d9d13` Fix writing repeated protobuf enum values
5. `427bafa3` Reject INSERT into tables without message subject
6. `b50c4454` Fix protobuf schema registry INSERT test
7. `53b41833` Resolve Confluent type imports in protobuf schemas
8. `052827e6` Document protobuf optional and known imports

New tests: explicit map entries and fully qualified type names (builder); google/type imports never reach the resolver; explicit entries count in index paths, checked against Confluent's `ProtobufSchema`; an end-to-end decode of records written by Confluent's real `KafkaProtobufSerializer` from a protoc-ordered descriptor; proto3 optional presence (top level, path mapping, nested row); enum and repeated-enum INSERT, including unknown names; Confluent decimal/meta imports and reference precedence; `NullRowEncoderFactory`.
