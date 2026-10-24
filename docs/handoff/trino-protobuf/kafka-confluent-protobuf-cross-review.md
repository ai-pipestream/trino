# Cross-review: Opus's fixes on the Confluent protobuf branch

Review of the nine commits Opus 5.5 pushed to `claude/project-thread-rsk4u1`
(`456a1d92` to `fe31a5c7`, on top of Fable's `e798d395`), done 2026-10-22 by the
Fable review thread "Kafka Opus fixes cross-review". Fixes are three new commits
on the same branch, authored as Kristian, no AI trailers.

## Verdict

**Accepted.** The nine review commits are correct, each compiles on its own,
the `ci` profile checks pass, and every protobuf test passes with Docker,
including a record written by Confluent's real serializer. Three small things
were tightened; none of them is a defect in Opus's fixes.

## What was checked, commit by commit

| Commit | Fix | Review result |
|---|---|---|
| `456a1d92` Honor map entries | `option map_entry = true` carried into the descriptor; index path counts every nested message, implied entries go last | Correct. Verified against Confluent's `ProtobufSchema.toMessageIndexes` and `toMessageName` (Wire `MessageElement`s only, so `map<K, V>` entries are not counted but explicit entry messages are). `findMessage` rejects a path that lands on an entry. |
| `c2b244d9` Google common types | 17 `google/type/*.proto` files linked against `proto-google-common-protos` | Correct in substance. One file too many: `google/type/localized_text.proto` is not in Confluent's known-types list (checked in `ProtobufSchema` 8.1.1 source, `KNOWN_DEPENDENCIES`), so serializers register it as a reference. Fixed. |
| `60680ff1` proto3 optional as NULL | `isUnsetProto3Optional` = synthetic oneof and `!hasField` | Correct. Only proto3 `optional` has a synthetic oneof; real oneofs keep the JSON column; proto2 optional keeps the old default-value behavior (unchanged upstream behavior). Added an end-to-end check through the Confluent serializer. |
| `b51d9d13` repeated enum INSERT | per-element enum lookup, `CONSTRAINT_VIOLATION` for unknown names | Correct, covers scalar, repeated, row and map value positions through `setField`. |
| `427bafa3` key-only INSERT | `NOT_SUPPORTED` from `NullRowEncoderFactory` for the message side | Correct. The check runs at page sink creation, the same place the other encoder factories validate, so the error reaches the user at execution rather than planning. Acceptable; moving it to `beginInsert` would be optional polish. |
| `b50c4454` INSERT test | `buildKeepingLast` | Correct. |
| `53b41833` Confluent types | `confluent/meta.proto` and `confluent/type/decimal.proto` declared in Trino, reference wins over the declaration | Compiles and decodes; message names and field numbers match Confluent's files. The option extensions are skipped by the builder anyway. **Still Kristian's call** whether declaring these two messages is acceptable license-wise. Note the precedence differs from the Google files, where the bundled descriptor wins over a reference; harmless for real serializers. |
| `052827e6` docs, `fe31a5c7` PR draft | | Accurate. `{{breaking}}` release note is right for the `_field` JSON columns disappearing. |

## Fixes pushed (on top of `fe31a5c7`)

1. `40e1a55d` Bundle only the Google common types serializers treat as known.
   Drops `localized_text.proto` from the bundled list. New test in
   `TestConfluentProtobufSchema` iterates Confluent's own `ProtobufSchema.knownTypes()`
   and checks each resolves without a reference, and that `localized_text.proto`
   does not.
2. `23e78cc9` Reject malformed explicit map entry messages. A declared message
   with `option map_entry = true` that does not have exactly `key = 1` and
   `value = 2` (optional, no nested types, enums or oneofs) failed later with a
   `NullPointerException` while the table columns were derived (`findFieldByNumber`).
   Now `INVALID_PROTO_FILE`. Eight malformed shapes tested.
3. `a83e7791` Test proto3 optional presence against the Confluent serializer:
   a protoc-style descriptor with a synthetic oneof, serialized by
   `KafkaProtobufSerializer` unset and set-to-default, decoded through
   `ConfluentSchemaRegistryDynamicMessageProvider`.

## What ran (JDK 25, Maven 3.9.16, Docker 29, Confluent 8.1.1 jars from the `cp-schema-registry` image)

- At Opus's head `fe31a5c7`: `lib/trino-record-decoder` 121 tests pass;
  `plugin/trino-kafka` protobuf and Confluent classes 105 tests pass, including
  `TestKafkaProtobufWithSchemaRegistryMinimalFunctionality` 13 of 13 with Docker.
- `test-compile` of both modules passes at each of Opus's code commits and at
  each of mine.
- At my head `a83e7791`: `verify -DskipTests -P ci` passes for both modules
  (airstyle, checkstyle, modernizer, dependency analysis, sortpom);
  decoder 30 protobuf tests pass, Kafka protobuf suite rerun with Docker
  (107 tests, 0 failures, schema-registry Docker test 13 of 13).
- Not run: the full `TestKafkaConnectorTest` suite (Opus ran it at `fe31a5c7`,
  514 run, 0 failures), product tests, `plugin/trino-redis`.

## Still open (unchanged from Opus's report)

- Kristian's decision on `53b41833` (Confluent type declarations).
- Follow-ups, not for this PR: columns by field number (#26), registry auth/TLS
  properties (#27), INSERT of NULL into fields with presence (proto3 optional or
  message fields could be left unset instead of failing).
- Before an upstream PR: fold the twelve review commits into Fable's seven.
