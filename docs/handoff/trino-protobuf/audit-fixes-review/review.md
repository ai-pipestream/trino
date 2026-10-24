# Opus review of the issue audit fixes (c3e64b28..88fcba0b), 2026-10-24

Branch `feature/kafka-confluent-protobuf`, reviewed head 88fcba0b. Verdict: **accepted with 6 fix
commits**, author Kristian, no AI trailers, PUSHED 2026-10-24 ~01:30 UTC with Kristian's OK as a fast-forward
88fcba0b..fc0d7812 (patch in this folder). Fable cross-review of the 6 commits is next.

| Commit reviewed | Verdict |
|---|---|
| c3e64b28 subject message (#5) | Accepted, no defects. Subject-to-message matching is unambiguous (names cannot contain `-`), map entries excluded, docs and Docker tests fine. |
| 91a58fd9 reserved numbers/names (#41) | Accepted with 081aeddd: use Wire's `Reserved.matchesTag/matchesName`, drop the new `kotlin-stdlib` declaration, 3 edge cases. Enum `max` limit documented (#49). |
| a341708f duplicate client properties (#40) | Accepted, no defects. Note: the endpoint identification algorithm is always set, so an embedder provider setting it always fails startup, by design and with a clear error. |
| dfa91b0d pinned reader schema (#44) | Not accepted as delivered. INSERT still wrote with the latest schema (#47, fixed e787a605); handle columns, analysis schema and data schema came from separate lookups (#48, fixed ebb1e719); file-supplier INSERT error regressed (#50, fixed 4d759bef). |
| 88fcba0b PR draft | Accepted; 17c0618c updates it for the fixes. |

Fix commits: e787a605 (INSERT uses the table's schema id and message), ebb1e719 (KafkaMetadata takes columns
and table schema from the handle; new TestKafkaMetadata), 081aeddd (reserved check), 17c0618c (PR draft),
4d759bef (FileReadContentSchemaProvider error; new TestFileReadContentSchemaProvider), fc0d7812 (Docker test: INSERT into a
nested message with the TopicRecordNameStrategy, plus a PR draft line).

Verification (cloud container): `install -P ci` on trino-record-decoder and trino-kafka (checkstyle/airstyle,
sortpom, dependency analysis, modernizer, spotbugs, license) green; Error Prone green; trino-record-decoder 139
tests; Kafka protobuf/Confluent suites incl. Docker `TestKafkaProtobufWithSchemaRegistryMinimalFunctionality`
(17) and `TestKafkaWithConfluentSchemaRegistryMinimalFunctionality` (9), plus `TestKafkaConnectorTest` (276),
`TestMinimalFunctionality`, `TestKafkaIntegrationPushDown`, `TestKafkaLatestConnectorSmokeTest`,
`TestKafkaMetadata`, `TestKafkaPlugin`: 495 tests, 0 failures.

Fork issues: #47, #48, #49, #50.
