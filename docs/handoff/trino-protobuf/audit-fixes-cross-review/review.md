# Fable cross-review of the Opus review fixes (e787a605..fc0d7812), 2026-10-24

Branch `feature/kafka-confluent-protobuf`, reviewed head fc0d7812 (six Opus commits fixing fork issues
#47-#50). Verdict: **accepted with 3 fix commits**, author Kristian, no AI trailers, committed locally on top of
fc0d7812 (patch in this folder), PUSHED 2026-10-24 02:13 with Kristian's OK as a fast-forward fc0d7812..1722171d; #47-#50 closed.

| Commit reviewed | Verdict |
|---|---|
| e787a605 INSERT uses the table's schema id and message (#47) | Accepted. One refinement: a pinned schema id the registry no longer has (404) was a `KAFKA_SCHEMA_REGISTRY_ERROR`; it is now `KAFKA_SCHEMA_ERROR` "Schema id N of subject 'S' does not exist in the schema registry", as the decoder reports it (fc6e3bc5, new case in `testSchemaOfTheTableIsUsed`). |
| ebb1e719 columns and table schema from the handle (#48) | **Not accepted as delivered.** `LogicalPlanner.createInsertPlan` takes the columns it writes from `getTableMetadata(session, handle)`, which `KafkaMetadata` still answered by describing the table again (latest schema versions), while analysis used the handle. A schema registered between the two gave the INSERT plan columns the handle did not have (renamed column: `columns.get(name)` is null, the output no longer matches the page sink's columns). `TestKafkaMetadata` even asserted that `getTableMetadata` shows the latest version. Fixed in 9c690d0d: `KafkaColumnHandle` carries `Optional<String> comment` (the one piece of table metadata the handle lacked; `JdbcColumnHandle` and `HiveColumnHandle` do the same), `getTableMetadata(handle)` is built from the handle's columns, the `getTableSchema` override is dropped (the SPI default derives it from the table metadata), and `KafkaTopicFieldDescription.columnMetadata()` / `InternalField.getColumnMetadata()` are gone. Listing the columns of all tables (`streamRelationColumns`) still describes each table by name, without the internal-column conflict check, as before. `TestKafkaMetadata` now checks table metadata, schema, column handles and column metadata agree with the handle and carry the comments, and that the listing describes the table again. |
| 081aeddd reserved check via Wire's `Reserved` (#49) | Accepted, no defects. `Reserved.fromElements` / `matchesTag` / `matchesName` exist in Wire 7.1.0 with the semantics used; the enum `max` limit is correctly described. |
| 17c0618c PR draft | Accepted; 1722171d updates it for the metadata fix. |
| 4d759bef file schema provider error (#50) | Accepted, no defects. |
| fc0d7812 Docker INSERT into a nested message | Accepted, no defects. |

Fix commits: 9c690d0d (table metadata from the handle, comment in the column handle, 40 constructor call sites),
fc6e3bc5 (deleted pinned schema is a schema error), 1722171d (PR draft).

Verification (cloud container, JDK 25, Confluent 8.1.1 jars from the cp-schema-registry image): `install -P ci` on
trino-record-decoder and trino-kafka (airstyle, checkstyle, sortpom, dependency analysis, modernizer, spotbugs,
license) green; Error Prone green; trino-record-decoder 139 tests; trino-kafka 238 tests across the protobuf,
Confluent, encoder, file and metadata suites including Docker `TestKafkaProtobufWithSchemaRegistryMinimalFunctionality`
(17), `TestKafkaWithConfluentSchemaRegistryMinimalFunctionality` (9), `TestKafkaLatestConnectorSmokeTest`,
`TestMinimalFunctionality`, `TestKafkaIntegrationPushDown`, `TestInternalFieldConflict`, `TestKafkaPlugin`; plus
`TestKafkaConnectorTest` (276). 0 failures.

Fork issues #47-#50: closed 2026-10-24 02:15. New branch head: 1722171d.
