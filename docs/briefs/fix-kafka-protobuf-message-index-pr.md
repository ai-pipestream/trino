# Draft PR: Fix Kafka protobuf records of non-first messages

Not opened. Drop this file from the branch before opening the PR upstream.

## Description

The Kafka connector's Confluent table description supplier handled protobuf
schemas that define more than one message incorrectly in two places:

1. **Decoding.** `ConfluentSchemaRegistryDynamicMessageProvider` read the
   Confluent message index from each record and then ignored it. Every record
   was decoded as the first message in its schema. A record written with any
   other message, such as the second top-level message or a nested message,
   was decoded as the wrong type: fields were misread or turned into unknown
   fields.
2. **Table columns.** `ProtobufSchemaParser` always built the table columns
   from the first message in the schema, even when the subject names the
   message, as with `RecordNameStrategy` (`<full message name>`) and
   `TopicRecordNameStrategy` (`<topic>-<full message name>`).

This change:

- Resolves each record's message from its schema id and message index, the
  same way the Confluent deserializer does (`ProtobufSchema.toMessageName`),
  and caches descriptors by schema id and message index.
- Fails the query with `INVALID_PROTOBUF_MESSAGE` when a message index does
  not exist in the schema, instead of silently using the first message.
- Surfaces schema registry lookup failures as `TrinoException`. They used to
  arrive wrapped in Guava's `UncheckedExecutionException`.
- Builds table columns from the message the subject names, including nested
  messages. Subjects that do not name a message, such as `TopicNameStrategy`
  subjects, still use the first message.
- Documents how the columns are chosen in `kafka.md`.

## Additional context and related issues

Commits:

1. `Add tests for non-first Kafka protobuf messages`: tests only. They fail on
   master.
2. `Decode Kafka protobuf records with their message index`
3. `Map Kafka protobuf columns from the subject message`

Evidence, run with JDK 25 and Docker. With only commit 1, 10 of 24 tests fail:

- `TestConfluentSchemaRegistryDynamicMessageProvider` (6 of 8 fail): records
  of the second top-level message, a nested message, a message nested three
  levels deep, and several messages sharing one schema id all decode as the
  first message (`expected: "io.trino.protobuf.Order.Line" but was:
  "io.trino.protobuf.Header"`). The missing message index case throws
  nothing. The unknown schema id case gets `UncheckedExecutionException`
  instead of `TrinoException`.
- `TestProtobufSchemaParser`: for both record name strategies, the columns
  come out as `["name"]` (the first message) instead of the named message's
  fields.
- `TestKafkaProtobufWithSchemaRegistryMinimalFunctionality`: a
  `RecordNameStrategy` topic of the second message fails with `Column 'id'
  cannot be resolved`, and a `TopicRecordNameStrategy` topic of a nested
  message fails with `Column 'sku' cannot be resolved`.

With commit 2, only the 4 column failures remain. Commit 3 fixes those. With
all three commits, the Kafka protobuf and Confluent schema registry tests pass
(57 run, 0 failures). `verify -P ci`, the Error Prone build and
`airstyle:format` are also clean.

The test schema `protobuf/multiple_messages.proto` covers an enum declared
between messages, a map field (its generated entry type must not count
towards message indexes), and messages nested three levels deep. The tests
also cover several message types sharing one schema id (one cache entry
each), the single zero byte that stands for the first message, negative and
out-of-range indexes, and an unknown schema id.

Behavior change: with `TopicNameStrategy`, records written with a message
other than the first are still mapped to the first message's columns. Before
this change those columns held misread values. Now they hold values only
where the written message has a field with the same name, and are otherwise
NULL. A later change could fail such reads instead.

Related: trinodb/trino#20204 (schema evolution in the same decoder).

## Release notes

( ) This is not user-visible or is docs only, and no release notes are required.
( ) Release notes are required. Please propose a release note for me.
(x) Release notes are required, with the following suggested text:

```markdown
## Kafka connector
* Fix reading Protobuf messages other than the first message in a schema
  with the Confluent table description supplier. ({issue}`issuenumber`)
* Map table columns to the Protobuf message named by the subject when using
  the `RecordNameStrategy` or `TopicRecordNameStrategy`. ({issue}`issuenumber`)
```
