# PR draft: Confluent protobuf support without Confluent licensed libraries

Branch: `feature/kafka-confluent-protobuf` (fork only; not opened upstream yet).

## Description

The Kafka connector's Confluent table description supplier supported
PROTOBUF subjects only after copying `kafka-protobuf-provider` and
`kafka-protobuf-types` (Confluent Community License) into the plugin
directory by hand. This change makes protobuf support work out of the box,
using only Apache-licensed dependencies:

* `trino-record-decoder` builds protobuf descriptors from schema text with
  the Wire parser and protobuf-java linking, resolving imports through a
  resolver and the bundled Google well-known types. Import cycles,
  unresolvable imports, groups and link errors fail with `INVALID_PROTO_FILE`.
  The module no longer depends on any Confluent artifact.
* The connector registers a Trino-owned `SchemaProvider` for `PROTOBUF`
  schemas. The schema compiles lazily on first use, fetching the subjects it
  references through the existing `SchemaRegistryClient` (so the configured
  registry URLs, TLS and client properties apply unchanged) and linking them
  transitively. Dangling, cyclic and conflicting references and references to
  non-protobuf schemas fail with `KAFKA_SCHEMA_ERROR`; registry failures with
  the new `KAFKA_SCHEMA_REGISTRY_ERROR`. A failed compilation is not cached.
* Records are decoded with the schema id and message index path from their
  Confluent wire format frame, so a topic can hold records written with
  different schema versions and with message types other than the first one
  in the file. Fields that an older writer schema lacks decode as `NULL`,
  at the top level and within `ROW` columns (fixes trinodb/trino#20204).
  The message index path is resolved the way Confluent's deserializer does,
  counting the map entry messages that registered schema text declares
  explicitly, and such explicit entries are read as maps.
* Imports that Confluent serializers do not register as references resolve
  without one: the Google well-known types and common types (`google/type`,
  from the Apache licensed `proto-google-common-protos`), and
  `confluent/meta.proto` and `confluent/type/decimal.proto`, whose messages
  Trino declares with the same names and field numbers.
* `INSERT` into Confluent protobuf tables is supported: rows are encoded with
  the latest schema of the key and message subjects and framed in the
  Confluent wire format. Tables whose topic has no key subject write records
  with a null key; tables without a value subject reject `INSERT`, since a
  null value would be a tombstone.
* The Confluent protobuf libraries remain test dependencies only;
  `TestNoConfluentProtobufDependency` scans the connector's classes to make
  sure nothing references them.
* Docs: the copy-the-JARs section is removed; schema evolution, references
  and INSERT support are documented.

Behavior changes worth calling out for reviewers:

* Fields declared with the proto3 `optional` keyword are now regular columns,
  which are `NULL` when the field is not set. The Confluent provider exposed
  them as `JSON` oneof columns through their synthetic oneof.
* The descriptors follow protoc presence semantics for proto3 fields, so a
  proto3 scalar set to its default value is not serialized by the encoder
  (the Confluent provider's descriptors serialized it).
* File-based protobuf tables (`FileTableDescriptionSupplier`) gain the same
  descriptor builder; oneof fields no longer get dropped from their
  descriptors.

## Additional context and related issues

* Fixes trinodb/trino#20204 (schema evolution with nested messages).
* Related to trinodb/trino#23736 (Confluent licensed JARs for protobuf).
* Basic auth to the schema registry needs `basic.auth.credentials.source`
  in the client properties, which the connector does not expose today; this
  is unchanged by this PR (the REST test shows the provider works through
  the client with it set). A follow-up could add a config property.

Commits (each builds on its own):

1. Build protobuf descriptors from schema text
2. Decode missing protobuf row fields as NULL
3. Add Confluent protobuf wire format
4. Add registry backed protobuf schema provider
5. Drop Confluent protobuf provider from Kafka
6. Support INSERT into Confluent protobuf tables
7. Document protobuf support without extra JARs

The review commits on top of these (map entries, known imports, proto3
optional presence, enum and key-only INSERT fixes, docs) are to be folded
into the commits above before the PR is opened.

Tests:

* `lib/trino-record-decoder`: `TestProtoFileDescriptorBuilder` (18 cases:
  scalars, labels, enums, oneofs, maps, proto3 optional, well-known types,
  imports, public imports, transitive visibility, cycles, name resolution,
  map entry naming, groups, duplicates) and `TestProtobufDecoder`; `verify
  -P ci` and the error-prone compile pass.
* `plugin/trino-kafka`: `TestConfluentProtobufWireFormat` (including a
  byte-for-byte comparison with Confluent's `MessageIndexes`),
  `TestConfluentProtobufSchema`, `TestConfluentSchemaRegistryDynamicMessageProvider`,
  `TestConfluentProtobufRowEncoderFactory`, `TestConfluentProtobufSchemaRegistryRest`
  (in-process HTTP registry with basic auth: decode with references, encode
  with the latest schema, 401, 404, dangling reference, registry down),
  `TestProtobufSchemaParser`, `TestNoConfluentProtobufDependency`, protobuf
  cases in `TestConfluentSchemaRegistryTableDescriptionSupplier`, and new
  Docker cases in `TestKafkaProtobufWithSchemaRegistryMinimalFunctionality`
  (INSERT round trip, INSERT without key subject, schema references, nested
  schema evolution).

## Release notes

( ) This is not user-visible or is docs only, and no release notes are required.
( ) Release notes are required. Please propose a release note for me.
(x) Release notes are required, with the following suggested text:

```markdown
## Kafka
* Support protobuf schemas in the Confluent table description supplier
  without copying Confluent licensed JAR files into the plugin directory,
  including schema references, schema evolution and `INSERT`. ({issue}`23736`)
* Fix query failure when a protobuf record was written with a schema that
  lacks a field of a `ROW` column. ({issue}`20204`)
* {{breaking}} Map protobuf fields declared `optional` in proto3 schemas of the
  Confluent table description supplier to columns of the field's type, instead
  of `JSON` columns named after their synthetic oneof.
```
