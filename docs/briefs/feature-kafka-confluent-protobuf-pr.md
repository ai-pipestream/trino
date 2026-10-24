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
  in the file. Columns are matched to the fields of the writer schema by
  field number, the identity of a field on the wire: the table's data
  schema names the registry id of the schema the columns were derived from
  and the message within it, resolved together with the columns when the
  table is described, so a schema registered later does not change what a
  running query reads or writes, and
  the decoder maps each column's field to the writer field with the same
  number and encoding, at the top level and within `ROW`, `ARRAY` and `MAP`
  columns, and converts its value the way protobuf reads it with the reader
  type (`int32` to `int64`, `int64` to `int32`, `bool`, enums by number,
  zigzag and `string`/`bytes` changes). A renamed field or enum value keeps
  its column and name; fields that an older writer schema lacks, that it
  encodes differently under the same number, or that change between
  `google.protobuf.Timestamp` or `Any` and another message type decode as
  `NULL` (fixes trinodb/trino#20204). Records of another message type than
  the table's are matched by name.
  The message index path is resolved the way Confluent's deserializer does,
  counting the map entry messages that registered schema text declares
  explicitly, and such explicit entries are read as maps.
* Imports that Confluent serializers do not register as references resolve
  without one: the Google well-known types and common types (`google/type`,
  from the Apache licensed `proto-google-common-protos`), and
  `confluent/meta.proto` and `confluent/type/decimal.proto`, whose messages
  Trino declares with the same names and field numbers.
* A subject of the `RecordNameStrategy` or `TopicRecordNameStrategy` names
  the message its records hold, which can be declared after other messages
  or nested in one of them. The table columns, the reader schema and the rows
  written by `INSERT` follow that message; any other subject, such as one of
  the `TopicNameStrategy`, keeps the first message of the schema, which was
  the only choice before.
* `INSERT` into Confluent protobuf tables is supported: rows are encoded with
  the schema the table columns were derived from, the latest schema of the key
  and message subjects when the query starts, and framed in the Confluent wire
  format. Tables whose topic has no key subject write records
  with a null key; tables without a value subject reject `INSERT`, since a
  null value would be a tombstone.
* The protobuf encoder writes `NULL` as an unset field when the field tracks
  presence (`optional`, `oneof` member or message typed), for columns,
  flattened columns and `ROW` fields alike, and rejects it with
  `CONSTRAINT_VIOLATION` and a message naming the field for the others
  (proto3 scalars without `optional`, proto2 `required` fields, `repeated`
  and `map` fields, `ARRAY` elements and `MAP` values). Previously every
  `NULL` failed with `GENERIC_INTERNAL_ERROR`.
* New `kafka.confluent-schema-registry.basic-auth.*` and
  `kafka.confluent-schema-registry.ssl.*` properties configure basic
  authentication, a trust store, a client key store and host name
  verification for the registry connection, for Avro and protobuf alike.
  The connector now always passes the endpoint identification algorithm to
  the client, which otherwise accepts any certificate host name as soon as
  any client property is set.
* The descriptor builder rejects fields, `oneof` members and enum values
  that use a number or name their message or enum reserves, as `protoc`
  does.
* A schema registry client property set by both the connector and an
  embedder's `SchemaRegistryClientPropertiesProvider` fails startup with
  `CONFIGURATION_INVALID` naming the property and both providers, instead
  of a duplicate key error.
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
* An unset message-typed field decodes as `NULL` (as a column, through a
  flattened `a/b` mapping and within `ROW` values) instead of an empty
  message, so a `ROW` column of default values or a `TIMESTAMP` of
  1970-01-01 no longer stands in for a field that was never set. This is
  what makes `INSERT` of `NULL` into such a field round-trip. Applies to the
  file-based protobuf decoder as well.
* The same holds for every field that tracks presence: an unset proto2
  `optional` field (even with a declared default) and an unset `oneof`
  member decode as `NULL` instead of the default value, matching the
  encoder, which leaves such fields unset for `NULL`.
* File-based protobuf tables (`FileTableDescriptionSupplier`) gain the same
  descriptor builder; oneof fields no longer get dropped from their
  descriptors.

## Additional context and related issues

* Fixes trinodb/trino#20204 (schema evolution with nested messages).
* Related to trinodb/trino#23736 (Confluent licensed JARs for protobuf).
* Before this change the registry client received only the cache TTL
  property. The Confluent client builds its TLS context from the
  `schema.registry.ssl.*` properties whenever any property is set, and
  installs an accept-all host name verifier unless
  `ssl.endpoint.identification.algorithm` is among them, so HTTPS registries
  were reached without host name verification. The new security config
  always sets the algorithm (`https` by default).

Commits (each builds on its own):

1. Build protobuf descriptors from schema text
2. Decode missing protobuf row fields as NULL
3. Add Confluent protobuf wire format
4. Add registry backed protobuf schema provider
5. Drop Confluent protobuf provider from Kafka
6. Support INSERT into Confluent protobuf tables
7. Document protobuf support without extra JARs
8. Add schema registry auth and TLS configuration

The review commits on top of these (map entries, known imports, proto3
optional presence, enum and key-only INSERT fixes, docs, field number
mapping, `NULL` encoding, the subject's message, reserved numbers and names,
duplicate client properties, the reader schema pinned at table description
time) are to be folded into the commits above before the PR is opened; the
PR is to be opened as a single commit.

Tests:

* `lib/trino-record-decoder`: `TestProtoFileDescriptorBuilder` (29 cases:
  scalars, labels, enums, oneofs, maps, proto3 optional, well-known types,
  imports, public imports, transitive visibility, cycles, name resolution,
  map entry naming, groups, duplicates, package-less files, proto2 defaults
  including escaped bytes and special float values, reserved declarations
  and fields that reuse reserved numbers or names)
  and `TestProtobufDecoder` (including renamed fields read by number, a
  number reused with another encoding, name lookup without a reader
  schema, wire-compatible type changes checked against protobuf's own
  parse with the reader schema, incompatible message and map types, records
  of another message type, proto2 and oneof presence, and a decoder without
  columns); `verify -P ci` and the error-prone compile pass.
* `plugin/trino-kafka`: `TestConfluentProtobufWireFormat` (including a
  byte-for-byte comparison with Confluent's `MessageIndexes`),
  `TestConfluentProtobufSchema`, `TestConfluentSchemaRegistryDynamicMessageProvider`
  (including the reader schema reference and a rename evolution),
  `TestConfluentContentSchemaProvider`, `TestProtobufEncoder` (`NULL` left
  unset or rejected per field, proto2 `required`, `optional` and `oneof`
  fields read back as `NULL`),
  `TestConfluentProtobufRowEncoderFactory`, `TestConfluentProtobufSchemaRegistryRest`
  (in-process HTTP and HTTPS registries: decode with references, encode
  with the latest schema, basic auth from the security config, 401, 404,
  dangling reference, registry down, TLS with and without a trust store,
  host name verification and disabling it, a registry that requires a
  client certificate, and a client built from the connector configuration
  through all properties providers),
  `TestConfluentSchemaRegistrySecurityConfig` (defaults, mappings, client
  property names, validation), connector startup with the registry
  properties in `TestKafkaPlugin`, `TestConfluentModule` (client properties
  of all providers combined, a property set twice rejected), `TestKafkaMetadata`
  (the table handle, its columns and its data schema come from one description),
  `TestProtobufSchemaParser`, `TestNoConfluentProtobufDependency`, protobuf
  cases in `TestConfluentSchemaRegistryTableDescriptionSupplier`, and new
  Docker cases in `TestKafkaProtobufWithSchemaRegistryMinimalFunctionality`
  (INSERT round trip, INSERT without key subject, schema references, nested
  schema evolution, renamed fields read by number, a widened field and a
  renamed enum value, a non-first message with the `RecordNameStrategy`
  including INSERT, and a nested message with the `TopicRecordNameStrategy`).

## Release notes

( ) This is not user-visible or is docs only, and no release notes are required.
( ) Release notes are required. Please propose a release note for me.
(x) Release notes are required, with the following suggested text:

```markdown
## Kafka
* Support protobuf schemas in the Confluent table description supplier
  without copying Confluent licensed JAR files into the plugin directory,
  including schema references, schema evolution and `INSERT`. ({issue}`23736`)
* Add `kafka.confluent-schema-registry.basic-auth.*` and
  `kafka.confluent-schema-registry.ssl.*` configuration properties for
  authentication and TLS to the Confluent schema registry.
* {{breaking}} Verify the host name of an HTTPS Confluent schema registry
  against its certificate. Set
  `kafka.confluent-schema-registry.ssl.endpoint-identification-algorithm` to
  `disabled` to restore the previous behavior.
* Fix query failure when a protobuf record was written with a schema that
  lacks a field of a `ROW` column. ({issue}`20204`)
* Fix columns and `INSERT` of Confluent protobuf tables whose subject names a
  message other than the first one in the schema, as the `RecordNameStrategy`
  and `TopicRecordNameStrategy` do, including nested messages.
* Fix `NULL` results for protobuf records written before a field was renamed,
  and old names for records written before an enum value was renamed, in the
  Confluent table description supplier.
* Allow `INSERT` of `NULL` into protobuf fields that track presence, and fail
  with a clear error instead of an internal error for the others.
* {{breaking}} Return `NULL` for protobuf fields that track presence and are not
  set, such as message typed fields, proto2 `optional` fields and `oneof`
  members, instead of an empty message or the field's default value.
* {{breaking}} Map protobuf fields declared `optional` in proto3 schemas of the
  Confluent table description supplier to columns of the field's type, instead
  of `JSON` columns named after their synthetic oneof.
```
