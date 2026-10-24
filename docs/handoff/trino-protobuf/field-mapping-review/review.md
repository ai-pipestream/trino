# Review: protobuf field-number mapping and INSERT NULL

Opus 5.5 adversarial review of Fable's two commits on `feature/kafka-confluent-protobuf`:
f58d1195 "Map protobuf columns to writer fields by number" and 25191030 "Write NULL as an unset
protobuf field" (previous head c7f65309). Date: 2026-10-23.

## Verdict

Not accepted as delivered: wire-compatible type changes break queries, and NULL does not
round-trip for proto2 fields. **Accepted with the review fixes**, pending Fable's cross-review.
The design holds up (number mapping through a reader descriptor, `schema-registry-id:<id>` as
the data schema, presence-based NULL). No JSON fallback was found.

## Fixes (local, not pushed)

Branch `claude/project-thread-c01k0v` (fast-forward of 25191030):

- `e49622ae` Read protobuf fields as the reader schema reads them
- `e7b7257c` Update PR draft for field mapping review

Both are authored by Kristian with no AI trailer. Patch: `field-mapping-review-fixes.patch`.

| Issue | Finding | Status |
|---|---|---|
| #42 | Same-encoding type changes (int32↔bool, enum↔int, bytes↔string, uint32→int64, int64→int32, sint64→sint32) failed queries or misread values. Renamed enum values kept their old names. Map key/value types were not compared. Timestamp/Any mixed with other messages crashed. | Fixed: values are converted as protobuf reads them, maps are compared recursively, and Timestamp/Any must match by name |
| #43 | The decoder read NULL only for proto3 optional and message fields; the encoder used `hasPresence()`. So NULL into proto2 optional or oneof members read back as defaults, and NULL into proto2 `required` gave an internal error. | Fixed: the decoder uses `hasPresence()`, and `required` rejects NULL with CONSTRAINT_VIOLATION. **Behavior change for proto2, see below.** |
| #44 | Field numbers were applied to records of another message type. The reader schema was looked up even for decoders without columns. | Fixed |
| #44 | The columns come from the latest schema when the table is described, and the reader id comes from the latest schema at split time. | Not fixed (low), documented |

## Decision for Kristian

With the fix, an unset proto2 `optional` field, even one with `[default = X]`, and an unset
oneof member read as NULL instead of the default value. This also applies to file-based tables.
The PR draft lists it as `{{breaking}}`. The alternative is to keep proto2 defaults on read and
reject NULL on INSERT for proto2 optional scalars instead.

## Verification

- Baseline on 25191030: Fable's tests pass, 126/126, including the Docker
  `TestKafkaProtobufWithSchemaRegistryMinimalFunctionality` (14, with `testRenamedFieldsAreReadByNumber`
  and the changed NULL message). That is the Docker run Fable could not do.
- 7 new tests: 5 in `TestProtobufDecoder`, 1 in `TestProtobufEncoder`, and the Docker case
  `testWidenedFieldsAndRenamedEnumValues` (a real registry accepts int32→int64 plus an enum rename
  under BACKWARD). The 5 decoder tests fail on 25191030. The wire-compatibility test checks every
  value against `DynamicMessage.parseFrom(reader, bytes)`.
- After the fixes: all 138 trino-record-decoder tests, 129 Kafka protobuf/Confluent/plugin tests
  plus the Docker class (15/15), Error Prone with no warnings in the touched files, and
  `verify` with all checks for both modules.
- Product tests `TestKafkaProtobufWritesSmokeTest` (with the changed NULL message),
  `TestKafkaProtobufReadsSmokeTest` and `TestKafkaProtobufReadsSchemaRegistrySmokeTest`: 7/7 pass.
  Caveat: a full server image build was too heavy for the container, so the server is the
  `trinodb/trino:latest` (484) image with this branch's Kafka plugin swapped in. Its toolkit
  SPI-version resource was set to 484 so it would load. The plugin code under test is this branch's.

## Next

Fable cross-reviews e49622ae and e7b7257c. They are pushed only after Kristian's OK. No PR.
