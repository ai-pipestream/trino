# Fork issue audit (ai-pipestream/trino), 2026-10-23

44 issues: 43 open, 1 closed (#38). Checked against the fork branches on 2026-10-23 20:50 UTC.
Every issue whose fix landed is still open, and many still say "local, not pushed". The commit ids
in those issues were rewritten when the work was pushed (for example 8a9168e2 became d90a6dbe).

## Kafka Confluent protobuf PR (feature/kafka-confluent-protobuf, head e7b7257c)

33 commits on master e76d788b, all authored by Kristian, no AI trailers.

| Issue | What | Status | Commit on the branch |
|---|---|---|---|
| #6 | Confluent protobuf without CCL jars | Done (the feature) | cbdc7e7e..e798d395 |
| #22 | Explicit map entries and message indexes | Fixed | 456a1d92, 23e78cc9 |
| #23 | Known type imports (google/type, confluent) | Fixed | c2b244d9, 53b41833 (kept), 40e1a55d |
| #24 | Unset proto3 optional reads as default | Fixed | 60680ff1, a83e7791 |
| #25 | INSERT enums, key-only tables | Fixed | b51d9d13, 427bafa3, b50c4454 |
| #26 | Map columns by field number | Fixed | f58d1195, 25191030 |
| #27 | Registry basic auth and TLS | Fixed | 4d0418c4, 29fbcada |
| #39 | Escaped bytes defaults | Fixed, one known limit | d90a6dbe |
| #40 | Registry security validation and tests | Fixed, one item left for upstream review | f6b0c2b9, b88a52e2, 1fe021c6, c7f65309 |
| #41 | Invalid reserved range in a test | Test fixed, builder check not done | 69d1d459 |
| #42 | Wire-compatible type changes | Fixed | e49622ae |
| #43 | Presence NULL for proto2 optional, oneof, required | Fixed (proto2 NULL kept) | e49622ae |
| #44 | Other message types, eager reader lookup | Fixed, one race left | e49622ae |
| #5 | Message index ignored | Fix exists but NOT on the rebase target (see below) | 12d93bdf, fe40a248, 85e1f926 on claude/project-thread-c1g05b |

So the PR covers 13 fork issues (#6 plus 12 review findings), 14 once #5 is folded in, and upstream
trinodb/trino#23736 and #20204.

### Planned work that would be skipped today

1. **#5 is not done.** `fix/kafka-protobuf-message-index` is still at master e76d788b, so
   "rebase onto the fix branch" is a no-op. The fix (3 commits) sits on the scratch branch
   `claude/project-thread-c1g05b`. It was never run (the thread has waited since 11:16 for a
   Remote Control session) and never reviewed, and its commits are authored by Claude, not Kristian.
2. **Subject message for columns is missing from the PR.** Commit 85e1f926 maps columns from the
   message the subject names (RecordNameStrategy / TopicRecordNameStrategy). The Kafka branch still
   takes the first message (`ConfluentProtobufSchema.getMessageDescriptor()` returns
   `getMessageTypes().getFirst()`), for both columns and the #26 reader schema. Message-index
   decoding of records is already on the Kafka branch (be90488b). Five files overlap, so the rebase
   will conflict and 85e1f926 must be ported by hand.
3. **Review findings still unfixed** (Kristian's rule says they go into the PR):
   - #41: the descriptor builder accepts fields that use reserved numbers or names. Small check
     in `toDescriptorProto` that fails with `INVALID_PROTO_FILE`.
   - #40: an embedder's own `SchemaRegistryClientPropertiesProvider` that sets the same keys
     now fails startup with a duplicate-key error. Needs a clear error or documented precedence.
   - #44: columns and the reader schema id are both taken from "latest" at different times. A
     schema registered in between makes renamed columns fall back to name lookup. Closing it means
     carrying the schema id in the table handle (larger).
   - #39: a literal (unescaped) Latin-1 character in a bytes default becomes one byte, where protoc
     writes UTF-8. Wire does not keep escape information; document it, not fixable cheaply.
4. Housekeeping: #6's "Done when" boxes are unticked; #22-#27 and #39-#44 are open with stale text.
   The draft PR description does not mention #5.

## Other workstreams (not in this PR)

**Spooling encoding (#7, feature/protobuf-spooling-encoding at 1dd8dd39, done, no PR)**
- #29 concurrent encoding: fixed f0bb35eb. #30 tz offset: fixed 74629592. #31 legacy interval
  keys: fixed c2ba08f7 (range-end case documented, not fixed). #32 decoder checks: fixed 50716fc1.
  #33 differences from json: documented and pinned by tests. Fable added bfe24b28, 1dd8dd39.
- #34 columnar layout: deferred backlog by Kristian.

**Protobuf control plane (fork-only experiment, #8 brief, #9 impl, #10 review)**
- Fixed: #15 engine wiring and hive codecs (phase 3, claude/project-thread-tsljpr), #17
  /v1/task 406/415 (phase 3 commit 7), #19 hardening (25e2c181, folded), #28 task states
  (61637a5a, 8b0f28cf), #35 protoc config and #36 domain decoding (phase 3 review fixes, tsljpr
  8b972fe4). #38 closed.
- Open/planned: #16 commit restructuring (needed only if this goes upstream), #18 benchmark
  harness and raw data, #20 design follow-ups (partly addressed by phase 3), #21 phases 4-6,
  #37 phase 5 decode after catalogs load. Next step is phase 4/5.

**Write-ups and backlog**
- #1 epic (its table is stale), #2-#4 write-ups: reference, keep open.
- #11 type mapper / well-known types, #12 table functions, #13 gRPC connector, #14 event listener
  protobuf: deferred backlog, no branch.

## Update 2026-10-24: the gaps above are closed on the branch

Kristian asked for both fixes to go onto the branch (no PR yet). feature/kafka-confluent-protobuf
is now at **88fcba0b** (pushed 2026-10-24 00:20 UTC, 38 commits on master e76d788b, author Kristian,
no AI trailers), five commits on top of e7b7257c:

| Commit | Closes | What |
|---|---|---|
| c3e64b28 Map Confluent protobuf tables by subject message | #5 | Port of 85e1f926: columns, INSERT framing and the reader schema come from the message the subject names (RecordNameStrategy, TopicRecordNameStrategy, nested messages). Reader reference is now `schema-registry-id:<id>;message=<full name>`. Docs paragraph, two Docker tests, `protobuf/multiple_messages.proto`. |
| 91a58fd9 Reject reserved protobuf numbers and names | #41 | `ProtoFileDescriptorBuilder` rejects fields and enum values that reuse a reserved number, range or name (`INVALID_PROTO_FILE`). |
| a341708f Reject duplicate schema registry client properties | #40 | A property set by two `SchemaRegistryClientPropertiesProvider`s fails with `CONFIGURATION_INVALID` naming both providers, instead of an `ImmutableMap` duplicate-key error. |
| dfa91b0d Pin protobuf reader schema when describing table | #44 | `SchemaParser` receives the schema id; the table handle carries the reader reference and the page sink reads it through `ContentSchemaProvider`, so a schema registered between DESCRIBE and read no longer changes what a query reads or writes. |
| 88fcba0b Update PR draft for the issue audit fixes | — | docs/briefs/feature-kafka-confluent-protobuf-pr.md |

The PR therefore covers **14 fork issues**: #5, #6, #22-#27, #39-#44. #39 stays a documented limit
(Wire does not keep escape information), with no code change. The message-index branches
(`fix/kafka-protobuf-message-index` at master, scratch `claude/project-thread-c1g05b`) are superseded.

Verification in the cloud container: `install -P ci` (airstyle, sortpom, dependency analysis) and
Error Prone on trino-record-decoder and trino-kafka, trino-record-decoder 139 tests, and the Kafka
protobuf/Confluent suites including the Docker tests `TestKafkaProtobufWithSchemaRegistryMinimalFunctionality`
(17) and `TestKafkaWithConfluentSchemaRegistryMinimalFunctionality` (9), all green. The Docker run caught
one test bug (the new INSERT left the map and enum fields NULL, which the branch rejects by design); fixed
before the push.

Next step per the review workflow: an Opus 5.5 review of these five commits. Housekeeping still
open: the fork issues #5, #22-#27 and #39-#44 are open on GitHub with stale text.
