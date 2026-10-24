# Review: schema registry auth/TLS and descriptor builder edge-case tests

Scope: ai-pipestream/trino feature/kafka-confluent-protobuf a83e7791..a8fc94a2 (Fable commits 4d0418c4, 29fbcada, a8fc94a2). Reviewer: Opus 5.5, 2026-10-23.

## Verdict
Accepted with 6 review commits on local branch `claude/project-thread-9yrm7f` (fast-forward of a8fc94a2, NOT pushed). Patch: registry-security-review-fixes.patch in this folder.

| Commit | What |
|---|---|
| fc3bdb5a | Use a valid reserved range in descriptor builder test (#41) |
| 8a9168e2 | Fix escaped bytes defaults in protobuf schemas (#39) |
| 2bd6d982 | Reject schema registry store passwords without a store (#40) |
| 44869dba | Test schema registry client certificates (mTLS, client built through ConfluentModule) (#40) |
| fa0bb891 | Document schema registry TLS scope |
| 8b182311 | Update PR draft: tests list, host name verification as {{breaking}} release note |

## Findings
- #39 (bug, fixed): proto2 bytes defaults were unescaped twice (Wire unescapes, descriptor wants C-escaped): `\377` -> C3 BF, `a\\b` -> backspace. Absent proto2 bytes fields decoded to wrong value.
- #40 (fixed): keystore/truststore password without location crashed connector creation with an opaque client error; key password without keystore silently ignored. No connector-level, mTLS, or ConfluentModule-wiring tests. Open note: provider merge via toImmutableMap fails on duplicate keys from embedder providers.
- #41 (test fixed): new test declared field 536870911 inside `reserved 1000 to max` (protoc rejects). Builder still does not validate reserved ranges (low priority).
- Verified correct: Fable's claim that the Confluent 8.1.1 client used an accept-all host name verifier before this change (SslFactory is always built once any property is set; verifier is accept-all unless the algorithm is set). Property names match the client. Test certificates are valid until 2126.

## Verification (head 8b182311)
- `verify -DskipTests` (checkstyle, modernizer, dependency analysis, enforcer on Temurin 25) passes for trino-record-decoder and trino-kafka.
- Error Prone test-compile passes; no new warnings in touched files (fixed two AddressSelection warnings in the REST test).
- trino-record-decoder: 129 tests pass.
- trino-kafka: 164 tests across 20 Confluent/protobuf/config/plugin classes pass, including Docker tests TestKafkaProtobufWithSchemaRegistryMinimalFunctionality (13) and TestKafkaWithConfluentSchemaRegistryMinimalFunctionality (9) against cp-kafka/cp-schema-registry 8.1.1.
