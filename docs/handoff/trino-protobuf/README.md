# Protobuf work: handoff notes

Fork-only working notes for the protobuf work on `ai-pipestream/trino`. Not
for upstream. The current status, branch heads and next steps are in
ai-pipestream/trino#51; this folder holds the detailed reports behind it.

| Path | What it is |
| --- | --- |
| `cloud-build/` | Cloud container build setup: JDK 25, Maven 3.9.16, Confluent 8.1.1 jars from Docker images, Maven wrapper and the Kafka verification run. The scripts hard-code a scratchpad path (`/tmp/claude-0/...`); change it before use. |
| `kafka-confluent-protobuf-review.md`, `kafka-confluent-protobuf-cross-review.md` | First Opus review and Fable cross-review of the Kafka Confluent protobuf branch (#6, #22-#27) |
| `registry-security-review/` | Review of schema registry auth and TLS (#27, #39-#41), with fix patch |
| `field-mapping-review/` | Review of field-number mapping and INSERT NULL (#26, #42-#44), with fix patch |
| `issue-audit/` | Audit of which fork issues the Kafka branch covers |
| `audit-fixes-review/`, `audit-fixes-cross-review/` | Reviews of the audit fixes (#47-#50), with fix patches |
| `feature-kafka-confluent-protobuf-pr.md` | Earlier PR draft for the Kafka branch |
| `upstream-pr/` | Final upstream PR body and the squashed commit `61c9f678` as a patch |
| `protobuf-spooling-encoding-report.md`, `protobuf-spooling-encoding-review.md` | Spooling encoding report and review rounds (#7, #29-#33) |
| `spooling-cross-review/fable-cross-review-fixes.patch` | The 4 pending client decode commits (`git am` onto `9e22a234`) |
| `control-plane-review.md` | Review of control plane phases 1-3 (#10, #15-#21, #28) |
| `control-plane-phase3-results.md`, `phase3-review/` | Phase 3 results and its review (#35-#38), with fix patch |
| `attribution-rewrite.md` | Old to new head hashes after the 2026-10-31 commit identity rewrite |

Patches apply with `git am` onto the base named in each review file.
Hashes cited before 2026-10-31 may predate the identity rewrite; see
`attribution-rewrite.md`.
