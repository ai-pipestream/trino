# Commit identity rewrite (2026-10-31)

Sixteen fork commits named Claude as git author or committer. They were
rewritten with Kristian Rickert as author and committer. Trees, commit
messages and dates are unchanged, and each new head has the same tree
hash as the old one. No commit message on any fork branch carried an AI
trailer.

The reports in this folder were written before the rewrite and still
cite the old hashes. Map them as follows:

| Branch | Old head | New head |
| --- | --- | --- |
| feature/kafka-confluent-protobuf | 1722171d | 4ac01ca7 |
| claude/project-thread-pnppzx | 1722171d | 4ac01ca7 |
| claude/project-thread-c01k0v | e7b7257c | d53b8467 |
| claude/project-thread-c1g05b | 73c7f1b3 | d1b16da6 |
| experiment/protobuf-control-plane-review | 8b0f28cf | 4e0c99bd |

Commits below these heads on the same branches changed hash as well;
match them by subject. The upstream candidate kafka-confluent-protobuf
(61c9f678) and the other branches were not touched.
