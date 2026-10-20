# MP-10 — Evaluation, alternatives and evidence

Status: implemented and technically verified on `feat/mp10-evaluation-evidence`,
based on `main` after PR #9 merged (`cfe63fa`). Implementation and review fixes
are tracked in [PR #11](https://github.com/huyle1901/marketpulse-360/pull/11).
Technical acceptance was verified on 2026-10-16; tests, retained-app decisions,
fixture validation and evidence integrity were rechecked on 2026-10-20.
Human observations, group information and the narrated submission video remain
pending. Scope delegated to the backlog by the user.

## Purpose and boundaries

Provide repeatable, measured comparisons and a reviewable final submission kit.
Retain the FastAPI/Next.js, Flink, Iceberg, ClickHouse and operational PostgreSQL
architecture. Do not regenerate or republish MP-01–09 data. Use one source tree.

The isolated `evaluation` Compose profile in `compose.evaluation.yaml` owns its
Kafka, Flink, Kafka Streams, ClickHouse and PostgreSQL containers and volumes.
It has no production mounts, credentials, topics, publications or app database.
Only loopback ports are exposed. Credentials are generated into ignored files;
neither passwords nor sessions appear in evidence. Generated output is under
`data/generated/mp10/` or `.lakehouse/mp10-proof/`, never committed in bulk.

## Controlled processing comparison

Compare Flink 2.2.1 / Kafka connector 5.0.0-2.2 against Kafka Streams 4.3.1,
Java 17, on Kafka 4.3.1. Both use one partition, one processing lane, the same
deterministic input and exactly-once Kafka output. Both produce one versioned
audit/result record per input, containing sequence, classification, campaign,
gross/refund/net totals and send timestamp. Read outputs with `read_committed`.

Separate shaded artifacts use kafka-clients4.2.0 for the Flink connector's
internal transaction ABI and4.3.1 for Streams. Do not mix these classpaths;
report the client versions as part of each measured adapter.

This is a bounded campaign-finance workload, not a reimplementation or proof of
equivalence of the entire MP-03 session/funnel/consent pipeline. Document this
limit beside every comparison. The benchmark contract is intentionally separate
from production contracts. The shared deterministic reducer isolates engine costs;
an independent Python oracle checks every output, not just final sums.

Input JSON has `seq`, `event_id`, `campaign`, `event_ms`, `kind` (`order` or
`refund`), nonnegative integer `amount_minor` and `sent_ms`. Partition key is the
campaign (0–63). Invalid fields classify `invalid` without state mutation.
Valid event IDs deduplicate within their campaign for the bounded run; a duplicate
never changes money. Per-campaign maximum event time minus 300,000 ms is an
explicit deterministic watermark. Earlier events classify `late`; events earlier
than watermark minus 600,000 ms classify `too_late` and do not change money.
Late eligible events do change money. This deterministic benchmark watermark is
not presented as Flink's partition watermark implementation.

Named scales `eval-small` (10,000 inputs), `eval-medium` (100,000), optional
`eval-large` (1,000,000), seed 360. Inject duplicates, invalid amounts, eligible
late and too-late records with exact known positions. Save a canonical input hash
excluding wall-clock send times and output hash excluding timing, count by class,
exact money, software versions, run ID, Git revision and machine context.

Measure producer-start to final committed-output throughput and send-to-committed
consumer-observation p50/p95/p99. This includes Kafka transactions, polling and
backlog, excludes historical business-event age, and does not claim dashboard
latency. Separately time authenticated dashboard HTTP reads. Use nearest-rank
percentiles. An initial warmup is a separate run, never mixed into samples.
Record producer acknowledgement duration and sample count. No guessed numbers.

Recovery runs restart the Flink TaskManager or Streams container after committed
progress while the producer remains active. For Flink require a completed
checkpoint and restored checkpoint evidence; for Streams retain application ID
and state/changelog. Measure restart request to first newly committed output,
check input advances during interruption and verify all final records and totals.
Sample Docker CPU counters and memory during each run; report CPU seconds and
peak sampled working set, sample count/interval and coverage for engine and Kafka.
Flink resources include JobManager plus TaskManager. Never call one sample a peak.

## Controlled analytical comparison

Use ClickHouse and an isolated PostgreSQL 17.6 baseline with identical Gold-shaped
synthetic facts: integer row ID, campaign (0–63), day (0–29), platform (0–2),
VND net revenue `((row_id % 97)+1)*100`. Scales: `analytics-1m`, `analytics-10m`,
`analytics-20m`; `analytics-smoke` (10,000) is development only. These are scale
fixtures, not the MP-01 generator's demo/benchmark profile or actual customers.

Three identical ordered workloads: platform totals, campaign/day totals, and a
filtered campaign/date slice. Check all answers against an independent periodic
Python oracle before and during measurements. PostgreSQL gets a campaign/day
index; ClickHouse gets MergeTree order by campaign/day/row ID. Record DDL, load
time, engine versions and row counts. Disable ClickHouse query-result cache;
both engines use warm OS/buffer caches, 20 warmups and 100 serial measured queries
per workload, with p50/p95/p99, failures and client decode time. Run scales and
engines serially. No unsupported universal winner or capacity/SLA claim.

## Effectiveness and final kit

Map all ten business requirements from MP-06–08 to stakeholder, decision task,
expected result, application route and executable proof. Run read-only decision
checks against the retained authenticated app, preserving configuration/segments.
Automated role checks are explicitly not a human usability study. Capture role,
elapsed time, outcome, dataset/release identity and evidence link. Supply a human
task sheet with blank participant ID, role, task result and duration; require
real observations before claiming representative human completion. Group member
count is supplied by the user; until known, `2*n` coverage remains unconfirmed.

Produce a six-section Markdown technical report and PDF, a slide PDF, a 20–30
minute video/demo storyboard and executable recording instructions, an evidence
index with hashes, and a requirements/rubric checklist. PDFs contain at least
four concrete technology examples and clearly distinguish measured results,
prior verified evidence and pending human/video submission steps. No fabricated
human results, group ID, recording, cloud benchmark, throughput or hardware data.
Submission filenames require the real group ID before final hand-in. The final
kit may be technically ready while human testing and narrated video remain
explicit external acceptance items; do not label these passed.

## Verification

TDD on oracle/statistics/evidence gates, engine adapters and record completeness.
Run Python, existing Java and browser suites, fixture validation, builds, Compose
validation and Docker benchmark/recovery/query comparisons. A failed comparison,
missing output, missing recovery proof or wrong money prevents a passed run.
Update architecture and backlog with actual measurements and honest limitations.
