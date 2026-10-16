# MarketPulse 360 — technical evidence report

Draft for group submission. Generated tables are local observations, not an SLA.
The course project requires a report PDF, slide PDF, 20–30 minute narrated video
and application demonstration. Rename final files with the real group ID.

## 1. Context, stakeholders and business requirements

An omnichannel retailer must decide where to spend marketing budget, explain
conversion losses and identify appropriate lifecycle audiences. Ad platforms,
orders, CRM, web/mobile events and email disagree in grain and arrival time.
MarketPulse 360 prepares governed analytical data and exposes a Marketing Command
Center so those decisions can be inspected against source evidence.

CMO: budget pacing and what-if allocation. Performance manager: live campaign KPI
and explained alerts. Product manager: funnel stage reach. Analyst: first-touch,
last-touch and linear attribution. CRM marketer: Customer 360, segmentation,
cohorts and opportunity/risk identification. These are ten distinct business
requirements, mapped to executable proof in decision-tasks.md. A group of n
requires at least 2*n; ten capabilities cover at most five members. The real group size has not been supplied; requirement coverage remains unconfirmed.

## 2. Sources, data characteristics and engineering solution

All source PII is synthetic. The seed-360 fixture contains 615 rows in 22
compressed files. Cross-source customer/campaign/session/click/order/message IDs
preserve referential integrity. The deterministic generator models funnels,
delays, duplicates, malformed/missing fields, bots, refunds and consent changes.
Generated source data is separate from application users, settings and segments.

Technology example 1: Kafka KRaft transports versioned raw, clean and Gold-style
events. Flink is the stateful processor, not another name for Kafka. A checkpoint
restores source offsets, deduplication and aggregate state, while transactional
Kafka sinks prevent duplicate visible effects.

Technology example 2: MinIO and Iceberg preserve immutable Bronze payloads,
typed/governed Silver and Gold snapshots. Pinned snapshot receipts make historical
backfills reproducible; publication requires verified rowsets and source finance.

Technology example 3: Dagster schedules daily files and orchestrates dependencies;
dbt transforms and tests analytical models before publication. Failed candidates
do not replace the active release. Application consumers pin one certified release.

Technology example 4: ClickHouse serves analytics. PostgreSQL stores sessions,
accounts, configuration, saved segment revisions and operational/audit metadata.
The PostgreSQL analytical database used here is an isolated comparison baseline.

Technology example 5: FastAPI and Next.js expose campaign, attribution and
lifecycle decisions with server-side authorization, exact money strings and
lineage/freshness. Prometheus/Grafana show cached operational observations and
quality alerts; missing telemetry remains unavailable rather than becoming zero.

## 3. Correctness, governance and provenance

MP-01–09 retain deterministic fixture tests, schema/ID checks, duplicate and late
event tests, real checkpoint recovery, snapshot/backfill reconciliation, independent
query oracles, financial conservation, consent-aware profile/segment rules and RBAC.
The governed published fixture has gross 27,160,000, refunds 4,967,000 and net
22,193,000 VND minor units. The earlier ungoverned streaming population has net
24,751,000; these populations differ and must not be compared as equal cohorts.

MP-10 adds a separate deterministic finance workload and an independent Python
oracle for every committed output. It checks sequence completeness, classifications,
intermediate campaign money and cross-engine canonical input/output hashes.
Duplicates do not change totals. Invalid values do not mutate finance. Eligible
late records are included; beyond-horizon records are audited and excluded.
Analytical answers use a periodic independent integer oracle, not agreement
between two possibly incorrect database queries.

Evidence JSON records revision, source-file hashes, engine versions, named scales,
sample counts, timing scope and resource coverage. The package index hashes all
copied observations and PDFs. Production certification remains the authority for
application data; benchmark facts never publish into the demo.

## 4. Performance, recovery and controlled alternatives

Flink 2.2.1 and Kafka Streams 4.3.1 process identical 10k/100k finance workloads on
Kafka 4.3.1, Java 17, one partition/lane and one-second commit/checkpoint cadence.
Both use native persistent state and exactly-once output; the observer reads
committed records. This bounded reducer comparison is not complete MP-03 parity.
The adapters isolate Kafka client4.2.0 for Flink's transaction ABI and4.3.1 for
Streams; this necessary version difference is included in the comparison scope.
It omits production session/funnel outputs, full schema handling, state TTL and
Flink partition watermark behavior. A separate warmup run is excluded from tables;
each measured run creates a fresh job/application and includes its JIT warmup.

Throughput runs from host producer start to last expected committed observation;
latency is host send to committed observation, including queueing and transaction
visibility. Historical source-event age is excluded. Nearest-rank p50/p95/p99
are reported. This is not production event-to-dashboard latency. Authenticated
dashboard HTTP response timing is separately measured over 20 warmups/100 samples.

Recovery observations: flink: 4.476s to new committed output; checkpoint 4 restored; streams: 45.277s to new committed output; application ID/state retained

Docker: 15 CPUs; 15.60 GiB visible memory; aarch64; engine 29.4.0.

Resource JSON reports CPU counter deltas, sampled working set, sample interval,
coverage and failed/reset samples. Both Flink JVM services and Kafka are measured;
Streams is one JVM. Abrupt restart may lose CPU between samples and is flagged.

ClickHouse 26.8 and PostgreSQL 17.6 compare identical 1M/10M/20M Gold-shaped
synthetic facts with exact integer money, not millions of distinct customers.
Three ordered SQL workloads cover platform totals, campaign/day totals and an
indexed campaign/date filter. Both containers are capped at 1.5 GiB. PostgreSQL
uses a composite campaign/day index; ClickHouse sorts by campaign/day/row ID.
Both use warm OS/buffer caches; ClickHouse result caching is disabled. Each
workload runs 20 warmups and 100 serial measured queries with zero answer failures.



Engine / scale | inputs/s | p50 ms | p95 ms | p99 ms
flink / eval-medium | 79812.6 | 1085.17 | 1094.34 | 1096.43
flink / eval-small | 7887.2 | 1133.39 | 1240.90 | 1242.23
streams / eval-medium | 52039.8 | 1635.03 | 1648.05 | 1651.72
streams / eval-small | 11394.2 | 852.90 | 858.45 | 858.76

Engine / scale / query | p50 ms | p95 ms | p99 ms
clickhouse / analytics-10m / campaign_day | 71.06 | 82.23 | 86.73
clickhouse / analytics-10m / filtered | 3.60 | 4.47 | 5.22
clickhouse / analytics-10m / platform | 47.88 | 53.59 | 54.39
clickhouse / analytics-1m / campaign_day | 10.98 | 16.12 | 18.08
clickhouse / analytics-1m / filtered | 1.93 | 2.86 | 3.11
clickhouse / analytics-1m / platform | 7.13 | 11.04 | 12.22
clickhouse / analytics-20m / campaign_day | 148.52 | 196.89 | 221.05
clickhouse / analytics-20m / filtered | 4.28 | 5.02 | 5.29
clickhouse / analytics-20m / platform | 98.07 | 109.05 | 122.08
postgres / analytics-10m / campaign_day | 389.00 | 518.30 | 583.10
postgres / analytics-10m / filtered | 88.86 | 109.82 | 158.55
postgres / analytics-10m / platform | 333.15 | 422.23 | 617.17
postgres / analytics-1m / campaign_day | 37.06 | 43.22 | 45.25
postgres / analytics-1m / filtered | 2.53 | 3.28 | 3.87
postgres / analytics-1m / platform | 32.03 | 38.43 | 49.69
postgres / analytics-20m / campaign_day | 849.06 | 977.09 | 1298.05
postgres / analytics-20m / filtered | 378.42 | 518.54 | 651.49
postgres / analytics-20m / platform | 740.74 | 826.55 | 1074.45

Engine / scale | Process | CPU s | Peak MiB | Samples | Cover s
clickhouse/analytics-10m | clickhouse | 15.37 | 858.2 | 16 | 15.3
clickhouse/analytics-1m | clickhouse | 2.62 | 1183.6 | 4 | 2.7
clickhouse/analytics-20m | clickhouse | 32.75 | 1028.7 | 31 | 31.6
postgres/analytics-10m | postgres | 196.61 | 337.5 | 99 | 103.9
postgres/analytics-1m | postgres | 16.94 | 250.3 | 10 | 9.1
postgres/analytics-20m | postgres | 485.16 | 534.2 | 235 | 249.3
flink/eval-small/recovery | jobmanager | 2.29 | 617.7 | 31 | 35.2
flink/eval-small/recovery | kafka | 12.18 | 553.2 | 31 | 35.1
flink/eval-small/recovery | taskmanager | 11.91* | 569.7 | 31 | 35.2
streams/eval-small/recovery | kafka | 11.24 | 575.6 | 43 | 47.3
streams/eval-small/recovery | streams | 3.16* | 173.1 | 43 | 47.3
flink/eval-medium | jobmanager | 0.13 | 579.9 | 4 | 2.6
flink/eval-medium | kafka | 0.45 | 471.4 | 4 | 2.6
flink/eval-medium | taskmanager | 3.10 | 542.7 | 4 | 2.6
flink/eval-small | jobmanager | 0.13 | 519.8 | 4 | 2.6
flink/eval-small | kafka | 0.29 | 474.9 | 4 | 2.6
flink/eval-small | taskmanager | 1.04 | 512.0 | 4 | 2.6
streams/eval-medium | kafka | 2.20 | 572.5 | 4 | 3.0
streams/eval-medium | streams | 3.81 | 278.7 | 4 | 3.1
streams/eval-small | kafka | 0.13 | 467.5 | 3 | 2.0
streams/eval-small | streams | 1.15 | 176.5 | 3 | 2.0

CPU/working set cover query warmups/oracles/measurements and stream observation,
not database loading or process startup. The asterisk flags a CPU counter reset
that can undercount restart consumption. Peaks are sampled; see JSON for coverage.

These are single-run local measurements on a shared Docker machine, with the
retained application also running. Row distribution, state backend, fixed commit
cadence, warm caches, indexes, parallelism, container budgets and queueing explain
what is measured. Do not extrapolate a universal winner or production capacity.
Flink remains the primary engine because the implemented platform needs its
stateful event-time pipeline and recovery integration. ClickHouse remains serving;
the PostgreSQL baseline provides a useful controlled comparison. BigQuery is
optional and was not measured; no cloud credit or benchmark result is assumed.

## 5. Application effectiveness and demonstration

The retained app is authenticated as an operator. Ten automated decision-readiness
checks inspect campaign KPI, alert evidence, pacing, simulation, stage reach,
attribution, profile timelines, consent-eligible segment preview/saved revisions,
cohort population and high-value opportunity. Configuration is compared before
and after; no budget, rule, segment or publication is changed. Normal auth/audit
records are expected. Each task records role, result, elapsed API time and hash.

Dashboard HTTP observations: p50: 37.05 ms; p95: 54.35 ms; p99: 56.12 ms; 100 samples after 20 warmups.

API elapsed time is not human task completion time. Existing browser regression
tests exercise UI behavior, but neither those nor scripted API checks establish
representative stakeholder usability. Use decision-tasks.md to collect real
participant observations; report failed tasks and correction attempts as well as
successful decisions. Explain model scope, historical freshness and activation
consent during the live demo.

## 6. Reproduction, conclusions and submission readiness

Use docs/runbooks/evaluation.md for the isolated Docker stack and scripts/evaluate.py
commands. Existing startup/replay/backfill/recovery runbooks remain authoritative
for the primary platform. No MP-01–09 source regeneration is needed for this
evaluation. Benchmark tables are owned by each run and dropped on exit; Kafka
topics/checkpoints remain in the evaluation project for inspection. Stop that
project without deleting volumes when preserving evidence.

The delivered kit contains this six-section report, slide PDF, raw measurement
JSON, content hashes, ten-task traceability and a timed video/demo storyboard.
Review the actual group size and selected requirements before final submission.
Record a narrated 20–30 minute video showing the application and evidence;
a storyboard is not a finished recording. Representative human trials, real
group filenames and the final video are explicitly pending.

Acceptance state: {"business_requirements": 10, "final_submission_ready": false, "group_coverage": "unconfirmed", "group_size": null, "human_study": "pending", "narrated_video": "pending"}

The solution demonstrates reproducible engineering and usable decision paths;
production sizing, distributed HA, formal SLA and human effectiveness require
further evidence. The rubric checklist exposes outstanding items instead of
asserting a grade or a percentage of missing content without observations.
