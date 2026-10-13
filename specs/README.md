# MarketPulse 360 - Product and Engineering Backlog

This backlog translates the course brief into ten demonstrable stories. A story is
complete only when its data path, correctness evidence, performance evidence, and
application value can all be shown. Detailed specifications are added under this
directory immediately before a story is implemented.

## Definition of project success

MarketPulse 360 succeeds when a reviewer can start the system locally, replay a
realistic omnichannel marketing dataset, observe data move through the engineering
pipeline, use the application to answer marketing questions, and inspect evidence
that the answers are correct and timely.

The course requires at least `2 * n` business requirements for a group of `n`
members. Stories 6-8 therefore expose ten candidate application capabilities so
the group can select enough after its final member count is known.

## Stakeholders and decisions

| Stakeholder | Decision supported | Required data |
|---|---|---|
| CMO / Marketing Director | Allocate channel budget and assess growth | Spend, revenue, ROAS, attribution, pacing |
| Performance Marketing Manager | Pause, scale, or repair campaigns | Near-real-time impressions, clicks, cost, conversions, anomalies |
| CRM / Lifecycle Marketer | Select audiences and journeys | Consent, identity, email engagement, segments, lifetime value |
| Marketing / Data Analyst | Explain performance and validate KPIs | Governed event-level and aggregate tables with lineage |
| E-commerce / Product Manager | Improve landing and checkout funnels | Sessions, product events, carts, orders, refunds |
| Data Engineer / Platform Operator | Keep pipelines reliable | Lag, throughput, freshness, DLQ, reconciliation, resource use |
| Data Owner / Privacy Officer | Enforce acceptable data use | Consent state, synthetic PII policy, retention, access history |
| Lecturer / Project Evaluator | Verify the engineering solution works | Reproducible demo, correctness tests, benchmarks, application proof |

## Ordered stories

### MP-01 - Reproducible omnichannel mock data

**Status:** Implemented

Generate coherent, deterministic data that resembles Google Ads, Meta Ads, TikTok
Ads, web/mobile analytics, CRM, commerce, and email systems.

**Acceptance criteria**

- A seed and profile reproduce identical files and checksums.
- Cross-source IDs support campaign attribution and Customer 360 joins.
- Small committed fixtures and large ignored datasets use the same generator.
- A manifest records configuration, row counts, files, hashes, time bounds, and
  injected anomaly counts.
- Validation proves schema, referential integrity, funnel invariants, and checksums.
- Files preserve event time and can later be replayed with a speed parameter.

**Proof:** `python -m unittest discover -s tests -v`, fixture manifest, and a local
demo-profile generation report. Seed `360` currently proves 69 passing tests, a
committed 615-row/22-file fixture, and a validated 1,200,225-row/130-file demo run.

### MP-02 - Kafka ingestion and replay controls

**Status:** Implemented

Replay event sources into versioned Kafka topics while batch sources arrive as
scheduled files. Support `--speed`, `--events-per-second`, pause/resume, idempotent
producer mode, and a no-sleep benchmark mode.

**Acceptance criteria**

- Docker Compose starts Kafka in KRaft mode, schema registry, and Kafka UI.
- Topic names and keys match the architecture document.
- Replay maintains event-time order per partition and exposes observed throughput.
- Invalid records are routed to a validation DLQ with a reason code.
- Replaying the same deterministic input does not change reconciled totals.

**Proof:** 118 automated tests pass. The pinned Docker stack runs Kafka `4.3.1`
in KRaft mode, Apicurio Registry `3.3.3`, and Kafbat UI `1.5.0`. A clean smoke
run acknowledged and consumed exactly 199 web, 14 order, 6 email, and 4 DLQ
records. Its no-sleep replay delivered all 223 records in 0.595 seconds
(374.5 records/second on the verification machine); this is a reproducibility
measurement, not a production capacity claim. The smoke verifier also compares
the exact consumed event IDs and DLQ reason codes with the fixture.

### MP-03 - Stateful stream processing with Flink

**Status:** Implemented

Validate, deduplicate, watermark, sessionize, enrich, and aggregate streaming events
with Flink using event time.

**Acceptance criteria**

- Duplicate IDs never inflate KPIs.
- Configured late events are handled consistently and measured.
- Session, funnel, campaign, and revenue outputs have versioned schemas.
- A TaskManager restart demonstrates checkpoint recovery without lost totals.

**Selected implementation:** Java 17 DataStream job on Flink `2.2.1`, Kafka
connector `5.0.0-2.2`, transactional Kafka output, five-minute watermarks,
seven-day deduplication state, 30-minute sessions, and versioned clean/Gold-style
Kafka topics. Durable Iceberg sinks remain MP-04 scope.

**Proof:** 135 Python tests and 30 Java tests pass. The isolated Docker recovery
test consumed all six outputs with `read_committed` isolation after restarting the
TaskManager: 217 unique fixture events, two rejected duplicate copies, 73 session
keys, one malformed processing-DLQ record, and one watermark-late audit event.
The producer stayed active during the restart: raw Kafka input advanced from 83 to
219 records while checkpoint `18` restored into checkpoint `19` in 16 seconds on
the verification machine. Gross `29,718,000`, refunds `4,967,000`, and net
`24,751,000` minor units match the fixture and per-campaign oracle. Six output
schemas plus Flink accepted/duplicate/late/invalid counters are verified. This is
a correctness/recovery proof, not a capacity benchmark.

### MP-04 - Bronze/Silver/Gold lakehouse

**Status:** Implemented; depends on MP-01, MP-02, and MP-03

Persist immutable raw data and governed tables with MinIO and Apache Iceberg.

**Acceptance criteria**

- Bronze preserves source payloads and ingestion metadata.
- Silver applies typed schemas, identity rules, consent filters, and deduplication.
- Gold exposes campaign, funnel, attribution, customer, and cohort models.
- Time travel and a backfill from Bronze reproduce a documented Gold snapshot.

### MP-05 - Batch orchestration and analytical serving

**Status:** Complete; local acceptance and final review fixes verified,
GitGuardian Security Checks passed on PR #5. Depends on MP-04. Daily source selection and immutable
receipt contracts, Docker runtime and operational ledger implemented;
snapshot export and isolated ClickHouse copy verified;
dbt models/tests and certification verified;
queued Dagster refresh, publication and isolated backfill verified;
six independent query oracles, fixture benchmarks and restart/recovery verified.

Detailed scope: [`05-batch-orchestration-and-serving.md`](05-batch-orchestration-and-serving.md).

Use Dagster and dbt Core for scheduled ingestion, transformations, tests, backfills,
and publishing selected Gold models to ClickHouse.

**Acceptance criteria**

- Ad reports, CRM snapshots, and historical files have observable Dagster assets.
- dbt relationships, uniqueness, accepted-value, and reconciliation tests pass.
- ClickHouse serves required dashboard queries at documented p95 latency.
- Failed assets are retryable without double-counting.

### MP-06 - Live campaign and budget operations

**Status:** Planned; depends on MP-03 and MP-05

Provide live KPI monitoring, abnormal-spend alerts, budget pacing, and a what-if
budget allocation view for marketing leaders and performance managers.

**Business requirements:** live campaign KPI, anomaly alerting, budget pacing,
and budget simulation.

**Acceptance criteria**

- Every metric links to a Gold model and freshness timestamp.
- Users can filter by time, platform, campaign, objective, and market.
- Alert examples from MP-01 are visible and explain why they fired.

### MP-07 - Funnel and attribution analysis

**Status:** Planned; depends on MP-05

Show cross-channel funnels and compare first-touch, last-touch, and linear
attribution without changing the underlying orders.

**Business requirements:** funnel analysis and attribution comparison.

**Acceptance criteria**

- Funnel stages reconcile with the golden truth dataset.
- Attribution results show model definitions and sum to reconciled revenue.
- Users can drill from channel to campaign and representative journeys.

### MP-08 - Customer 360 and lifecycle intelligence

**Status:** Planned; depends on MP-04 and MP-05

Provide consent-aware Customer 360 timelines, RFM/behavior segments, cohort
retention, and high-value or churn-risk identification.

**Business requirements:** Customer 360, segmentation, cohort retention, and
customer opportunity/risk identification.

**Acceptance criteria**

- Identity resolution exposes confidence and never joins opted-out activity into
  activation datasets.
- Segment rules are reproducible, versioned, and saveable in PostgreSQL.
- A user can move from a cohort aggregate to an anonymized customer timeline.

### MP-09 - Quality, observability, privacy, and operations

**Status:** Planned; spans MP-02 through MP-08

Make the system's health and data trust visible through Prometheus, Grafana,
quality checks, lineage, retention controls, and role-based access.

**Acceptance criteria**

- Dashboards show throughput, consumer lag, freshness, DLQ rate, latency, and
  compute/memory use.
- Reconciliation alerts detect a deliberately corrupted run.
- Synthetic PII is documented; secrets never enter Git or generated datasets.
- Operator runbooks cover reset, replay, backfill, recovery, and demo startup.

### MP-10 - Evaluation, alternatives, and final evidence package

**Status:** Planned; depends on all implemented stories

Evaluate correctness, performance, and application effectiveness, then package the
evidence for the final report, slides, video, and executable demonstration.

**Acceptance criteria**

- Correctness: golden totals, duplicates, malformed data, and late events are tested.
- Performance: throughput, p50/p95/p99 pipeline latency, query latency, recovery time,
  CPU, and memory are reported at named data scales.
- Effectiveness: representative stakeholders complete defined decision tasks and
  every selected business requirement has a successful demo.
- Flink is compared with Kafka Streams on the same inputs and outputs.
- ClickHouse is compared with a PostgreSQL baseline; BigQuery is optional when cloud
  credit is available.
- Final report has 4-7 logical sections, at least four technology examples, and less
  than 10% missing required content, targeting the top rubric band.

## Traceability to the course brief

| Course requirement | Stories |
|---|---|
| Context, stakeholders, business requirements | MP-06, MP-07, MP-08 |
| Expected sources and data characteristics | MP-01 |
| Data engineering solution | MP-02 through MP-05 |
| Data management and processing technologies | MP-03, MP-04, MP-05 |
| Alternative solution | MP-10 |
| Application development | MP-06 through MP-08 |
| Data correctness | MP-01, MP-03, MP-05, MP-10 |
| Performance | MP-02, MP-03, MP-05, MP-10 |
| Application effectiveness | MP-06 through MP-08, MP-10 |
