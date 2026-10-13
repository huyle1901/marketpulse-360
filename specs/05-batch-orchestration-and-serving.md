# MP-05 - Batch orchestration and analytical serving

**Status:** In progress. Daily source selection and immutable local preparation
receipt contracts implemented; Docker/orchestration/serving proof still pending.
**Dependency:** MP-04, merged in PR #4.

## Outcome

Make the proved lakehouse executable as observable, scheduled Dagster assets and
serve certified marketing datasets from ClickHouse. A failed ingestion, model
test, copy, or publication must never expose incomplete data or inflate KPIs.

## Scope

- Observable ad-report, CRM-snapshot, master/historical-file and bounded-Kafka assets.
- UTC daily batch selection, a schedule, explicit replay of recorded snapshots,
  persistent run metadata, retry receipts, and a visible asset dependency graph.
- Reuse MP-04 Spark transformations; retain immutable Bronze and historical tables.
- Export pinned snapshots, load isolated ClickHouse candidates, run dbt Core
  models/tests, reconcile complete business rows, and certify a release.
- Five exact Gold serving tables plus required reference dimensions and derived
  dashboard read models. No synthetic contact fields in serving tables.
- One consistent active release, parameterized query tools, and measured latency.
- Docker profiles, executable failure/recovery proof, and an operator runbook.

Application pages/APIs, continuous Flink-to-ClickHouse updates, alerts, cloud
runtime, production HA/TLS, and automatic dataset deletion are excluded.

## Acceptance

1. Dagster shows real ad-report, CRM and historical-file materializations with
   input paths/hashes, selected dates, observed/inserted/no-op counts, snapshots,
   duration and failure state. Missing expected daily sources fail explicitly.
2. A schedule definition and daemon operate; the local schedule starts disabled
   so historical fixtures are not falsely treated as today's vendor files.
3. Rerunning the same input selection adds zero immutable Bronze rows. Existing
   first-ingestion provenance is not relabeled to a new Dagster run ID.
4. All five Gold serving tables equal their recorded Iceberg snapshots by typed
   schema, keys, row count, complete business checksum and financial totals.
5. Real dbt not-null, uniqueness, accepted-value, relationship and reconciliation
   tests pass. Injected bad rows fail tests and block publication.
6. Partial-copy failure, failed dbt checks, lost promotion acknowledgement and
   overlapping publication attempts do not change a valid active dataset or
   double-count its rows; an authorized retry can complete.
7. A historical rebuild uses recorded Bronze snapshots, reproduces documented
   results in isolation and does not change active publication by default.
8. An interrupted pipeline can resume after container restart using persisted
   receipts. Catalog, ClickHouse data and active release survive restart.
9. Six dashboard query workloads report independent correctness checks and p50,
   p95, maximum, failures, sample size and dataset scale. Fixture p95 is at most
   500 ms per workload, including client request time, after 20 warmups and 100
   measured requests. This is not a production/capacity claim; MP-10 owns that.
10. Python/Java regression suites, real Dagster/dbt/ClickHouse integration,
    generated-data validation, Compose validation and diff checks pass. GitGuardian
    passes; no secrets, large data or local instructions are committed.

## Design authority

The detailed runtime, release protocol, source-selection semantics and failure
contracts are in
[`2026-10-13-batch-serving-design.md`](../docs/superpowers/specs/2026-10-13-batch-serving-design.md).
