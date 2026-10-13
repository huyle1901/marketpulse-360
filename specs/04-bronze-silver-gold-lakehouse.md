# MP-04 - Bronze/Silver/Gold Lakehouse

**Status:** Implemented. The executable acceptance proof is
`scripts/lakehouse-smoke-test.sh`; operating and demo instructions are in
`docs/runbooks/lakehouse-local.md`.

## Intent

Persist the complete marketing input history in Iceberg on MinIO and turn it into
governed, consent-aware analytical products whose correctness can be reproduced
from Bronze.

## Scope

### Included

- MinIO object storage, Nessie catalog, and dedicated PostgreSQL catalog metadata.
- Spark-based bounded Kafka ingestion from MP-02 raw plus MP-03 clean/DLQ topics.
- Content-addressed archival of exact compressed batch files before parsing.
- Immutable, idempotent Bronze Kafka/file tables with complete lineage.
- Typed and deduplicated Silver events, customers, campaigns, ad spend, and quality
  summary tables.
- Gold campaign, funnel, attribution, customer 360, and cohort models.
- Iceberg snapshots, time travel, catalog restart persistence, and a from-Bronze
  rebuild proof.
- Docker Compose profile, tests, smoke verifier, and operator runbook.

### Excluded

- Dagster/dbt scheduling and ClickHouse publication (MP-05).
- FastAPI/Next.js application behavior (MP-06 through MP-08).
- BigQuery/BigLake runtime dependencies (optional MP-10 benchmark only).
- Production authentication, TLS, replication, and high availability.

## Processing contract

1. Capture complete Kafka topic-partition `[start,end)` ranges and file checksums
   before ingestion.
2. Preserve exact Kafka bytes and complete Kafka/file lineage before parsing.
3. Insert each Kafka position or source-file row once and never mutate Bronze.
4. Parse the proved MP-03 clean contract into typed Silver tables and reconcile it
   with the archived raw/DLQ topics.
5. Choose one event with the documented total ordering per `event_id`; later copies
   cannot affect Gold and different-payload conflicts remain visible.
6. Use an explicit identity fallback and exclude analytics-consent-denied records
   from analytical models while retaining their Bronze audit trail.
7. Build all Gold tables only from recorded Bronze/Silver snapshots.
8. Reconcile first-touch, last-touch, and linear gross/refund/net attribution to the
   governed consent-eligible totals and separately retain MP-03 source totals.
9. Write an evidence manifest containing source/output snapshot IDs, counts,
   canonical checksums, and financial totals.
10. Rebuild isolated Silver/Gold tables from the recorded Bronze snapshots and
    require exact canonical equality with the documented Gold snapshot.

## Acceptance criteria

1. Docker Compose starts healthy MinIO, PostgreSQL, Nessie, and the on-demand Spark
   tool using pinned versions and persistent named volumes.
2. Sampled Bronze Kafka payloads decode to the exact source bytes; topic, partition,
   offset, timestamp type, key, ordered/duplicate headers, value, run ID, and hashes
   are present.
3. Exact compressed source objects can be recovered from content-addressed MinIO;
   parsed rows retain dataset, file/object path, one-based row number, source date,
   file hash, payload, run ID, and row hash.
4. Re-running the same Kafka offsets and files adds zero Bronze rows and changes no
   canonical business checksum.
5. Silver reconciles with MP-03 accepted/DLQ outputs, selects one record per event
   ID, applies documented identity stitching, and summarizes consent, bot, late,
   duplicate, conflict, and validation exclusions.
6. Gold exposes campaign performance, funnel, three attribution models, customer
   360, and cohort retention without synthetic contact fields.
7. First-touch, last-touch, and linear attribution each reconcile exact gross,
   refund, and net amounts to governed eligible order revenue, including explicit
   unattributed rows.
8. Iceberg time-travel reads a documented earlier snapshot after a later run.
9. Restarting catalog/object-store containers preserves the tables and snapshots.
10. An isolated rebuild from recorded Bronze snapshot IDs matches every documented
    Gold table by schema, row count, checksum, and reconciled monetary totals.
11. Python tests, Docker asset tests, Spark integration checks, Compose validation,
    real-container smoke test, and `git diff --check` pass from a clean checkout.

## Demo evidence to capture

- MinIO console showing Bronze/Silver/Gold Iceberg objects.
- Nessie API/catalog contents before and after a restart.
- Table list, snapshot IDs, time-travel query result, and source offset bounds.
- Bronze/Silver/Gold row counts and quality/consent/duplicate counts.
- Campaign totals and three attribution reconciliation totals.
- Original versus rebuilt Gold checksums.
- Runtime, CPU, memory, and object-size observations for later MP-10 reporting.

## Authority

The complete table shapes, rules, runtime topology, failure semantics, and command
contracts are defined in
`docs/superpowers/specs/2026-10-12-iceberg-lakehouse-design.md`.
