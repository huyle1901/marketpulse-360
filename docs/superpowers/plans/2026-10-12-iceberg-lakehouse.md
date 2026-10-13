# Bronze/Silver/Gold Lakehouse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and prove a Dockerized MinIO/Iceberg lakehouse that preserves raw Kafka and file inputs, publishes governed Silver and marketing Gold tables, and reproduces a documented Gold snapshot from Bronze.

**Architecture:** One-shot Spark 4.0 jobs archive MP-02 raw and MP-03 clean/DLQ Kafka records plus exact source files, then write Iceberg 1.12 tables to MinIO through a persistent Nessie catalog backed by dedicated PostgreSQL. Deterministic Spark transformations build bounded Silver/Gold outputs from explicit Bronze snapshots; an evidence manifest and isolated rebuild compare complete canonical business rows across Iceberg snapshots.

**Tech Stack:** Python 3.12+, PySpark/Apache Spark 4.0.4, Apache Iceberg 1.12.0, Project Nessie 0.109.0, MinIO `RELEASE.2025-04-22T22-12-26Z`, PostgreSQL 17.6, Kafka 4.3.1, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-12-iceberg-lakehouse-design.md`

**Execution result (2026-10-13):** Tasks 1-6 are implemented. Fresh verification
passed 218 Python tests, 30 Java tests, fixture validation, Compose validation,
and the real Docker acceptance proof for all ten governed tables. Final review
closed the recovered-click conflict finding with a RED/GREEN Spark regression.
The live architecture and runbook record the proof and operating commands.

## Global Constraints

- Keep Dagster, dbt, ClickHouse, and application behavior out of MP-04.
- PostgreSQL stores Nessie metadata only; analytical table data and Iceberg metadata live in MinIO.
- Use Iceberg's native `NessieCatalog` plus `S3FileIO`; do not depend on experimental Iceberg REST behavior.
- Bronze preserves exact Kafka key/value bytes, ordered headers, exact compressed
  files, and complete lineage before parsing.
- Bronze records are insert-only by deterministic ingest identity; re-ingestion cannot mutate accepted rows.
- Silver/Gold business equality excludes processing time, Spark file layout, and Iceberg operational metadata.
- Every monetary value is an integer minor unit; allocation remainders are deterministic.
- Silver consumes MP-03 clean events and reconciles them with raw/DLQ Bronze; Spark
  does not invent a competing validation contract.
- The committed fixture and large ignored profiles use the same production entry points.
- Every feature commit updates `docs/architecture/high-level-design.md` when its implemented topology or flow changes.
- Never commit MinIO/PostgreSQL volumes, generated lakehouse data, credentials, Maven/Ivy caches, Spark work files, or smoke evidence.

## Review Focus

- Kafka tombstones and non-UTF-8 values must remain lossless in Bronze; `payload_text` may be null but `payload_base64` and hash must be exact.
- Replaying identical Kafka offsets/files must insert zero rows, not merely produce the same Gold totals.
- Consent filtering must not delete Bronze history or leak synthetic email/phone fields into Gold.
- `event_id` duplicates use the design's total winner order; different-payload
  conflicts remain visible in quality evidence without changing that winner.
- Linear attribution integer division must preserve the exact order revenue total, including remainders.
- Time-travel evidence must query an actual earlier Iceberg snapshot after a later commit.
- The rebuild must read the recorded Bronze snapshot IDs and compare canonical rows, not current tables or counts alone.
- Restarting Nessie/PostgreSQL/MinIO must preserve catalog contents and historical snapshot reads.

---

### Task 1: Lakehouse domain contracts and deterministic business primitives

**Files:**
- Create: `src/marketpulse_lakehouse/__init__.py`
- Create: `src/marketpulse_lakehouse/contracts.py`
- Create: `src/marketpulse_lakehouse/identity.py`
- Create: `src/marketpulse_lakehouse/attribution.py`
- Create: `src/marketpulse_lakehouse/evidence.py`
- Create: `tests/test_lakehouse_contracts.py`
- Create: `tests/test_lakehouse_identity.py`
- Create: `tests/test_lakehouse_attribution.py`
- Create: `tests/test_lakehouse_evidence.py`
- Modify: `pyproject.toml`
- Modify: `specs/README.md`
- Modify: `docs/architecture/high-level-design.md`

**Interfaces:**
- Produces immutable `TableContract` definitions for three Bronze, five Silver, and five Gold tables, including ordered business columns and Iceberg partition transforms.
- Produces `kafka_ingest_id(topic, partition, offset)`, `file_ingest_id(file_sha256, row_number)`, identity precedence, exact integer attribution allocation, canonical row checksum, and evidence comparison APIs.
- Evidence format version is `1` and names catalog ref, input snapshots, output snapshots, row counts, checksums, quality counts, and financial reconciliations.

- [ ] **Step 1: Write failing contract/identity/attribution/evidence tests**

Use literal expected table names/columns. Test stable Kafka/file ingest IDs, invalid negative positions, identity fallbacks and missing identity, one/two/three-touch allocation including a non-divisible `100` minor-unit example (`34,33,33` in deterministic order), canonical checksum insensitivity to row/dict order, checksum sensitivity to values, and evidence mismatch diagnostics.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_lakehouse_contracts tests.test_lakehouse_identity tests.test_lakehouse_attribution tests.test_lakehouse_evidence -v`

Expected: FAIL because the `marketpulse_lakehouse` package does not exist.

- [ ] **Step 3: Implement the minimal typed domain layer**

Use frozen dataclasses and typed public interfaces. Canonicalize JSON with sorted keys and compact separators; sort canonical row strings before hashing. Reject duplicate touch IDs before allocation and assign the remainder in event-time/campaign order. Evidence comparison reports table-specific schema/count/checksum/reconciliation differences.

- [ ] **Step 4: Mark MP-04 in progress, update architecture, and verify GREEN**

Run the focused command, `PYTHONPATH=src python3 -m unittest discover -s tests -v`, and `git diff --check`.

Expected: all existing and new Python tests PASS; exactly 13 lakehouse table contracts exist.

- [ ] **Step 5: Commit**

```bash
git add src/marketpulse_lakehouse tests/test_lakehouse_*.py pyproject.toml specs docs/architecture/high-level-design.md
git commit -m "feat: define lakehouse data contracts"
```

### Task 2: Persistent MinIO, Nessie, PostgreSQL, and Spark Compose profile

**Files:**
- Create: `docker/lakehouse/Dockerfile`
- Create: `docker/lakehouse/entrypoint.sh`
- Create: `docker/minio/Dockerfile.server`
- Create: `docker/minio/Dockerfile.client`
- Create: `docker/minio/create-bucket.sh`
- Create: `src/marketpulse_lakehouse/session.py`
- Create: `scripts/lakehouse-smoke-test.sh`
- Modify: `compose.yaml`
- Modify: `tests/test_docker_assets.py`
- Create: `tests/test_lakehouse_session.py`
- Modify: `.gitignore`
- Modify: `docs/architecture/high-level-design.md`

**Interfaces:**
- Consumes the existing `streaming` network/services when Kafka ingestion runs.
- Produces Compose `lakehouse` storage/catalog services, separate `lakehouse-tools`
  Spark service, `lakehouse_data` and `lakehouse_catalog` named volumes, MinIO
  bucket `marketpulse-lakehouse`, Nessie ref `main`, and an on-demand
  `spark-lakehouse` tool.
- `create_spark_session(app_name, catalog_name="lakehouse", ref="main")` configures native NessieCatalog, S3FileIO, Iceberg extensions, UTC, and deterministic shuffle settings from environment variables.

- [ ] **Step 1: Write failing executable Docker/session tests**

Render `docker compose config`; require pinned service images, separate profiles,
health checks, dependency conditions, non-root Spark runtime, PostgreSQL `JDBC2`
Nessie configuration, named volumes, MinIO bucket initializer, no published
PostgreSQL port, host UI/API ports, and exact Spark catalog/S3/local[2]/UTC settings.
Test session configuration through a small recording builder rather than importing
PySpark in the host test suite.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_docker_assets tests.test_lakehouse_session -v`

Expected: FAIL because the lakehouse image/services/session builder do not exist.

- [ ] **Step 3: Implement the profile and pinned Spark image**

Build from `apache/spark:4.0.4-scala2.13-java17-python3-ubuntu`; bake
`iceberg-spark-runtime-4.0_2.13:1.12.0`, `iceberg-aws-bundle:1.12.0`, and
`spark-sql-kafka-0-10_2.13:4.0.4`; install the local package; run as the image's
non-root user; and execute the CLI through `spark-submit`. Configure Nessie `JDBC2`
and native S3/MinIO settings. Build MinIO and `mc` for amd64/arm64 from pinned
official GitHub release binaries and reject any binary whose SHA-256 does not match
the release checksum; this avoids depending on mutable or retired public image tags.

- [ ] **Step 4: Verify build and real service health**

Run focused tests, `docker compose config --quiet`, and
`scripts/lakehouse-smoke-test.sh`. The smoke script builds `spark-lakehouse`, MinIO,
and `mc`, starts
an isolated `lakehouse` profile, verifies every service health endpoint, creates and
reads an Iceberg table, restarts Nessie/PostgreSQL/MinIO, and reads the same table
again.

Expected: every command exits zero and the probe row survives restart.

- [ ] **Step 5: Update architecture and commit**

Record exact service names, versions, ports, volumes, and verified catalog persistence.

```bash
git add docker/lakehouse docker/minio compose.yaml src/marketpulse_lakehouse/session.py tests .gitignore docs/architecture/high-level-design.md
git commit -m "feat: add the local Iceberg lakehouse stack"
```

### Task 3: Immutable Bronze Kafka and source-file ingestion

**Files:**
- Create: `src/marketpulse_lakehouse/bronze.py`
- Create: `src/marketpulse_lakehouse/source_files.py`
- Create: `src/marketpulse_lakehouse/cli.py`
- Modify: `docker/lakehouse/entrypoint.sh`
- Create: `tests/test_lakehouse_bronze.py`
- Create: `tests/test_lakehouse_source_files.py`
- Create: `tests/test_lakehouse_cli.py`
- Modify: `tests/test_docker_assets.py`
- Modify: `pyproject.toml`
- Modify: `docs/architecture/high-level-design.md`

**Interfaces:**
- Produces `bootstrap`, `capture-offsets`, `ingest-kafka`, and `ingest-files` commands.
- Kafka ingestion reads the three MP-02 raw topics plus MP-03 clean and processing
  DLQ topics from a complete versioned `[start,end)` manifest, then insert-only
  merges by `ingest_id` into `bronze.kafka_records`.
- File ingestion archives exact allowlisted `.gz` files and manifest by content hash
  before canonicalizing rows into `bronze.source_file_rows`; replayable event files,
  `ground_truth`, and `quality_cases` are excluded from business file ingestion.

- [ ] **Step 1: Write failing ingestion tests**

Use literal Kafka rows including null key, ordered duplicate/null headers, tombstone,
duplicate position, same position/different content, duplicate event at a different
position, non-UTF-8 value, offset gaps, and partition drift. Use temporary gzip
CSV/JSONL files with a minimal manifest, repeated file, and checksum mismatch.
Assert exact archive bytes/base64/hash/lineage, one-based rows, stable source dates,
zero insertions on repeat, and fail-closed behavior.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_lakehouse_bronze tests.test_lakehouse_source_files tests.test_lakehouse_cli -v`

Expected: FAIL because Bronze ingestion and CLI commands do not exist.

- [ ] **Step 3: Implement schema bootstrap and file normalization**

Create namespaces/tables from Task 1 contracts. Archive files before parsing. Keep
transformation-free row normalizers testable without Spark, then use Spark
DataFrames and insert-only Iceberg `MERGE`. Compare existing-position content before
merge. Record requested/observed/inserted bounds and manifest hashes.

- [ ] **Step 4: Implement bounded Kafka ingestion**

Use Spark's Kafka source with manifest `startingOffsets`/`endingOffsets`,
`includeHeaders=true`, and `failOnDataLoss=true`. Encode key/value/headers before
any UTF-8 cast; only populate text when strict decoding is valid. Fail on absent,
new, or retention-truncated partitions.

- [ ] **Step 5: Verify unit and real integration behavior**

Run focused/full tests. Against the isolated stack, publish the committed fixture, ingest it twice, ingest all eligible fixture files twice, and query Bronze. Compare sampled decoded Kafka bytes with the source record and require the second run to insert zero rows.

- [ ] **Step 6: Update architecture and commit**

Record implemented Bronze tables, sources, idempotency, and observed fixture counts.

```bash
git add src/marketpulse_lakehouse tests pyproject.toml docs/architecture/high-level-design.md
git commit -m "feat: ingest immutable Iceberg Bronze data"
```

### Task 4: Typed, deduplicated, consent-aware Silver tables

**Files:**
- Create: `src/marketpulse_lakehouse/silver.py`
- Create: `tests/test_lakehouse_silver.py`
- Modify: `src/marketpulse_lakehouse/cli.py`
- Modify: `docs/architecture/high-level-design.md`

**Interfaces:**
- Consumes recorded snapshots of both Bronze tables.
- Produces `silver.customer_events`, `silver.customers`, `silver.campaigns`,
  `silver.ad_spend_daily`, and `silver.quality_summary` from explicit Bronze
  snapshots through `marketpulse-lakehouse build-silver`.
- `build-silver` accepts source snapshot IDs and target table prefix so the same implementation serves main publication and isolated rebuilds.

- [ ] **Step 1: Write failing Silver rule tests**

Cover complete MP-03 clean-schema validation; raw/clean/DLQ reconciliation;
malformed/unsupported DLQ reasons; total-order dedup; different-payload conflict;
session identity stitching; analytics-consent true/false; late ineligibility; whole
bot-session exclusion; latest CRM snapshot; separate marketing consent; platform
campaign mapping; source-specific spend conversions; and quality counts. Use
hand-derived literal expectations.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_lakehouse_silver -v`

Expected: FAIL because Silver transformations do not exist.

- [ ] **Step 3: Implement Silver transforms**

Keep deterministic row-level rules in typed helpers and implement equivalent Spark
expressions/window ordering. Materialize atomically. Retain lineage/hashes, omit
contact fields, emit namespaced profile keys, and calculate quality from all Bronze
outcomes using MP-03's ten reasons plus MP-04 governance reasons.

- [ ] **Step 4: Verify fixture invariants and commit**

Run focused/full tests and a real Spark build. Compare Silver event IDs, accepted/duplicate/consent/invalid counts, campaign mappings, spend totals, and order/refund totals to an independent fixture oracle; run `git diff --check`.

```bash
git add src/marketpulse_lakehouse tests/test_lakehouse_silver.py docs/architecture/high-level-design.md
git commit -m "feat: build governed Iceberg Silver tables"
```

### Task 5: Marketing Gold models, snapshots, and evidence manifest

**Files:**
- Create: `src/marketpulse_lakehouse/gold.py`
- Create: `src/marketpulse_lakehouse/verify.py`
- Create: `tests/test_lakehouse_gold.py`
- Create: `tests/test_lakehouse_verify.py`
- Modify: `src/marketpulse_lakehouse/cli.py`
- Modify: `docs/architecture/high-level-design.md`

**Interfaces:**
- Consumes the Task 4 Silver tables at explicit snapshot IDs.
- Produces five Gold tables plus `lakehouse-evidence.json` through `build-gold`, `verify`, and `backfill-proof` commands.
- Evidence serializes table schemas, snapshot IDs, counts, canonical checksums, quality counts, three attribution reconciliation totals, and gross/refund/net revenue.

- [ ] **Step 1: Write failing Gold tests with literal cases**

Test non-sequential distinct-session funnel stages and bot exclusion; 30-day paid
page-view touches keyed by click ID; first/last/linear allocation and remainder;
unattributed orders; refund reuse of order touches; zero-denominator ROAS;
anonymous customer profiles; known/anonymous cohort rules; fixed-scale retention;
full-outer campaign performance; and absence of contact fields. Include a literal
multi-campaign journey that differs across all three models.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_lakehouse_gold -v`

Expected: FAIL because Gold transformations do not exist.

- [ ] **Step 3: Implement all five Gold models**

Build complete bounded outputs from explicit Silver snapshots. Allocate linear attribution in integer minor units with chronological deterministic remainders. Validate each model's grain/uniqueness before atomic replacement.

- [ ] **Step 4: Write failing snapshot/evidence tests**

Require evidence schema version 1, complete table coverage, an actual prior/current snapshot pair, canonical full-row checksums, exact financial reconciliations, clear mismatch output, and isolated target-table naming for backfill.

- [ ] **Step 5: Implement evidence, verification, and backfill proof**

Query Iceberg metadata tables for snapshot IDs and read business rows using `VERSION AS OF`. Rebuild from the recorded Bronze snapshots into isolated prefixed tables, generate fresh evidence, and compare schemas/counts/checksums/reconciliations to the original. Never drop or overwrite original Gold tables during proof.

- [ ] **Step 6: Verify complete fixture evidence and commit**

Run focused/full tests and real build/verify/backfill. Assert source totals
`29,718,000/4,967,000/24,751,000`, governed totals
`27,160,000/4,967,000/22,193,000`, three anonymous orders totaling `4,046,000`,
each model's exact governed reconciliation, a baseline read after a genuinely later
snapshot, and original/rebuilt checksums; run `git diff --check`.

```bash
git add src/marketpulse_lakehouse tests/test_lakehouse_gold.py tests/test_lakehouse_verify.py docs/architecture/high-level-design.md
git commit -m "feat: publish reproducible Iceberg Gold models"
```

### Task 6: End-to-end smoke proof, runbook, and milestone evidence

**Files:**
- Create: `scripts/lakehouse-smoke-test.sh`
- Create: `docs/runbooks/lakehouse-local.md`
- Modify: `README.md`
- Modify: `specs/README.md`
- Modify: `specs/04-bronze-silver-gold-lakehouse.md`
- Modify: `docs/architecture/high-level-design.md`
- Modify: `tests/test_docker_assets.py`

**Interfaces:**
- Consumes an isolated Compose project, committed fixture, MP-03 Flink outputs,
  Kafka replay, the lakehouse CLI, and fixture-derived independent oracles.
- Produces a non-zero-on-mismatch smoke script that starts services, ingests twice, builds, time-travels, restarts storage/catalog services, rebuilds, verifies, prints evidence/UI locations, and cleans only its project unless `KEEP_STACK=1`.

- [ ] **Step 1: Write failing smoke/runbook asset tests**

Require safe isolated project naming, cleanup trap, explicit offset capture, repeated ingestion, sampled byte comparison, catalog restart, prior snapshot read, isolated backfill target, full verifier invocation, `KEEP_STACK`, and documented UI/inspection/reset commands.

- [ ] **Step 2: Run Docker asset tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_docker_assets -v`

Expected: FAIL because the lakehouse smoke script/runbook do not exist.

- [ ] **Step 3: Implement the smoke wrapper and runbook**

Reuse the existing replay/Registry/Flink stack. Wait for MP-03 clean/DLQ output,
capture bounded raw/clean/DLQ offsets, ingest, then stop Flink before Spark transforms
to release memory. Capture long output to ignored logs and use bounded readiness
timeouts with actionable failures.

- [ ] **Step 4: Run all acceptance verification**

Run:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
mvn -q -f flink-job/pom.xml clean verify
docker compose config --quiet
./scripts/lakehouse-smoke-test.sh
git diff --check
git status --short
```

Expected: every command exits zero; the smoke evidence proves exact Bronze byte/file lineage, repeat-ingest zero inserts, Silver quality/consent/dedup rules, five Gold models, exact attribution/revenue reconciliations, an earlier snapshot query, survival across catalog restart, and original/rebuild canonical checksum equality.

- [ ] **Step 5: Finalize documentation and commit**

Set MP-04 to Implemented only after Step 4. Record exact service versions, test counts, table counts, snapshot IDs, checksums, totals, catalog restart result, rebuild result, elapsed time, and machine caveat.

```bash
git add scripts/lakehouse-smoke-test.sh docs/runbooks/lakehouse-local.md README.md specs docs/architecture/high-level-design.md tests/test_docker_assets.py
git commit -m "docs: prove lakehouse reproducibility"
```

## Completion gate

- [ ] Review the design goals/non-goals and map each to an implemented task or explicit exclusion.
- [ ] Review every MP-04 acceptance criterion in `specs/README.md` and the detailed spec against fresh evidence.
- [ ] Confirm PostgreSQL contains catalog metadata only and no application/source analytical tables.
- [ ] Confirm no generated lakehouse data, credentials, caches, or smoke evidence are tracked.
- [ ] Run the full Python, Java, Compose, Docker smoke, whitespace, and Git-status checks from Task 6.
- [ ] Request a fresh whole-branch code review and fix every Critical/Important finding with RED→GREEN evidence.
- [ ] Use `superpowers:finishing-a-development-branch` before any push, PR, or merge action.
