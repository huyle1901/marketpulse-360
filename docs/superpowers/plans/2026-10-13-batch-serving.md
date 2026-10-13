# MP-05 Batch Serving Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Proposed execution: native, in the main source directory, pending user review.

**Goal:** Run observable batch refreshes and serve certified, retry-safe marketing datasets through ClickHouse.

**Architecture:** Dagster invokes the existing Spark lakehouse logic against pinned snapshots. Exported Gold copies pass dbt and complete equality checks before publication. An immutable release database and monotonic journal keep every dashboard request on one certified dataset.

**Tech Stack:** Python 3.12+, existing Spark/Iceberg/Nessie/MinIO, Dagster, dbt Core/dbt-clickhouse, ClickHouse, PostgreSQL, Docker Compose.

**Spec:** `specs/05-batch-orchestration-and-serving.md` and `docs/superpowers/specs/2026-10-13-batch-serving-design.md`.

## Global Constraints

- Implementation authorized by user continuation; do not claim services exist until runtime verification.
- Preserve all MP-04 table contracts and default CLI behavior; no second attribution implementation.
- Candidate pins: Dagster/webserver 1.13.26, dagster-dbt/postgres 0.29.26, dbt-core 1.12.5, dbt-clickhouse 1.10.3, clickhouse-connect 1.10.0, ClickHouse LTS 26.8.25.14. Resolve and lock dependencies before accepting them.
- Spark steps use 2 GiB heap/local[2]; worker limit 4 GiB; one queued refresh; no Docker socket.
- Daily schedule: 02:00 UTC, previous UTC day, initially stopped.
- Required report platforms default to google_ads/meta_ads/tiktok_ads; require a nonempty CRM snapshot. File partition and every row day must match.
- Five Gold rowsets must match pinned Iceberg snapshots by exact typed business content, not only row counts.
- Retry partial copies in a fresh owned database; completed preparation receipts and certified databases are immutable.
- Historical rebuilds require explicit day-to-receipt mappings; no implicit latest receipt or current heads; no default promotion.
- Journal delivery may repeat identical publication rows; business rows must never double-count. Conflicting journal payloads fail closed.
- Query intervals are UTC [from,to); monetary queries group/filter by currency; fixture p95 <=500 ms after 20 warmups and 100 measured requests per workload.
- All credentials stay in ignored `.env`; no secrets in images, logs or evidence; no reset/deletion of existing volumes.
- Update living architecture/specs when behavior changes. Before every commit run relevant tests and `git diff --check`.

## Review Focus

1. Mixed-day source files must fail daily selection without changing original file/row provenance (Task 1).
2. Money beyond 2**53, nulls, negative refunds and decimal scales must survive export/load exactly (Task 3).
3. Same-count but altered Gold rows must fail certification (Task 4).
4. Lock loss followed by a delayed older publication cannot roll back a newer release (Task 5).
5. Restart with an uncertain acknowledgement must recover one logical publication without inflated query totals (Task 6).

## File ownership and test commands

New production package: `src/marketpulse_batch/`; keep selection, receipt types,
ledger SQL, export, copying, certification, publication and queries separate.
Dagster definitions import these interfaces instead of embedding business logic.
`dbt/marketpulse_serving/` owns serving SQL and data tests.
`docker/batch/` owns launcher configuration and runtime images.
Existing `compose.yaml`: extend existing profiles, never replace user configuration.

Use a Python 3.12+ environment with package extras installed. Commands below use
`PYTHONPATH=src python -m unittest ...`; keep dependency-free unit tests runnable
without importing Dagster or connecting to Docker. Docker tests explicitly opt in.
Each task's focused tests run RED before production implementation, GREEN after,
then `PYTHONPATH=src python -m unittest discover -s tests -v` and diff checks before
its focused commit. Stage exact owned paths, never `git add .`.

### Task 1: Source selection and immutable receipt contracts

**Files:** Create `src/marketpulse_batch/{__init__,contracts,selection,receipts}.py`;
modify `src/marketpulse_lakehouse/source_files.py`; test
`tests/test_batch_selection.py`, `tests/test_batch_receipts.py`, and existing source tests.

**Interfaces:**
- Frozen `BatchSelection(logical_day: date, required_platforms: tuple[str, ...])`.
- `select_daily_sources(plan: SourceArchivePlan, selection: BatchSelection) -> SourceArchivePlan` validates daily file/row dates and source completeness; shared master files remain included.
- Extend `ingest_source_files` with optional `selection: BatchSelection | None = None`, preserving existing arguments and defaults. Avoid circular imports by keeping shared selection types dependency-free.
- Frozen `PreparationReceipt(identity: str, logical_day: date, model_digest: str, bronze_snapshots: Mapping[str, int], output_snapshots: Mapping[str, int], namespace: str, proof_digest: str, required_platforms: tuple[str, ...] = REPORT_PLATFORMS)`; persist selection so identity is verifiable when read.
- `preparation_identity(selection: BatchSelection, bronze_snapshots: Mapping[str, int], model_digest: str) -> str`; `write_receipt(receipt: PreparationReceipt, path: Path) -> None`; `read_receipt(path: Path) -> PreparationReceipt` reject conflicts and malformed identities.

- [ ] Write failing tests `test_daily_selection_preserves_master_and_lineage`, `test_row_partition_mismatch_rejected`, `test_missing_required_platform_rejected`, `test_empty_crm_rejected`, `test_default_ingest_unchanged`, `test_identity_order_independent`, `test_conflicting_receipt_rejected`.
  Assert day `2026-01-01` selects only that day's daily sources plus masters; row date `2026-01-02` fails; removing tiktok_ads fails with its name; reordered snapshot map yields identical identity; changing one snapshot changes identity; writing another proof to the same receipt path raises ValueError.
- [ ] Run `PYTHONPATH=src python -m unittest discover -s tests -p 'test_batch_*.py' -v`; expect new-interface failures.
- [ ] Implement the interfaces, canonical JSON hashing and atomic owned-file receipt persistence. Preserve content-addressed original archive bytes and first-ingestion provenance; do not rewrite existing fixtures.
- [ ] Run the focused tests plus `test_lakehouse_source_files.py`; expect PASS and unchanged default ingestion assertions.
- [ ] Run full suite/diff check, document source-day contract in architecture, and commit `feat: add batch source selection and immutable receipts`.

### Task 2: Authenticated batch/serving runtime and operational ledger

**Files:** Create `docker/batch/{Dockerfile,dagster.yaml}`, `src/marketpulse_batch/{settings,ledger}.py`, `src/marketpulse_batch/sql/ledger.sql`, pinned dependency lock; modify tracked Compose, `.env.example`, `pyproject.toml`; tests `tests/test_batch_runtime.py`, `tests/test_batch_ledger.py`.

**Interfaces:**
- Frozen `BatchSettings` contains MinIO/Nessie/Kafka endpoints, source root, state path, ClickHouse host/port/user/password and ledger DSN; `load_settings(environ: Mapping[str, str]) -> BatchSettings` rejects empty required secrets; repr redacts secrets.
- Frozen `PublicationReservation(generation: int, identity: str, database: str, logical_day: date, proof_digest: str)`.
- `BatchLedger(dsn: str)` methods `reserve_publication(receipt: PreparationReceipt, database: str) -> PublicationReservation`, `acknowledge(reservation: PublicationReservation) -> None`, `record_receipt(receipt: PreparationReceipt, uri: str) -> None`, `get_receipt(identity: str) -> tuple[PreparationReceipt, str]`; transactional advisory lock and uniqueness constraints own generation/payload immutability.
- Persist preparation attempts and receipt identity; reservation high-water mark rejects older logical days. A retry gets the same generation and exact frozen payload.

- [ ] Write RED tests `test_missing_secret_fails_without_echo`, `test_repr_redacts`, `test_no_docker_socket`, `test_serving_native_port_not_published`, `test_reservation_retry_same_generation`, `test_reservation_payload_conflict`, `test_older_day_rejected`.
  Assert empty CLICKHOUSE_PASSWORD raises without its value; generation for one identity stays equal on retry; competing new identity increments; changing reserved database fails; older day fails.
- [ ] Run focused unit tests; expect failures. Add opt-in real PostgreSQL integration for concurrent reservations, transaction rollback and restart persistence.
- [ ] Resolve proposed versions, add batch extra/lock, build images and authenticated Compose services. Generate local secrets without output, retain empty template values. Add health checks and dedicated PostgreSQL volume. Implement ledger migrations idempotently.
- [ ] Run focused/full tests, real PostgreSQL integration and Compose validation without printing interpolated secrets. Verify amd64/arm64 image availability and actual Python dependency resolution; document any pin adjustments with evidence.
- [ ] Commit `feat: add authenticated batch and serving runtime` with relevant architecture updates; never stage `.env` or generated lock artifacts containing credentials.

### Task 3: Exact snapshot export and isolated ClickHouse candidates

**Files:** Create `src/marketpulse_batch/{export,copying,canonical}.py`; tests `tests/test_batch_export.py`, `tests/test_batch_copying.py`.

**Interfaces:**
- Frozen `ExportObject(uri: str, sha256: str, byte_size: int, row_count: int)` and `ExportRelation(name: str, snapshot_id: int, columns: tuple[tuple[str, str, bool], ...], objects: tuple[ExportObject, ...], row_count: int, business_checksum: str)`.
- Frozen `ExportManifest(identity: str, relations: tuple[ExportRelation, ...])` covers five Gold models plus two reference dimensions.
- `export_snapshots(spark: Any, receipt: PreparationReceipt, output_prefix: str) -> ExportManifest` reads exact snapshots and uses partitioned output, not driver collection.
- `clickhouse_type(type_name: str, nullable: bool) -> str`; `canonical_row(columns: tuple[tuple[str, str, bool], ...], row: Mapping[str, object]) -> bytes` enforces types and stable representation.
- `load_candidate(client: Any, manifest: ExportManifest, *, attempt: int, s3: Any) -> str` validates checksums and creates a fresh owned database, streaming bounded chunks. Namespace/database identifiers are validated before SQL.

- [ ] Write RED tests `test_money_exact_above_float_precision`, `test_nullable_decimal_date_roundtrip`, `test_unknown_columns_fail`, `test_export_snapshot_not_head`, `test_partial_copy_new_attempt`, `test_bad_object_hash_rejected`.
  Assert integer `9007199254740993` is unchanged, Decimal `1.230000` retains scale 6, null differs from empty string, date remains `2026-01-01`; negative refund stays negative; current-head changes do not affect a pinned export; failed attempt 1 is never reused for partial append by attempt 2.
- [ ] Run focused tests; expect missing-interface failures.
- [ ] Implement Spark schema-driven export and canonical multiset checksum: sorted canonical row hashes with multiplicities, externally sorted/aggregated without collecting all data in Python. Validate timestamp UTC and decimal/int ranges; read only job-owned outputs. Use ordinary MergeTree candidate tables.
- [ ] Run tests plus real Spark/MinIO/ClickHouse export/load roundtrip; assert all seven relations' exact schemas, counts and checksums, including large integers and malformed fixtures.
- [ ] Run full suite/diff check and commit `feat: export pinned lakehouse snapshots into serving candidates` with architecture updates.

### Task 4: dbt models, semantic tests and certification

**Files:** Create `dbt/marketpulse_serving/{dbt_project.yml,profiles.yml,models/,tests/}` and `src/marketpulse_batch/certification.py`; tests `tests/test_batch_certification.py`.

**Interfaces:**
- Frozen `Certification(identity: str, database: str, proof_digest: str, manifest_digest: str, dbt_invocation_id: str, checked_relations: tuple[str, ...])`.
- `certify_candidate(client: Any, receipt: PreparationReceipt, manifest: ExportManifest, database: str, dbt_artifacts: Path) -> Certification` requires genuine successful dbt results and source/served equality. Persist proof, no passwords or raw PII.
- dbt relations: exact five MP-04 Gold names, campaigns/customers dimensions, `campaign_dashboard`. Runtime env supplies candidate database; asset keys remain stable.

- [ ] Write RED tests `test_equal_count_changed_row_fails`, `test_missing_dbt_result_blocks`, `test_failed_relationship_blocks`, `test_gold_financial_conservation`, `test_campaign_dashboard_independent_oracle`.
  Alter one campaign's net revenue by 1 without changing row count: certification must fail. Remove one successful relation from artifact: fail. Verify gross-refund=net, all three attribution models, exact source checksum and fixture CTR/CPC using independently computed expected values.
- [ ] Run focused tests; expect failures.
- [ ] Implement dbt raw-to-serving SQL without changing business semantics. Add required-column/composite-key/accepted-value/relationship and financial/cohort/funnel/consent tests, explicit anonymous/unattributed exceptions, and complete typed rowset verification. Build manifest at runtime, not with image-baked credentials.
- [ ] Run actual `dbt build` in candidate runtime and inspect real artifacts; inject bad rows and assert failures block certification. Verify financial sums widen to Int128; ratios handle zero denominators and currency boundaries.
- [ ] Run full suite/diff check and commit `feat: certify dbt marketing serving models` with architecture and proof contract updates.

### Task 5: Dagster graph, bounded jobs and monotonic publication

**Files:** Create `src/marketpulse_batch/{definitions,resources,jobs,publication}.py`; tests `tests/test_batch_jobs.py`, `tests/test_batch_publication.py`.

**Interfaces:**
- `prepare_refresh(selection: BatchSelection, *, settings: BatchSettings, ledger: BatchLedger) -> PreparationReceipt` archives selected files/captured Kafka bounds and runs existing Silver/Gold logic in a new owned namespace. Successful retries reuse pinned receipts.
- `rebuild_receipt(receipt: PreparationReceipt, *, settings: BatchSettings) -> PreparationReceipt` builds a distinct namespace and compares complete output content without publication.
- `publish_certified(client: Any, ledger: BatchLedger, receipt: PreparationReceipt, certification: Certification) -> PublicationReservation` freezes the ledger payload and appends an identical journal row on uncertain retry.
- `resolve_active(client: Any) -> PublicationReservation` validates duplicate/conflicting payloads and returns greatest actually published generation; an empty journal raises a clear unpublished exception.
- Dagster definitions export named refresh, historical_rebuild and source_history jobs; daily partitions; stopped 02:00 UTC schedule; observable source/assets and DbtCliResource events. Historical rebuild config maps every requested day to an explicit receipt URI.

- [ ] Write RED tests `test_receipt_retry_does_not_read_heads`, `test_missing_backfill_mapping_fails`, `test_wrong_day_receipt_fails`, `test_backfill_no_publish`, `test_old_inflight_after_lock_loss_cannot_rollback`, `test_identical_journal_duplicate_collapses`, `test_conflicting_generation_fails`, `test_pending_generation_does_not_hide_active`.
  Publish generation 2 then deliver delayed generation 1: active stays 2. Duplicate generation 2 with same payload: one logical result. Different digest at generation 2: fail. Reserve 3 without inserting: active remains 2. Backfill invokes zero publication operations.
- [ ] Run focused tests; expect missing-interface failures.
- [ ] Implement jobs/resources/publication and real Dagster metadata. Reuse MP-04 transformations and read_committed Kafka boundary capture. Fail closed on incomplete raw/clean reconciliation. Keep all first-ingestion source lineage unchanged.
- [ ] Run real queued Dagster refresh through webserver/daemon/code location; verify graph, source dates, observed/no-op counts and dbt events. Run fault interleavings against PostgreSQL/ClickHouse and prove no older generation rollback; no host Docker socket.
- [ ] Run full suite/diff check and commit `feat: orchestrate batch refreshes with retry-safe publication` with actual implemented architecture status.

### Task 6: Query tools, executable recovery proof and runbook

**Files:** Create `src/marketpulse_batch/{queries,benchmark,cli}.py`, `scripts/batch-serving-smoke-test.sh`, `scripts/verify-batch-serving.py`, `docs/runbooks/batch-serving.md`; tests `tests/test_batch_queries.py`, `tests/test_batch_cli.py`, `tests/test_batch_proof.py`; modify `pyproject.toml`, specs and architecture.

**Interfaces:**
- Frozen `QueryRequest(workload: str, from_date: date, to_date: date, currency: str | None = None, customer_id: str | None = None, limit: int = 20)`.
- `run_query(client: Any, release: PublicationReservation, request: QueryRequest) -> list[dict[str, object]]` supports six specified workloads, parameters values and allowlists identifiers.
- `benchmark_queries(client: Any, release: PublicationReservation, requests: Sequence[QueryRequest], *, warmups: int = 20, samples: int = 100) -> dict[str, object]` uses monotonic client timing, query cache disabled and documented nearest-rank percentiles.
- CLI `marketpulse-batch`: refresh, rebuild with receipt, publish with certified receipt, query, benchmark, verify; all configuration validated before mutation, no arbitrary SQL endpoint. `main(argv: Sequence[str] | None = None) -> int`.

- [ ] Write RED tests `test_query_parameters_not_sql_interpolation`, `test_request_pins_one_release`, `test_currency_separation`, `test_invalid_interval_rejected`, `test_nearest_rank_p95`, `test_uncertain_ack_restart_no_inflation`.
  Assert UTC upper bound excluded; limit <=0 fails; changing journal during multi-query session does not change pinned database; timings 1..100 ms yield p95 95 ms; one lost acknowledgement plus restart preserves exact fixture totals and logical release identity.
- [ ] Run focused tests; expect missing-interface failures.
- [ ] Implement query/benchmark CLI and isolated Compose proof with safe unique project/ports. Reuse existing fixture bootstrap, never reset user's project. Inject partial copy, failed dbt, lost acknowledgement, delayed stale publication and explicit historical rebuild. Persist evidence and validate it independently.
- [ ] Run complete Python suite, Flink Java regression suite, source-fixture validation, actual Docker/Dagster/dbt/ClickHouse proof, restart and full equality/query oracles. Measure 20 warmups/100 samples per workload; require p95 <=500 ms on fixture and disclose actual dataset size/resources. Check `git diff --check` and fresh Git status; scan artifacts for credentials.
- [ ] Write exact startup, refresh/query, Dagster UI, failure retry, evidence inspection and stop commands in runbook. Mark MP-05 complete only with observed proof, update architecture, commit `feat: prove batch serving correctness and recovery`.

## Execution handoff and self-review

Coverage: source/date/provenance and receipts (1); authentication/deployment/ledger
(2); exact snapshot/type/memory/candidate contracts (3); real dbt/equality gates
(4); scheduler/backfill/retry/concurrency/Dagster graph (5); benchmark/restart/
Docker failure proof/runbook (6). Review Focus cases each have owning RED tests.
Interfaces use shared frozen types and preserve MP-04 defaults. Pins remain
candidates until task 2 resolves/builds them; no completion or latency claim is
made by this plan. Proposed native execution keeps the agreed single source tree
and avoids paying for a fresh implementation agent on every task.
