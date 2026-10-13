# MP-05 local batch / serving

Status: runtime bootstrap, operational ledger, pinned snapshot export and isolated
ClickHouse raw copy, dbt models/tests, certification, marketing Dagster graph and
publication implemented. Real queued refresh, isolated historical rebuild,
six independent query oracles, benchmark and restart/lost-acknowledgement proof
passed locally. The final review's Important findings were fixed with regression
tests and actual native backfill/re-execution checks. Remote GitGuardian/CI remain
separate release checks.

## Build and start

Use Docker Compose v2+ with Docker running and a Python 3.12+ environment.
Keep the MP-04 MinIO/Nessie passwords already in ignored `.env`. Initialize only
missing MP-05 passwords (existing nonempty values are never rotated):

```bash
PYTHONPATH=src python -m marketpulse_batch.local_env --path .env
bash scripts/build-batch-runtime.sh
docker compose --profile batch --profile serving up -d --wait \
  clickhouse batch-postgres batch-code dagster-webserver dagster-daemon
```

The build script first builds the local Spark base; it is not a public Docker Hub
image. Python transitive dependencies are pinned with hashes in
`docker/batch/requirements.lock`. The mandatory upstream images remain version-pinned.
Default UI: http://localhost:3000. Default ClickHouse HTTP: localhost:8123,
authenticated with CLICKHOUSE_USER/password from `.env`; native port 9000 is not
published. PostgreSQL and code-location gRPC remain internal. Never paste secrets
into command history or print an interpolated Compose config.

Missing batch/serving passwords fail service startup; they do not block an
MP-04-only Compose configuration. The daily schedule starts stopped; marketing
assets are visible in Dagster. No host Docker socket is mounted.

## Refresh a fixture partition

Source services must also be running: Kafka/topics/registry, the Flink job, and
MinIO/Nessie. The following Bash commands bootstrap a **fresh** fixture project.
If using a named project, set `COMPOSE_PROJECT_NAME` and port overrides before all
commands, including build/start/replay. Do not submit a second copy of an already
running Flink job. Its dashboard must show `marketpulse-stream-v1` running.

```bash
dc=(docker compose --profile streaming --profile flink --profile tools \
  --profile lakehouse --profile lakehouse-tools --profile batch --profile serving)
"${dc[@]}" up -d --build --wait kafka schema-registry \
  flink-jobmanager flink-taskmanager lakehouse-postgres minio minio-init nessie
"${dc[@]}" run --rm --no-deps flink-job-submit
"${dc[@]}" run --rm --no-deps replay kafka register-contracts \
  --registry-url http://schema-registry:8080
"${dc[@]}" run --rm --no-deps replay kafka replay \
  --input /app/data/mock/fixtures/seed-360 --bootstrap-servers kafka:29092 \
  --events-per-second 100 --include-dlq
```

Wait for the Flink dashboard's raw source to process 219 records and complete a
subsequent checkpoint. The replay includes four extra producer-DLQ records
(223 delivered in total). Verify committed clean events before proceeding:

```bash
"${dc[@]}" run --rm --no-deps -T --entrypoint python replay -c '
import tempfile
from pathlib import Path
from marketpulse_mock import flink_smoke_verify as verifier
verifier.OUTPUT_TOPICS = ("marketing.clean.customer-events.v1",)
with tempfile.TemporaryDirectory() as root:
    counts = verifier.consume_outputs("kafka:29092", Path(root))
assert counts[verifier.OUTPUT_TOPICS[0]] == 217, counts
print(counts)
'
"${dc[@]}" run --rm --no-deps spark-lakehouse ingest-files \
  --run-id mp05-bootstrap-source-history \
  --input /opt/marketpulse/data/mock/fixtures/seed-360
```

The first archive contains 12 files/380 rows; repeat ingestion reports no-op rows.
It retains both days' CRM/report history. Launch through the configured queue:

```bash
docker compose --profile batch exec -T dagster-webserver \
  dagster job launch -w /opt/marketpulse/workspace.yaml \
  -j marketing_refresh_job --tags '{"dagster/partition":"2026-01-02"}'
```

The graph shows source/master/ad/CRM/Kafka ingestion, pinned Bronze, Silver/Gold,
snapshot export, seven raw copies, eight dbt models/checks, certification and
active publication. Failed/partial candidates never enter active queries.
An identical refresh reuses its recorded certified bundle and dbt invocation;
Dagster labels these results as reused, not newly executed tests. Serving rows are
re-verified before the existing generation is acknowledged again.
Preparation receipts and per-run results are persisted under `batch_state`.
Use `historical_rebuild_job` with explicit
`ops.pinned_bronze.config.receipt_mapping` mapping each partition date to its
recorded `file:///.../receipt.json` URI. Missing mappings fail; there is no implicit
latest receipt. Historical jobs do not publish. Model-version mismatch fails
rather than rebuilding an old receipt with different transformation code.

## Current isolated developer proof

The active proof uses project `marketpulse-mp05-proof`, Dagster port 43000 and
ClickHouse HTTP port 48123, keeping previous projects untouched:

```bash
export COMPOSE_PROJECT_NAME=marketpulse-mp05-proof
export CLICKHOUSE_HOST_PORT=48123 DAGSTER_HOST_PORT=43000
export KAFKA_HOST_PORT=49192 REGISTRY_HOST_PORT=48181 KAFKA_UI_HOST_PORT=48180
export FLINK_HOST_PORT=48182 MINIO_API_HOST_PORT=49100 MINIO_CONSOLE_HOST_PORT=49101
export NESSIE_HOST_PORT=49220
docker compose --profile batch --profile serving \
  up -d --wait clickhouse batch-postgres batch-code dagster-webserver dagster-daemon
docker compose -p marketpulse-mp05-proof --profile batch --profile serving ps
```

Do not omit the overridden ports when recreating that proof project.
Ledger integration tests deliberately require a dedicated database named
`mp05_test`; they reset only its test-owned `marketpulse_ops` schema. Never point
them at the application's operational database. Normal migrations are idempotent
and do not drop tables or volumes.

## Query and benchmark

After the queued run has status `SUCCESS` in Dagster, query the active release:

```bash
docker compose --profile batch run --rm --no-deps batch-code \
  python3 -m marketpulse_batch.cli query campaign_overview \
  --from-date 2026-01-01 --to-date 2026-01-03 --currency VND
docker compose --profile batch run --rm --no-deps batch-code \
  python3 -m marketpulse_batch.cli verify \
  --from-date 2026-01-01 --to-date 2026-01-03 \
  --output /opt/marketpulse/batch-state/benchmark.json
```

Six allowlisted workloads: `campaign_overview`, `daily_funnel`,
`attribution_comparison`, `cohort_retention`, `customer_drill`, `top_campaigns`.
`customer_drill` requires `--customer-id` from the synthetic source fixture.
Intervals are UTC `[from,to)`. Funnel results are campaign-level rows; session
counts must not be summed across campaigns as if globally unique. Cohorts filter
`activity_month`. Customer drill filters `last_activity_at` and explicitly labels
money as `snapshot_lifetime`; it is not a period-only customer revenue report.
Campaign money groups by currency. Attribution/customer schemas lack currency,
so those workloads require a single-currency release and reject mixed-currency
data. No implicit conversion or summing different currencies is performed.

`verify`/`benchmark` reconcile all six queries against independent Python
aggregations of validated pinned export objects, then run 20 warmups and 100
measured queries each with ClickHouse query cache disabled. Evidence includes
release IDs, source counts/snapshots, exact money, timings, failures, software
versions and visible container resources. Percentiles use nearest rank and
include client request/decoding. A failed oracle or fixture p95 >500 ms fails.

## Retry, historical rebuild and recovery proof

Repeat the same queued refresh to verify receipt/certified-bundle reuse. For a
historical rebuild, copy the original `receipt_uri` from its run's
`candidate.json` under `/opt/marketpulse/batch-state/dagster-runs/<run-id>/` into
Dagster launch config (replace the URI below with that recorded value):

```bash
docker compose --profile batch exec -T dagster-webserver \
  dagster job launch -w /opt/marketpulse/workspace.yaml -j historical_rebuild_job \
  --tags '{"dagster/partition":"2026-01-02"}' \
  --config-json '{"ops":{"pinned_bronze":{"config":{"receipt_mapping":{"2026-01-02":"file:///opt/marketpulse/batch-state/REPLACE_WITH_RECORDED_RECEIPT.json"}}}}}'
```

Failed copy attempts remain isolated; a retry allocates a new database. Failed
dbt/certification never publishes. Inspect Dagster logs and persisted artifacts
before retrying; do not alter a sealed candidate or manually edit the journal.
The historical job excludes publication. Promotion is a separate explicit
`publish --receipt <path> --certification <path>` operation and rechecks contents.

All daily stages use one partition per backfill run. In Dagster, use
`source_history_job` partition backfills for **first-time ingestion** without
receipts. For asset backfills of already prepared dates, supply the explicit
receipt mapping; these run in receipt-only mode and skip promotion. Re-execution
from dbt/certification/publication uses retained artifacts from the exact retry
parent chain. Keep the original run metadata and `batch_state` volume. A
standalone dbt materialization with no upstream candidate or retry ancestry must
include its upstream assets; it does not select an arbitrary latest artifact.

With the port exports above, run the executable recovery proof against the
existing **test-owned** project:

```bash
MP05_PROJECT=marketpulse-mp05-proof bash scripts/batch-serving-smoke-test.sh
PYTHONPATH=src python3 scripts/verify-batch-serving.py \
  .lakehouse/mp05-proof/before.json .lakehouse/mp05-proof/after.json
```

This command injects acknowledgement loss using a dedicated `mp05_test` ledger
and test journal, terminates that publisher with `acknowledged_at` still null,
restarts the five batch/serving services, and repairs the same reservation in a
new publisher process. It verifies one publication row and reruns all six
oracle/benchmark workloads. It compares
the release, snapshots, row counts and exact financial totals before/after.
The test database's test-owned schema is reset; application operational metadata
is retained. Bootstrap and source replay are separate steps above so recovery
never silently reseeds a running project. Logs/evidence stay under ignored
`.lakehouse/mp05-proof/`. Set `MP05_PYTHON` if Python 3.12+ has another path.

Observed fixture: 22 source files/615 rows; seven copied relations contain
13 campaign-performance, 12 funnel, 27 attribution, 21 cohort, 60 customer-360,
6 campaign-dimension and 100 customer-dimension rows. Governed VND totals are
27,160,000 gross − 4,967,000 refunds = 22,193,000 net, with 551,737 ad spend.
Local fixture p95 was under 10 ms for each workload; consult the generated report
for current measured values. Docker had 15 CPUs/about 15.6 GiB available; batch
code is limited to 4 GiB and ClickHouse to 2 GiB. This fixture validates correctness
and recovery; MP-10 owns capacity testing.

## Stop without deleting data

```bash
docker compose --profile batch --profile serving stop \
  dagster-daemon dagster-webserver batch-code batch-postgres clickhouse
```

For the proof project add `-p marketpulse-mp05-proof`. Do not use `down -v` unless
you explicitly intend to erase its persistent volumes.
