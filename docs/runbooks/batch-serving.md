# MP-05 local batch / serving

Status: runtime bootstrap, operational ledger, pinned snapshot export and isolated
ClickHouse raw copy, dbt models/tests, certification, marketing Dagster graph and
publication implemented. Real queued refresh and isolated historical rebuild
passed; query benchmark and final restart/recovery proof remain in progress.

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
MinIO/Nessie. Replay the source fixture into Kafka and allow Flink's committed
outcomes to catch up. For a complete two-day fixture demo, first archive all
historical files with the existing `spark-lakehouse ingest-files` command so both
days' CRM/report history is represented. Then launch through the configured queue:

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
CLICKHOUSE_HOST_PORT=48123 DAGSTER_HOST_PORT=43000 \
  docker compose -p marketpulse-mp05-proof --profile batch --profile serving \
  up -d --wait clickhouse batch-postgres batch-code dagster-webserver dagster-daemon
docker compose -p marketpulse-mp05-proof --profile batch --profile serving ps
```

Do not omit the overridden ports when recreating that proof project.
Ledger integration tests deliberately require a dedicated database named
`mp05_test`; they reset only its test-owned `marketpulse_ops` schema. Never point
them at the application's operational database. Normal migrations are idempotent
and do not drop tables or volumes.

## Stop without deleting data

```bash
docker compose --profile batch --profile serving stop \
  dagster-daemon dagster-webserver batch-code batch-postgres clickhouse
```

For the proof project add `-p marketpulse-mp05-proof`. Do not use `down -v` unless
you explicitly intend to erase its persistent volumes.
