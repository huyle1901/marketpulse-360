# MP-05 local batch / serving

Status: runtime bootstrap, operational ledger, pinned snapshot export and isolated
ClickHouse raw copy implemented. dbt models, business Dagster assets and
publication/query proof are still in progress.
An empty asset graph at this stage is intentional, not a completed MP-05 demo.

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
MP-04-only Compose configuration. The schedule/business assets are added by the
orchestration task, not by bootstrap. No host Docker socket is mounted.

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
