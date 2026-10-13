# MarketPulse 360

MarketPulse 360 is an end-to-end marketing data product for proving that a data
engineering solution is correct, performant, and useful. The target system combines
Kafka, Flink, Iceberg on MinIO, ClickHouse, PostgreSQL, Dagster, dbt, FastAPI, and
Next.js in a Docker-based local environment. BigQuery/BigLake remains an optional
cloud benchmark rather than a runtime dependency.

The repository implements **MP-01: reproducible omnichannel mock data**,
**MP-02: Kafka ingestion and replay controls**, and **MP-03: stateful Flink stream
processing with checkpoint recovery**, **MP-04: governed Iceberg lakehouse**, and
**MP-05: Dagster/dbt orchestration and certified ClickHouse serving**.
The project generates coherent data
shaped like Google Ads, Meta Ads, TikTok Ads, web/mobile analytics, CRM, commerce,
and email systems, then replays event-shaped sources to versioned Kafka topics.
Dagster ingests batch sources, builds pinned lakehouse snapshots and publishes
validated analytical releases for the future application.

## Current proof

- The Python suite and 30 Java tests cover mock-data correctness, Kafka contracts, bounded
  replay ordering, pacing, idempotent delivery, registry behavior, CLI behavior,
  Docker assets, raw-event validation, checkpointed deduplication, late events,
  sessions, campaign KPIs, all six output schemas, Kafka serde, and the executable
  Flink graph, immutable Bronze ingestion, governed Silver, marketing Gold models,
  physical schemas, historical snapshot evidence, and isolated rebuilds.
- The committed fixture contains 615 rows in 22 compressed files.
- The validated `demo` profile produces 1,200,225 rows in 130 files (about 90 MB)
  for seed `360`.
- Every run writes a manifest with configuration, file hashes, row counts, time
  bounds, source totals, and injected anomaly counts.
- A real Docker smoke run delivered and consumed all 223 replay records: 199 web,
  14 order, 6 email, and 4 validation-DLQ records.
- The MP-03 recovery smoke test restarted a TaskManager while the producer remained
  active, advanced raw Kafka input from 83 to 219 records across recovery, retained
  217 unique event IDs and 73 session keys, rejected two duplicate copies, and
  reconciled net revenue to `24,751,000` minor units.
- The MP-04 Docker proof archives 439 raw Kafka records and 217 clean events,
  verifies and rebuilds all ten Silver/Gold tables after catalog/storage restart,
  and retrieves all 13 archived source objects byte-for-byte. Each attribution
  model reconciles to governed net revenue of `22,193,000` minor units; all three
  contain paid-click attribution. A Spark regression checks conflicting recovered
  click IDs, including nulls, without changing deterministic winner selection.
- MP-05 runs a queued Dagster refresh and isolated historical rebuild, builds
  eight dbt models with 28 tests, and reconciles seven ClickHouse relations to
  pinned export rows. Six query workloads pass independent Python oracles and
  20-warmup/100-sample benchmarks. A server restart and lost acknowledgement
  preserve release identity, row counts and exact financial totals. See the
  [batch/serving runbook](docs/runbooks/batch-serving.md) for scope and evidence.

## Quick start

Python 3.12 or newer is required. The generator has no runtime dependencies.

Before any Docker Compose command, copy `.env.example` to `.env` and set
`LAKEHOUSE_POSTGRES_PASSWORD` and `MINIO_ROOT_PASSWORD` to independently generated
local values (for example, generate each with `openssl rand -hex 24`). MinIO requires
at least eight characters. Compose fails if either value is missing or empty;
there is no built-in password, including when resolving streaming-only profiles.
Keep `.env` local: it is ignored by Git and excluded from image builds. Commit only
the empty template. Each teammate creates their own values for their own stack.

```bash
cp .env.example .env
# Edit .env and fill both password fields before starting Docker.
```

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .

marketpulse-mock profiles
marketpulse-mock validate --input data/mock/fixtures/seed-360
marketpulse-mock generate \
  --profile demo \
  --seed 360 \
  --output data/generated/demo-seed-360
marketpulse-mock validate --input data/generated/demo-seed-360
```

Use `--overwrite` only when intentionally replacing the exact output directory.
Generated demo and benchmark outputs are ignored by Git.

Run all tests with:

```bash
python -m unittest discover -s tests -v
```

Start and test the local streaming slice with:

```bash
docker compose --profile streaming up -d --build --wait
docker compose --profile streaming ps
./scripts/kafka-smoke-test.sh
```

Start Kafka and the stateful Flink job together with:

```bash
docker compose --profile streaming --profile flink up -d --build --wait
docker compose --profile streaming --profile flink ps
curl -fsS http://localhost:8082/jobs/overview
```

The Flink dashboard is at <http://localhost:8082>; Kafka UI remains at
<http://localhost:8080>. The one-shot submitter is idempotent: it detects the named
job in every non-terminal state, including `RESTARTING`, instead of creating a
second job.

The smoke test uses an isolated Compose project and cleans it up. For manual
replay, contract registration, pause/resume, UI inspection, and reset commands,
follow the [local Kafka runbook](docs/runbooks/kafka-local.md). For Flink job
submission, checkpoint recovery, output inspection, and the MP-03 proof, follow
the [local Flink runbook](docs/runbooks/flink-local.md).

Run the complete isolated MP-03 recovery proof with:

```bash
./scripts/flink-smoke-test.sh
```

Run the MP-04 Kafka/Flink → Bronze → Silver → Gold proof, including historical
snapshot verification, storage/catalog restart, and an isolated rebuild, with:

```bash
./scripts/lakehouse-smoke-test.sh
```

The [lakehouse runbook](docs/runbooks/lakehouse-local.md) describes retained
evidence, inspection commands, demo URLs, and cleanup.

For MP-05, initialize the additional local passwords and build/start the batch
runtime using the [batch/serving runbook](docs/runbooks/batch-serving.md). Dagster
defaults to <http://localhost:3000> and authenticated ClickHouse HTTP to port 8123.
The retained isolated development proof uses ports 43000 and 48123 respectively.
The application UI/API remain later milestones.

## Documentation map

- [High-level architecture](docs/architecture/high-level-design.md)
- [Ten-story delivery backlog](specs/README.md)
- [Mock-data contract](specs/01-mock-data-foundation.md)
- [Mock-data operations guide](data/mock/README.md)
- [Local Kafka replay runbook](docs/runbooks/kafka-local.md)
- [Local Flink processing runbook](docs/runbooks/flink-local.md)
- [Batch orchestration and ClickHouse runbook](docs/runbooks/batch-serving.md)
- [Kafka ingestion and replay specification](specs/02-kafka-ingestion-replay.md)
- [Stateful Flink processing specification](specs/03-stateful-flink-processing.md)

## Data boundaries

The generated source data represents business activity: campaigns, clicks,
sessions, customers, messages, orders, and refunds. It is input to the data
engineering pipeline.

The future application's operational data is different. Accounts, roles, saved
segments, alert rules, annotations, and configuration are created by application
users and will live in PostgreSQL. Analytical results shown by the application will
come from ClickHouse and Iceberg, not from that operational database.
