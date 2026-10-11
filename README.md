# MarketPulse 360

MarketPulse 360 is an end-to-end marketing data product for proving that a data
engineering solution is correct, performant, and useful. The target system combines
Kafka, Flink, Iceberg on MinIO, ClickHouse, PostgreSQL, Dagster, dbt, FastAPI, and
Next.js in a Docker-based local environment. BigQuery/BigLake remains an optional
cloud benchmark rather than a runtime dependency.

The repository currently implements **MP-01: reproducible omnichannel mock data**
and **MP-02: Kafka ingestion and replay controls**. It generates coherent data
shaped like Google Ads, Meta Ads, TikTok Ads, web/mobile analytics, CRM, commerce,
and email systems, then replays event-shaped sources to versioned Kafka topics.
Batch-shaped sources remain inputs to the later Dagster milestone.

## Current proof

- 118 automated tests cover mock-data correctness plus Kafka contracts, bounded
  replay ordering, pacing, idempotent delivery, registry behavior, CLI behavior,
  and Docker assets.
- The committed fixture contains 615 rows in 22 compressed files.
- The validated `demo` profile produces 1,200,225 rows in 130 files (about 90 MB)
  for seed `360`.
- Every run writes a manifest with configuration, file hashes, row counts, time
  bounds, source totals, and injected anomaly counts.
- A real Docker smoke run delivered and consumed all 223 replay records: 199 web,
  14 order, 6 email, and 4 validation-DLQ records.

## Quick start

Python 3.12 or newer is required. The generator has no runtime dependencies.

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

The smoke test uses an isolated Compose project and cleans it up. For manual
replay, contract registration, pause/resume, UI inspection, and reset commands,
follow the [local Kafka runbook](docs/runbooks/kafka-local.md).

## Documentation map

- [High-level architecture](docs/architecture/high-level-design.md)
- [Ten-story delivery backlog](specs/README.md)
- [Mock-data contract](specs/01-mock-data-foundation.md)
- [Mock-data operations guide](data/mock/README.md)
- [Local Kafka replay runbook](docs/runbooks/kafka-local.md)
- [Kafka ingestion and replay specification](specs/02-kafka-ingestion-replay.md)

## Data boundaries

The generated source data represents business activity: campaigns, clicks,
sessions, customers, messages, orders, and refunds. It is input to the data
engineering pipeline.

The future application's operational data is different. Accounts, roles, saved
segments, alert rules, annotations, and configuration are created by application
users and will live in PostgreSQL. Analytical results shown by the application will
come from ClickHouse and Iceberg, not from that operational database.
