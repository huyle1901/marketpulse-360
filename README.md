# MarketPulse 360

MarketPulse 360 is an end-to-end marketing data product for proving that a data
engineering solution is correct, performant, and useful. The target system combines
Kafka, Flink, Iceberg on MinIO, ClickHouse, PostgreSQL, Dagster, dbt, FastAPI, and
Next.js in a Docker-based local environment. BigQuery/BigLake remains an optional
cloud benchmark rather than a runtime dependency.

The repository currently implements **MP-01: reproducible omnichannel mock data**.
It generates coherent data shaped like Google Ads, Meta Ads, TikTok Ads, web/mobile
analytics, CRM, commerce, and email systems. Later stories replay event-shaped data
to Kafka and ingest batch-shaped sources through Dagster.

## Current proof

- 69 automated tests cover configuration, deterministic IDs, cross-source journeys,
  file rotation, checksums, CLI behavior, and semantic validation.
- The committed fixture contains 615 rows in 22 compressed files.
- The validated `demo` profile produces 1,200,225 rows in 130 files (about 90 MB)
  for seed `360`.
- Every run writes a manifest with configuration, file hashes, row counts, time
  bounds, source totals, and injected anomaly counts.

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

## Documentation map

- [High-level architecture](docs/architecture/high-level-design.md)
- [Ten-story delivery backlog](specs/README.md)
- [Mock-data contract](specs/01-mock-data-foundation.md)
- [Mock-data operations guide](data/mock/README.md)

## Data boundaries

The generated source data represents business activity: campaigns, clicks,
sessions, customers, messages, orders, and refunds. It is input to the data
engineering pipeline.

The future application's operational data is different. Accounts, roles, saved
segments, alert rules, annotations, and configuration are created by application
users and will live in PostgreSQL. Analytical results shown by the application will
come from ClickHouse and Iceberg, not from that operational database.
