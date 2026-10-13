# MP-04 local lakehouse runbook

This runbook operates the reproducible Bronze/Silver/Gold proof. It uses an
isolated Docker Compose project, Kafka/Flink for the streaming input, and
Spark/Iceberg/MinIO/Nessie for durable processing. BigQuery is not required.

## Prerequisites

- Docker Desktop with at least 8 GiB of memory available to Docker.
- Docker Compose v2, `jq`, and a free set of the default ports listed below.
- Run commands from the repository root.
- Copy `.env.example` to `.env` and fill `LAKEHOUSE_POSTGRES_PASSWORD` and
  `MINIO_ROOT_PASSWORD` with independently generated local values. MinIO needs at
  least eight characters; `openssl rand -hex 24` can generate each value. Never
  commit `.env` or use a production credential. Compose requires these variables
  even for streaming-only profiles because it interpolates the complete file.

Existing persistent volumes retain their credentials: editing `.env` is not
password rotation. Use the values originally configured for an existing stack,
or perform a deliberate credential rotation. Each smoke run starts a fresh,
isolated project and does not change credentials in other running stacks.

## Rotate an existing local stack without deleting data

Use the exact retained `COMPOSE_PROJECT_NAME` and the same host-port overrides
used when that stack was created. Pause lakehouse writers during the change.
Set new, independently generated passwords in the ignored `.env`, then stop
Nessie while the database credential is changed:

```bash
docker compose --profile lakehouse stop nessie
docker compose --profile lakehouse exec lakehouse-postgres \
  psql -X -U nessie -d nessie -c '\password nessie'
```

At the two prompts, enter the new PostgreSQL password from `.env`. The role/database
names above are the defaults; use the existing role if your stack was initialized
with another username. `psql` encrypts the password before sending the role change,
so do not put the password in SQL text, shell arguments, or documentation.

```bash
docker compose --profile lakehouse up -d --force-recreate --wait \
  lakehouse-postgres minio minio-init nessie
```

This reloads MinIO's root credential and Nessie's PostgreSQL credential and
reuses the existing catalog/object volumes. **Do not use `down --volumes` for
credential rotation.** Verify TCP PostgreSQL login and MinIO access with the new
credentials, reject the former credentials, and re-run the historical lakehouse
`verify` command below. A health check alone does not prove password rotation.

The two retained MP-04 demonstration projects were rotated on 2026-10-13 without
deleting volumes. New PostgreSQL and MinIO credentials were accepted and the
former credentials were rejected in both projects.
The retained full fixture proof still verified all ten historical governed tables
and recovered all thirteen archived source objects byte-for-byte after rotation.

## Run the complete proof

```bash
./scripts/lakehouse-smoke-test.sh
```

The script builds the required images, starts an isolated stack, publishes the
seed-360 fixture twice, waits for Flink output, captures bounded Kafka offsets,
and stops the Flink containers before Spark starts. It then proves idempotent
Bronze ingestion and builds Silver and Gold twice. Time travel reads the recorded
baseline after a later snapshot exists for every governed table. The script
restarts MinIO/Nessie/PostgreSQL, verifies historical checksums and reconciliations,
and runs `backfill-proof` in a
new collision-resistant namespace. A separate fixture oracle compares exact Kafka
key/value bytes and full broker audit fields for all three raw sources, exercises
ordered duplicate/null/binary headers, and retrieves all 13 archived source objects
after restart. It requires paid attribution in all three models, asserts three
anonymous orders with gross revenue of 4,046,000 minor units, and runs a Spark
regression for conflicting raw-lineage click IDs with deterministic winner selection.

Success ends with:

```text
MP-04 lakehouse smoke test passed with 10 verified and rebuilt tables.
```

The expected fixture revenue is:

| Layer | Gross | Refund | Net |
|---|---:|---:|---:|
| MP-03 source | 29,718,000 | 4,967,000 | 24,751,000 |
| Consent-governed Silver/Gold | 27,160,000 | 4,967,000 | 22,193,000 |

First-touch, last-touch, and linear attribution must each equal the governed
totals. The difference from source revenue is intentional consent governance.

## Keep the stack and evidence for a demo

```bash
KEEP_STACK=1 KEEP_EVIDENCE=1 \
SMOKE_EVIDENCE_DIR="$PWD/data/generated/mp04-evidence" \
./scripts/lakehouse-smoke-test.sh
```

The script prints the unique `COMPOSE_PROJECT_NAME`. Export that exact value before
using Compose commands against the retained stack.
`SMOKE_EVIDENCE_DIR` is a parent directory; each invocation creates its own
`mp04-proof.*` child without changing permissions on the parent.

```bash
export COMPOSE_PROJECT_NAME=marketpulse-mp04-smoke-12345
```

Demo endpoints use these defaults:

| Surface | URL | What to show |
|---|---|---|
| Kafka UI | `http://localhost:28080` | raw, clean, and DLQ topics |
| Flink dashboard | `http://localhost:28082` | available during the streaming phase; stopped before Spark starts |
| MinIO console | `http://localhost:29001` | Bronze/Silver/Gold Iceberg objects |
| Nessie API | `http://localhost:29120/api/v2` | persisted catalog/reference state |

The exported `lakehouse-evidence.json` contains embedded and hash-verified input
manifests, baseline/later snapshot IDs, physical schema fingerprints, canonical
rowset checksums, quality counts, revenue reconciliation, and rebuilt checksums.
`fixture-oracle.json` reports independent raw-byte/header samples, recovered source
objects, paid attribution models, click-conflict regression, and anonymous order
revenue. The evidence contains no credentials or real PII.

## Inspect or re-run verification

With the retained project name exported:

```bash
docker compose \
  --profile lakehouse --profile lakehouse-tools \
  run --rm --no-deps spark-lakehouse verify \
  --evidence /opt/marketpulse/evidence/lakehouse-evidence.json
```

`verify` must read every baseline using Iceberg `VERSION AS OF`, prove that the
recorded later snapshot is the current head and descends from the baseline, and
recompute rowsets, quality, source revenue, governed revenue, and attribution.

Do not repeat `backfill-proof` for the same evidence file: its namespace is
deliberately stable, and a second attempt fails closed instead of overwriting the
first proof.

## Cleanup

The normal smoke run cleans up automatically. For a retained stack:

```bash
docker compose \
  --profile streaming --profile flink --profile tools \
  --profile lakehouse --profile lakehouse-tools \
  down --volumes --remove-orphans
```

Confirm `COMPOSE_PROJECT_NAME` first. This command removes only that Compose
project's containers, network, and named volumes; a retained exported evidence
directory remains on the host.

## Troubleshooting

- Port already allocated: override the corresponding `*_HOST_PORT` environment
  variable before running the smoke test.
- Required password not set: fill both fields in the ignored `.env`. Empty fields
  in `.env.example` are intentional; copying the template alone is not enough.
- Authentication fails on an existing stack after editing `.env`: match the
  credentials used to initialize that stack or rotate them explicitly; do not
  delete persistent volumes to work around authentication.
- Flink output does not reach 217: inspect the Kafka UI and the files in the
  evidence directory, then rerun with a fresh isolated project.
- Docker runs out of memory: stop unrelated containers. The script already stops
  the Flink job/task managers before Spark to reduce peak usage.
- `rebuild namespace already exists`: expected for a repeated proof. Create a new
  evidence run ID or remove the disposable isolated smoke project and rerun.
- Catalog verification fails after restart: inspect Nessie and MinIO health, then
  keep the stack with `KEEP_STACK=1` so the failure state remains available.
