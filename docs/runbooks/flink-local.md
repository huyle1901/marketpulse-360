# Local Flink Processing Runbook

This guide runs and verifies MP-03: a Java 17 Flink `2.2.1` job that consumes the
three MP-02 raw Kafka topics, validates and deduplicates events, maintains
event-time sessions and campaign KPIs, and writes six exactly-once Kafka outputs.
Run every command from the repository root.

## Prerequisites and endpoints

- Docker Desktop or Docker Engine with Compose v2.
- At least 4 GB of memory available to Docker.
- Ports `9092`, `8080`, `8081`, and `8082` available.

| Local endpoint | Purpose |
|---|---|
| `localhost:9092` | Kafka clients on the host |
| <http://localhost:8080> | Kafbat Kafka UI |
| <http://localhost:8081> | Apicurio Registry API |
| <http://localhost:8082> | Flink Web UI |

## Start the stack and submit the job

Start the persistent services first, then submit the named job once:

```bash
docker compose --profile streaming --profile flink up -d --build --wait \
  kafka schema-registry kafka-ui flink-jobmanager flink-taskmanager

docker compose --profile streaming --profile flink run --rm flink-job-submit
```

The submitter is idempotent. Running it again reports the existing
`marketpulse-stream-v1` job instead of creating another copy.

Check the containers and Flink job:

```bash
docker compose --profile streaming --profile flink ps
curl -fsS http://localhost:8082/jobs/overview
```

The job must be `RUNNING`, with one TaskManager and two occupied slots visible in
the Flink UI.

## Register contracts and replay data

Register all ten MP-02/MP-03 JSON Schema contracts, then replay the valid fixture:

```bash
docker compose --profile streaming --profile flink --profile tools run --rm replay \
  kafka register-contracts --registry-url http://schema-registry:8080

docker compose --profile streaming --profile flink --profile tools run --rm replay \
  kafka replay \
  --input /app/data/mock/fixtures/seed-360 \
  --bootstrap-servers kafka:29092 \
  --events-per-second 20
```

The replay summary must report `219` delivered records. Replaying the fixture a
second time appends raw copies, but the Flink deduplication state prevents KPI and
clean-event inflation.

## Inspect topics and checkpoints

Use Kafbat at <http://localhost:8080> or list the output topics from the CLI:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --list
```

MP-03 writes:

```text
marketing.clean.customer-events.v1
marketing.clean.sessions.v1
marketing.gold.funnel-metrics.v1
marketing.gold.revenue-metrics.v1
marketing.gold.campaign-metrics.v1
marketing.dlq.flink-validation.v1
```

The clean topics retain seven days. Session and Gold topics are keyed upsert
streams with compact-plus-delete cleanup. Kafka record timestamps use publish
time for retention; business `event_time` remains inside each JSON payload and is
used by Flink watermarks.

Completed checkpoints appear under the job's **Checkpoints** tab in the Flink UI.
They are stored in the `flink_checkpoints` named volume, so restarting only the
TaskManager preserves recovery state.

## Demonstrate TaskManager recovery manually

While a slow replay is active, wait for a completed checkpoint and restart the
TaskManager:

```bash
docker compose restart flink-taskmanager
```

The job briefly enters `RESTARTING`, returns to `RUNNING`, restores the latest
checkpoint, and creates a newer completed checkpoint. Do not remove the checkpoint
volume during this test.

## One-command recovery proof

```bash
./scripts/flink-smoke-test.sh
```

The script creates an isolated Compose project and performs the complete proof:

1. Builds a clean Kafka/Flink stack and registers all contracts twice.
2. Replays 219 fixture records at 20 events/second.
3. Leaves the producer active, captures a checkpoint mid-replay, and restarts the
   TaskManager while raw Kafka offsets continue to advance.
4. Replays the same fixture to prove restored deduplication state.
5. Injects one malformed record and one event beyond the correction horizon.
6. Reads all six outputs with Kafka `read_committed` isolation.
7. Verifies exact event IDs, 73 full session contracts, every campaign/day
   funnel/revenue snapshot, the exact late probe, deterministic DLQ lineage, raw
   input progress, Flink accepted/duplicate/late/invalid counters, and checkpoint
   IDs before and after recovery. Every consumed value is also checked against its
   committed JSON Schema.

A passing run reports 217 unique fixture event IDs, two duplicate copies, gross
revenue `29,718,000`, refunds `4,967,000`, and net revenue `24,751,000` minor
units. The latest reference run advanced raw input from 83 to 219 records across
checkpoint `18` to `19` and recovered in 16 seconds. The isolated stack is removed
automatically. Use
`KEEP_STACK=1 KEEP_EVIDENCE=1 ./scripts/flink-smoke-test.sh` only for diagnosis.

## Stop or reset the manual stack

Preserve Kafka and checkpoint volumes:

```bash
docker compose --profile streaming --profile flink down
```

Reset only this Compose project's local Kafka and Flink state:

```bash
docker compose --profile streaming --profile flink down --volumes --remove-orphans
```

The reset does not modify committed fixtures or locally generated source files.
