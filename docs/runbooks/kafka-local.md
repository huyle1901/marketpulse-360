# Local Kafka Replay Runbook

This guide starts and verifies the implemented MP-02 slice: Apache Kafka in
single-node KRaft mode, four versioned topics, Apicurio Registry, Kafbat UI, and
the MarketPulse replay tool. All commands run from the repository root.

## Prerequisites and ports

- Docker Desktop or Docker Engine with Compose v2.
- Ports `9092`, `8080`, and `8081` available.
- The committed fixture at `data/mock/fixtures/seed-360`.

| Local endpoint | Purpose |
|---|---|
| `localhost:9092` | Kafka clients running on the host |
| <http://localhost:8080> | Kafbat Kafka UI |
| <http://localhost:8081> | Apicurio Registry API |

The containers use `kafka:29092` for Kafka and `schema-registry:8080` for
Registry. Do not use the host addresses from one container to reach another.

## Start and check the stack

```bash
docker compose --profile streaming up -d --build --wait
docker compose --profile streaming ps
```

`kafka`, `schema-registry`, and `kafka-ui` should show `healthy`.
`topic-init` should show `exited (0)` because it is a one-shot initializer.

List and describe topics:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --list

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --describe \
  --topic marketing.raw.web-events.v1
```

The four project topics must each have six partitions and replication factor one:

- `marketing.raw.web-events.v1`
- `marketing.raw.order-events.v1`
- `marketing.raw.email-events.v1`
- `marketing.dlq.validation.v1`

## Register data contracts

The command is idempotent. The first run reports four created artifacts; the
second reports four unchanged artifacts.

```bash
docker compose --profile streaming run --rm replay kafka register-contracts \
  --registry-url http://schema-registry:8080

docker compose --profile streaming run --rm replay kafka register-contracts \
  --registry-url http://schema-registry:8080

curl -fsS http://localhost:8081/apis/registry/v3/groups/marketpulse/artifacts
```

## Replay the fixture

Use exactly one pacing mode. Every command prints a JSON summary after all
delivery callbacks and the final producer flush complete.

Accelerated event time (1,000 simulated seconds per real second):

```bash
docker compose --profile streaming run --rm replay kafka replay \
  --input /app/data/mock/fixtures/seed-360 \
  --bootstrap-servers kafka:29092 \
  --speed 1000 \
  --include-dlq
```

Fixed rate:

```bash
docker compose --profile streaming run --rm replay kafka replay \
  --input /app/data/mock/fixtures/seed-360 \
  --bootstrap-servers kafka:29092 \
  --events-per-second 50 \
  --include-dlq
```

No-sleep correctness/throughput run:

```bash
docker compose --profile streaming run --rm replay kafka replay \
  --input /app/data/mock/fixtures/seed-360 \
  --bootstrap-servers kafka:29092 \
  --no-sleep \
  --include-dlq
```

Omitting all pacing flags defaults to `--speed 1000`. Omitting `--include-dlq`
publishes only the 219 valid web, order, and email source events.

## Pause, resume, and stop

Run a slow replay in terminal A:

```bash
docker compose --profile streaming run --rm replay kafka replay \
  --input /app/data/mock/fixtures/seed-360 \
  --bootstrap-servers kafka:29092 \
  --speed 100 \
  --include-dlq
```

In terminal B, find that one-off replay container and send `USR1`. Sending the
same signal again resumes without a catch-up burst. `INT` requests a graceful
stop and producer flush.

```bash
replay_container=$(docker ps \
  --filter label=com.docker.compose.service=replay \
  --format '{{.ID}}' | head -n 1)
docker kill --signal=USR1 "${replay_container}"  # pause
docker kill --signal=USR1 "${replay_container}"  # resume
docker kill --signal=INT "${replay_container}"   # graceful stop
```

## Inspect and consume

Open <http://localhost:8080>, select `marketpulse-local`, and inspect Topics,
Messages, keys, payloads, and headers. Consume a small sample from the CLI with:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic marketing.raw.web-events.v1 \
  --from-beginning \
  --max-messages 5
```

For the first replay into an empty Kafka volume, expected counts are 199 web,
14 order, 6 email, and 4 DLQ records. A normal replay appends another deterministic
copy; stable event IDs let downstream MP-03 deduplication prevent KPI inflation.

## One-command smoke test

```bash
./scripts/kafka-smoke-test.sh
```

The script uses its own Compose project, deletes only that project's prior smoke
volume, starts healthy services, initializes topics twice, registers contracts
twice, replays the fixture, asserts all four exact topic counts, and cleans up.
Use `KEEP_STACK=1 ./scripts/kafka-smoke-test.sh` only when you want to inspect its
containers afterward.

## Stop or reset the manual stack

Stop containers but preserve the Kafka named volume:

```bash
docker compose --profile streaming down
```

Reset the local Kafka log as well. This permanently removes only the
`marketpulse-360` Compose project's named volume; the committed fixture is not
affected.

```bash
docker compose --profile streaming down --volumes --remove-orphans
```

If startup reports that a port is already allocated, inspect the owner before
stopping it:

```bash
docker ps --format 'table {{.Names}}\t{{.Ports}}'
```
