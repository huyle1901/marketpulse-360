# MP-02 - Kafka Ingestion and Replay Controls

## Intent

MP-02 turns the deterministic event files from MP-01 into an observable local
streaming source. A reviewer must be able to start Kafka with Docker, replay the
same fixture at a human-visible or benchmark rate, inspect messages and schemas,
pause/resume the replay, and prove that invalid records reach a DLQ without changing
the valid event totals.

## Scope

### Included

- Single-broker Apache Kafka `4.3.1` in KRaft combined mode for local development.
- Kafbat UI `1.5.0` for topics, messages, consumer groups, and broker inspection.
- Apicurio Registry `3.3.3` for versioned JSON Schema artifacts.
- Topic bootstrap with explicit partitions, retention, and local replication factor.
- Streaming readers for MP-01 `web_events`, `orders`, and `email_events` files.
- DLQ publication for MP-01 `quality_cases` with deterministic reason codes.
- Relative event-time speed, fixed events/second, and no-sleep benchmark modes.
- Pause/resume through `SIGUSR1`, graceful `SIGINT` shutdown, delivery accounting,
  and machine-readable replay summaries.
- Idempotent Kafka producer configuration and stable record keys/event IDs.
- Unit tests without Docker plus a real Docker integration smoke test.

### Excluded

- CRM snapshots and ad-platform reports remain batch inputs for Dagster in MP-05.
- Flink validation, deduplication, watermarking, and sinks belong to MP-03.
- Kafka authentication, TLS, multi-broker replication, and production ACLs are
  documented production concerns, not laptop-demo requirements.
- Replaying a run intentionally publishes the same stable event IDs again. MP-03
  proves downstream deduplication; Kafka producer idempotence only protects retries
  within a producer session.

## Local topology

| Service | Image | Port | Responsibility |
|---|---|---:|---|
| `kafka` | `apache/kafka:4.3.1` | `9092` | KRaft broker/controller with external and Docker listeners |
| `kafka-ui` | `ghcr.io/kafbat/kafka-ui:v1.5.0` | `8080` | Local operations UI |
| `schema-registry` | `apicurio/apicurio-registry:3.3.3` | `8081` | JSON Schema artifacts and versions |
| `topic-init` | `apache/kafka:4.3.1` | none | Idempotently creates the MP-02 topics |

The broker uses replication factor `1` locally and six partitions for raw event
topics. Production guidance remains three or more brokers with replication factor
`3`.

## Topic contract

| Dataset | Topic | Record key | Schema artifact |
|---|---|---|---|
| `web_events` | `marketing.raw.web-events.v1` | `customer_id`, else `session_id` | `web-events-v1` |
| `orders` | `marketing.raw.order-events.v1` | `customer_id`, else `order_id` | `order-events-v1` |
| `email_events` | `marketing.raw.email-events.v1` | `customer_id`, else `message_id` | `email-events-v1` |
| `quality_cases` | `marketing.dlq.validation.v1` | `case_id` | `validation-dlq-v1` |

Every valid Kafka value is the canonical JSON object from the MP-01 gzip file,
encoded as UTF-8 with stable compact separators. Headers carry `dataset`,
`schema-version=1`, `event-id`, and `source-file`. DLQ values contain `case_id`,
`event_time`, `source_dataset`, `source_topic`, `reason_code`, and `failed_record`.

## Reading and ordering

The reader verifies `manifest.json` before replay. It opens only the replayable files
listed by the manifest and performs a heap merge with at most one buffered row per
open part. Output is nondecreasing by `event_time`; ties use dataset, relative path,
and row ordinal for deterministic order. Quality cases are separately merged when
DLQ replay is enabled.

Kafka only guarantees ordering within a partition. Stable identity keys ensure all
events for one customer/session/order use the same partition, so their order is
preserved even though different keys may be processed concurrently later.

## Pacing modes

Exactly one mode is active:

- `--speed N`: replay event-time gaps `N` times faster than real time. `1` means
  real time and `1000` means a 1,000x accelerated timeline.
- `--events-per-second N`: target a fixed average rate from replay start.
- `--no-sleep`: publish as fast as acknowledgements/backpressure allow.

All numeric rates must be positive. Scheduling uses monotonic time and injected
clock functions so tests never sleep. `SIGUSR1` toggles paused/running state; paused
time does not count toward pacing or throughput.

## Producer guarantees

The real adapter uses `confluent-kafka` with:

- `enable.idempotence=true`
- `acks=all`
- `retries` left effectively unlimited by the client
- bounded `delivery.timeout.ms`
- `client.id=marketpulse-replay`

Delivery callbacks count acknowledged and failed records. Queue-full backpressure
polls the producer and retries the same record. A final flush timeout or any failed
delivery returns a non-zero CLI exit code.

## CLI contract

```text
marketpulse-mock kafka topics --bootstrap-servers localhost:9092
marketpulse-mock kafka register-contracts --registry-url http://localhost:8081
marketpulse-mock kafka replay --input data/mock/fixtures/seed-360 \
  --bootstrap-servers localhost:9092 --speed 1000 --include-dlq
```

`replay` prints its PID and selected pacing mode, then emits a final JSON summary:

```json
{
  "attempted": 0,
  "delivered": 0,
  "failed": 0,
  "dlq": 0,
  "duration_seconds": 0.0,
  "events_per_second": 0.0,
  "first_event_time": null,
  "last_event_time": null,
  "topic_counts": {}
}
```

## Acceptance evidence

1. `docker compose --profile streaming up -d` makes Kafka, Kafbat UI, and Apicurio
   healthy and creates all four topics.
2. Contracts register idempotently and can be retrieved from Apicurio.
3. Unit tests prove deterministic merge order, keys, headers, all pacing modes,
   pause accounting, queue backpressure, delivery failures, and CLI validation.
4. Replaying the committed fixture publishes the manifest's three replayable source
   counts and all quality cases; consuming from the beginning recovers the same
   event IDs, reason codes, and per-topic totals.
5. A no-sleep fixture run reports observed throughput and exits zero only after all
   delivery acknowledgements arrive.
6. Architecture, backlog status, README, and an operator runbook reflect the tested
   commands and the exact implemented versions.

