# MP-03 - Stateful Stream Processing with Flink

## Intent

Process the MP-02 raw Kafka streams into deterministic, versioned streaming data
products while proving duplicate safety, late-data behavior, stateful session and
KPI computation, and checkpoint recovery.

## Scope

### Included

- Apache Flink `2.2.1` session cluster on Java 17.
- Flink Kafka connector `5.0.0-2.2`.
- Raw topic parsing and version-1 contract validation.
- A separate `marketing.dlq.flink-validation.v1` topic for malformed/runtime input,
  leaving MP-02 quality-case DLQ records unchanged.
- Seven-day keyed `event_id` deduplication state.
- Five-minute bounded event-time watermark and one-minute partition idleness.
- Ten-minute aggregate correction horizon with late audit preservation.
- Thirty-minute event-time web sessionization.
- Clean event, session, funnel, campaign, and revenue output topics.
- Exactly-once checkpoints and transactional Kafka output.
- Unit/operator tests and a Docker recovery smoke test.

### Excluded

- MinIO and Iceberg sinks (MP-04).
- CRM and ad-platform batch ingestion, Dagster, and dbt (MP-05).
- ClickHouse and dashboard APIs (MP-05/MP-06).
- Flink high availability and production security.

## Processing contract

1. Preserve Kafka lineage before parsing each source value.
2. Reject unsupported topic/source/event/schema combinations before state updates.
3. Accept the first valid `event_id` and drop later copies for seven days.
4. Assign event timestamps from `event_time` and generate bounded watermarks.
5. Preserve every accepted event in the clean stream with a late-status field.
6. Exclude events beyond the correction horizon from session and aggregate updates.
7. Emit provisional session upserts immediately and a closed upsert after thirty
   minutes of event-time inactivity.
8. Emit deterministic campaign/day upsert snapshots for funnel, revenue, and their
   combined campaign view.
9. Commit source offsets, keyed state, timers, and Kafka output atomically through
   Flink checkpoints and Kafka transactions.
10. Store business event time in the JSON contract, but let Kafka assign output
    record timestamps at publish time so topic retention is independent of replayed
    historical dates.

## Topic contract

| Topic | Partitions | Retention / cleanup |
|---|---:|---|
| `marketing.clean.customer-events.v1` | 6 | 7 days, delete |
| `marketing.clean.sessions.v1` | 6 | 7 days, compact + delete |
| `marketing.gold.funnel-metrics.v1` | 3 | 30 days, compact + delete |
| `marketing.gold.revenue-metrics.v1` | 3 | 30 days, compact + delete |
| `marketing.gold.campaign-metrics.v1` | 3 | 30 days, compact + delete |
| `marketing.dlq.flink-validation.v1` | 6 | 7 days, delete |

All six topics have JSON Schema artifacts registered in the `marketpulse` Apicurio
group. The existing `marketing.dlq.validation.v1` remains the MP-02 quality-case
DLQ and is not reused for arbitrary malformed bytes.

## Acceptance criteria

1. Replaying the committed fixture twice produces one clean record per unique valid
   `event_id`; funnel and revenue values remain unchanged after the second replay.
2. All MP-01 source-marked late records appear in the clean stream with an explicit
   late status and metric; events beyond the watermark correction horizon do not
   mutate final aggregate snapshots.
3. Web events form 73 deterministic latest session keys for the committed fixture,
   with stage counts, event count, campaign/customer identity, start/end timestamps,
   and explicit `open|closed` status.
4. Final campaign/day funnel and revenue snapshots reconcile with unique fixture
   events, including refund subtraction.
5. Every output contract is versioned, registered idempotently, and inspectable in
   Kafbat UI.
6. Flink Web UI reports a running job, completed checkpoints, and duplicate/late/
   invalid counters.
7. A 20 event/second replay has records on both sides of a completed checkpoint;
   restarting the TaskManager mid-replay returns the job to `RUNNING`, and a
   `read_committed` latest-by-key verifier shows no lost or double-counted events.
8. `docker compose config`, Java tests, Python tests, contract validation, and the
   real Docker smoke test all pass from a clean checkout.
9. An injected malformed record reaches `marketing.dlq.flink-validation.v1`, and
   an injected record behind the watermark/correction horizon is preserved in clean
   output with `watermark_late` without changing final aggregate values.

## Demo evidence to capture

- Flink and Kafka UI screenshots showing the running job and output topics.
- Input, unique, duplicate, late, invalid, session, and final aggregate counts.
- Latest final aggregate JSON values compared with fixture-derived expectations.
- Checkpoint ID before restart, recovery duration, and checkpoint ID after restart.
- Job and container versions plus CPU/memory observations for MP-10.

## Implemented evidence

The clean isolated recovery run completed with checkpoint `20` before restart and
checkpoint `21` after restore, with a measured recovery time of 13 seconds. The
`read_committed` verifier observed:

| Evidence | Result |
|---|---:|
| Valid fixture rows | 219 |
| Unique fixture event IDs | 217 |
| Duplicate copies rejected | 2 |
| Latest session keys | 73 |
| Source-marked late events | 3 |
| Injected watermark-late audit events | 1 |
| Processing DLQ records | 1 |
| Gross revenue | 29,718,000 |
| Refund amount | 4,967,000 |
| Net revenue | 24,751,000 |

The test also uncovered and fixed a retention-boundary defect: forwarding fixture
event timestamps as Kafka record timestamps made old replay data immediately
eligible for seven-day deletion. Output records now use Kafka publish time while
retaining `event_time` in their payloads for Flink event-time semantics.
