# Stateful Flink Processing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and prove a Dockerized Apache Flink job that converts MP-02 raw Kafka events into validated, deduplicated, late-aware session and marketing KPI topics with checkpoint recovery.

**Architecture:** A Java 17 Flink DataStream job consumes the three raw topics, routes parse/contract failures to a dedicated processing DLQ, assigns event-time watermarks, deduplicates by `event_id`, and fans accepted events into clean, session, and campaign-state branches. One dynamic transactional Kafka sink writes six versioned topics, while a Docker smoke harness verifies exact latest-by-key outputs across a mid-replay TaskManager restart.

**Tech Stack:** Java 17, Maven 3.9+, Apache Flink 2.2.1, Flink Kafka connector 5.0.0-2.2, Jackson, JUnit 5, Python 3.12+, Apache Kafka 4.3.1, Apicurio Registry 3.3.3, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-11-flink-stream-processing-design.md`

## Global Constraints

- Keep MP-03 sinks Kafka-only; MinIO/Iceberg starts in MP-04.
- Use Java 17 bytecode and pin Flink `2.2.1` plus Kafka connector `5.0.0-2.2`.
- Preserve raw Kafka lineage and original payloads at the validation boundary.
- Use `marketpulse-flink-v1` as source group ID and `marketpulse-flink-v1-` as the transactional ID prefix.
- Start from earliest offsets only when no checkpointed offsets exist.
- Enable exactly-once checkpoints every 5 seconds, one concurrent checkpoint, 60-second timeout, retained externalized checkpoints, and a fixed-delay restart strategy of three attempts at ten seconds.
- Use five-minute bounded watermarks, one-minute source idleness, a ten-minute correction horizon behind the current watermark, seven-day deduplication TTL, and 30-minute session inactivity.
- The smoke profile may override checkpoint interval to 2 seconds and idleness to 5 seconds without changing production defaults.
- Keep `processed_at` and `detected_at` out of deterministic equality checks.
- Every feature commit updates `docs/architecture/high-level-design.md` with the implemented MP-03 slice and current evidence.
- Never commit generated Maven targets, Docker runtime state, credentials, or large generated data.

## Review Focus

- Malformed non-UTF-8 Kafka bytes must reach the processing DLQ without crashing the job; Task 2 tests raw-byte base64 and nullable UTF-8.
- A record can be both source-marked and watermark-late; Task 3 tests that `watermark_late` wins and that the correction threshold is strictly greater than ten minutes behind the current watermark.
- Unbounded sources may never close final timers; Task 3 tests immediate open session upserts and later closed upserts rather than assuming a terminal watermark.
- Recovery can expose aborted Kafka transactions; Task 6 consumes with `read_committed` and reduces compacted logs to the latest value per key.
- A replay may occur while old output/checkpoint state exists; Task 6 isolates its Compose project/volumes and proves a second replay changes neither clean keys nor final KPI values.

---

### Task 1: Versioned output contracts and Kafka topics

**Files:**
- Create: `contracts/kafka/customer-events-v1.json`
- Create: `contracts/kafka/sessions-v1.json`
- Create: `contracts/kafka/funnel-metrics-v1.json`
- Create: `contracts/kafka/revenue-metrics-v1.json`
- Create: `contracts/kafka/campaign-metrics-v1.json`
- Create: `contracts/kafka/flink-validation-dlq-v1.json`
- Modify: `src/marketpulse_mock/kafka_contracts.py`
- Modify: `src/marketpulse_mock/kafka_client.py`
- Modify: `src/marketpulse_mock/registry.py`
- Modify: `docker/kafka/create-topics.sh`
- Modify: `tests/test_kafka_contracts.py`
- Modify: `tests/test_kafka_client.py`
- Modify: `tests/test_registry.py`
- Modify: `tests/test_docker_assets.py`
- Modify: `docs/architecture/high-level-design.md`

**Interfaces:**
- Consumes: existing `TopicContract`, source `TOPIC_CONTRACTS`, Registry loader, and idempotent bootstrap behavior.
- Produces: `SOURCE_TOPIC_CONTRACTS`, `FLINK_TOPIC_CONTRACTS`, and `ALL_TOPIC_CONTRACTS`; each `TopicContract` exposes `partitions`, `retention_ms`, and `cleanup_policy` for Task 4/5 deployment.

- [ ] **Step 1: Write failing Python contract tests**

Add tests that require exactly four source contracts and six Flink output contracts; load every JSON Schema; assert the output required fields/enums from the design; and assert session/Gold topics use their exact partition, retention, and cleanup settings. Extend client/bootstrap tests so the created topic configuration is contract-driven and configuration drift includes both `retention.ms` and `cleanup.policy`. Extend Registry tests to require all ten artifacts on first registration and ten unchanged artifacts on the second registration.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_kafka_contracts tests.test_kafka_client tests.test_registry tests.test_docker_assets -v`

Expected: FAIL because the six output contracts and generalized topic settings do not exist.

- [ ] **Step 3: Implement schemas and generalized topic contracts**

Add the six Draft 2020-12 schemas exactly matching the design. Extend `TopicContract` with `retention_ms: int = 604800000` and `cleanup_policy: str = "delete"`. Preserve `TOPIC_CONTRACTS` as the source-only compatibility alias used by replay, expose `FLINK_TOPIC_CONTRACTS`, and use `ALL_TOPIC_CONTRACTS` for Registry registration and topic creation. Bootstrap clean/DLQ topics with six partitions and seven days, sessions with six partitions plus `compact,delete`, and Gold topics with three partitions, thirty days, and `compact,delete`.

- [ ] **Step 4: Update architecture progress and verify GREEN**

Add an MP-03 implementation-progress table marking contracts/topics implemented. Run the focused command, the complete Python suite, and `git diff --check`.

Expected: focused and complete suites PASS; all ten schema artifacts load; bootstrap expectations match every contract.

- [ ] **Step 5: Commit**

```bash
git add contracts/kafka src/marketpulse_mock/kafka_contracts.py src/marketpulse_mock/kafka_client.py src/marketpulse_mock/registry.py docker/kafka/create-topics.sh tests docs/architecture/high-level-design.md
git commit -m "feat: define Flink streaming contracts"
```

### Task 2: Flink module, raw record parsing, and validation DLQ

**Files:**
- Create: `flink-job/pom.xml`
- Create: `flink-job/src/main/java/io/marketpulse/flink/model/RawKafkaRecord.java`
- Create: `flink-job/src/main/java/io/marketpulse/flink/model/MarketingEvent.java`
- Create: `flink-job/src/main/java/io/marketpulse/flink/model/InvalidRecord.java`
- Create: `flink-job/src/main/java/io/marketpulse/flink/model/OutputRecord.java`
- Create: `flink-job/src/main/java/io/marketpulse/flink/serde/JsonSupport.java`
- Create: `flink-job/src/main/java/io/marketpulse/flink/validation/EventParser.java`
- Create: `flink-job/src/test/java/io/marketpulse/flink/validation/EventParserTest.java`
- Create: `flink-job/src/test/resources/events/*.json`
- Modify: `.gitignore`
- Modify: `docs/architecture/high-level-design.md`

**Interfaces:**
- Consumes: MP-02 raw JSON envelopes and exact source topic names.
- Produces: `EventParser.parse(RawKafkaRecord, Instant) -> ParseResult`, where `ParseResult` contains exactly one `MarketingEvent` or `InvalidRecord`; `InvalidRecord.toOutputRecord()` targets `marketing.dlq.flink-validation.v1`; `JsonSupport.writeCanonical(Object) -> byte[]` is reused by all sinks.

- [ ] **Step 1: Add Maven configuration and failing parser tests**

Pin Java release 17, Flink 2.2.1, connector 5.0.0-2.2, Jackson, JUnit 5, Surefire, and Shade. Tests use literal valid web/order/email fixtures and assert all ten reason codes, topic/source/event combinations, UTC timestamps, UUIDs, v1 schema, required identity and nested payloads, lineage preservation, deterministic UUIDv5 error IDs, non-UTF-8 base64, and nullable decoded UTF-8.

- [ ] **Step 2: Run parser tests and verify RED**

Run: `mvn -q -f flink-job/pom.xml -Dtest=EventParserTest test`

Expected: FAIL because parser/model classes do not exist.

- [ ] **Step 3: Implement the minimal parser boundary**

Use Jackson tree parsing and explicit topic-specific validation. `RawKafkaRecord` stores topic, partition, offset, timestamp, nullable key, headers, and exact value bytes. `MarketingEvent` stores validated/typed common fields plus the original payload tree and lineage. `InvalidRecord` stores the exact raw bytes and reason code; it never assumes valid JSON.

- [ ] **Step 4: Verify parser and complete suites**

Run the focused Maven test, `mvn -q -f flink-job/pom.xml test`, the complete Python suite, and `git diff --check`.

Expected: all parser cases PASS; Maven builds on Java 17; Python behavior remains green.

- [ ] **Step 5: Update architecture progress and commit**

Mark parsing/DLQ implemented with the Maven test count.

```bash
git add .gitignore flink-job docs/architecture/high-level-design.md
git commit -m "feat: validate raw events for Flink"
```

### Task 3: Deduplication, late classification, sessions, and KPI state

**Files:**
- Create: `flink-job/src/main/java/io/marketpulse/flink/model/EnrichedEvent.java`
- Create: `flink-job/src/main/java/io/marketpulse/flink/model/SessionSnapshot.java`
- Create: `flink-job/src/main/java/io/marketpulse/flink/model/CampaignSnapshot.java`
- Create: `flink-job/src/main/java/io/marketpulse/flink/processing/DeduplicateFunction.java`
- Create: `flink-job/src/main/java/io/marketpulse/flink/processing/LateEventFunction.java`
- Create: `flink-job/src/main/java/io/marketpulse/flink/processing/SessionizeFunction.java`
- Create: `flink-job/src/main/java/io/marketpulse/flink/processing/CampaignAggregateFunction.java`
- Create: `flink-job/src/test/java/io/marketpulse/flink/processing/DeduplicateFunctionTest.java`
- Create: `flink-job/src/test/java/io/marketpulse/flink/processing/LateEventFunctionTest.java`
- Create: `flink-job/src/test/java/io/marketpulse/flink/processing/SessionizeFunctionTest.java`
- Create: `flink-job/src/test/java/io/marketpulse/flink/processing/CampaignAggregateFunctionTest.java`
- Modify: `docs/architecture/high-level-design.md`

**Interfaces:**
- Consumes: validated `MarketingEvent` with assigned event timestamp.
- Produces: keyed `DeduplicateFunction(Duration ttl)`; `LateEventFunction(Duration correctionHorizon, Clock)` producing `EnrichedEvent`; `SessionizeFunction(Duration gap, Clock)` producing keyed `SessionSnapshot`; `CampaignAggregateFunction(Clock)` producing `CampaignSnapshot`; each snapshot converts to its exact `OutputRecord` projection.

- [ ] **Step 1: Write failing real-operator tests for deduplication and lateness**

Use Flink operator test harnesses, not mocked state. Assert the first ID passes, a duplicate is dropped, distinct IDs pass, restored snapshot state still drops a duplicate, seven-day TTL allows expiry, `watermark_late` overrides the source marker, exactly ten minutes behind the watermark remains eligible, and one millisecond beyond becomes ineligible.

- [ ] **Step 2: Run focused operator tests and verify RED**

Run: `mvn -q -f flink-job/pom.xml -Dtest=DeduplicateFunctionTest,LateEventFunctionTest test`

Expected: FAIL because the processing functions do not exist.

- [ ] **Step 3: Implement checkpointed deduplication and late enrichment**

Use `ValueState<Boolean>` with processing-time TTL for IDs. Register counters `accepted`, `duplicates`, `source_marked_late`, `watermark_late`, and `aggregate_ineligible`. Derive identity/channel/date/campaign fields, and use injected UTC `Clock` for operational timestamps.

- [ ] **Step 4: Write failing session and campaign state tests**

Assert every eligible web event emits an `open` session upsert; a 30-minute event-time timer emits `closed`; stale timers do not close an updated session; first non-null identity/campaign wins by event-time/event-ID order; stage and revenue fields are exact. Campaign tests assert distinct session reach, repeat stage suppression, zero defaults, order/refund/net arithmetic, and the literal committed-fixture totals gross `29718000`, refund `4967000`, net `24751000`.

- [ ] **Step 5: Run state tests and verify RED**

Run: `mvn -q -f flink-job/pom.xml -Dtest=SessionizeFunctionTest,CampaignAggregateFunctionTest test`

Expected: FAIL because session/campaign functions do not exist.

- [ ] **Step 6: Implement session and combined campaign state**

Use keyed managed state and event-time timers. Emit provisional session snapshots on every event and a closed snapshot only for the current timer. Store stage reach in managed map state keyed by session/stage. Emit one combined campaign snapshot per eligible web/order event; projection methods produce funnel, revenue, and combined JSON with numeric zero defaults.

- [ ] **Step 7: Verify all Java/Python tests, update architecture, and commit**

Run all Maven tests, all Python tests, and `git diff --check`. Mark the four stateful processing responsibilities implemented and record test counts.

```bash
git add flink-job docs/architecture/high-level-design.md
git commit -m "feat: add stateful Flink transformations"
```

### Task 4: Kafka source/sink wiring and executable Flink job

**Files:**
- Create: `flink-job/src/main/java/io/marketpulse/flink/config/JobConfig.java`
- Create: `flink-job/src/main/java/io/marketpulse/flink/kafka/RawKafkaDeserializationSchema.java`
- Create: `flink-job/src/main/java/io/marketpulse/flink/kafka/OutputRecordSerializationSchema.java`
- Create: `flink-job/src/main/java/io/marketpulse/flink/MarketPulseFlinkJob.java`
- Create: `flink-job/src/test/java/io/marketpulse/flink/config/JobConfigTest.java`
- Create: `flink-job/src/test/java/io/marketpulse/flink/kafka/KafkaSerdeTest.java`
- Create: `flink-job/src/test/java/io/marketpulse/flink/JobGraphTest.java`
- Modify: `docs/architecture/high-level-design.md`

**Interfaces:**
- Consumes: Task 2 parse result, Task 3 functions/snapshots, and Task 1 output topic constants.
- Produces: `JobConfig.fromArgs(String[])`; Kafka metadata-aware raw deserializer; dynamic topic/key/value serializer; `MarketPulseFlinkJob.build(StreamExecutionEnvironment, JobConfig) -> DataStream<OutputRecord>` and `main(String[])`.

- [ ] **Step 1: Write failing config and Kafka serde tests**

Assert exact defaults and positive-duration validation; source deserialization retains all Kafka metadata/bytes; sink serialization chooses record topic, UTF-8 key, canonical JSON value, and schema/event headers; invalid output topic names are rejected.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `mvn -q -f flink-job/pom.xml -Dtest=JobConfigTest,KafkaSerdeTest test`

Expected: FAIL because config and Kafka serde classes do not exist.

- [ ] **Step 3: Implement configuration and Kafka serde**

Use Flink `ParameterTool`; accept bootstrap servers, group ID, checkpoint URI/interval, watermark/idleness/correction/session/TTL durations, parallelism, and transaction prefix. Build the raw `KafkaSource` with earliest fallback and the output `KafkaSink` with `DeliveryGuarantee.EXACTLY_ONCE`.

- [ ] **Step 4: Write a failing job-graph test**

Build the graph without external services and assert it contains raw source, parser/DLQ, timestamp/watermark, dedup, late classifier, clean, session, campaign projections, and exactly one dynamic Kafka sink. Assert checkpointing/restart settings on the environment.

- [ ] **Step 5: Run graph test and verify RED**

Run: `mvn -q -f flink-job/pom.xml -Dtest=JobGraphTest test`

Expected: FAIL because the executable topology is absent.

- [ ] **Step 6: Wire the job and package the shaded JAR**

Use parser side output for invalid records, assign bounded watermarks to valid events, deduplicate before every branch, preserve all accepted events in clean output, filter ineligible records from state branches, and union all six output record streams into one sink. Assign stable operator UIDs for checkpoint/savepoint compatibility.

- [ ] **Step 7: Verify package, update architecture, and commit**

Run `mvn -q -f flink-job/pom.xml clean verify`, complete Python tests, inspect `flink-job/target/marketpulse-flink-job.jar`, and run `git diff --check`. Mark the executable job and guarantees implemented.

```bash
git add flink-job docs/architecture/high-level-design.md
git commit -m "feat: wire the Flink Kafka job"
```

### Task 5: Dockerized Flink cluster and idempotent submission

**Files:**
- Create: `docker/flink/Dockerfile`
- Create: `docker/flink/submit-job.sh`
- Modify: `compose.yaml`
- Modify: `tests/test_docker_assets.py`
- Modify: `README.md`
- Modify: `docs/architecture/high-level-design.md`

**Interfaces:**
- Consumes: shaded Task 4 JAR, Kafka service/network, named checkpoint volume.
- Produces: Compose `flink` profile with healthy `flink-jobmanager`, `flink-taskmanager`, and one-shot `flink-job-submit`; Flink UI at `localhost:8082`; idempotent submission keyed by job name `marketpulse-stream-v1`.

- [ ] **Step 1: Write failing executable Docker tests**

Extend Python tests to render `docker compose config`, require pinned `flink:2.2.1-java17`, Java 17 Maven build stage, non-root runtime, two task slots, checkpoint volume mounts, health checks, dependency conditions, port 8082, all exact job arguments, and a submit script that returns success when the named job is already RUNNING.

- [ ] **Step 2: Run Docker tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_docker_assets -v`

Expected: FAIL because the Flink image/services do not exist.

- [ ] **Step 3: Implement Flink image, cluster, and submitter**

Use a pinned Maven builder stage and `flink:2.2.1-java17` runtime; copy only the shaded JAR. Add JobManager/TaskManager REST health checks, a shared checkpoint volume, Kafka dependency, and explicit resource limits suitable for a laptop. The submitter queries `/jobs/overview`, submits only when the named job is absent, and prints the resulting job ID.

- [ ] **Step 4: Verify Compose and local service health**

Run Docker asset tests, `docker compose config --quiet`, build the Flink image, and start `streaming` plus `flink` profiles. Verify Kafka, Registry, Kafbat, JobManager, and TaskManager health; verify the submitted job reaches `RUNNING`; verify <http://localhost:8082> responds.

- [ ] **Step 5: Update architecture/README and commit**

Document actual Compose service names, ports, profiles, resource expectations, and the tested job state.

```bash
git add docker/flink compose.yaml tests/test_docker_assets.py README.md docs/architecture/high-level-design.md
git commit -m "feat: run Flink processing with Docker"
```

### Task 6: Recovery smoke verifier, runbook, and acceptance evidence

**Files:**
- Create: `src/marketpulse_mock/flink_smoke_verify.py`
- Create: `tests/test_flink_smoke_verify.py`
- Create: `scripts/flink-smoke-test.sh`
- Create: `docs/runbooks/flink-local.md`
- Modify: `README.md`
- Modify: `specs/README.md`
- Modify: `specs/03-stateful-flink-processing.md`
- Modify: `docs/architecture/high-level-design.md`

**Interfaces:**
- Consumes: isolated Compose project, committed fixture, Flink REST API, read-committed Kafka consumer dumps, and six output contracts.
- Produces: `python -m marketpulse_mock.flink_smoke_verify --fixture ... --input-dump ... --output-dir ...` with non-zero exit on any mismatch; `scripts/flink-smoke-test.sh` that proves mid-replay recovery and cleans only its isolated project unless `KEEP_STACK=1`.

- [ ] **Step 1: Write failing verifier tests with literal oracles**

Fixture tests require 219 valid raw records, 217 unique event IDs, two duplicate copies, three source-marked-late unique events, 73 latest session keys, 12 created orders, two refunds, gross `29718000`, refund `4967000`, and net `24751000`. Synthetic dumps assert latest-by-key reduction, ignored operational timestamps, exact clean IDs/late statuses, distinct-session funnel values, revenue/campaign equality, malformed-input DLQ reason/error ID, watermark-late audit inclusion, and unchanged aggregates after duplicate replay.

- [ ] **Step 2: Run verifier tests and verify RED**

Run: `PYTHONPATH=src python3 -m unittest tests.test_flink_smoke_verify -v`

Expected: FAIL because the verifier does not exist.

- [ ] **Step 3: Implement the deterministic verifier**

Read fixture gzip files independently of Java code. Parse Kafka consumer dumps, discard aborted/uncommitted records at the consumer boundary, reduce session/Gold streams by key and highest partition/offset observation, and compare stable fields only. Report machine-readable expected/actual counts, checkpoint IDs, recovery duration, and every mismatch.

- [ ] **Step 4: Write the recovery smoke script**

The script creates a unique Compose project; removes only that project's volumes;
starts streaming/Flink services; registers all contracts twice; starts a 20 EPS
fixture replay in the background; waits for a completed checkpoint while replay is
still active; restarts the TaskManager; confirms post-restart input progress and job
`RUNNING`; waits for replay; replays the fixture again; injects malformed and late
records after the 5-second smoke idleness interval has advanced the watermark;
queries Flink REST counters for accepted/duplicate/late/invalid records; consumes
outputs with `isolation.level=read_committed`; runs the verifier;
and cleans up through a trap unless `KEEP_STACK=1`.

- [ ] **Step 5: Run focused, full, and real-container verification**

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_flink_smoke_verify -v
PYTHONPATH=src python3 -m unittest discover -s tests -v
mvn -q -f flink-job/pom.xml clean verify
docker compose config --quiet
./scripts/flink-smoke-test.sh
git diff --check
```

Expected: every command exits zero; smoke evidence includes at least one checkpoint
before restart, records after restart, 217 clean unique IDs after two replays, 73
latest sessions, exact revenue totals, one injected processing DLQ record, and one
watermark-late clean audit record that does not change aggregates.

- [ ] **Step 6: Finalize documentation and commit**

Set MP-03 to Implemented only after Step 5 evidence. Update architecture with exact
versions, topics, service health, test counts, fixture totals, checkpoint IDs,
recovery duration, and throughput. Add the operator runbook and commands for UI,
replay, pause, restart, metrics, logs, and safe reset.

```bash
git add src/marketpulse_mock/flink_smoke_verify.py tests/test_flink_smoke_verify.py scripts/flink-smoke-test.sh docs/runbooks/flink-local.md README.md specs docs/architecture/high-level-design.md
git commit -m "docs: prove Flink recovery and correctness"
```

## Completion gate

- [ ] Review spec coverage: every design goal/non-goal maps to one task.
- [ ] Review public type/topic names across tasks for exact consistency.
- [ ] Run both language suites, Compose validation, schema registration, and real
  recovery smoke from the feature worktree.
- [ ] Confirm `git status --short` contains no generated targets, dumps, or secrets.
- [ ] Request a fresh whole-branch review and resolve every Critical/Important issue
  with a failing regression test before marking MP-03 implemented.

## Plan self-review

- Spec coverage: all goals map to Tasks 1-6; every non-goal remains outside created
  services and sinks.
- Interface scan: Task 1 contracts feed Task 4 and Task 5; Task 2 parse/output types
  feed Tasks 3-4; Task 3 snapshots feed Task 4; Task 4 JAR feeds Tasks 5-6; Task 5
  service names feed Task 6. Names and versions match across those boundaries.
- Review focus: all five high-risk cases have an explicit failing test or real smoke
  assertion in the owning task.
- Proportion: the plan fixes public interfaces, test oracles, and deployment values;
  method bodies remain implementation decisions.
