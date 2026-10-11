# Kafka Ingestion and Replay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Docker-hosted Kafka ingestion layer that deterministically replays MP-01 event files with controllable pacing, contracts, DLQ routing, and verifiable delivery evidence.

**Architecture:** A dependency-light replay core heap-merges compressed source parts and produces transport-neutral records. An injected clock handles pacing while a thin `confluent-kafka` adapter handles idempotent delivery; Docker Compose supplies Kafka KRaft, Kafbat UI, Apicurio Registry, and topic bootstrap.

**Tech Stack:** Python 3.12+, `confluent-kafka`, Apache Kafka 4.3.1, Kafbat UI 1.5.0, Apicurio Registry 3.3.3, Docker Compose, `unittest`.

**Spec:** `specs/02-kafka-ingestion-replay.md`

## Global Constraints

- Build on the exact MP-01 manifest and canonical JSON bytes; do not create a second mock-data model.
- Raw topics have six partitions and replication factor one only for the laptop environment.
- Kafka keys and event IDs are deterministic; retries must never substitute a new ID.
- Replayable source order is nondecreasing by event time with bounded row buffering.
- `speed`, `events_per_second`, and `no_sleep` are mutually exclusive and numeric rates are positive.
- Unit tests must not require Docker, wall-clock sleeps, or a live Kafka broker.
- Docker integration evidence uses the committed fixture, not the ignored demo dataset.
- Every architecture or topic-contract change updates `docs/architecture/high-level-design.md` in the same commit.

## Review Focus

- A rotated source with equal timestamps still has deterministic order; Task 1 tests path and ordinal tie-breaking.
- A queue-full producer must retry the same record without skipping or double-counting; Task 3 tests backpressure.
- Pausing during a scheduled gap must not create a burst on resume; Task 2 tests paused-time accounting.
- Delivery callback failure or flush timeout must make the command fail; Task 3 and Task 4 test both exits.
- Re-running bootstrap/contract registration must be idempotent; Task 5 tests two consecutive invocations.

---

### Task 1: Topic contracts and bounded source reader

**Files:**
- Create: `src/marketpulse_mock/kafka_contracts.py`
- Create: `src/marketpulse_mock/replay_reader.py`
- Create: `contracts/kafka/*.json`
- Create: `tests/test_kafka_contracts.py`
- Create: `tests/test_replay_reader.py`
- Modify: `docs/architecture/high-level-design.md`

**Interfaces:**
- Produces: `TopicContract`, `TOPIC_CONTRACTS`, `ReplayRecord`, `iter_replay_records(root: Path, include_dlq: bool = False) -> Iterator[ReplayRecord]`, and `record_key(dataset: str, value: dict[str, object]) -> str`.

- [ ] Write failing tests for exact topic mapping, key fallback, compact bytes, headers, manifest verification, three-source counts, optional DLQ mapping, bounded heap merge, and deterministic equal-time order.
- [ ] Run `PYTHONPATH=src python3 -m unittest tests.test_kafka_contracts tests.test_replay_reader -v`; expect missing modules.
- [ ] Implement immutable contracts and streaming gzip heap merge using only manifest-listed files.
- [ ] Add JSON Schema files for the three event envelopes and DLQ wrapper.
- [ ] Update architecture topic names, key rules, JSON Schema decision, and MP-01/MP-02 boundary.
- [ ] Run focused and complete suites; commit `feat: define Kafka replay contracts and source reader`.

### Task 2: Deterministic pacing and pause controller

**Files:**
- Create: `src/marketpulse_mock/replay_pacing.py`
- Create: `tests/test_replay_pacing.py`

**Interfaces:**
- Consumes: ordered `ReplayRecord.event_time` values.
- Produces: `PacingConfig`, `ReplayPacer.wait(event_time: datetime, ordinal: int) -> None`, and `ReplayController.pause()`, `resume()`, `toggle()`, `wait_until_running()`.

- [ ] Write failing fake-clock tests for `speed=1`, accelerated speed, fixed EPS, no-sleep, invalid/mutually-exclusive modes, pause duration exclusion, and resume without burst.
- [ ] Run the focused test; expect missing module.
- [ ] Implement monotonic target scheduling and a condition-based pause controller with injected time/sleep functions.
- [ ] Run focused and complete suites; commit `feat: add deterministic Kafka replay pacing`.

### Task 3: Idempotent producer and replay service

**Files:**
- Create: `src/marketpulse_mock/kafka_client.py`
- Create: `src/marketpulse_mock/replay.py`
- Create: `tests/test_kafka_client.py`
- Create: `tests/test_replay.py`
- Modify: `pyproject.toml`

**Interfaces:**
- Consumes: `ReplayRecord` and `ReplayPacer`.
- Produces: `KafkaPublisher`, `ReplaySummary`, `replay(records, publisher, pacer, controller) -> ReplaySummary`, `create_topics(...)`, and `ConfluentKafkaPublisher`.

- [ ] Write failing tests for idempotent producer configuration, exact record delivery, queue-full retry, callback errors, flush timeout, per-topic counts, DLQ counts, throughput, and stable event IDs across repeated runs.
- [ ] Run focused tests; expect missing modules.
- [ ] Add the `kafka` optional dependency and implement a lazy-import adapter so core tests work without native Kafka libraries.
- [ ] Implement replay orchestration with acknowledgement-based success accounting and graceful stop.
- [ ] Run focused and complete suites; commit `feat: publish deterministic replays to Kafka`.

### Task 4: Kafka CLI and registry client

**Files:**
- Create: `src/marketpulse_mock/registry.py`
- Create: `tests/test_registry.py`
- Modify: `src/marketpulse_mock/cli.py`
- Modify: `tests/test_cli.py`

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces: `kafka topics`, `kafka register-contracts`, and `kafka replay` commands with exit codes `0` for acknowledged success and `2` for configuration, validation, registry, or delivery failure.

- [ ] Write failing tests for command parsing, mutually exclusive pacing flags, summary JSON, signal wiring boundary, idempotent registry create/version behavior, and non-zero delivery/registry failures.
- [ ] Run focused tests; verify expected missing-command failures.
- [ ] Implement registry HTTP operations with standard-library `urllib` and add nested CLI commands.
- [ ] Run focused and complete suites; commit `feat: expose Kafka replay operations in CLI`.

### Task 5: Docker streaming stack and real smoke test

**Files:**
- Create: `compose.yaml`
- Create: `docker/replay/Dockerfile`
- Create: `docker/kafka/create-topics.sh`
- Create: `scripts/kafka-smoke-test.sh`
- Create: `tests/test_docker_assets.py`
- Modify: `.gitignore`
- Modify: `docs/architecture/high-level-design.md`

**Interfaces:**
- Consumes: MP-02 CLI and contracts.
- Produces: Compose profile `streaming`, healthy local services, idempotent topic initialization, and a smoke-test script that starts, registers, replays, consumes, and verifies fixture totals.

- [ ] Write failing asset/integration-boundary tests that parse Compose config, build the replay image, and reject missing healthchecks, wrong listeners, mutable core image tags, or topic mismatches.
- [ ] Run focused tests; expect missing assets.
- [ ] Implement the pinned Compose stack, replay image, topic bootstrap, named volumes, and healthchecks.
- [ ] Implement the smoke script with cleanup limited to this Compose project and assertion of exact fixture topic/DLQ counts.
- [ ] Run `docker compose config`, build/start the streaming profile, register contracts twice, replay the fixture, run the smoke assertions, and stop the stack.
- [ ] Run focused and complete suites; commit `feat: add Docker Kafka streaming stack`.

### Task 6: Operator guide and MP-02 evidence

**Files:**
- Create: `docs/runbooks/kafka-local.md`
- Modify: `README.md`
- Modify: `specs/README.md`
- Modify: `docs/architecture/high-level-design.md`

**Interfaces:**
- Consumes: verified Task 5 commands and measurements.
- Produces: exact local startup/test/reset instructions and MP-02 implementation evidence.

- [ ] Document prerequisites, startup, health checks, schema registration, speed/EPS/no-sleep replay, pause/resume, Kafka UI inspection, CLI consumption checks, smoke test, shutdown, and recoverable reset.
- [ ] Update MP-02 to Implemented only after real Docker evidence succeeds; record exact versions, fixture counts, test count, and observed no-sleep throughput.
- [ ] Run the full unit suite, Compose validation, fixture validation, real smoke test, `git diff --check`, and ignored-data checks.
- [ ] Commit `docs: add Kafka replay runbook and MP-02 evidence`.
