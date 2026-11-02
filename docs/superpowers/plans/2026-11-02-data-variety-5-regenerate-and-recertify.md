# Data Variety 5 — Regenerate and Recertify Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (operations run in this session; they are long and stateful). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the new story world in a second Compose project, certify it, stream September, prove it with an independent oracle and record the evidence.

**Architecture:** `marketpulse-story-demo-v2` gets its own env script and ports; the old project's heavy services are stopped (not deleted). A tested oracle script compares certified ClickHouse tables with the generator files.

**Tech Stack:** Docker Compose, Kafka, Flink, Spark/Iceberg, Dagster/dbt, ClickHouse, Python 3.12.

**Spec:** `docs/superpowers/specs/2026-11-02-data-variety-5-regenerate-and-recertify-design.md`. Requires plans 1–4 merged to `main`.

## Global Constraints

- Project `marketpulse-story-demo-v2`; ports = v1 + 100 (web 43210, api 48210, ClickHouse 48213, Dagster 43220, Kafka 49293, registry 48283, Kafka UI 48284, Flink 48285, MinIO 49210/49211, Nessie 49321, Prometheus 49191, Grafana 43102).
- `BATCH_REQUIRED_PLATFORMS=google_ads,meta_ads,tiktok_ads,shopee_ads` for v2 only.
- History `2025-01-01 → 2026-09-01`; certify logical day 2026-08-31; September replay speed 20, batch size 20.
- Ungoverned measures equal ground truth exactly; governed measures equal an independent governed computation; raw−governed gaps recorded with reasons.
- Old project volumes are never removed by this plan; passwords only via stdin from `.secrets/`.

## Review Focus

- Port collisions with v1 services still running (Postgres, MinIO) → v2 must start with v1's app up — checked in Task 1.
- Spark driver memory on 3× CRM volume → archive windows halve if a month fails — handled in Task 3.
- A replay cursor from v1 must never be reused for v2 (different world identity) — checked in Task 4.
- Oracle rounding: money in minor units, integers only — test in Task 2.
- Live View during the walkthrough shows September, not leftover history — checked in Task 5.

---

### Task 1: v2 environment and stopped v1 heavy services

**Files:** Create `scripts/story-demo-v2-env.sh` (from `story-demo-env.sh`, project and ports per Global Constraints, `BATCH_SOURCE_ROOT=/opt/marketpulse/data/generated/story-demo-v2`, `BATCH_REQUIRED_PLATFORMS`); Modify `docs/runbooks/story-demo.md` (v2 section).

- [ ] **Step 1:** `docker stop marketpulse-story-demo-continuation` (the replay is a `run` container), then `source scripts/story-demo-env.sh && docker compose --profile '*' stop flink-taskmanager flink-jobmanager clickhouse` — Expected: stopped, volumes intact (`docker volume ls | grep story-demo` unchanged).
- [ ] **Step 2:** `source scripts/story-demo-v2-env.sh && docker compose --profile '*' config -q` — Expected: no output; `docker compose --profile '*' config | grep -E "published"` shows only v2 ports.
- [ ] **Step 3: Commit** `chore: add a second story-demo project for the varied world`.

### Task 2: Oracle script

**Files:** Create `scripts/story_demo_oracle.py`; Test `tests/test_story_demo_oracle.py`.

**Interfaces:** `ground_truth_totals(root: Path) -> Totals` (from generator `ground_truth` and vendor files: impressions, clicks, spend by platform×month; orders, gross, refunds by month; sessions by `traffic_source`; email sends by type×month; revenue by city via `master/customers`); `certified_totals(client, database: str) -> Totals`; `compare(truth: Totals, certified: Totals) -> list[Check]` with `Check(name, expected, actual, equal: bool, reason: str | None)`; CLI prints a Markdown table.

- [ ] **Step 1: Failing tests** with small synthetic `Totals`: equal values → all `equal`; a 1-minor-unit spend difference → that check false; governed checks attach the reason text when raw and governed differ.
- [ ] **Step 2: Run** `PYTHONPATH=src .venv/bin/python -m unittest tests.test_story_demo_oracle -v` — Expected: FAIL.
- [ ] **Step 3: Implement**; also run `ground_truth_totals` on `data/mock/fixtures/seed-360` in a test to pin parsing.
- [ ] **Step 4: Run** — Expected: PASS.
- [ ] **Step 5: Commit** `feat: add an independent oracle for the story demo`.

### Task 3: Generate, replay history, archive, certify

- [ ] **Step 1:** `PYTHONPATH=src .venv/bin/python -m marketpulse_mock generate --profile story-demo --seed 360 --output data/generated/story-demo-v2` then `... validate --input data/generated/story-demo-v2` — Expected: valid; note row counts and manifest checksum.
- [ ] **Step 2:** Start v2 streaming and lakehouse services, submit Flink, register contracts (runbook steps 2–3 with the v2 env); replay `2025-01-01 → 2026-09-01 --no-sleep --include-dlq`; wait for zero Flink lag (Flink UI on 48285).
- [ ] **Step 3:** Archive ad (four platforms) and CRM files month by month (`ingest-files --from-date … --to-date …`); if a month fails for driver memory, split it into halves.
- [ ] **Step 4:** Start ClickHouse, batch Postgres, batch code, Dagster; launch `marketing_refresh_job` for `2026-08-31`; then `journey-publish` — Expected: run succeeded; journey events count printed.
- [ ] **Step 5:** Record durations and counts in the runbook draft.

### Task 4: App, accounts and September replay

- [ ] **Step 1:** Start `app-postgres api web campaign-worker live-events-worker`; create accounts from `.secrets/` via stdin.
- [ ] **Step 2:** Run the continuation with a **new** cursor file `story-demo-v2-september.json` and `--init-state`, `--speed 20 --batch-size 20` — Expected: replay logs pacing; ClickHouse `marketpulse_live.events` grows each minute.

### Task 5: Oracle, walkthrough, evidence, switch

- [ ] **Step 1:** Run `scripts/story_demo_oracle.py --root data/generated/story-demo-v2 --clickhouse localhost:48213` — Expected: every ungoverned check equal; governed checks equal to the independent governed computation, gaps listed with reasons.
- [ ] **Step 2:** Walk all ten pages in Chrome (light and dark): city spread, four-channel shares, hourly peaks, a double-day spike, growing email, 2025–2026 cohorts, Live View in real time.
- [ ] **Step 3:** Replace the runbook evidence tables (Insights, Products, Geography, Live View included) with the new measurements and list the numbers the presentation must update.
- [ ] **Step 4:** Move v1 values to `scripts/story-demo-v1-env.sh` and point `scripts/story-demo-env.sh` at v2.
- [ ] **Step 5: Commit** `docs: record the varied story world evidence and switch the demo to v2`; open the PR.
