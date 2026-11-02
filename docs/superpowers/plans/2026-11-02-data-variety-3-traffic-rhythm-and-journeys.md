# Data Variety 3 — Traffic Rhythm and Journeys Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give story traffic a Vietnam-time daily rhythm, weekends, sale days and Tết, organic and direct visits, and device-specific conversion.

**Architecture:** A pure calendar module scores each hour by its Vietnam date; the generator sizes each UTC day from those scores and draws sorted event times from them. Non-paid sessions carry `traffic_source` and null campaign fields through widened v1 contracts and the Flink parser.

**Tech Stack:** Python 3.12, JSON Schema (Kafka contracts, Schema Registry), Java 17 + Maven (Flink job).

**Spec:** `docs/superpowers/specs/2026-11-02-data-variety-3-traffic-rhythm-and-journeys-design.md`. Builds on plans 1–2 (`_pick_campaign(..., shopee_boost)`, `_funnel_steps(..., strength)`).

## Global Constraints

- Hourly weights in Vietnam time (GMT+7, no DST), index 0–23: `1,0.6,0.4,0.3,0.3,0.4,0.8,1.4,2.0,2.4,2.6,2.8,3.4,3.2,2.6,2.4,2.4,2.6,3.0,3.6,4.2,4.4,4.0,2.4`.
- Weekend (Vietnam Saturday, Sunday) ×1.15; double days (month = day) ×1.8; Black Friday 2025-11-28 ×1.6; 10 days before Tết (2025-01-29, 2026-02-17) ×1.4; Tết eve through the fourth day ×0.5; multipliers multiply.
- On double days Shopee spend share ×2 and strength ×1.5.
- Organic + direct = 25 % of a day's non-email sessions (organic 60 %, direct 40 %); purchase strength ×0.8; `traffic_source` ∈ {paid, organic, direct, email}.
- Device strength: mobile ×0.85, desktop ×1.15, tablet ×1.0.
- Contracts stay `schema_version` 1; `flat` output byte-identical.

## Review Focus

- Event-time order across the UTC midnight boundary on a double day (VN date spans two UTC days) — test in Task 2.
- A non-paid order from a customer with an earlier paid click in 7 days keeps that click's attribution; with none, attributed fields are null and Flink accepts it — test in Task 3.
- A paid web event with a null `campaign_id` must still be rejected by Flink (only non-paid may be null) — test in Task 3.
- Vendor reports must still sum to ground truth per UTC day after volumes vary — test in Task 2.
- Bots and duplicates still scheduled at today's rates on varying daily volumes — test in Task 2.

---

### Task 1: Vietnam calendar module

**Files:** Create `src/marketpulse_mock/calendar.py`; Test `tests/test_calendar.py`.

**Interfaces:** `VIETNAM = timezone(timedelta(hours=7))`; `HOURLY_WEIGHTS: tuple[float, ...]` (24); `day_multiplier(vn_date: date) -> float`; `is_double_day(vn_date: date) -> bool`; `is_promo_day(vn_date: date) -> bool` (double days and the 3 days before Tết; used by plan 4); `utc_hour_weights(utc_day: date) -> tuple[float, ...]` (24 values: hourly weight of the VN hour × multiplier of that hour's VN date).

- [ ] **Step 1: Failing tests:** `day_multiplier(date(2025,11,11)) == 1.8`; Saturday 2025-11-15 → 1.15; 2025-11-28 → 1.6; 2026-02-10 (pre-Tết) → 1.4; 2026-02-17 → 0.5; 2026-12-12 Saturday → 1.8×1.15; `utc_hour_weights(date(2025,11,10))[17:]` use the 11/11 multiplier (VN 00:00–07:00 on 11/11).
- [ ] **Step 2: Run** `PYTHONPATH=src .venv/bin/python -m unittest tests.test_calendar -v` — Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — Expected: PASS.
- [ ] **Step 5: Commit** `feat: add a Vietnam retail calendar for the story world`.

### Task 2: Daily volume and sorted event times

**Files:** Modify `src/marketpulse_mock/journeys.py`; Test `tests/test_journeys_variety.py`.

**Interfaces:** Consumes Task 1. Produces `_day_impressions(config, utc_day) -> int` (= round(base × Σ utc_hour_weights / Σ HOURLY_WEIGHTS)); `_impression_times(config, day_index) -> list[tuple[datetime, int]]` (time, ordinal) sorted by time, ordinal ties ascending; the impression loop iterates this list; the Shopee hook passes `shopee_boost=2.0` and strength ×1.5 when the impression's VN date is a double day.

- [ ] **Step 1: Failing tests** (full story config; call `iter_day_records` only for day indices 304–333, i.e. 2025-11-01 → 2025-11-30): sessions 20:00–22:00 VN ≥ 4× sessions 02:00–04:00 VN; 11/11 sessions ≥ 1.5× median of 11/04–11/18 excluding 11/11; Shopee spend share on 11/11 > its share on 11/12; every day's records are non-decreasing in `event_time`; vendor reports equal ground truth per UTC day; anomaly counts non-zero.
- [ ] **Step 2: Run** — Expected: FAIL.
- [ ] **Step 3: Implement**; hour from inverse CDF of `utc_hour_weights` on `stable_int(seed,"impression-hour",day,ordinal,modulo=1_000_000)`, seconds from `stable_int(...,"impression-second",...,modulo=3600)`.
- [ ] **Step 4: Run** journeys, generator, validator, scenario tests — Expected: PASS, fixture unchanged.
- [ ] **Step 5: Commit** `feat: shape story traffic by hour, weekday and sale calendar`.

### Task 3: Organic and direct sessions through widened contracts

**Files:** Modify `src/marketpulse_mock/journeys.py`, `contracts/kafka/web-events-v1.json`, `contracts/kafka/order-events-v1.json`, `flink-job/src/main/java/io/marketpulse/flink/validation/EventParser.java`, `docs/architecture/high-level-design.md`; Test `tests/test_journeys_variety.py`, `flink-job/src/test/java/io/marketpulse/flink/validation/EventParserTest.java`, `tests/test_kafka_contracts.py`.

**Interfaces:** `traffic_source` on every web `page_view` payload and every order payload (`paid` for ad sessions; story only — `flat` omits it); contracts: `campaign_id`, `platform_campaign_id`, `click_id` typed `["string","null"]`; order `attributed_platform`, `attributed_campaign_id` typed `["string","null"]`. Flink: web valid if `campaign_id` non-blank **or** payload `traffic_source` ∈ {organic, direct, email}; order valid likewise for `campaign_id` and both attributed fields.

- [ ] **Step 1: Failing tests:** Python — over 30 days organic+direct are 22–28 % of non-email sessions; none has `click_id`; organic orders with a prior paid click carry that click's attribution, others null. Java — `EventParser` accepts an organic web event with null campaign; rejects a paid web event with null campaign; accepts an organic order with null attributed fields. Contract test — a fixture web event and an organic web event both validate against v1.
- [ ] **Step 2: Run** Python tests and `cd flink-job && mvn -q test -Dtest=EventParserTest` — Expected: FAIL.
- [ ] **Step 3: Implement**; non-paid count per day = round(paid_sessions / 3); times drawn like Task 2; strength ×0.8; architecture doc gains one paragraph on non-paid traffic and the widened fields.
- [ ] **Step 4: Run** all Python tests, `mvn -q test`, and `scripts/kafka-smoke-test.sh` against a local registry if available — Expected: PASS.
- [ ] **Step 5: Commit** `feat: add organic and direct story visits with compatible v1 contracts`.

### Task 4: Device-specific conversion

**Files:** Modify `src/marketpulse_mock/journeys.py`; Test `tests/test_journeys_variety.py`.

- [ ] **Step 1: Failing test:** over 30 days purchase/sessions desktop > tablet > mobile.
- [ ] **Step 2: Run** — Expected: FAIL.
- [ ] **Step 3: Implement** by multiplying `strength` with the device multiplier.
- [ ] **Step 4: Run** journeys and generator tests — Expected: PASS.
- [ ] **Step 5: Commit** `feat: let devices convert differently in the story world`; open the PR.
