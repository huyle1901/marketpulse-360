# Data Variety 1 — Geography and Customer Growth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the story-demo world a scenario with 13 weighted cities, customers joining through 2025–2026 with tenure decay, and returning anonymous devices, and show the 13 cities on the Geography map.

**Architecture:** A frozen `Scenario` attached to each profile selects behaviour; `flat` keeps today's code paths byte for byte, `vietnam-retail` (story-demo) switches to weighted draws. A per-day `CustomerPicker` and a pure per-impression planner keep generation deterministic and fast.

**Tech Stack:** Python 3.12 (`src/marketpulse_mock`, `unittest`), Next.js 16 + Playwright (`web`).

**Spec:** `docs/superpowers/specs/2026-11-02-data-variety-1-geography-and-customers-design.md` (overview `2026-11-01-story-data-variety-design.md`).

## Global Constraints

- `fixture`, `smoke`, `demo`, `benchmark` use scenario `flat`; their output is byte-identical to today (committed `data/mock/fixtures/seed-360` manifest and checksums unchanged).
- `story-demo` uses `vietnam-retail`; customers 3,000; `impressions_per_day` 4,000; campaigns stay 12 in this plan (plan 2 changes them).
- City weights: Ho Chi Minh City 28, Ha Noi 24, Bien Hoa 6, Thu Dau Mot 6, Hai Phong 6, Da Nang 6, Can Tho 5, Nha Trang 4, Hue 3, Vung Tau 3, Quy Nhon 3, Buon Ma Thuot 3, Ha Long 3 (country `VN`).
- 40 % of customers created in the 730 days before `start_date`; 60 % during the window with joins per day at the end ≈ 2× the start.
- Tenure weight `1 / (1 + months_since_join / 6)`; each customer joining inside the window has a session on the join day.
- 30 % of anonymous sessions reuse a device first seen in the previous 60 days.
- Deterministic: same seed → identical output; different seed → different output. No wall-clock reads.
- Python 3.12, typed public interfaces; TDD; commit messages without any assistant attribution.

## Review Focus

- A join day with more joiners than identified sessions that day → every joiner still gets a session (overflow sessions are allowed) — test in Task 3.
- `_latest_purchase_touch` recomputing touches for other impressions must use the same `CustomerPicker` as the session itself, or attribution diverges — test in Task 3.
- Day 0 of the window has no previous days → returning-device lookup must not reach before `start_date` — test in Task 4.
- A customer created on the last day of the window appears in that day's CRM snapshot and nowhere earlier — test in Task 3.
- Thirteen bubbles in the South cluster (HCMC, Bien Hoa, Thu Dau Mot, Vung Tau, Can Tho) → default callouts must not overlap each other — test in Task 5.

---

### Task 1: Scenario plumbing with a byte-identical `flat`

**Files:**
- Modify: `src/marketpulse_mock/config.py`
- Modify: `src/marketpulse_mock/masters.py` (read cities from the scenario)
- Modify: `docs/architecture/high-level-design.md` (mock-data section: one paragraph on scenarios)
- Test: `tests/test_scenario.py` (new)

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True, slots=True) class Scenario` with fields `name: str`, `cities: tuple[tuple[str, str, int], ...] | None` (None = legacy uniform five cities), `customers_before_start_share: float | None`, `join_ramp: float`, `tenure_months: float | None`, `returning_device_share: float`, `returning_window_days: int`.
  - `SCENARIOS: Mapping[str, Scenario]` with `"flat"` (all optional fields None/0) and `"vietnam-retail"`.
  - `ScaleProfile.scenario: str = "flat"`; `GenerationConfig.scenario -> Scenario` (property).
  - `resolve_config(..., days: int | None = None)`: optional shorter window (1 ≤ days ≤ profile.days) for tests and smoke runs.
- Consumes: nothing.

- [ ] **Step 1: Write the failing tests** in `tests/test_scenario.py`:
  - `test_flat_profiles_reproduce_the_committed_fixture`: generate `fixture`, seed 360 into a temp dir and assert every `files[*].sha256` and row count in its `manifest.json` equals `data/mock/fixtures/seed-360/manifest.json`.
  - `test_story_demo_uses_vietnam_retail`: `resolve_config("story-demo", 360, tmp).scenario.name == "vietnam-retail"`, `.profile.customers == 3000`, `.profile.impressions_per_day == 4000`.
  - `test_days_override_shortens_the_window`: `resolve_config("story-demo", 360, tmp, days=30).profile.days == 30`; `days=0` and `days=639` raise `ValueError`.
- [ ] **Step 2: Run** `PYTHONPATH=src .venv/bin/python -m unittest tests.test_scenario -v` — Expected: FAIL (`AttributeError: scenario`).
- [ ] **Step 3: Implement** the interfaces above. `days` override uses `dataclasses.replace` on the profile. Keep `campaigns % 3` validation for now (plan 2 moves it).
- [ ] **Step 4: Run** the new tests and `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests` — Expected: all pass (41 integration skips as today).
- [ ] **Step 5: Commit** `feat: attach a generation scenario to each profile`.

### Task 2: Weighted cities and customer join dates

**Files:**
- Modify: `src/marketpulse_mock/masters.py` (`_generate_customers`)
- Test: `tests/test_masters_variety.py` (new)

**Interfaces:**
- Consumes: `GenerationConfig.scenario` (Task 1).
- Produces: `_customer_city(config: GenerationConfig, ordinal: int) -> tuple[str, str]`; `_customer_created_at(config: GenerationConfig, ordinal: int) -> datetime` (UTC). With `scenario.cities is None` both return today's values exactly.

- [ ] **Step 1: Write the failing tests** (story-demo, seed 360, `generate_master_data` only):
  - `test_city_shares_follow_the_weights`: for each city, `abs(share*100 - weight) <= 3` over 3,000 customers.
  - `test_forty_percent_joined_before_the_window`: share with `created_at.date() < start_date` within 0.37–0.43; earliest ≥ `start_date - 730 days`; latest ≤ last window day.
  - `test_joins_ramp_up`: joins in the window's last 60 days ≥ 1.6 × joins in its first 60 days.
- [ ] **Step 2: Run** `... -m unittest tests.test_masters_variety -v` — Expected: FAIL.
- [ ] **Step 3: Implement.** City: cumulative weights over `stable_int(seed, "customer-city", ordinal, modulo=100)`. Join date: `stable_int(seed, "customer-join", ordinal, modulo=1000) < 400` → before window uniform over 730 days; else inverse-CDF of a linear ramp density `1 + (join_ramp - 1) * t` over the window, `t` from `stable_int(seed, "customer-join-t", ordinal, modulo=1_000_000)`.
- [ ] **Step 4: Run** the new tests and `tests.test_scenario`, `tests.test_masters` — Expected: PASS (flat unchanged).
- [ ] **Step 5: Commit** `feat: spread story customers over cities and join dates`.

### Task 3: Customers act only after joining, with tenure decay

**Files:**
- Modify: `src/marketpulse_mock/journeys.py` (`_select_customer`, `_latest_purchase_touch`, `_iter_crm_snapshots`, `_email_audience`)
- Create: `src/marketpulse_mock/customer_picker.py`
- Test: `tests/test_journeys_variety.py` (new)

**Interfaces:**
- Consumes: `_customer_created_at` results via `Customer.created_at` (Task 2).
- Produces: `class CustomerPicker` with `@classmethod for_day(cls, config, customers, day: date) -> CustomerPicker`, `pick(self, day_index: int, impression_ordinal: int) -> Customer | None` (None = anonymous; keeps the 70 % identified rule), and `joiners: tuple[Customer, ...]` (customers whose `created_at.date() == day`). Built once per day in `iter_day_records` and passed to every `_select_customer` / `_latest_purchase_touch` call for that day. `flat` → uniform pick identical to today.

- [ ] **Step 1: Write the failing tests** (story-demo, `days=30`, iterate `iter_day_records` for all 30 days in memory):
  - `test_no_customer_event_before_join`: every record with a `customer_id` has `event_time >= created_at`.
  - `test_every_joiner_has_a_session_on_the_join_day`: for customers with `created_at` inside the window, a `page_view` exists on that UTC date with their id.
  - `test_tenure_decays_activity`: sessions per customer per day for tenure ≥ 12 months < 0.6 × those for tenure < 1 month.
  - `test_crm_snapshot_starts_on_join_day`: a customer created on window day k appears in CRM snapshots from day k only.
  - `test_attribution_uses_the_same_picker`: for every purchase, the `attributed_campaign_id` equals the campaign of the latest click by the same customer within 7 days among that day's generated sessions.
- [ ] **Step 2: Run** `... -m unittest tests.test_journeys_variety -v` — Expected: FAIL.
- [ ] **Step 3: Implement** `CustomerPicker`: joined customers with weights `1/(1+months/6)` scaled to integers; cumulative list + `bisect` on `stable_int(seed, "session-customer", day, ordinal, modulo=total)`. The first identified sessions of the day take `joiners` in order before weighted picks; if joiners outnumber identified sessions, emit extra join-day sessions after the impression loop (paid campaign from the same allocation as today). CRM and the legacy email audience filter on `created_at.date() <= day`.
- [ ] **Step 4: Run** new tests, `tests.test_journeys`, `tests.test_generator`, `tests.test_validator`, `tests.test_scenario` — Expected: PASS.
- [ ] **Step 5: Commit** `feat: let story customers act only after joining, with tenure decay`.

### Task 4: Returning anonymous devices

**Files:**
- Modify: `src/marketpulse_mock/journeys.py` (anonymous id assignment)
- Test: `tests/test_journeys_variety.py`

**Interfaces:**
- Produces: `_anonymous_id(config, masters, day_index: int, impression_ordinal: int) -> str` — fresh `stable_id(seed, f"anonymous-{day}", ordinal)` as today, or, for `returning_device_share` of anonymous sessions, the fresh id of an anonymous session on an earlier day `day - 1 - (u % returning_window_days)` (≥ 0) found by `_is_fresh_anonymous_session(config, masters, day, ordinal) -> bool` (recomputes the click and identified decisions; tries up to 8 candidates, else fresh).

- [ ] **Step 1: Write the failing tests:**
  - `test_a_fifth_of_anonymous_sessions_return`: over 30 days, ≥ 20 % of anonymous session ids reuse an `anonymous_id` first seen on an earlier day.
  - `test_day_zero_never_returns`: day 0 has no reused anonymous ids.
  - `test_determinism_with_returning_devices`: two in-memory runs give identical record streams; seed 361 differs.
- [ ] **Step 2: Run** — Expected: FAIL.
- [ ] **Step 3: Implement** as in Interfaces; `flat` always fresh.
- [ ] **Step 4: Run** the journeys, generator, validator and scenario tests — Expected: PASS.
- [ ] **Step 5: Commit** `feat: bring some anonymous story visitors back`.

### Task 5: Thirteen cities on the Geography map

**Files:**
- Modify: `web/lib/cities.ts`, `web/components/VietnamMap.tsx`, `web/app/geography/page.tsx`
- Test: `web/tests/geography.spec.ts`

**Interfaces:**
- Produces: `CITY_COORDINATES[city] = {lat, lon, label: 'left' | 'right', dy: number}`; `CITY_REGION` for all 13; `VietnamMap` prop `callouts: number` (default 6) — the six largest by value show callouts; others show theirs when `active`.

Coordinates and regions: Ho Chi Minh City 10.776/106.701 South; Ha Noi 21.028/105.854 North; Bien Hoa 10.957/106.843 South; Thu Dau Mot 10.980/106.652 South; Hai Phong 20.845/106.688 North; Da Nang 16.054/108.202 Central; Can Tho 10.045/105.747 South; Nha Trang 12.239/109.197 Central; Hue 16.463/107.590 Central; Vung Tau 10.346/107.084 South; Quy Nhon 13.776/109.224 Central; Buon Ma Thuot 12.667/108.038 Central; Ha Long 20.951/107.080 North. Starting label offsets (adjust after the screenshot): HCMC right 0, Bien Hoa right −46, Thu Dau Mot left −40, Vung Tau right +44, Can Tho left +12, Ha Noi left 0, Hai Phong right +8, Ha Long right −40, Da Nang left 0, Hue left −40, Quy Nhon right 0, Nha Trang right +36, Buon Ma Thuot left +20.

- [ ] **Step 1: Write the failing tests** (13-city fixture):
  - `all thirteen cities plot`: `circle[data-city]` count 13.
  - `six callouts by default, more on hover`: `[data-callout]` visible count 6; hovering the Hue ranking row makes `[data-callout="Hue"]` visible.
  - `default callouts never overlap`: bounding boxes of the visible callouts pairwise disjoint.
  - `every city belongs to a region`: region legend sums to located revenue.
- [ ] **Step 2: Run** `cd web && npx playwright test tests/geography.spec.ts` — Expected: FAIL.
- [ ] **Step 3: Implement** per Interfaces; tune `dy` until the overlap test passes.
- [ ] **Step 4: Run** `npx tsc --noEmit -p .` and the full `npx playwright test` — Expected: all pass.
- [ ] **Step 5: Commit** `feat: map all thirteen story cities with collision-free callouts`; open the PR with the spec link.
