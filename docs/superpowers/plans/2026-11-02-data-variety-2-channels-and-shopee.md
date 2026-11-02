# Data Variety 2 — Channel Economics and Shopee Ads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give story channels distinct economics and add Shopee Ads end to end, from the generator's vendor report to the Command Center.

**Architecture:** The `vietnam-retail` scenario carries platforms, spend shares and personas; impressions are drawn by spend share ÷ cost multiplier. A `shopee_ads` dataset flows through one shared vendor parser in Silver, the batch selection and contracts, and a single channel registry in the web app.

**Tech Stack:** Python 3.12, PySpark/Iceberg (Silver), Dagster/dbt, FastAPI, Next.js + Playwright.

**Spec:** `docs/superpowers/specs/2026-11-02-data-variety-2-channels-and-shopee-design.md`. Builds on plan 1 (`Scenario`, `CustomerPicker`).

## Global Constraints

- Platforms (`vietnam-retail`): google, meta, tiktok, shopee; story-demo campaigns 16 (4 per platform, 2 ads each); Shopee prefixes campaign `S`, ad `SA`, click id `shpclid`.
- Spend shares: Google 30, Meta 28, TikTok 24, Shopee 18; objective spend weights conversions 1.3, traffic 1.0, awareness 0.7.
- Personas (cost / CTR / strength m): Google 1.4/0.8/1.3; Meta 1.0/1.0/1.0; TikTok 0.6/1.6/0.5; Shopee 1.0/1.1/1.6.
- Funnel: view and cart cut-offs unchanged (850, 400 per 1,000); checkout|cart = min(0.9, 0.625·√m); purchase|checkout = min(0.9, 0.48·m).
- Shopee report: CSV `shopee_ads`, columns `date` (DD/MM/YYYY), `ad_id`, `impression`, `clicks`, `expense`, `conversions`, `gmv` (whole VND).
- `flat` output byte-identical; default `BATCH_REQUIRED_PLATFORMS` stays `google_ads,meta_ads,tiktok_ads`.
- Web palette for 4 channels + unattributed grey passes the dataviz validator with `--pairs all` in light and dark.

## Review Focus

- A Shopee date like `05/09/2026` must parse as 5 September, never 9 May — test in Task 4.
- A three-platform stack archiving by month must still pass the range check without any Shopee file — test in Task 5.
- `platform=shopee` on an API with a three-platform release returns an empty result, not an error — test in Task 6.
- Unknown datasets (e.g. `lazada_ads`) stay rejected everywhere they were — test in Tasks 4 and 5.
- Channel colour follows the channel, not its rank, when a filter removes one — test in Task 7.

---

### Task 1: Scenario platforms and 16 story campaigns

**Files:** Modify `src/marketpulse_mock/config.py`, `src/marketpulse_mock/masters.py`; Test `tests/test_masters_variety.py`.

**Interfaces:**
- Produces: `Scenario.platforms: tuple[str, ...]` (flat: google, meta, tiktok), `Scenario.platform_spend_share: Mapping[str, int] | None`, `Scenario.objective_spend_weight: Mapping[str, float] | None`, `Scenario.personas: Mapping[str, tuple[float, float, float]] | None`; prefix maps include shopee; `campaigns % len(scenario.platforms) == 0` validated in `GenerationConfig.__post_init__` (not `ScaleProfile`).

- [ ] **Step 1: Failing tests:** `test_story_has_four_campaigns_per_platform` (16 campaigns, 4 each, ids `S-360-…` for Shopee, two ads each with `SA-` ids); `test_flat_keeps_three_platforms` (fixture campaigns unchanged).
- [ ] **Step 2: Run** `PYTHONPATH=src .venv/bin/python -m unittest tests.test_masters_variety -v` — Expected: FAIL.
- [ ] **Step 3: Implement**; story-demo profile `campaigns=16`.
- [ ] **Step 4: Run** masters, scenario and generator tests — Expected: PASS.
- [ ] **Step 5: Commit** `feat: add Shopee as a fourth story platform`.

### Task 2: Spend-weighted impressions and channel personas

**Files:** Modify `src/marketpulse_mock/journeys.py`; Test `tests/test_journeys_variety.py`.

**Interfaces:**
- Produces: `_pick_campaign(config, masters, day_index: int, impression_ordinal: int, shopee_boost: float = 1.0) -> Campaign` (weights = spend share × objective weight ÷ cost multiplier; flat → today's round-robin); `_impression_cost(config, campaign, day_index, ordinal) -> int` (today's 100–1,000 range × cost multiplier); `_click_threshold(config, campaign) -> int` (today's objective threshold × CTR multiplier, flat unchanged); `_funnel_steps(config, campaign, score: int, strength: float) -> tuple[bool, bool, bool, bool]` (view, cart, checkout, purchase). Plan 3 passes extra strength through `strength`.

- [ ] **Step 1: Failing tests** (story, `days=30`, from in-memory ground truth): spend share per platform within ±3 points of 30/28/24/18; TikTok clicks/impressions > Google's; TikTok purchases/sessions < Google's; Shopee purchases/sessions highest; checkout and purchase conditional rates ≤ 0.9.
- [ ] **Step 2: Run** — Expected: FAIL.
- [ ] **Step 3: Implement** the four helpers and use them in `iter_day_records` and `_latest_purchase_touch` (both must call the same `_pick_campaign`).
- [ ] **Step 4: Run** journeys, generator, validator, scenario tests — Expected: PASS; fixture checksums unchanged.
- [ ] **Step 5: Commit** `feat: give story channels their own costs, clicks and conversion`.

### Task 3: Shopee Ads vendor report

**Files:** Modify `src/marketpulse_mock/journeys.py` (`_vendor_report_payload`, `_click_id`), `writers.py` (columns), `generator.py` (`SCHEMA_VERSIONS`), `validator.py` (`EXPECTED_SCHEMA_VERSIONS`, vendor set, file ordering 4); Test `tests/test_journeys_variety.py`, `tests/test_validator.py`.

**Interfaces:** Produces dataset `shopee_ads` (CSV, schema version 1) with the Global Constraints columns; `date` formatted `f"{d:%d/%m/%Y}"`.

- [ ] **Step 1: Failing tests:** `test_shopee_report_reconciles_with_ground_truth` (per campaign-day impressions, clicks, expense, conversions, gmv equal truth); `test_shopee_dates_are_day_first`; validator accepts a generated story world of 3 days and rejects a Shopee row whose `ad_id` maps to no campaign.
- [ ] **Step 2: Run** — Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** generator, validator, journeys tests; `python -m marketpulse_mock validate --input data/mock/fixtures/seed-360` — Expected: PASS, fixture unchanged.
- [ ] **Step 5: Commit** `feat: write Shopee Ads reports in Seller Centre shape`.

### Task 4: One vendor parser in Silver, Shopee included

**Files:** Modify `src/marketpulse_lakehouse/silver.py`, `src/marketpulse_lakehouse/source_files.py`; Test `tests/test_lakehouse_silver.py`, `tests/test_lakehouse_source_files.py`.

**Interfaces:** Produces `VENDOR_DATASETS: Mapping[str, str]` (dataset → platform) and `parse_vendor_spend(dataset: str, payload: Mapping[str, object]) -> VendorSpend` (`spend_date`, `platform`, `platform_campaign_id`, `impressions`, `clicks`, `conversions`, `spend_minor`, `conversion_value_minor`); both `build_ad_spend_daily` and the Spark `parse_spend` call it; the Spark filter uses `VENDOR_DATASETS`.

- [ ] **Step 1: Failing tests:** existing Google/Meta/TikTok cases through `parse_vendor_spend` give today's values; `shopee_ads` row `{"date":"05/09/2026","ad_id":"S-360-00004","impression":"120","clicks":"9","expense":"81000","conversions":"2","gmv":"3200000"}` → 2026-09-05, shopee, spend 81,000, value 3,200,000; `lazada_ads` raises `ValueError`; `shopee_ads` is an allowed source dataset.
- [ ] **Step 2: Run** `... -m unittest tests.test_lakehouse_silver tests.test_lakehouse_source_files -v` — Expected: FAIL.
- [ ] **Step 3: Implement** (refactor first, then add Shopee).
- [ ] **Step 4: Run** the lakehouse tests; with `MP_SPARK_TEST=1` where Spark is available run the Spark Silver test — Expected: PASS.
- [ ] **Step 5: Commit** `refactor: parse vendor spend once in Silver and accept Shopee`.

### Task 5: Batch selection and required platforms

**Files:** Modify `src/marketpulse_batch/contracts.py`, `selection.py`, `compose.yaml` (pass `BATCH_REQUIRED_PLATFORMS` to `batch-code`, `dagster-webserver`, `dagster-daemon` with today's default); Test `tests/test_batch_selection.py`.

**Interfaces:** `REPORT_PLATFORMS` gains `shopee_ads`; `DAY_FIELDS["shopee_ads"] = ("date",)`; `source_day(dataset: str, value: str) -> date` (day-first for `shopee_ads`, ISO otherwise); `select_range_sources(plan, from_date, to_date, required_platforms: tuple[str, ...])` requires `crm` plus `required_platforms` on every day.

- [ ] **Step 1: Failing tests:** a three-platform plan passes the range check with `required_platforms=("google_ads","meta_ads","tiktok_ads")`; a four-platform plan missing one Shopee day fails when Shopee is required; Shopee `date` `01/02/2026` selects 1 February; `lazada_ads` still rejected.
- [ ] **Step 2: Run** `... -m unittest tests.test_batch_selection -v` — Expected: FAIL.
- [ ] **Step 3: Implement**; update callers (`definitions.py`, CLI) to pass `BatchSelection.required_platforms`.
- [ ] **Step 4: Run** batch tests and `docker compose config -q` — Expected: PASS.
- [ ] **Step 5: Commit** `feat: let batch stacks declare Shopee as a required report`.

### Task 6: App contract and dbt domains

**Files:** Modify `src/marketpulse_app/contracts.py`, `dbt/marketpulse_serving/tests/accepted_domains.sql`; Test `tests/test_app_contracts.py`, `tests/test_dbt_contract_gates.py`.

- [ ] **Step 1: Failing tests:** `platform="shopee"` accepted; `platform="lazada"` raises `ValueError("unsupported platform")`; an API request with `platform=shopee` on a three-platform fixture release returns 200 with empty rows.
- [ ] **Step 2: Run** — Expected: FAIL.
- [ ] **Step 3: Implement** (`PLATFORMS = {"google","meta","tiktok","shopee"}` in one place; dbt lists the same four).
- [ ] **Step 4: Run** app tests — Expected: PASS.
- [ ] **Step 5: Commit** `feat: accept Shopee in the app filter and dbt domains`.

### Task 7: One channel registry in the web app

**Files:** Create `web/lib/channels.ts`; Modify `web/app/globals.css` (channel tokens), `web/app/ads/page.tsx`, `web/app/page.tsx`, `web/app/analysis/page.tsx`, `web/components/{ChannelBreakdown,CampaignTable,CampaignScatter,AttributionChart}.tsx`, `web/lib/format.ts` (`channelName` reads the registry), `web/app/dashboard-theme.css` (platform chips); Test `web/tests/channels.spec.ts` (new) plus existing specs.

**Interfaces:** `CHANNELS: readonly {key: 'google'|'meta'|'tiktok'|'shopee'; label: string; color: string}[]` with `color` = `var(--channel-<key>)`; `UNATTRIBUTED_COLOR = 'var(--channel-unattributed)'`; `channelOf(key: string | null)`.

- [ ] **Step 1: Validate the palette:** `node <dataviz>/scripts/validate_palette.js "<google>,<meta>,<tiktok>,<shopee>,<grey>" --mode light --pairs all` and `--mode dark`; start from light `#2a78d6,#eb6834,#1baf7a,#c2378f,#94a3b8` and dark `#3b82f6,#c9480c,#26ad84,#c03a95,#8fa3bd`; adjust steps until both pass (grey may fail only the chroma floor).
- [ ] **Step 2: Failing tests:** Shopee appears with its colour in the Overview donut legend, Ads small multiples and scatter legend, analysis channel select and bars; the same channel keeps its colour when the platform filter removes another; legend marks visible and distinct (`legend-marks.ts`) in both themes.
- [ ] **Step 3: Run** `cd web && npx playwright test tests/channels.spec.ts` — Expected: FAIL.
- [ ] **Step 4: Implement**; remove every local `channelColors`/`channelNames` copy.
- [ ] **Step 5: Run** `npx tsc --noEmit -p .` and the full Playwright suite — Expected: PASS. **Commit** `feat: drive every channel view from one registry with Shopee`; open the PR.
