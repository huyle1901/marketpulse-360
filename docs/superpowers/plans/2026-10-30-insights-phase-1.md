# Insights Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a serving-only **Insights** workspace covering when customers act, order value distribution, revenue by loyalty tier, and web vs app. It needs no pipeline change.

**Architecture:** A new `InsightsService` extends `LifecycleService` to reuse its release pinning, single-currency guard and projection consistency gate. Its four reads use certified ClickHouse relations: the journey projection `events`, plus `customer_360` joined to `customers`. Four viewer-level routes go through the existing proxy allowlist; one Next.js page renders them with shadcn/ui and Recharts.

**Tech Stack:** Python 3.12, FastAPI, ClickHouse SQL, Next.js 16, React 19, shadcn/ui, Recharts, Playwright.

**Spec:** `docs/superpowers/specs/2026-10-30-richer-marketing-insights-design.md` (Phase 1)

## Global Constraints

- Work on branch `feat/insights-phase-1`, stacked on `feat/visual-command-center-story-demo`; open its PR against that branch.
- No pipeline, Gold, dbt, export or fixture change. Read only already-certified and published relations.
- Every read is release-pinned. An unpublished projection returns `status: 'unavailable'`, never zeros. Mixed currency returns 409 `unsupported_currency` (existing guard).
- Money stays integer strings; ratios use `marketpulse_app.math.ratio` (six places, truncated). The UI formats with BigInt.
- Times are UTC; weekday uses ISO numbering (1 = Monday … 7 = Sunday).
- Bot-flagged web/mobile sessions are excluded from session counts. Commerce orders keep the MP-04 policy: all `order_created` events count.

## Review Focus

1. **Mixed-currency release:** order values and loyalty must fail closed with 409 and show unavailable, not a summed amount. Test in Task 1.
2. **No orders:** histogram bands empty, median/P90 null, no division by zero. Test in Task 1.
3. **Order without a known session source:** counted as `unknown`, never assigned to web. Test in Task 1.
4. **Anonymous and unmatched profiles:** form a separate `none` tier and are never folded into bronze. Test in Task 1.
5. **Insights page receives an unexpected body:** shows unavailable, not a crash. Test in Task 3.

---

### Task 1: InsightsService reads

**Files:**
- Create: `src/marketpulse_app/insights.py`
- Test: `tests/test_app_insights.py` (fake Session pattern from `tests/test_app_engagement.py`)

**Interfaces:**
- Consumes: `LifecycleService._context(detail: bool) -> (session, publication|None, as_of, provenance)`; `ratio(int, int) -> str|None`; `wire`.
- Produces `class InsightsService(LifecycleService)` with:
  - `activity_heatmap() -> {'status','timezone':'UTC','cells':[{'weekday':int,'hour':int,'sessions':str,'orders':str}],'busiest':{'weekday','hour','sessions'}|None,'provenance'}`. Exactly 168 cells, every weekday × hour, including zero cells.
  - `order_values() -> {'status','currency','orders':str,'gross_minor':str,'median_minor':str|None,'p90_minor':str|None,'band_width_minor':str|None,'bands':[{'from_minor':str,'to_minor':str|None,'orders':str,'gross_minor':str}],'provenance'}`.
  - `loyalty() -> {'status','currency','tiers':[{'tier':'gold'|'silver'|'bronze'|'none','customers':str,'orders':str,'net_revenue_minor':str,'net_per_customer_minor':str|None}],'provenance'}`. Tiers in the order gold, silver, bronze, none.
  - `channels() -> {'status','sources':[{'source':'web'|'mobile'|'unknown','sessions':str,'orders':str,'conversion_rate':str|None}],'provenance'}`.

- [ ] **Step 1: Write failing tests** `test_heatmap_returns_168_cells_and_busiest_slot`: the fake returns two rows. Assert 168 cells, the zero cell `(weekday=3,hour=4)` has `'0'`, busiest is the max-sessions cell, the SQL uses `toDayOfWeek` and `toHour` on session start, and excludes `is_bot_session`. `test_order_values_bands_use_nice_width_from_p95`: with p95 = 3,900,000, `band_width_minor == '500000'`; there are 8 bands and the last has `to_minor is None`; band order counts sum to `orders`. `test_order_values_without_orders_is_empty_not_error`: `bands == []`, `median_minor is None`. `test_loyalty_keeps_unmatched_profiles_in_none_tier_and_reconciles`: tier net sum equals the fake's Gold total. `test_channels_count_unknown_session_source_separately`. `test_mixed_currency_and_missing_projection`: `LifecycleError('unsupported_currency')` propagates; `activity_heatmap()['status'] == 'unavailable'` when the projection is missing.
- [ ] **Step 2: Run** `PYTHONPATH=src .venv/bin/python -m unittest tests.test_app_insights` and expect failures (import error).
- [ ] **Step 3: Implement** `InsightsService` in `src/marketpulse_app/insights.py`. Band width is the smallest value in `{1,2,5}×10^k` that is ≥ `ceil(p95/8)`, with band k covering `[k·w,(k+1)·w)` for k = 0..6 and band 7 being `[7w, ∞)`. Median, P90 and P95 come from ClickHouse `quantilesExact(0.5,0.9,0.95)` over `order_created.gross_amount_minor`. Session source is the `any(source)` of a session's web/mobile events; an order joins by `session_id`, and no match means `unknown`. Loyalty uses `_context(detail=False)` and joins `customer_360` (profile_type `customer`) to `customers.loyalty_tier`; every other profile goes to `none`.
- [ ] **Step 4: Run** the same command and expect OK.
- [ ] **Step 5: Commit** `feat: add insights reads for activity, order values, loyalty and channels`.

### Task 2: HTTP routes and access

**Files:**
- Create: `src/marketpulse_app/insights_api.py` (`register_insights_routes(app, analytics)`)
- Modify: `src/marketpulse_app/api.py` (register routes), `src/marketpulse_app/security.py` (add `('GET', r'insights/(?:activity|order-values|loyalty|channels)', 'viewer', 'insights/aggregate')`), `web/app/api/v1/[...path]/route.ts` (allow `GET insights/(activity|order-values|loyalty|channels)`)
- Test: `tests/test_app_insights.py` (`InsightsRouteTests`)

**Interfaces:** Consumes the Task 1 methods. Produces `GET /api/v1/insights/{activity,order-values,loyalty,channels}` (no-store), with `LifecycleError` mapped exactly as in lifecycle routes.

- [ ] **Step 1: Write the failing test** `test_insights_routes_are_viewer_aggregates`: `policy('GET', path)[1] == 'viewer'` for all four paths; with methods patched, each returns 200 and `cache-control: no-store`; `unsupported_currency` returns 409.
- [ ] **Step 2: Run** `PYTHONPATH=src:tests .venv/bin/python -m unittest tests.test_app_insights` and expect failure.
- [ ] **Step 3: Implement** the routes, policy and allowlist.
- [ ] **Step 4: Run** the focused tests, then the full Python suite, and expect OK.
- [ ] **Step 5: Commit** `feat: expose viewer-level insights routes`.

### Task 3: Insights workspace UI

**Files:**
- Create: `web/app/insights/page.tsx`, `web/components/ActivityHeatmap.tsx`, `web/components/OrderValueChart.tsx`
- Modify: `web/components/SessionGate.tsx` (nav item `['/insights','Insights',Sparkles]` after Overview)
- Test: `web/tests/insights.spec.ts`

**Interfaces:** Consumes the four Task 2 routes and `useLifecycleRead`-style shape validation (generalize it to take a full path: `useRead<T>(path: string)` in `web/components/Engagement.tsx`, keeping `useLifecycleRead` as a wrapper).

- [ ] **Step 1: Write failing tests:**
  - `insights leads with a weekday-hour heatmap and a busiest-slot callout`: region `When customers act`; a grid with role `table` named `Sessions by weekday and hour (UTC)`; cell for Wed 10:00 has an accessible label containing `12 sessions`; callout text `Busiest: Wednesday 10:00–11:00 UTC`; a toggle `Show orders` switches labels to orders.
  - `order value distribution shows median, P90 and exact bands`: cards `Median order` / `90th percentile` show `₫1,250,000` / `₫3,400,000`; img `Orders by order value`; exact table row `₫0 – ₫500,000` with the order count.
  - `loyalty tiers stay separate from customers without a tier`: tier cards Gold/Silver/Bronze plus `No CRM tier`.
  - `web vs app compares sessions, orders and conversion`, with an `unknown` row when present.
  - `unexpected insight bodies show unavailable, not a crash`.
  - `insights fits a phone`.
- [ ] **Step 2: Run** `npx playwright test tests/insights.spec.ts` (in `web/`) and expect failure.
- [ ] **Step 3: Implement** the page and components. The heatmap is a CSS grid of 7 rows × 24 columns. The color scale is linear in the cell value relative to the max, with an exact count in each cell's `aria-label` and title; an "Exact values" disclosure holds a plain table. Use the shadcn Card and Recharts BarChart.
- [ ] **Step 4: Run** the focused tests, the full browser suite, `npx tsc --noEmit` and `npm run build`; expect all to pass.
- [ ] **Step 5: Commit** `feat: add insights workspace with activity heatmap and order values`.

### Task 4: Story-demo verification, docs and PR

**Files:** Modify `docs/architecture/high-level-design.md` (one paragraph in §4.10), `docs/runbooks/story-demo.md` (Insights evidence), `specs/README.md` (phase status).

- [ ] **Step 1:** Rebuild `api` and `web` in story-demo, start the app, and capture headless screenshots of `/insights` at 1440×1000 and 390×844 with no page errors.
- [ ] **Step 2: Oracle checks** against the live release, all of which must hold exactly:
  - heatmap order total = certified Gold orders (11,124);
  - heatmap session total = distinct non-bot web/mobile sessions in the projection;
  - order-value band order counts and gross = certified orders and gross;
  - tier net sum = `customer_360` net total;
  - channel orders, including `unknown`, = projection `order_created` count.
- [ ] **Step 3:** Update the docs with the observed numbers; run the full Python suite, browser suite, build and `git diff --check`.
- [ ] **Step 4:** Commit `docs: record insights phase 1 evidence`, push `feat/insights-phase-1`, and open the PR against `feat/visual-command-center-story-demo`.
