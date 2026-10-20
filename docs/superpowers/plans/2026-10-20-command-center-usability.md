# Command Center Usability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement all five agreed usability improvements with exact analytical semantics and existing access boundaries.

**Architecture:** Extend the pinned ClickHouse dashboard read with daily/prior-period aggregates. Shared Next.js components own presentation, URL state and local table interactions; PostgreSQL operational contracts stay unchanged.

**Tech Stack:** Python 3.12+, FastAPI, Next.js, React, TypeScript, Playwright, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-20-command-center-usability-design.md`

## Global Constraints

- Keep FastAPI + Next.js; add no runtime dependencies.
- ClickHouse owns analytics; PostgreSQL owns accounts, budgets, rules, segments, and audit metadata.
- Preserve exact integer strings and BigInt money; never sum currencies together.
- Preserve certified/provisional separation, release provenance, stale-result labels, race guards, consent, and RBAC.
- Missing data stays unavailable; never invent historical points, prior periods, or cohort zeros.
- All mandatory services remain runnable through Docker Compose profiles.

## Review Focus

- Huge and negative financial values must remain exact in display, aggregation and numeric sorting.
- Query restoration/back navigation must trigger current reads without stale responses replacing them.
- Empty or sparse coverage and cohort cells must remain distinguishable from observed zero values.
- Viewer/session-expired states must never mount privileged profile or operations reads.
- Partial failures and stale releases must not produce misleading healthy status, trends or actionable alerts.

## Task 1: Unified application shell

**Files:** Modify `web/components/SessionGate.tsx`, `web/app/{page.tsx,analysis/page.tsx,customers/page.tsx,styles.css}`; create `web/tests/usability.spec.ts`.

**Interfaces:** Consumes `useSession()` and existing page routes. Produces one `navigation[aria-label="Workspaces"]`, active links and `#main-content` skip target; page-local section anchors.

- [x] Write browser tests for single role-aware navigation, active link, keyboard skip target, mobile width and accessible collapsed configuration.
- [x] Run `npm --prefix web test -- usability.spec.ts`; expect missing shell assertions to FAIL.
- [x] Implement shared shell, remove duplicate menus, add page anchors and expandable campaign settings/rules/simulation with original actions intact.
- [x] Run shell tests and existing browser suite; update interaction tests only to open deliberately collapsed controls.
- [x] Run `git diff --check`; commit `feat: unify command center navigation and page hierarchy` with architecture/backlog/spec/plan.

## Task 2: Decision-oriented campaign overview

**Files:** Modify `src/marketpulse_app/analytics.py`, `web/app/types.ts`, `web/app/page.tsx`; create `web/components/CampaignOverview.tsx`, `web/lib/format.ts`; extend `tests/test_app_analytics.py`, `web/tests/usability.spec.ts`.

**Interfaces:** `Dashboard.certified.daily_series` has `event_date`, `currency`, exact SUM_FIELDS strings. `comparison` has `from_date`, `to_date`, `status`, `observed_days`, `expected_days`, `totals`. Overview consumes those, provenance, current rows/totals and alerts.

- [x] Write Python tests with independent hand-derived current/prior daily totals, sparse/empty coverage and dimension filters; prove all query database names match pinned release and integers beyond JS precision survive.
- [x] Run `.venv/bin/python -m unittest discover -s tests -p test_app_analytics.py`; expect absent trend fields to FAIL.
- [x] Add exact daily/prior aggregation in the pinned analytics session; retain existing JSON fields.
- [x] Write browser tests for exact trend tables, missing prior comparison, currency precision, and relevant release/date/campaign alert drill; watch FAIL.
- [x] Implement SVG/chart-table overview, currency/ROAS helpers and alert shortcuts; preserve historical/provisional labels and raw provenance.
- [x] Run focused Python/browser tests, `npm --prefix web run typecheck`, diff check; commit with architecture contract update.

## Task 3: Readable operations summaries

**Files:** Modify `web/app/operations/page.tsx`, styles; create `web/components/OperationsSummary.tsx`; extend usability/auth-operation tests.

**Interfaces:** Existing `/ops/status`, `/quality`, `/lineage`, `/retention` payloads unchanged; technical evidence rendered in closed native details.

- [x] Write tests covering service up/down/unknown, latest live report versus controlled probe, unavailable sections and collapsed technical evidence; watch FAIL.
- [x] Render service cards, quality/probe and lineage/retention summaries; collapse quality history; retain raw evidence and original mutation controls/race guards.
- [x] Run focused tests and browser suite, diff check; commit `feat: summarize platform health and data trust`.

## Task 4: Shareable filters and usable tables

**Files:** Create `web/lib/useUrlState.ts`, `web/components/FilterActions.tsx`, `web/components/CampaignTable.tsx`; modify three workspaces; extend usability/browser tests.

**Interfaces:** `useUrlState<T extends Record<string,string>>(defaults:T, normalize?:(value:T)=>T)` returns `[T, Dispatch<SetStateAction<T>>]`; serializes allowlisted keys only and listens to popstate. CampaignTable consumes Metric rows plus active shared filters and exposes exact BigInt sort/search and Analysis links. FilterActions consumes date-setting/reset callbacks and published data-through anchor.

- [x] Write deep-link/reload/back tests for campaign, analysis model and customer purpose/offset; test unknown/enumeration/offset normalization and stale response guards. Watch FAIL.
- [x] Implement hook and workspace integration without passing UI-only query keys to APIs.
- [x] Write tests for published-date presets, reset/copy feedback, exact negative/huge numeric sorting, search and campaign-to-analysis links. Watch FAIL.
- [x] Implement filter actions, campaign search/sort and explicitly page-local profile search/sort.
- [x] Run all browser tests/typecheck, diff check; commit with architecture URL-state responsibility update.

## Task 5: Visual customer exploration

**Files:** Create `web/components/CohortHeatmap.tsx`, `web/components/ProfileDetail.tsx`; modify customer workspace/segments, SessionGate viewer cohort view, styles; extend browser tests.

**Interfaces:** Heatmap consumes existing `Cohort[]`, optional drill callback. ProfileDetail supplies labelled modeless panel and close/focus behavior around existing timeline content. No segment-member API or storage change.

- [ ] Write tests for observed zero/missing heatmap cells, exact denominators, cohort drill, viewer aggregate-only requests, profile close/focus/error/consent behavior, and live segment rule summary. Watch FAIL.
- [ ] Implement accessible heatmap shared by viewer/operator, focused profile panel beside list and concise segment summary; preserve all digest/revision/race protections.
- [ ] Run all Python/browser tests, typecheck/build, fixture validation, Compose config and diff checks; commit with architecture presentation update.

## Task 6: Docker verification and final review

**Files:** Add `docs/runbooks/command-center-usability.md`; update backlog/plan evidence.

**Interfaces:** Retained project `marketpulse-mp05-proof`, API48000/web43100, same release and PostgreSQL configuration.

- [ ] Build API/web and recreate only those services using existing port/environment configuration; expect healthy containers.
- [ ] Authenticated read-only smoke: independent current/previous/daily totals, status summaries, aggregate/detail permissions; capture all four desktop/mobile views under ignored `.lakehouse/app-review/`.
- [ ] Run full required validation, inspect fresh Git status; record real results, no invented counts.
- [ ] Dispatch one fresh whole-branch reviewer per requesting-code-review; fix Important/Critical findings with failing regression tests then green suite.
- [ ] Commit verification/runbook/evidence, retain feature branch for review; do not merge automatically.
