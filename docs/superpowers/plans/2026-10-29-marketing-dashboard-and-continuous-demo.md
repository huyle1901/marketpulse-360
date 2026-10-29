# Marketing Dashboard and Continuous Demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a visual, decision-focused Marketing Command Center and an opt-in synthetic timeline whose September replay follows certified January 2025–August 2026 history.

**Architecture:** Keep FastAPI/Next.js and existing ClickHouse-certified/live separation. Generate one deterministic world, prepare history with the lakehouse and batch path, and replay only the continuation window in an isolated Compose project.

**Tech Stack:** Python 3.12, Kafka/Flink, Spark/Iceberg/MinIO, ClickHouse, PostgreSQL, FastAPI, Next.js/React, TypeScript, CSS, Playwright.

**Spec:** `docs/superpowers/specs/2026-10-29-marketing-dashboard-and-continuous-demo-design.md`

## Global Constraints

- Use one source tree in the current folder; preserve `work/` and existing Docker project volumes.
- Keep certified and provisional values visually and mathematically separate.
- No generated full dataset or credentials in Git.
- Every production behavior starts with a failing focused test, then a passing focused test.
- Architecture and runbook changes accompany data-flow/contract changes.

## Review Focus

- Changing dates/seed across a replay boundary must fail or preserve IDs, never silently merge worlds.
- A 608-day read must not expand period-budget mutation limits.
- An incomplete prior period or missing currency baseline must not display a numeric change.
- A failed filter request must not make stale campaign actions active.
- A restart after ack must not double count or roll back the live snapshot.

---

### Task 1: Demo timeline contract and long-range reads

**Files:** Modify `src/marketpulse_mock/config.py`, `src/marketpulse_app/contracts.py`, `src/marketpulse_app/api.py`; test `tests/test_config.py`, `tests/test_app_contracts.py`, `tests/test_app_analytics.py`; update architecture.

**Interfaces:** `resolve_config('story-demo', seed, path)` spans 638 UTC days from `2025-01-01`; `DashboardFilter` permits 731-day reads; `CampaignSetting` remains capped at 366 days.

- [ ] Add failing tests for deterministic profile, Aug 31/Sep 1 identity, 608-day DashboardFilter, and rejected 367-day CampaignSetting.
- [ ] Run focused tests and observe expected failure.
- [ ] Implement profile and read/write interval separation.
- [ ] Run focused tests and full Python suite.
- [ ] Commit feature with architecture update.

### Task 2: Historical preparation and resumable continuation

**Files:** Create focused modules under `src/marketpulse_mock/` and `src/marketpulse_batch/` for world manifest/window selection and demo orchestration; tests under `tests/`; modify `compose.yaml` and `docs/runbooks/`; update architecture.

**Interfaces:** An opt-in CLI `demo prepare`, `demo continue`, `demo status` verifies world/checksums, prepares the 608-day release using existing transforms and certifications, then publishes September events in event-time order with a durable cursor. The dedicated Compose project is `marketpulse-story-demo`, UI port 43110.

- [ ] Write failing tests for window partitioning, immutable prior IDs/files, cursor resume after acknowledged publish, exclusive world lock, and date-boundary pause on failed certification.
- [ ] Run focused tests and observe failures.
- [ ] Implement the smallest safe orchestration, reusing existing generator, replay and batch functions; fail closed on identity mismatch.
- [ ] Run focused/full Python tests and validate a small boundary dataset.
- [ ] Run bounded Docker boundary verification, then full 608-day preparation and September replay smoke; record elapsed time/storage/checksums/oracles.
- [ ] Commit with architecture and runbook updates.

### Task 3: Visual application shell and overview

**Files:** Modify `web/components/SessionGate.tsx`, `web/app/page.tsx`, `web/components/CampaignOverview.tsx`, `web/app/styles.css`; add small chart/summary components under `web/components/`; tests `web/tests/`.

**Interfaces:** Existing authenticated FastAPI responses stay authoritative. The overview renders four currency-safe KPIs, a selectable one-unit trend, channel breakdown, stage summary, matching alerts and compact campaign drill-down. Settings/rules/simulation remain usable in details.

- [ ] Add failing browser tests for first viewport hierarchy, metric toggle exact values, channel filtering, drill-down, keyboard access and stale-selection restrictions.
- [ ] Run focused browser tests and observe expected failures.
- [ ] Implement shadcn-inspired shell/design tokens and overview with no illustrative data in the product.
- [ ] Run focused browser tests, typecheck and production build.
- [ ] Commit UI changes with any architecture update that reflects component/data-flow changes.

### Task 4: Analysis, customers and data health

**Files:** Modify `web/app/analysis/page.tsx`, `web/app/customers/page.tsx`, `web/app/operations/page.tsx`, supporting components and CSS; tests `web/tests/`; update UI runbook.

**Interfaces:** Existing endpoints retain their definitions and role restrictions. Charts, heatmaps and progressive disclosure replace long introductory text and raw tables as the first view; exact data remain available.

- [ ] Add failing browser tests for attribution model chart, retention missing-cell behavior, health freshness and viewer permissions.
- [ ] Run focused tests and observe expected failures.
- [ ] Implement visual summaries with correct empty/partial/negative states.
- [ ] Run full browser suite, typecheck/build and responsive verification at 1440×1000 and 390×844.
- [ ] Commit app/runbook changes.

### Task 5: End-to-end evidence and PR

**Files:** Verification record under `docs/runbooks/`; no unrelated source changes.

- [ ] Start isolated Docker demo; independently compare source/Gold/ClickHouse/API totals and live latest keys across the boundary.
- [ ] Verify restart/resume, daily certification and all browser workspaces against actual processed data.
- [ ] Run full required tests, fixture validation, Compose checks, `git diff --check`, Git status and secret scan.
- [ ] Request one whole-branch code review and fix Important findings.
- [ ] Push feature branch and open a PR describing observed behavior and measured limits.
