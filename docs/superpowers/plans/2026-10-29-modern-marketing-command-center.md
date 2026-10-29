# Modern Marketing Command Center Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans. Steps use checkbox syntax.

**Goal:** A populated story-demo history behind a shadcn/ui marketing command center with plain-language business views.

**Spec:** `docs/superpowers/specs/2026-10-29-modern-marketing-command-center-design.md`

## Global Constraints

- Same checkout and branch; preserve `work/` and the uncommitted Task 2 replay work until it is committed deliberately.
- TDD for production behavior. Every commit runs the relevant tests and `git diff --check`.
- Certified and provisional values stay separate. Exact money uses BigInt or Decimal. Missing data is never zero.

### Task 1: Flink memory and checkpoint restore
Files: `compose.yaml`, `docker/flink/submit-job.sh`, `scripts/story-demo-env.sh`, `tests/test_docker_assets.py`.
- [ ] RED: assets test expects `${FLINK_TASKMANAGER_PROCESS_SIZE:-1024m}`, a configurable limit, and `FLINK_RESTORE_PATH` → `-s`.
- [ ] GREEN, then resume the story-demo job from its retained checkpoint and verify zero Flink lag.

### Task 2: History certification (operational)
- [ ] Commit the Task 2 range-ingest and replay-cursor work with its tests.
- [ ] Range-ingest the history files, run the Dagster refresh for 2026-08-31, and publish the journey projection. Record timings and totals in the runbook.

### Task 3: Email and new-vs-returning serving reads
Files: `src/marketpulse_app/engagement.py` (new), the API route modules, tests.
- [ ] RED/GREEN pure query builders and API contract tests (release pinning, unavailable projection, empty months).

### Task 4: shadcn/ui foundation and shell
- [ ] Tailwind v4, shadcn components (card, badge, button, tabs, table, sheet, select, chart) and Recharts at pinned versions; sidebar shell; all browser tests green.

### Task 5: Overview and Ad performance
- [ ] RED browser tests for KPI changes, sparklines, range tabs and plain-language labels; implement.

### Task 6: Restyle Live, Journeys, Customers, Data health; add Email
- [ ] RED/GREEN browser tests per page; responsive checks at 1440×1000 and 390×844.

### Task 7: Story-demo app, browser verification and PR
- [ ] Run the app on 43110 against the certified history, verify in Chrome, update architecture and runbooks, push and open the PR.
