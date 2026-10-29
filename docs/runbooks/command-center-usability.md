# Command Center usability

This extension follows merged MP-10 and changes application reads/presentation.
It does not replay sources, rebuild Gold, publish a release or migrate operational
storage. See the [design](../superpowers/specs/2026-10-20-command-center-usability-design.md)
and [plan](../superpowers/plans/2026-10-20-command-center-usability.md).

## Use the workspaces

- **Campaign Operations:** one workspace menu and local section links. Certified
  KPI cards show currency amounts. Daily revenue/spend lines use observed dates;
  expand **Exact daily values** for raw minor-unit integers. Prior-period changes
  require complete published-day coverage in both periods. Missing selected currency/campaign baselines stay unavailable. **Needs attention** includes only
  release/date/campaign-matching alert evidence. Use **Investigate** to filter the
  campaign. Budgets/rules and simulation are expandable tools below the overview.
- **Funnel & Attribution:** existing stage reach, model definitions, reconciliation
  and representative journeys remain available. Dates/dimensions/model are encoded
  in the URL. Campaign-table **Analyze** links carry dates, dimensions and currency.
- **Customer 360:** retention heatmap distinguishes observed zero from unavailable
  cells and displays active/original-cohort counts. Operators drill a cohort, inspect
  a profile in the adjacent modeless panel, and close it with Escape or **Close
  timeline**. Focus returns to the profile button. Search/sort applies only to the
  loaded profile page. Segment drafts summarize conditions before preview/save;
  saved definitions do not imply stored members or delivered messages.
- **Operations:** admins see source availability, latest live quality, controlled
  probe state, lineage and retention summaries. Expand technical details for original
  JSON. Unavailable sources stay unknown. Quality reports require the current release
  and an age of at most three minutes; local ten-second clocks age reports/source
  observations without additional API reads. Recorded results remain inspectable.
  Quality history and probe controls retain
  the MP-09 semantics. Flink/Dagster may be deliberately stopped in the retained
  proof project; their cards then show unavailable/stale.

Share a view by copying its URL. Reload and Back/Forward restore allowlisted state.
Automatic server-selected dates replace history; user filter changes push history.
Campaign drill links are disabled while an unsuccessful/new selection retains older
rows. Local search/sort parameters are not sent to analytical APIs. Campaign/Analysis
presets end at the selected certified data-through day; a seven-day preset can
include missing historical days and does not create observations. All intervals
are UTC `[from,to)` with the end exclusive. Budget/scenario forms and segment money
thresholds continue to use exact currency minor units.

## Visual workspaces (2026-10-29 redesign)

The [visual redesign](../superpowers/specs/2026-10-29-marketing-dashboard-and-continuous-demo-design.md)
renames the sidebar workspaces. The behavior described above is unchanged except
where noted here.

- **Overview** (`/`): four KPIs; one trend whose metric is Revenue, Spend or Orders
  and whose interval is Daily, Weekly or Monthly. Weeks start on UTC Monday, and long
  ranges default to monthly. Each bucket sums only observed certified days and lists
  its observed-day count under **Exact … values**. Channel finance and stage reach
  (release-matched) sit beside each other. Select a campaign name, or **Investigate**
  an alert, to open its detail panel: daily results, pacing, settings, rules and
  campaign alerts. Escape closes it and returns focus. **All alert evidence** and the
  **Budget allocation simulator** are collapsed below the table.
- **Live activity** (`/live`): provisional committed snapshots. The default window
  runs from seven days before certified data-through to real UTC tomorrow; widen it
  with the date fields. **Updated** marks a replaced snapshot. Replay state is
  unknown until the demo controller exists.
- **Journey analysis** (`/analysis`): grouped net-credit bars for all three models by
  channel, plus campaign credit for the model chosen under **Attribution model**.
  The same choice drives journey sampling. Exact tables are under **Exact credit
  tables**, and reconciliation stays visible.
- **Customers** (`/customers`): exact profile-mix and opportunity counts for the
  displayed release; select a count to filter profiles. Unavailable detail stays
  unavailable.
- **Data health** (`/operations`, admin): a status strip summarizes certified
  coverage (the served release must match validated lineage), live quality, source
  counts with failing stages named, and Kafka/replay input. Detailed evidence follows.

## Local validation

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests
PYTHONPATH=src .venv/bin/python -m marketpulse_mock validate --input data/mock/fixtures/seed-360
npm --prefix web test
npm --prefix web run typecheck
npm --prefix web run build
docker compose --profile '*' config --quiet
git diff --check
```

Run browser tests before the local production build: both use `web/.next` and
must not run concurrently. No dependency installation or generated dataset is
needed when the existing development environment is present.

## Retained Docker proof project

Reuse the existing `.env`, secret files, volumes and published release from the
[operations runbook](operations.md). Rebuild/recreate only API and web:

```bash
API_HOST_PORT=48000 WEB_HOST_PORT=43100 docker compose -p marketpulse-mp05-proof --profile '*' build api web
API_HOST_PORT=48000 WEB_HOST_PORT=43100 docker compose -p marketpulse-mp05-proof --profile '*' up -d --no-deps --wait api web
```

Open <http://localhost:43100>. API readiness is
<http://localhost:48000/api/v1/health/ready>. The Next.js same-origin proxy and
PostgreSQL-backed sessions/RBAC remain authoritative. Use existing accounts and
credential files; never put passwords or cookies in commands, screenshots or Git.

## Verification evidence

Verified on 2026-10-20 against source commit `6ef0575`:

- 428 Python tests: 388 pass, 40 opt-in skips. All 54 browser tests pass.
- TypeScript and Next.js production build pass. Deterministic fixture: 615 rows,
  22 files. Compose configuration and `git diff --check` pass.
- API and web rebuilt and healthy in `marketpulse-mp05-proof`; no earlier pipeline
  service or data volume was recreated. One source checkout was used throughout.
- Authenticated read-only checks cover all four workspaces at 1440×1000 and
  390×844: no browser errors, document overflow or default visible raw operations
  JSON; profile Escape/focus return and URL reload/Back work.
- Viewer gets aggregate heatmap, issues no profile/segment/operations reads, and
  receives HTTP403 on privileged API routes. Budget settings, rules and segment
  definitions match before/after UI verification; login/access audit is expected.
- One independent whole-branch review found five Important issues: default-date
  history, missing prior baselines, sparse current coverage, stale/release-mismatched
  quality, and retained-row drill context. All were reproduced with failing tests,
  fixed, and covered by the green suites. No Critical or deferred Minor findings.

Direct ClickHouse SQL against the pinned Gold campaign relation matches every
API daily field and full/current/prior total. The release stays
`31ffb7c713c694b9082b0f1daad7a32d50ca76e9b842fcb988029d3da4fdef06`.

| UTC day | Spend (VND minor) | Net revenue (VND minor) | Orders |
|---|---:|---:|---:|
| 2026-01-01 | 284,410 | 12,582,000 | 5 |
| 2026-01-02 | 267,327 | 9,611,000 | 6 |
| Full published interval | 551,737 | 22,193,000 | 11 |

For a real prior-period demonstration choose January 2–3 (exclusive): January 1–2
is a complete preceding period. The default January 1–3 interval correctly has no
published prior period. Authenticated API measurements with 20 warmups / 100 samples
per route: default dashboard p50=33.557 ms, p95=44.829 ms, p99=51.208 ms; one-day selection
p50=32.312 ms, p95=40.100 ms, p99=42.424 ms. These measure the local fixture, not capacity
or an SLA.

Generated screenshots, independent SQL oracle, latency samples and smoke JSON remain
ignored under `.lakehouse/app-review/2026-10-20-usability/`. `verification.json`
records source commit, release, checks and viewport sizes; credential files and
session cookies are excluded. No user-generated `work/` material was changed.
