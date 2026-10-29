# Marketing dashboard and continuous demo — design proposal

Status: implementation authorized by the user's 2026-10-29 request; delivery is
tracked in the linked implementation plan.
Date: 2026-10-29.

## Intent

The current Command Center requires too much reading. Marketers should be able
to understand business performance, compare channels, and find a campaign to
investigate from the first screen. The user explicitly requested research into
real marketing products and established UI templates, a substantial visual
redesign, and a populated historical dataset that continues into streaming.

The earlier interactive browser mockup contains clearly labeled synthetic
illustrative values and makes no API calls. The subsequent user request explicitly
authorized implementation and a PR after planning and brainstorming.

## Current evidence

- PR #12, `feat/command-center-usability`, merged into `main` on 2026-10-29.
  The next implementation must start on a new feature branch based on that
  merged state, in the existing checkout; no extra worktree.
- The current overview combines performance, streaming, settings, rules,
  evidence and simulation into one long document. Other workspaces also favor
  large tables. Previous usability work retained these structures.
- Existing APIs already expose daily certified campaign data, previous-period
  coverage, live campaign snapshots, attribution, funnel, cohorts and profiles.
  Reuse these contracts and their correctness protections.
- Existing generator profiles span 2, 3, 14 or 30 days. CLI start-date does not
  provide an arbitrary history/resume mode. IDs depend on seed/entity/ordinal;
  changing dates or seeds for independent runs is not a continuation strategy.
- The current application interval contract allows only 1..366 days. A
  608-day historical view requires an explicit read-contract change.
- File Bronze currently allowlists ads/CRM/master, while event history enters
  through Kafka/Flink archives. There is no complete one-command long-history
  initialization and persistent continuous-demo controller today.
- Docker has been stopped at the user's request. Research and mockups must
  not restart it. Docker implementation verification occurs in the later
  authorized execution phase.

## Research and recommended direction

These are primary vendor/template sources, consulted on 2026-10-29. They are
references for information hierarchy and visual language, not evidence that our
own data supports every metric shown by those products.

| Reference | Observed pattern | Apply to MarketPulse |
| --- | --- | --- |
| [shadcn/ui dashboard-01](https://ui.shadcn.com/blocks?category=dashboard) | Sidebar, KPI strip, large chart, supporting table | Primary app-shell/component style |
| [Tremor dashboard](https://dashboard.tremor.so/overview) | Quiet surfaces, aligned numbers, restrained color and sparklines | Chart density, spacing, metric hierarchy |
| [Funnel Insights](https://help.funnel.io/en/articles/14323570-review-marketing-performance-in-insights) | Global filters, summary metrics, selectable trend, performance table | Marketing overview interaction model |
| [Northbeam overview](https://www.northbeam.io/features/overview-page) | Cross-channel revenue/spend/ROAS for business assessment | Decision-oriented marketing vocabulary |
| [Whatagraph examples](https://whatagraph.com/blog/articles/marketing-dashboard-examples) | Key KPIs, trends, channel comparisons and investigation context | Favor a short visual story over many metrics |

Recommendation: a light analytics workspace using the official shadcn/ui
dashboard composition, with a dark compact sidebar, white cards, neutral gray
canvas, emerald primary accent and stable channel colors. Use selective
accessible primitives and a chart library; do not import a whole unrelated
admin application. Tremor is a visual reference, not a second component system.

Alternative considered: copy a complete generic admin template. It saves shell
work but brings irrelevant navigation, authentication, demo data and conflicting
styles. Alternative considered: merely add SVGs to current sections. It leaves
the overloaded page structure intact. Neither is the recommended approach.

The builder may choose exact compatible dependency versions after checking
Next.js/React compatibility and official licenses. Recharts is the proposed
chart engine, following [shadcn's Chart approach](https://ui.shadcn.com/docs/components/radix/chart).
Retain required license notices for reused code. Do not require a paid template
or load runtime assets/fonts from a third-party CDN.

## Product scope and visual hierarchy

### Shared shell

- Compact sidebar: Overview, Live activity, Journey analysis, Customers, Data
  health. Preserve all existing capabilities and role restrictions.
- Page title states the task; one short subtitle. Date/channel/campaign/market
  controls are grouped consistently, with advanced filters disclosed on demand.
- Display currency and UTC interpretation. A user-facing inclusive date range
  must map unambiguously to the existing exclusive API upper bound.
- Preserve URL filters and browser Back/Forward; automatic default-date hydration
  replaces history, while user changes create history entries.
- Synthetic-demo identity and business clock are visible. Data details contain
  release identity, coverage and precise timestamps without dominating the page.
- English product labels remain the default for this redesign, matching the
  existing app. The data explanation and demo instructions are Vietnamese.
  This language choice is the author's proposal, not user approval.

### Overview

Top-to-bottom: four KPI cards (net revenue, ad spend, orders, net ROAS), a large
trend beside a compact Needs attention panel, channel revenue comparison,
stage-reach summary, and a compact campaign table.

- KPI comparisons appear only with complete compatible current/prior coverage.
  An unavailable baseline is not zero. Spend increase is not automatically
  colored as a favorable outcome. Negative net revenue remains representable.
- Revenue, Spend and Orders share a metric toggle. Use one meaningful unit on
  each axis; do not squeeze spend onto an unreadable revenue scale or invent
  a misleading second axis. Exact values are available by keyboard as well as
  pointer; a data table is available as an alternate representation.
- Daily/weekly/monthly aggregation is based on actual dated rows. Missing
  observations are gaps; certified zero-activity dates are explicit zeros.
  ROAS is sum(net)/sum(spend), not mean(daily ROAS). Sparkline ROAS uses actual
  ratio values, not a revenue sparkline reused under another metric label.
- Channel bars use source campaign finance, explicitly distinguished from
  attribution credit. Unknown/unattributed values remain identifiable.
- Negative revenue uses a signed bar with a zero baseline; no donut or share
  percentage when the total is non-positive or components are negative.
- Stage bars show distinct sessions reaching a stage, with a common linear
  scale. They do not imply a sequential funnel or global unique-person count.
- Top alert cards match the selected release/campaigns/period. Investigate opens
  the associated campaign. Show the empty state when none match.
- The table retains names/channel/spend/net/orders/ROAS. Select a campaign to
  open a keyboard-accessible side panel with daily results, pacing, settings,
  rules, evidence and an entry to simulation. Avoid a giant form below the chart.
- Currency groups stay separate. Never add monetary values across currencies.

### Live activity

This is a separate workspace using committed campaign/day snapshots. Show
session/order/provisional-net summaries and changing campaign rows, with a
small change indicator when a key is replaced. Exact event, processing and
arrival times are in details. Preserve mapping/staleness state.

Do not label cumulative snapshots as raw events or events/second. The current
backend does not provide a raw per-event feed or historical time series for live
campaign snapshots. Do not fabricate either. Any future throughput chart needs
a separately specified sampled/observable metric source.

Show replay state (running/paused/finished), the virtual business time, actual
processing health and last new input independently. Polling success is not fresh
input. No live ad spend or live certified ROAS until an appropriate source exists.

### Journey analysis

Use a stage-reach bar chart, campaign-level attribution grouped bars with an
explicit first/last/linear selector, and a consent-governed journey timeline in
drill-down. Keep source finance and attribution credit distinct. Detail tables
and reconciliation are available without being the primary view.

### Customers

Lead with customer/cohort summaries, readable retention heatmap and RFM
distribution where data supports it. Provide exact counts/denominators, no
zero-filled missing cohort cells, and accessible non-color equivalents.
Customer search and segment rules remain available; profile selection opens
the existing governed timeline. Saving a segment stores versioned rules in
PostgreSQL, not a fabricated external activation result.

### Data health

Show certified coverage, latest successful publication, input/replay state,
processing/serving health and failed stages in a compact visual status layout.
Expandable evidence retains lineage and diagnostic links. Quality is current
only for matching lineage and within its existing 180-second validity window;
age continues updating locally even if requests fail.

### States and responsive behavior

Loading skeletons preserve card/chart space. Explicit empty, partial, unavailable,
stale-retained and error states apply to each view. Failed new selections must
not enable actions based on old displayed rows. Do not draw fabricated curves in
empty charts. Show synthetic demo guidance when input is paused or exhausted.

Desktop 1440×1000: the first viewport contains title/filters, all KPIs, principal
trend and actionable attention context. Target overview page height <=2000px for
the 12-campaign demo; detail panels own long forms. Mobile 390×844: two-column
KPI layout, stacked charts, compact menu, usable touch targets, no document-wide
horizontal overflow. Table scrolling stays inside its container. Focus trapping,
Escape/return focus, descriptive chart labels, reduced motion and WCAG AA text
contrast are required. Status is never communicated by color alone.

## Continuous historical demo

### Timeline and identity

- History: `[2025-01-01T00:00:00Z, 2026-09-01T00:00:00Z)` — 608 days.
- First continuation window: `[2026-09-01T00:00:00Z, 2026-10-01T00:00:00Z)`.
  It is a bounded 30-day stream, not an infinite source. Clearly show completion;
  extension to later windows keeps the same world/identity contract.
- One immutable world identity, seed, generator version, campaign/customer
  master and global day ordinal. Independent `--start-date` runs are not used
  as history chunks. World extension must leave existing partitions unchanged.
- Proposed named local profile: `story-demo`, 1,000 customers, 12 paid campaigns,
  100 products and a baseline of 2,000 simulated impressions/day. Final row counts
  and disk/runtime cost must be measured; no capacity claim before verification.
  Larger existing benchmark profiles remain separate.
- Realistic deterministic weekly effects, gradual growth, campaign-specific
  performance and fixed synthetic promotion periods should arise in source
  generation, not hardcoded chart series. Orders/refunds/consent/attribution
  stay coherent. Cross-boundary purchases/refunds retain original order/touch
  references. Preserve existing MP-01–10 fixtures and their contracts unchanged.
- Source ad budgets remain separate from optional app-created demo period
  budgets/rules. The latter are explicitly labeled, idempotently provisioned in
  app PostgreSQL, and never overwrite the user's existing configuration.

### Preparation and reuse

Preparation is explicit and visible, with phases and resumable state:
generate/validate → ingest history → build governed models → certify/publish →
ready. Do not spend hours silently regenerating history on every `compose up`.

History must traverse real production transformations. Do not seed chart output
into app PostgreSQL or ClickHouse final tables. The proposed path is ordered,
fast history publication through Kafka/Flink, bounded Bronze archival of the
complete history and ad/CRM/master files, then range preparation using existing
Silver/Gold and serving/certification logic. A range bootstrap must validate
completeness per date and pin all inputs while avoiding 608 repeated full Spark
rebuilds. This orchestration is new scope; current daily jobs are reused and
remain supported. A small boundary spike must establish runtime and correctness
before running the full 608 days.

Persist world manifest/checksums, per-stage receipts, history release, input
offset bounds, replay cursor, virtual business clock and errors. Use dedicated
Compose volumes for generated data/control state and the existing operational
ledger patterns. Keep large generated datasets ignored under `data/generated/`.

Author's isolation decision: use a dedicated Compose project named
`marketpulse-story-demo` from this same source tree. Isolate Kafka topic/cluster
identity, Flink checkpoints/state, MinIO/Iceberg/catalog, ClickHouse, application
PostgreSQL and batch metadata. Audit explicit volume/container names so a project
flag alone cannot accidentally share state. Do not bootstrap 2025 history into
the existing October 2026 pipeline, whose watermark has already advanced. Leave
`marketpulse-mp05-proof` containers, volumes and user configuration intact.
Expose the new demo UI at `localhost:43110`; other host ports are configurable
non-conflicting demo defaults specified in the implementation plan. Preflight
port/resource conflicts and report them without stopping another deployment.
This decision adds runtime isolation, not a second source tree or worktree.

On later starts validate the existing world and reuse the certified release.
An explicit demo continuation service starts/resumes only after readiness;
ordinary infrastructure startup does not silently mutate user datasets. Provide
one documented demo command that starts the opt-in service and app together.
Never clear volumes, reset offsets or silently choose a new seed to recover.

### Replay continuity and truthfulness

Replay reads only the continuation range, using a persisted stable total-order
cursor and acknowledged progress. Support existing events_per_second and relative
speed controls. Initial recommendation: event-time speed 120× (one business day
in about 12 minutes), adjustable by the operator. This is a virtual clock;
processing freshness uses actual UTC time. Stopping Docker pauses demo time;
restarting resumes it without wall-clock catch-up by default.

Hold exclusive replay ownership per world. Persist confirmed progress after
broker acknowledgement, flush on graceful stop, and cover crash-after-ack
retries with business idempotency. Kafka idempotent producer mode alone does
not guarantee cross-process exactly-once publication. Existing 7-day Flink
dedupe TTL cannot be the only guard for historical restart. Verify that resume
preserves durable processing state or rebuilds the appropriate keyed state
before it can replace cumulative snapshots. Fail closed on incompatible topic
identity, lost state, changed manifest or replay/world mismatch.

At each completed virtual UTC day, make that day's ad/CRM files available to
the existing batch path, certify, then atomically publish the extended historical
release. Do not expose tomorrow's reports, orders or CRM to the UI early. Finance
for a live date stays provisional until publication; current day values must not
be added on top of a certified version of the same date.

Author's boundary decision: pause virtual-clock advancement at each day boundary
while acknowledged input is drained/checkpointed and that day is certified and
published. Do not start the next business day while publication is behind. Show
`Awaiting daily certification` with last certified day and stage progress. On
failure stay paused, retain the old certified release, expose an actionable
failure, and allow an idempotent operator retry. Resume automatically only after
successful publication. Late corrections use the existing governed correction/
backfill path and a new release; they never silently mutate a certified result.
This trades faster virtual-time advancement for a simple, explainable demo and
bounded certification backlog. Do not claim a wall-clock publication SLA.

The proposed controls/status endpoint is a narrow authenticated demo facility,
not a replacement for Dagster or the Kafka consumer ledger. Operator/admin may
pause/resume or change replay pacing; viewer is read-only. App PostgreSQL stores
only demo control metadata, never source events or analytical history.

### Long-range reads

Read-only dashboard/analysis/customer selections must accept intervals through
731 days. Mutation contracts for app period budgets/rules keep their existing
limits unless separately justified. Split read vs mutation interval validation;
do not globally change a helper and accidentally expand every writable contract.
Long ranges default to monthly charts; daily values remain available. Never
silently clip a requested 608-day period to the current 366-day cap. Paginated
journey/profile details remain bounded.

## Delivery order and architecture

This is one user experience with two separately reviewable implementation phases:

1. Historical demo foundation: deterministic long timeline, bounded history
   preparation, replay continuity/control, daily publication and range reads.
2. Visual application redesign: shell/components and all five workspaces,
   connected to real certified/live data; no illustrative fallback values.

After feedback and design approval, write a focused implementation plan for each
phase using writing-plans. Execute in the current source tree with focused
feature branches/PRs. No MP-01–10 reimplementation. Update the living architecture
alongside changes to demo orchestration, contracts, components and data flow.

Keep FastAPI/Next.js; ClickHouse remains analytics; PostgreSQL remains operational
state; Kafka/Flink streaming and MinIO/Iceberg lakehouse remain authoritative.
No mandatory BigQuery, hosted dashboard or paid external service.

## Acceptance evidence

- Same world/config yields identical source checksums. Month/window extension
  does not change prior files, master identity or existing event IDs.
- Boundary tests cover Aug 31 → Sep 1, duplicates, refunds crossing the boundary,
  consent changes, late records and attribution lookback continuity.
- Independent SQL/oracles reconcile history, channel/day totals, finance and
  attribution, plus latest live keys. Compare a continuous run to stop/resume and
  crash-after-ack runs. Verify no missing/duplicated business contributions.
- Prepare twice is a no-op for completed phases; interrupted prepare resumes;
  mismatched world/topic identity produces an actionable error.
- Full long-history generation report records seed, rows, dates, checksums,
  storage and elapsed time. Small deterministic boundary fixtures are committed;
  the full dataset is not.
- 608-day reads, missing dates, negative values, zero spend, missing prior period,
  multi-currency, unmapped campaigns and stale selected data have meaningful UI
  verification. Preserve all previously tested safety/correctness contracts.
- Browser tests prove filtering, chart exact values, navigation, drill-down,
  keyboard behavior and viewer restrictions. Compare aggregates independently
  with API/SQL, not solely screenshot similarity.
- Review screenshots at 1440×1000 and 390×844 across all workspaces, alongside
  prior UI screenshots. Document first-viewport clarity and overflow checks.
- Appropriate Python/Java/frontend tests, typecheck/build, source validation,
  Compose checks, real Docker end-to-end verification and `git diff --check` pass
  before claiming the implementation complete. Do not restart Docker during
  this proposal-only phase.

## Prototype coverage and outstanding feedback

The interactive prototype covers overview layout, channel/metric filtering,
20-month history mode, campaign panel, and a static live-state illustration.
Secondary workspaces show a visual direction, not a finished interaction spec
for every existing feature. Numbers are invented and explicitly marked; the
production redesign will not use them.

Local preview source: `.lakehouse/doc-artifacts/dashboard-direction.html`.
The companion copy is ignored under `.superpowers/brainstorm/`.
The user should judge information hierarchy, chart readability and whether the
new direction is sufficiently different from the current dense application.
Exact implementation sequencing follows only after this design is accepted.
