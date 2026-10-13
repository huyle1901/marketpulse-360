# MP-06 — Live campaign and budget operations

**Status:** Implemented and locally verified on 2026-10-13 after the user delegated
scope to the backlog. Review findings were reproduced and fixed through
RED/GREEN regressions; fresh Docker acceptance passed. Evidence and limitations:
[campaign-operations.md](../docs/runbooks/campaign-operations.md).
**Base:** PR #5 merged; `origin/main` at `a8843ae` contains MP-05 head `19788c7`.
**Execution:** Inline in the existing source tree; no new worktree.

## Outcome and boundaries

A performance manager can identify a risky campaign, inspect the evidence behind
an alert, compare spend with a configured budget, and preview a budget allocation.
A marketing leader can filter the same Marketing Command Center by UTC dates,
platform, campaign, objective and configured market, with visible provenance and
freshness. These are the four business capabilities in the MP-06 backlog.

Reuse MP-01 fixtures, MP-03 aggregate contracts and MP-05 certified publications.
Do not change their business rules, regenerate their fixtures, rebuild their
pipelines, or replace their verification. No funnel/attribution workspace,
Customer 360, activation, ad-platform writes, ML forecasting, production auth/RBAC,
notification delivery, cloud runtime or capacity claim is included.

Builder choices: Next.js App Router with TypeScript, a FastAPI Python 3.12+
service, a small Python Kafka-to-ClickHouse worker, HTTP polling every 5 seconds,
and a dedicated app PostgreSQL database. App ports bind to localhost. The local
demo has one operator and no generated application accounts. All new packages
and container versions must be explicitly pinned and locked during implementation.
Reuse ClickHouse 26.8.25.14, clickhouse-connect 1.10.0, psycopg 3.3.4 and
confluent-kafka 2.16.0. Record resolved FastAPI/Next.js/Node pins in the runbook.

## Two metric sources

| Source | Metrics | Meaning |
|---|---|---|
| MP-05 `campaign_dashboard` / `gold.campaign_performance_daily` | impressions, clicks, spend, orders, gross/refund/net, CTR, CPC, source ROAS | Certified, consent/bot-governed snapshot |
| MP-03 `marketing.gold.campaign-metrics.v1` | campaign/day sessions, orders, refunds, gross/refund/net | Provisional streaming aggregate, before MP-04 governance |

Show both sources separately; never add their revenue/orders together or calculate
ROAS using live revenue and batch spend. Streaming sessions remain per campaign,
not a globally unique audience total. Budget and financial decisions use certified
data. Streaming is useful for activity monitoring, not an activation audience.
ClickHouse stores all analytical rows; PostgreSQL stores app configuration and
alert evidence only. The API does not query MinIO/Iceberg or scan source files.

Resolve MP-05 `PublicationJournal` once per dashboard/evaluation/simulation request;
pin its immutable release database for every analytical subquery. Preserve
generation, identity and certification digest. No API accepts arbitrary SQL or
candidate database names. Parameterize all filter values; widen money sums to
Int128 and serialize money/counts as decimal strings so JavaScript cannot lose
integers above 2**53. Ratios use Decimal, truncate to six decimal places, and
return null for zero denominators.

Certified provenance includes Gold model, serving relation, pinned generation,
identity, digest, actual maximum selected `event_date` and its UTC day-end timestamp
(`data_through`, explicitly day resolution). `logical_day` is the source-selection
label, not an inferred event cutoff. `observed_at` means API read time. Empty
selections have null data_through, not a fabricated timestamp. Streaming provenance
includes Gold topic, max_event_time, processed_at, sink first received_at, Kafka
source identity and offset bounds. A 5-second poll must not update data freshness.
Historical fixtures are labeled historical; freshness age uses real UTC now,
never a hidden demo clock. Streaming transport freshness and business event
freshness are displayed independently; stale business age >300 seconds.

## Streaming serving projection

Consume only `marketing.gold.campaign-metrics.v1` with `read_committed`, a dedicated
group `marketpulse-campaign-serving-v1`, automatic commits disabled, and earliest
start for a new group. It is a serving adapter; Flink remains the stateful engine.
Validate the existing JSON schema plus UTC timestamps, Int64 counter/money ranges,
net=gross-refund, Kafka key=`metric_key`=`campaign_id|event_date`, nonnegative
counters and stage<=sessions. Do not enforce an invented monotonic funnel: stages
can be skipped in real input. Null currency is valid for a zero-money snapshot;
nonzero money requires currency. Bad records stop the worker without committing
past them and produce a bounded reason without payload/secrets in logs.

Append synchronous, bounded writes to `marketpulse_live.campaign_snapshots`, with
cluster/topic identity, partition, offset, payload checksum, full typed snapshot
and first received_at. Commit Kafka offsets only after ClickHouse acknowledges.
Lost insert/commit acknowledgement can repeat physical rows; queries collapse
identical source positions and select one whole snapshot by maximum source offset
per metric key. Never sum every snapshot, rely on background merges, order by
arrival time, or independently argMax nullable fields. A repeated position with
different content or a key crossing partitions fails closed. Retain first
received_at across retries so replay does not appear freshly generated.
Bootstrap source identity from Kafka cluster ID and topic ID, persist it in a
ClickHouse control relation and reject a changed cluster/recreated topic. Recovery
does not silently mix independent offset sequences. Explicit future reset is
outside this milestone. Missing data produces a visible unavailable state.

## Filters and app state

Dates are UTC `[from_date,to_date)`, maximum 366 days; defaults on initial page
come from actual published data bounds. Supported platforms are google/meta/tiktok;
objectives awareness/traffic/conversions. Currency is required for budgeting and
simulation; dashboard money is grouped by currency. Unknown campaign/market
selections produce empty results; invalid enum/date/money inputs return 422.

Gold campaigns have no market field. `marketpulse_app.campaign_settings` stores
an optional user-assigned market (two uppercase letters) and a period budget.
Filter label is **Configured market**, not customer geography. Unassigned campaigns
remain visible with market unknown; no hardcoded VN or market inferred from currency.
Streaming campaigns absent from the pinned dimension are visibly unmapped and
excluded by dimension/market filters. Configuration can target only a real campaign
in the pinned release, and currency must match that dimension. Source daily budgets
can be shown as references, but no app settings are silently generated from them.

One current setting per campaign: campaign_id, market (nullable), currency,
period_start, period_end (exclusive), budget_minor (>0), version, updated_at.
PUT creates/updates using expected_version (0 for create); stale versions return
409. No DELETE needed. Setting period length <=366 days. SQL migrations are
idempotent, additive, and touch only `marketpulse_app` in the dedicated app DB.
Persist `alert_rules` and `alert_evaluations`/`alert_occurrences` there. Store
immutable compact operands, provenance and explanations in alert evidence; this
is operational decision history, not an analytical fact-table mirror. All writes
come from UI/API or explicitly invoked demo steps; startup never seeds app state.

## Alerts

Rules are campaign scoped, user created, versioned, enabled/disabled and typed:
`budget_overrun` (known closed-day spend > full period budget, including during an active period), `spend_spike` (current complete UTC day
spend > baseline_multiplier * prior complete day spend, baseline >0), and
`refund_share` (refund_amount / gross > threshold, gross >0). Thresholds are
Decimal strings; multiplier >1; share in [0,1]. Missing baseline/zero denominator
returns insufficient_data, never a trigger or infinity. Currency must match.

A worker evaluates enabled rules against the latest pinned release every 5 seconds
and the UI can explicitly request an evaluation. Daily rules use one global latest
complete date in the pinned release before real UTC today. A campaign must have
a row on that date; spend_spike also requires its immediately preceding day;
missing calendar day is not replaced by another row. Budget rules compare against
the full configured budget, evaluating `[period_start,min(today,period_end))`.
Known nonnegative spend exceeding that budget triggers even with missing dates;
otherwise missing dates return insufficient_data rather than a confident clear.
No future/current-day aggregate is treated as a completed day.
The evaluator snapshots rule and setting versions; its key hashes release
identity, rule/version, setting/version, evaluation-contract version and exact
evaluated closed-day interval. A UTC day transition can change that interval and
must never reuse an immutable result for different operands. Unique DB
constraints make polling/concurrent evaluation/restart produce one logical result.
Occurrences are immutable and remain visible after a rule/setting edit. Record
outcome triggered/clear/insufficient_data, exact observed operands, comparator,
threshold, UTC interval, currency, versions and release/model provenance.
No alerts from unmapped campaigns or from provisional money.

MP-01 has refunds but no injected spend spike. The executable demo creates a
refund_share rule (threshold 0) against a refund-bearing certified campaign and a
deliberately small period budget to show budget_overrun. Mark the latter
**Configured demo budget**. Spend spike is proved by controlled test snapshots;
do not mislabel it as an MP-01 generator feature. Existing duplicate/bot/late
cases remain upstream evidence, not newly fabricated app alerts.

## Budget pacing

For a configured `[start,end)` period of D days, only certified closed daily spend
is usable. Let K be completed calendar days since start, bounded to [0,D], based
on a contiguous sequence of certified rows beginning at start and real UTC today.
Missing calendar days are not zero spend. A missing date followed by a later
observed date is an internal gap: report spend from all observed closed dates,
but set forecast/pace ratio null and status insufficient_data. No data/zero K
also gives insufficient_data and null forecast. Missing tail after the contiguous prefix
marks the result stale/incomplete and forecast is labeled **as of data_through**.
Do not invent intraday spend rates from daily vendor reports.

Compute observed spend S, expected spend B*K/D, remaining B-S (may be negative),
pace ratio S/(B*K/D), and projected total S*D/K. Decimal display values truncate
to six places; forecast_minor truncates toward zero to an integer. Period not
started has no forecast; period ended with all D days observed has forecast S.
Overspend/underspend is descriptive, not automatic budget execution.
The pacing request requires the exact configured period; a dashboard's separate
time filter must not silently reinterpret its budget denominator.

## What-if simulation

POST a UTC historical training interval, one currency, total_budget_minor >0,
and 1..100 distinct campaign allocations with nonnegative integer amounts whose
sum equals the total exactly. Reject unmapped campaigns, dimension currency
mismatch, duplicate IDs, negative/overflow values and mixed currencies. Each
campaign's certified net/spend ratio in the requested interval is its fixed
response coefficient. Expected net revenue = allocated_budget * historical
net/spend (Decimal); zero historical spend means insufficient_data, not zero
or infinity. Negative historical net yields a negative coefficient/forecast.
Zero allocation produces zero projected money even if its baseline is unknown.
Return each baseline, proposed allocation, projected net, totals, provenance,
and the explicit assumption **Linear scenario using historical net ROAS; not a
prediction**. If any positive allocation lacks a baseline, total projection is
null. Do not save allocations or alter campaign budgets when simulating.

## HTTP and UI contract

Routes under `/api/v1`:

- GET `/dashboard` with filter parameters returns certified rows/totals by
  currency, separate provisional rows, campaign options, provenance and freshness.
- GET `/campaign-settings`; PUT `/campaign-settings/{campaign_id}` with
  expected_version and the setting contract above.
- GET `/alert-rules`; POST `/alert-rules`; PUT `/alert-rules/{rule_id}` with
  expected_version; POST `/alerts/evaluate`; GET `/alerts` (bounded latest 100).
- GET `/budget-pacing/{campaign_id}` uses its current setting and pinned release.
- POST `/budget-simulation` performs the read-only scenario described above.
- GET `/health/live` reports process health; GET `/health/ready` checks app DB and
  ClickHouse access without requiring published data. Dependencies down ->503;
  no publication ->409 with `no_published_release` on analytical routes. Missing
  streaming projection remains a dashboard warning, not total dashboard failure.

Use FastAPI request/response models and stable error codes without raw DB exceptions.
All analytical responses are no-store. Use same-origin Next.js route handlers to
proxy a fixed allowlist of API paths to the internal FastAPI address; no arbitrary
upstream URL, backend credentials or DSNs in browser bundles. UI shows loading,
empty, stale, backend failure and retry states; failed polls retain old data with
a visible last-success/error marker. Polls are cancelled on filter changes and
unmount, with no older response overwriting newer selections.

One responsive Command Center: filter bar; certified KPI cards and campaign table;
separate streaming activity panel; alert explanations; campaign budget/market and
rule forms; pacing panel; editable simulation table with before/after results.
Form validation, labels, keyboard access, exact currency formatting and explicit
save/simulate buttons are required. No dead navigation to MP-07/08 or pretend
charts. English UI copy is acceptable; runbook may use English. Historical dates
are prominently visible. Budget simulations are never presented as saved budgets.

## Runtime and acceptance

Add Compose `app` profile: app-postgres, API, worker and Next.js; persistent
app_metadata volume, internal PostgreSQL, localhost API:8000 and web:3100 (Dagster
already uses 3000). Worker needs streaming+serving dependencies; run app alongside
existing MP-05 proof services without rebuilding/reseeding MP-01–05. Dedicated app
secrets live in ignored .env; committed template fields stay empty. Disabled app
profile must not introduce a global requirement for new app secrets. Nonroot app
images, bounded resources, health checks, no Docker socket, no volume deletion.

1. Focused RED/GREEN tests prove strict boundaries, exact money, query parameters,
   release pinning, filters, pacing, simulation and alert rules/idempotency.
2. Real Kafka/ClickHouse proof consumes committed MP-03 aggregates; live projection
   equals independently selected latest Kafka snapshots, including replay/restart
   and lost commit acknowledgement. Certified totals stay equal to MP-05 baseline.
3. Real app PostgreSQL/API proof saves settings/rules, rejects stale edits, triggers
   explainable refund/budget alerts, repeats evaluation without extra occurrences,
   and retains settings/evidence across app restart.
4. Next.js build/type checks plus browser tests exercise filtering, exact displays,
   edits, alerts, pacing, simulation, empty/error states and stale polling responses.
   Verify the actual running Docker UI visually at desktop/mobile widths.
5. Report fixture API p95 <=500ms for dashboard/pacing/simulation after 20 warmups
   and 100 serial uncached requests each; disclose counts, versions/resources and
   failures. Worker-to-API transport latency measured separately from historical
   event age; report observed measurements without an event-to-dashboard SLA claim.
6. Full Python suite, source fixture validation, Compose config/health, Docker
   integration, architecture/spec updates, git diff --check and fresh Git status.
   Retain evidence under ignored `.lakehouse/mp06-proof/`. MP-06 remains incomplete
   until actual results are recorded in the runbook/backlog.

## References consulted

Implementation follows [FastAPI container deployment](https://fastapi.tiangolo.com/deployment/docker/),
[Next.js self-hosted deployment](https://nextjs.org/docs/app/getting-started/deploying),
[Confluent consumer APIs](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
and [ClickHouse argMax](https://clickhouse.com/docs/reference/functions/aggregate-functions/argMax).
Exact replay semantics and nullable snapshot selection are application contracts
above, not guarantees inferred from those tools.
