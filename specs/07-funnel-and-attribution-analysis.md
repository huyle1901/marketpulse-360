# MP-07 — Funnel and attribution analysis

**Status:** Scope chosen from the backlog under the user's continuation; implementation pending.
**Base:** `5b236b9`, pushed MP-06 branch; MP-07 is stacked on MP-06 until it merges.
**Execution:** Same checkout, no worktree. Spec and plan precede code.

## Outcome and scope

An analyst can compare paid channels, drill into campaigns, inspect stage reach,
compare first-touch/last-touch/linear attribution and inspect representative
anonymized order journeys. Extend FastAPI and Next.js; analytical reads use
ClickHouse only. App market settings remain operational PostgreSQL metadata.
Reuse the certified MP-05 Gold tables and MP-04 attribution policy unchanged.
Do not regenerate sources, rebuild MP-01–06 pipelines, add activation/Customer
360 segmentation, write ad-platform budgets or introduce cloud requirements.

Builder choices: a dedicated `/analysis` Next.js page, existing 5-second polling
and same-origin proxy, three read-only API routes, and an explicit one-shot
Spark sidecar publisher for snapshot-pinned journey detail. Existing dependency
pins/locks and app services remain; no new library is required.

## Filters and request identity

Routes `/api/v1/analysis/funnel`, `/analysis/attribution`, `/analysis/journeys`
share UTC `[from_date,to_date)` (1..366 days), platform google/meta/tiktok,
campaign, objective, configured market and optional currency filters. Platform
is the campaign's paid channel, not a claimed customer acquisition channel.
Unattributed records have channel `unattributed`, campaign `__unattributed__`.
Users can drill by clicking a channel then campaign and return to all channels.
Unknown filters return empty selection; invalid dates/enums/limits return 422.

Each request resolves and pins one MP-05 release. A UI refresh performs the three
requests, accepts them only if release identities match, and retains old results
with an explicit loading/error/previous-selection marker while requests are in
flight. No old query or old journey request may overwrite a new selection.
Money/counts are exact decimal strings; allocated order counts retain six-place
Decimal representation. BigInt formats integral amounts, never Number. Ratios
truncate to six places and return null for zero denominators. No-store responses;
no arbitrary SQL, database, customer identity or upstream URL input.

## Funnel semantics

Read certified `gold.funnel_daily` / ClickHouse `funnel_daily` and pinned campaigns.
Stages: sessions, page_view_sessions, view_item_sessions, add_to_cart_sessions,
checkout_sessions, purchase_sessions. Report channel and campaign aggregates,
each with stage/session reach ratios. Stage events may skip earlier stages;
this is **stage reach, not a strict sequential funnel**. Do not invent a
monotonic funnel, causal drop-off or a global distinct-audience count.
Across campaigns/dates, sums are **campaign-day session incidences**: one session
can appear in multiple buckets. Label that unit on the chart/table and show the
definitions. Counts reconcile with governed Silver/Gold, not ungoverned MP-01
totals that intentionally include consent/bot-excluded activity.
Funnel filtering by currency uses campaign dimension currency; unattributed
counts have unknown currency and are excluded by an explicit currency filter.

## Attribution semantics and reconciliation

Read certified `gold.attribution_daily` / ClickHouse `attribution_daily`. Preserve
MP-04's thirty-day inclusive paid-touch window ending at original order time:
eligible web/mobile page_view touches with click/campaign, deduplicated by click
within resolved profile; deterministic event-time/campaign/click/event tie order.
First-touch selects earliest touch; last-touch latest; linear divides integer
gross/refund/order-count micros among unique touches and assigns remainders in
that same order. With no eligible touch, all credit is unattributed. Refunds
reuse the original order's touches and are recognized on refund event date.
The UI displays these definitions, not a newly chosen attribution algorithm.

For every model show gross, refund, net and allocated order count; compare channel
and campaign contributions side by side. User-selected model controls journey
sampling. Changing model does not alter orders or recompute the underlying Gold.
Net contribution ratios may be negative or exceed one in refund-only slices.
Financial analysis rejects mixed-currency releases with a stable 409
`unsupported_currency`, because the existing attribution contract has no currency
column. Declared currency comes from both campaigns and campaign performance;
there is no conversion or currency inferred for a mixed release.

Return **global reconciliation** for the requested dates/currency before platform,
campaign, objective or market filtering: every model's complete gross/refund/net
must equal the corresponding source totals in certified campaign performance.
Filtered campaign allocations are a selected view, not expected to equal source
campaign revenue. Missing model or unequal totals fails closed with 409
`reconciliation_failed`; do not display a green badge from counts alone.
An empty interval has three zero model totals and an explicit empty state.

Every response includes Gold model, serving relation, release generation/identity/
proof digest, actual maximum selected event day and UTC day-end data_through,
day resolution and read time. Historical data stays labeled historical. Journey
provenance additionally identifies exact Silver snapshot and projection checksum.

## Journey serving sidecar

MP-05 does not export event-level Silver. Add an explicit `app-tools` Compose
one-shot `journey-publish` image based on the existing pinned batch/Spark image.
It resolves the active release, validates its certified bundle and recorded
receipt, then reads `silver.customer_events` from the receipt's namespace and
exact `VERSION AS OF` snapshot. Never read current heads/source files or publish
an invented journey. Runtime APIs do not read Iceberg/MinIO/PostgreSQL analytical
rows. Existing MP-05 export/certification/publication contracts are unchanged.

Projection fields: release-scoped SHA-256 `profile_ref` instead of profile_key,
opaque synthetic event/session/click/order UUIDs, event name/time/date, source,
campaign ID, nullable currency and exact gross/refund amounts, preserved bot-session flag.
No customer/anonymous ID, contact fields, URLs, payload JSON or raw PII. Internal
opaque event/click IDs preserve existing tie order; HTTP journey references hash
them with release identity. This synthetic-data pseudonymization is not a claim
of production privacy protection. Refuse consent-denied rows, web/mobile bot rows
or duplicate event identities. MP-04 intentionally retains commerce/email events
sharing a bot-marked session; preserve those rows and their flag so financial
totals remain unchanged. The projection preserves all governed events, including history
needed for pre-interval touches and original purchases of in-interval refunds.

Under an MP-07 PostgreSQL advisory lock (operational coordination only), copy in
bounded chunks to a fresh owned immutable ClickHouse candidate. Compare exact
typed schema, source/copy multiset row count/checksum and key uniqueness before
appending a complete publication row in `marketpulse_serving.journey_publications`.
Bind release identity/proof, Silver snapshot, projection version and checksum.
Partial copies never publish; retry uses a new candidate. Identical semantic
publication duplicates are allowed; conflicting content fails closed. If copies
have identical proof but different database names, choose deterministically;
database names are never user input. Existing publication reuse re-verifies
the serving contents. Old release projections stay available for pinned reads.
No automatic deletion, MP-05 promotion or source reseeding. A missing projection
is a visible unavailable journey state; aggregates remain usable.

## Representative journey contract

`GET /analysis/journeys` accepts model first_touch/last_touch/linear and limit
1..10 (default 5). Financial events in the selected interval nominate orders,
including refunds whose purchase precedes the interval. Scan at most 100 ordered
candidates. Each candidate loads at most 500 eligible paid-touch context rows
plus 100 financial events; excessive context is explicitly unavailable and is
never turned into partial attribution credit. Reuse the existing MP-04 Python
`build_attribution_daily` only for a bounded representative explanation; main
aggregates always come from certified Gold. No second attribution implementation.

Campaign/channel/market filters apply to **credited campaigns under the selected
model**, not the source order's campaign field. Include all qualifying touch
context only after canonicalizing each profile/click across the entire pinned
snapshot in ClickHouse (earliest time/campaign/event tie order), then applying the
thirty-day order window and context limit. A later copy of an older out-of-window
click must never become eligible simply because the query starts at the window.
Pass those canonical touches to the unchanged Gold explanation helper. Include
context for a selected order; label context outside the report interval. Display
timeline, original purchase time, financial recognition dates and all three
model allocations for that order in the requested financial interval. Hide
profile_ref and raw identities in HTTP. A sample is descriptive, not a population
claim or Customer 360 activation export. Return candidate/sample/truncation
counts and honest empty/unavailable states. Definitions match main attribution.

## UI, runtime and acceptance

Add real navigation between Campaign Operations and Funnel & Attribution. A
responsive analysis page contains filters/drill breadcrumbs, labeled stage-reach
bars, channel/campaign tables, three model comparison cards, global reconciliation
with exact operands, source/freshness details and representative journey cards.
Selected model/campaign changes cancel old requests; no stale selection or old
release scenario. Accessible labels, keyboard actions and mobile overflow checks.

Use the existing app profile; tools add only explicit publisher service. Start
on the existing proof project's network/volumes and ports without recreating or
rerunning MP-01–05. Rebuild only changed app/tool images. No startup job seeds
settings or silently publishes journeys. Document publication for every new
certified release and restart behavior.

Acceptance: focused RED/GREEN domain/query/API tests; full Python suite and
MP-06 browser regressions; real Spark pinned-Silver-to-ClickHouse sidecar with
full-row equality, retry and failed-copy/conflicting-proof cases; independent
Python aggregations of validated pinned MP-05 exports plus sidecar events for
funnel/attribution totals; real HTTP filter/drill/journey checks and app restart;
browser tests/build and actual desktop/mobile UI inspection; 20 warmups/100
uncached HTTP samples per workload with fixture p95 <=500 ms and disclosed scale;
fixture validation, Compose/diff checks and fresh Git status. Evidence stays in
ignored `.lakehouse/mp07-proof/`; implementation is complete only after observed
proof and a fresh whole-branch review/fix pass.
