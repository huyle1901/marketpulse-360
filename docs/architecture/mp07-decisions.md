# MP-07 decisions and evidence

MP-07 is stacked on pushed MP-06 `5b236b9` in the existing source tree. PR #5
was merged before MP-06; MP-05 remains the certified serving foundation.
Spec/plan were committed before production code. No source regeneration or
MP-01–06 pipeline rebuild was needed.

## Analytical boundaries

FastAPI reads request-pinned immutable ClickHouse Gold; Next.js `/analysis`
compares channels/campaigns and three attribution models. PostgreSQL supplies
operator-created market settings and publisher coordination only. Analytical
events are never stored there. Every UI refresh accepts three responses together
only when release identities match; cancelled older selections cannot overwrite
current reports. Retained old results are explicitly labeled.

Funnel sums are campaign-day session incidences, not globally distinct audiences
or sequential conversion/drop-off. All six stage counts, exact integer finance,
six-place allocated orders, global reconciliation operands and source/freshness
are inspectable. Dimension-filtered credit is distinct from globally reconciled
source campaign revenue. Currency-less attribution Gold requires a single-currency
release; mixed currency fails with 409. Empty selections are explicit.

## Journey projection and policy

MP-05's five Gold/two reference export contract stays intact. Explicit `app-tools`
`journey-publish` reads the certified release receipt's exact Silver snapshot,
projects only allowed fields and release-scoped profile hashes, verifies schema,
keys/count/full-row checksum and publishes a separately bound immutable sidecar.
Publication is coordinated by PostgreSQL advisory lock 36007. Failed candidates
remain isolated; retry verifies/reuses the published copy. There is no automatic
publication, deletion, source reseeding or MP-05 promotion.

Global profile/click canonicalization precedes the thirty-day order window and
context limits. Commerce/email bot-session flags are retained as MP-04 intends;
web/mobile bot activity and consent-denied records are rejected. Removing flagged
commerce events would wrongly reduce sales. The existing public Gold helper
explains bounded orders; no second attribution policy is introduced. Refund
credit uses original purchase context but recognition occurs on the refund day.
HTTP hashes order/event/click references and excludes raw profile/contact IDs.
This is synthetic-data pseudonymization, not a production privacy guarantee.

The Spark entry point is a thin root wrapper: invoking the package-local file
would put sibling `math.py` ahead of the standard library. Python modules remain
ordinary imports. Same checkout/no worktree follows the user's explicit request.

## Observed Docker proof

Existing test-owned project `marketpulse-mp05-proof`; API 48000, web 43100;
15 Docker CPUs, 16,747,331,584 memory bytes. Images retain the existing MP-06
app/web tags with newly built MP-07 code; the publisher is `journeys:mp07`.
Only changed API/web/tool images were built; only API/web were restarted.
Existing Kafka/Flink/Iceberg/Dagster/ClickHouse state and MP-06 app state stayed.

Release `31ffb7c713c694b9082b0f1daad7a32d50ca76e9b842fcb988029d3da4fdef06`,
Silver snapshot `505365576434850409`, 191 governed events, projection checksum
`c218a8023e749c22f328fbe4d70020020ec8a73c951d0fc3a14c23caacc88654`.
Publisher retry reused the same snapshot/checksum. Real ClickHouse fault tests
cover duplicate event keys, durable insert with lost acknowledgement, conflicting
snapshot/proof and duplicate semantic publications. Global canonical clicks retain
an earlier out-of-window touch rather than crediting its later duplicate.

Independent validated export objects and recomputed sidecar Gold helpers prove
full funnel/attribution row equality. Eight HTTP cases cover full/date slices,
channel/objective/campaign/unattributed/unknown selection, with all three sampling
models checked. `[2026-01-01,2026-01-03)` has 67/67/52/26/20/9 stage incidences;
each attribution model has gross 27,160,000, refunds 4,967,000, net 22,193,000 VND
and 11.000000 allocated orders. Five default journey examples are shown.
API/web restart preserves release, exact aggregates, sample IDs and MP-06
settings/rules/alerts. Fixtures validate 22 files/615 rows.

Each HTTP workload used 20 warmups and 100 serial uncached samples, zero failures:

| Workload | p50 ms | p95 ms | Max ms |
| --- | ---: | ---: | ---: |
| Funnel | 13.85 | 26.58 | 41.58 |
| Attribution | 15.55 | 18.96 | 30.67 |
| Journeys (5 examples) | 39.17 | 77.84 | 96.07 |

Python 3.12.13, Next.js 16.4.0, ClickHouse 26.8.25.14, Spark 4.0.4 and Iceberg
1.12.0. These fixture measurements establish local correctness/performance,
not production capacity. Samples scan at most 100 candidate orders, 500 canonical
touches/100 financial events per order, with visible unavailable/capped states.

All fourteen Playwright tests pass, including nine MP-06 regressions. TypeScript
and production builds pass. Actual Docker desktop/mobile screenshots, drill
checks, no overflow and no browser errors were inspected. Evidence is retained
under ignored `.lakehouse/mp07-proof/`. Fresh whole-branch review is the remaining
completion gate; its result and any fixes are recorded below before completion.
