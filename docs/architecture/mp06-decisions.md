# MP-06 decisions and verification

Scope/plan were committed as `369560b` before code, after the user delegated
scope to the MP-06 backlog. PR #5 was verified merged; new branch starts at main
merge `a8843ae` containing MP-05 `19788c7`. All work stays in the existing tree.

| Decision | Reason / practical limit |
|---|---|
| Separate certified finance and provisional Flink snapshots | MP-03 totals precede MP-04 consent/bot governance; they must never be added or used as one ROAS numerator/denominator. |
| Append snapshots and select whole latest payload by Kafka offset | Handles cumulative upserts, nullable currency and retries without background merge assumptions. Typed scalar columns plus canonical payload are retained. |
| Collapse identical Kafka positions; retain first receipt time | Lost offset/insert acknowledgement cannot freshen or double-count a snapshot. Changed payload/partition/source identity fails closed. |
| DDL remains in LiveStore rather than a separate unused SQL asset | Projection creation and column list have one owner; app PostgreSQL migrations stay in package SQL. |
| App PostgreSQL separate from Dagster/catalog | Keeps real user settings/rules/evidence outside generated sources and analytical storage. |
| Configured market, initially unassigned | Existing campaign Gold has no geography; no upstream contract changes or fabricated VN default. |
| Daily alerts use global newest closed release day | A missing campaign row cannot be substituted with an older campaign-specific day. |
| Pacing distinguishes an internal gap from a missing tail | Internal gaps suppress forecasts; contiguous-prefix forecasts are explicitly incomplete/as-of. |
| Threshold-based refund/spend/budget rules | Explainable evidence; spend-spike fixture is controlled in tests because MP-01 does not inject spend spikes. |
| Active-period budget alerts evaluate the observed closed-day interval | Known spend already above the full-period budget triggers immediately; other missing-date cases remain insufficient. Evaluation-contract v2 preserves earlier local evidence while fixing day-transition identity. |
| Regrade the review's three provenance/proof minors as required correctness fixes | Exact key-set equality is required for the independent oracle; source identity and rule/setting versions are explicit traceability contracts promised to the demo user. |
| Budget demo uses a deliberately configured small budget | Demonstrates overrun without altering source data or claiming the generator injected it. |
| Read-only fixed historical net-ROAS simulation | Exact Decimal scenario, zero baseline unavailable, no automatic allocation or forecast-model claim. |
| Next.js same-origin allowlisted proxy and 5-second polling | No backend credentials in the browser; preserves HTTP errors/no-store, aborts obsolete reads, rejects old scenario responses after input changes. |
| Added app services to existing test-owned MP-05 proof project | Reuses verified upstream services; no replay, reseeding or volume reset. A separate ephemeral `mp06_test` PostgreSQL exercised concurrency. |

Verification evidence is in [campaign-operations.md](../runbooks/campaign-operations.md)
and ignored `.lakehouse/mp06-proof/`. Initial real proof independently matched
14 keyed Kafka snapshots, retained app state/live receipt times and certified
totals across app restart, and met all three fixture HTTP p95 budgets.
The fresh whole-branch reviewer found six Important issues (budget lifecycle,
Decimal threshold precision, mutable pending draft, old filter display, old release
scenario, malformed threshold status) and three initially Minor traceability/proof
issues (exact key-set oracle, source identity, alert versions). They were regraded
as required correctness contracts, reproduced and fixed through RED/GREEN tests.
Final results: 333 Python tests OK (27 opt-in skips), all 27 app tests green with
three real integration cases enabled, nine browser tests, TypeScript/build and
final Docker acceptance. All four app services remain healthy. No unresolved
review findings are deferred.

The review explicitly excluded production auth/RBAC/notification/ad writes,
capacity/event-to-dashboard SLA and source reset/retention recovery because
they are outside this local milestone. They require MP-09/10 or explicit future
source recovery work. The serving adapter's DDL remains owned by LiveStore,
avoiding an unused duplicate SQL asset. Next's generated environment type file
is ignored; build/dev regenerate it rather than committing environment-dependent
imports. No separate worktree was created; existing local source material was
preserved. Scoped decisions cost additional provenance reads/evidence fields;
evaluation-contract v2 can retain a separate occurrence beside earlier local
development evidence, which is intentionally preserved.
