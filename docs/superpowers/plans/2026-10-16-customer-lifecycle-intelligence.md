# MP-08 Customer Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans
> inline in this existing source tree. No worktree or implementer agents; fresh
> builder check and final whole-branch reviewer only. Steps use checkboxes.

**Goal:** Let CRM analysts drill certified cohorts into anonymized RFM timelines and preview/save consent-safe versioned segments.

**Architecture:** FastAPI pins immutable ClickHouse Gold/CRM plus the MP-07 sidecar. Next.js adds `/customers`; PostgreSQL stores only rule definitions and immutable revisions. No new analytical pipeline, dependency or source regeneration.

**Tech Stack:** Existing Python3.12+, FastAPI, Next.js/TypeScript, ClickHouse, psycopg/PostgreSQL, Docker Compose pins.

**Spec:** `specs/08-customer-lifecycle-intelligence.md`.

## Global constraints

- Base0512e3d, complete MP07; same tree new feat/mp08-customer-lifecycle-intelligence branch; preserve untracked work/.
- Analytics ClickHouse only, PostgreSQL operational definitions/revisions only.
- Existing release-scoped hash joins; no raw profile/contact/source IDs in HTTP.
- Activation requires identified CRM match and both latest certified consent flags true, even for empty/permissive rules.
- Historical cohorts do not become activation denominators; no sum of repeated cohort_size.
- As_of=UTC day-end after Gold max last_activity, fixed to snapshot, never today. Null recency never matches numeric predicates.
- Exact integer JSON strings/BigInt; six-place source ratios; single-currency guard.
- Rules schema mp08-segment-v1, <=8 conjunctive predicates; fixed fields/operators, bound SQL, canonical value strings <=2**63-1 magnitude.
- Profile/timeline limit1..100, offset0..100000; preview first20 plus exact matched_total.
- Missing detail is unavailable; conflicting binding fails409; denied analytics suppresses timeline.
- All architecture/spec changes travel with corresponding feature commits. No source reseed/replay or broad cleanup.

## Review focus

- Empty/permissive activation rules must still exclude denied/unknown consent and anonymous/session profiles.
- Refund after purchase must not count its gross context as another purchase or reset recency.
- A cohort drill must select the exact active profiles of that cohort/activity month, not every cohort member.
- A slower previous profile/draft/release preview must not appear under newer selection or overwrite saved edits.
- A reused old rule revision remains immutable and evaluates reproducibly against the same release despite newer revisions.

## Task 1: Strict segment contracts and lifecycle metrics

**Files:** Create src/marketpulse_app/lifecycle_contracts.py, lifecycle_math.py; tests/test_app_lifecycle_contracts.py, test_app_lifecycle_math.py.
**Interfaces:** `SegmentRule.from_payload(value: object) -> SegmentRule`, `.payload() -> dict`, `.digest: str`;
`Condition(field: str,op: str,value: str)`; `lifecycle_metrics(profile: Mapping[str,object], detail: Mapping[str,object], as_of: datetime) -> dict[str,object]`;
`identity_evidence(profile_type: str) -> dict`, `activation_eligible(profile: Mapping[str,object]) -> bool`.

- [x] Write RED tests: canonical order-independent digest; reject extra/hostile fields, bool/exponent/noncanonical numbers, duplicates, >8 conditions; signed net; invalid month/type/purpose. Literal >2**53 exact and null recency rules.
- [x] Run `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_app_lifecycle_*.py' -v`. Expected missing interfaces FAIL.
- [x] Implement frozen strict contracts with schema_version mp08-segment-v1, fixed numeric/enum/month bounds and pure metrics/consent categorical evidence. Verify original-purchase recency, refund-only/never-purchased, heuristic operands and unknown/denied consent.
- [x] Run focused/full Python suite, diff check; update architecture and commit feat: define consent-safe lifecycle and segment contracts. Expected PASS.

## Task 2: Request-pinned lifecycle analytical reads

**Files:** Create src/marketpulse_app/lifecycle.py; tests/test_app_lifecycle_queries.py.
**Interfaces:** `LifecycleError(code: str)`; `LifecycleService(analytics: Analytics)` with
`cohorts() -> dict`, `profiles(*,cohort_month: str|None=None,activity_month: str|None=None,profile_type: str|None=None,purpose: str='analytics',opportunity: str|None=None,limit: int=20,offset: int=0) -> dict`,
`timeline(profile_ref: str,*,limit: int=50,offset: int=0) -> dict`, `preview(rule: SegmentRule) -> dict`.
Consumes Task1 rules/metrics and existing Analytics/JourneyJournal/single_currency.

- [x] Write RED query/service tests: all reads pin one release; hashes only; explicit CRM deny/unknown/missing gates for all purposes; original-order aggregate; month membership via full governed activity; signed net and exact comparisons; hostile values bound; stable pagination; missing/conflicting detail; unknown404/invalid422/restricted timeline; global as_of not selected rows. Test two-campaign independent identities do not merge and anonymous/session confidence stays limited.
- [x] Run focused tests. Expected missing service FAIL.
- [x] Implement fixed-schema ClickHouse CTE profile join to certified CRM and aggregated sidecar (last original purchase/cart/email), fixed source-bound as_of, safe DTOs and exact SQL predicates. Use bound identity/hash/filter/numeric values; no arbitrary SQL. Profiles/preview paginated server-side, not full Python warehouse scans. Cohort/source summaries usable without detail; missing detail returns unavailable and preview fails409. Existing Gold fields remain source totals.
- [x] Run focused/full tests and actual CH queries when Docker restored, inspect consent/retention reconciliation. Expected PASS. Architecture/diff check, commit feat: serve pinned Customer 360 and lifecycle insights.

## Task 3: Immutable saved segment revisions and HTTP/proxy

**Files:** Create src/marketpulse_app/segment_store.py, lifecycle_api.py, sql/segments.sql;
 tests/test_app_segment_store.py, test_app_lifecycle_api.py; modify api.py and web/app/api/v1/[...path]/route.ts.
**Interfaces:** `SegmentStore(store: AppStore)` with `initialize()`, `list() -> list[dict]`,
`save(segment_id: str,name: str,rule: SegmentRule,expected_version: int) -> dict`, `revision(segment_id: str,version: int) -> dict|None`;
`register_lifecycle_routes(app: FastAPI,analytics: Analytics,store: AppStore) -> None`.

- [x] Write RED tests: empty/idempotent migration, exact canonical JSON/digest, version1/create and version2 update, immutable old revision; stale/concurrent edits one winner409, restart still replays old rule. Dedicated mp08_test DB reset only. Invalid payload/unknown ID/model/name/extra keys422; errors409/404/503, no-store; read-only preview never saves; privacy at HTTP boundary.
- [x] Run focused tests (opt-in PG tests run later actual Docker). Expected missing interfaces FAIL.
- [x] Implement additive schema/current row+immutable revisions transaction, optimistic CAS and parameterized SQL. Initialize with existing app startup migration under separate lock36008; seed nothing. Register fixed lifecycle routes and stable errors; expand proxy allowlist only specified GET/POST/PUT paths. Preserve MP06–07 behavior.
- [x] Run focused/full Python suite and real PG revision/concurrency tests in dedicated mp08_test, architecture/diff check. Expected PASS. Commit feat: save versioned lifecycle segments and expose APIs.

## Task 4: Cohort-to-customer workspace and segment editor

**Files:** Create web/app/customers/{page.tsx,types.ts}, web/tests/customers.spec.ts; modify shared navigation/styles.
**Interfaces:** Task2/3 route DTOs. Combined cohort/profile identities must match; previews must match current rule digest and workspace release. Selected timeline checks returned identity/reference against active profile context.

- [x] Write RED browser tests: cohorts drill exact activity month; RFM/consent/category labels; exact >2**53/negative net; saved rule create/edit/reload old version with immutable view; preview empty activation rule excludes population; unavailable/restricted/zero states; current filter/profile/rule/release suppresses delayed old responses; disabled save draft; mobile/keyboard/no page overflow.
- [x] Run new browser tests. Expected absent page FAIL.
- [x] Build responsive `/customers` with navigation, scope/as_of/source details, retention table, purpose/type/opportunity filters, pagination, RFM/consent table, timeline, fixed condition editor and saved revision selector. Use AbortController/request contexts and previous-selection markers. No invented ML confidence or activation delivery. Draft is preserved on errors and locked while saving.
- [x] Run full MP06–08 browser tests/typecheck/build and Python suite; Docker build/recreate only API/web; actual desktop/mobile/drill visual QA. Expected PASS. Architecture/diff check and commit feat: add Customer 360 lifecycle and segment workspace.

## Task 5: Independent proof, restart, benchmark, docs and final review

**Files:** Create scripts/lifecycle-smoke-test.py, tests/test_app_lifecycle_proof.py,
 docs/runbooks/customer-lifecycle.md, docs/architecture/mp08-decisions.md;
 update architecture/backlog/spec/plan observed status.
**Interfaces:** Task2 service DTOs and Task3 revisions; existing proof project/network/volumes. Oracle uses validated MP05 exports + MP07 events, never service's own rollup/predicate.

- [x] Write RED proof assertions detecting duplicate/missing keyed profiles, false activation eligibility, incorrect RFM/monetary and cohort membership. Expected absent helper FAIL.
- [x] Implement exact independent profile/cohort/timeline/activation oracles from bound typed exports and governed sidecar; include owned isolated denied/unknown consent cases, a refund-only/never-purchased case, fixed rule replay and immutable old revisions across stale writes. No source mutation or external delivery. Evidence .lakehouse/mp08-proof/.
- [x] Run Docker proof actual HTTP/write/PG/CH + API/web restart; compare unchanged MP06–07 finance/config/source release and MP08 revisions/results. Benchmark20warmups/100uncached serial samples each for cohorts/profiles/timeline/preview, p95<=500ms fixture target, disclose versions/resources/bounds. Expected PASS.
- [x] Run full Python/source fixture validation/all browser tests/typecheck/build/Compose/diff checks and actual visual QA. Expected PASS. Commit observed proof/docs, mark only observed acceptance.
- [ ] Fresh whole-branch review base0512e3d using most-capable reviewer, Review Focus verbatim plus ledger rulings. One Critical/Important fix pass via RED/GREEN/full suite, affected Docker proof; minor findings recorded. Final docs/commit and Git check; retain new local branch without creating worktree or unrequested PR/merge.

## Self-review

Spec maps to strict inputs/semantics(1), CH reads/gates(2), operational storage/HTTP(3),
usable workflow/races(4), independent correctness/runtime evidence(5). Shared interfaces
above use existing pin/projection/hash contracts. User's standing backlog delegation
and native same-tree execution continue; no additional approval stop is needed.
