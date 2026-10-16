# MP-06 Campaign Budget Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans
> inline, as requested, in the current source tree. No new worktree or implementer
> subagents. A fresh reviewer checks the design and the final branch.

> **Execution record:** Completed inline in the existing source tree on 2026-10-13.
> Scope/plan committed before code (`369560b`); tasks 1–5: `b322501`, `79c0c47`,
> `8252b89`, `b795744`, `11aa66d`. Final proof/review fixes are in the final MP-06
> acceptance commit. All nine reviewer findings were reproduced and fixed;
> 333 Python tests OK (27 opt-in skips), 27 app tests with real integrations,
> nine browser tests/typecheck/build and refreshed Docker proof passed.
> Actual commands/results/limits: `docs/runbooks/campaign-operations.md`.
> DDL for streaming remains in LiveStore; Next's generated environment types stay
> ignored. User's one-tree constraint overrides the skill's worktree default.

**Goal:** Deliver four usable MP-06 capabilities with exact, traceable metrics and persistent app configuration.

**Architecture:** FastAPI reads certified and provisional analytical data from ClickHouse, using one MP-05 release per request. A read_committed serving worker copies MP-03 keyed snapshots and evaluates rules; a dedicated PostgreSQL stores app settings and alert history. Next.js polls the API through same-origin handlers.

**Tech Stack:** Python 3.12+, FastAPI, Next.js/TypeScript, Kafka/Flink contracts already implemented, ClickHouse, psycopg, Docker Compose.

**Spec:** `specs/06-live-campaign-and-budget-operations.md` (binding scope and formulas).

## Global Constraints

- Existing source tree only; base `a8843ae` on `origin/main`, containing MP-05 `19788c7`.
- Reuse MP-01 fixtures, MP-03 aggregate contracts and MP-05 certified publications.
- ClickHouse stores all analytical rows; PostgreSQL stores app configuration and alert evidence only.
- Dates are UTC `[from_date,to_date)`, maximum 366 days.
- Money/counts cross JSON as decimal strings; ratios use Decimal truncated to six places.
- Streaming and certified metrics are separate, with independent provenance/freshness.
- Configured market is app metadata; no geography invented from currency.
- Python 3.12+, explicit package/container pins and transitive locks; no new mandatory cloud services.
- Startup never seeds app state; no credentials/log payloads, volume deletion or source reseeding.
- All mandatory new runtime services use the `app` Compose profile.
- Architecture/spec updates accompany implementation commits; full suite and diff check before commits.

## Review Focus

- A row above 2**53 must stay exact from ClickHouse through browser display and simulation input.
- A historical replay may be transport-current but business-stale; polling/retries cannot freshen it.
- An unknown campaign or missing currency/baseline must not leak into a confident forecast.
- A late old request/publication/configuration edit cannot overwrite a newer selection or budget.
- A daily source gap must not be treated as zero or extrapolated with an inflated elapsed-day count.

## Task 1: Domain contracts and deterministic budget math

**Files:** Create `src/marketpulse_app/{__init__,contracts,math}.py`,
`tests/test_app_contracts.py`, `tests/test_app_math.py`.

**Interfaces:** Frozen `DashboardFilter(from_date: date, to_date: date,
platform: str | None = None, campaign_id: str | None = None,
objective: str | None = None, market: str | None = None, currency: str | None = None)`;
`CampaignSetting(campaign_id: str, market: str | None, currency: str,
period_start: date, period_end: date, budget_minor: int, version: int = 0)`;
`AlertRule(rule_id: str, campaign_id: str, kind: str, threshold: Decimal,
enabled: bool = True, version: int = 0)`.
`pacing(setting: CampaignSetting, daily_spend: Mapping[date,int], today: date) -> dict[str,object]`;
`simulate(baselines: Mapping[str,tuple[int,int]], allocations: Mapping[str,int], total_budget_minor: int) -> dict[str,object]`;
`evaluate_rule(rule: AlertRule, operands: Mapping[str,object]) -> dict[str,object]`.

- [x] Write failing unittest cases: interval 0/367 days rejected; boolean/float money
  rejected; amount 9007199254740993 preserved; no baseline/negative net/zero allocation;
  pacing B=1000,D=10,K=2,S=300 -> expected 200, remaining 700, projection 1500;
  gap day 2 -> insufficient_data; future period -> null forecast; ended complete
  period -> S; refund_share 1/10 >0.05 triggers, gross 0 insufficient; spike 300/100
  >2 triggers, absent prior calendar day insufficient; spend==budget is clear.
- [x] Run `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_app_*.py' -v`;
  Expected: new interfaces unavailable, tests fail.
- [x] Implement strict dependency-free dataclasses and Decimal formulas; no clocks,
  I/O, HTTP or operational persistence in domain modules.
- [x] Run focused tests and complete Python suite; Expected: no failures.
- [x] Update architecture with proposed MP-06 boundary, run diff check, commit
  `feat: define campaign budget contracts and exact scenario math`.

## Task 2: Strict replay-safe Kafka serving adapter

**Files:** Create `src/marketpulse_app/{streaming,live_store}.py`,
`src/marketpulse_app/sql/live.sql`, `tests/test_app_streaming.py`.

**Interfaces:** `CampaignSnapshot` validates the v1 Kafka payload and holds
partition/offset/source identity/first-received provenance;
`parse_snapshot(value: bytes, key: bytes, partition: int, offset: int,
received_at: datetime) -> CampaignSnapshot`;
`LiveStore(client: Any)` with `initialize(source_identity: str) -> None`,
`write(snapshot: CampaignSnapshot) -> None`, `latest() -> list[dict[str,object]]`;
`deliver(message: Any, store: LiveStore, consumer: Any, now: datetime) -> None`.

- [x] Write RED tests for wrong key, unknown/missing fields, naive timestamp,
  bool/negative counter, Int64 overflow, currency-null zero money, inconsistent net;
  repeated/out-of-order cumulative snapshots select highest offset, not sum;
  whole nullable tuple preserved; conflicting position and moved partition rejected;
  failed write commits nothing; acknowledgement retry preserves first received_at;
  changed cluster/topic identity fails. Use opt-in real ClickHouse integration for
  projection SQL/conflicts (no test-only paths in production).
- [x] Run streaming tests; Expected: missing new interfaces FAIL.
- [x] Implement synchronous append/collapse/latest queries and consume loop with
  read_committed/manual commits, bounded payloads, stable Kafka source identity;
  validate before writing/committing. No changes to Flink.
- [x] Run focused/full suites and real serving projection tests; Expected: PASS,
  latest totals unaffected by duplicate storage or arrival order.
- [x] Update architecture, diff check and commit
  `feat: serve committed Flink campaign snapshots without replay inflation`.

## Task 3: Persistent settings, versioned rules and alert evaluation

**Files:** Create `src/marketpulse_app/{store,alerts}.py`,
`src/marketpulse_app/sql/app.sql`; `tests/test_app_store.py`, `tests/test_app_alerts.py`.

**Interfaces:** `AppStore(dsn: str)` with `initialize()`,
`settings() -> list[CampaignSetting]`, `save_setting(setting: CampaignSetting,
expected_version: int) -> CampaignSetting`, `rules() -> list[AlertRule]`,
`save_rule(rule: AlertRule, expected_version: int) -> AlertRule`,
`record_evaluation(key: str, evidence: Mapping[str,object]) -> bool`,
`alerts(limit: int = 100) -> list[dict[str,object]]`;
`evaluate_alerts(analytics: Analytics, store: AppStore, today: date) -> dict[str,object]`
uses Task 4's request-pinned data. Wire after Task 4 supplies Analytics.

- [x] Write failing tests using a dedicated `mp06_test` database: additive migration;
  setting create version 1; concurrent stale update ->409/conflict; no generated
  state at startup; versioned rules; repeated/concurrent evaluation same key ->one
  result/occurrence; threshold or setting edits preserve old evidence; DB restart
  retains exact operands. Unit cases exercise incomplete/future days and fixture refunds.
- [x] Run focused tests (integration explicitly opt-in); Expected: missing interfaces FAIL.
- [x] Implement schema-scoped migrations, parameterized transactions and immutable
  unique evaluation evidence; separate real app DB from Dagster/catalog.
- [x] Run focused/full suite and PostgreSQL integration; Expected: PASS; test-owned
  schema reset guards reject the application DB.
- [x] Update architecture, diff check, commit
  `feat: persist campaign configuration and idempotent alert evidence`.

## Task 4: Request-pinned analytical service and FastAPI

**Files:** Create `src/marketpulse_app/{analytics,api,settings,worker}.py`,
`tests/test_app_analytics.py`, `tests/test_app_api.py`; modify `pyproject.toml`.

**Interfaces:** `Analytics(client: Any, live_store: LiveStore)` methods
`dashboard(filters: DashboardFilter, settings: Sequence[CampaignSetting]) -> dict[str,object]`,
`campaigns() -> list[dict[str,object]]`, `daily(filters: DashboardFilter) -> list[dict[str,object]]`;
resolve once per public operation via existing `resolve_active`, or construct an
explicit request-pinned session used by its subqueries.
`create_app(analytics: Analytics, store: AppStore) -> FastAPI` exposes the spec's
routes; runtime dependency injection provides live clients. `load_settings(environ:
Mapping[str,str]) -> AppSettings` keeps secrets out of repr; worker coordinates
stream ingestion and 5-second evaluations, tolerating unavailable publication.

- [x] Write RED tests for parameterized hostile filter values; release changes
  between subqueries still use original release; currency-separated totals;
  configured market semantics; unknown/empty selections; unmapped stream rows;
  >2**53 response money strings; day-resolution freshness and historical stale state;
  simulation invalid currency/sums/IDs; stale edit ->409; bad body ->422;
  absent publication ->409; dependency down ->503 with stable code/no secrets.
- [x] Run API/query tests; Expected: missing interfaces FAIL.
- [x] Resolve/pin FastAPI/Uvicorn/Pydantic test-client dependencies, add app extra;
  implement route models, exact serialization, allowlisted parameterized queries,
  settings validation and worker entry points. Finish Task 3 evaluation wiring.
- [x] Run focused/full suite and real HTTP/DB/ClickHouse operations; Expected: PASS,
  no-store headers and one-release provenance across dashboard/pacing/simulation.
- [x] Update architecture/spec, diff check and commit
  `feat: expose campaign operations through FastAPI`.

## Task 5: Usable Next.js Marketing Command Center and Compose app runtime

**Files:** Create `web/` (package/lock, Next config, TypeScript app/components,
same-origin API handler, CSS, browser tests), `docker/app/{Dockerfile,Dockerfile.web,
requirements.lock}`, `tests/test_app_runtime.py`; modify Compose, ignores,
`.env.example`, README. Dependencies pinned; no font/CDN required at runtime.

**Interfaces:** Browser DTOs match Task 4 JSON contracts. Fetch only same-origin
`/api/v1/...`; proxy accepts the fixed spec path/method allowlist and preserves
statuses, query parameters and no-store. Poll lifecycle ignores aborted/older
responses; formatting uses string/BigInt, not Number for money.

- [x] Write RED browser tests: dashboard source panels/filtering/historical label;
  >2**53 exact display; config save and rule create; alert explanation;
  pacing insufficient-data state; allocation sum validation and before/after;
  failed poll keeps labeled previous success; old filter response cannot overwrite
  current; mobile layout and keyboard form navigation. Write runtime asset tests
  for profiles, private DB, nonroot processes, secret exclusion and health checks.
- [x] Run tests; Expected: absent UI/runtime FAIL.
- [x] Resolve/pin Next.js/React/Node and Playwright, implement the single Command
  Center and proxy, lock dependencies; add app-postgres/API/worker/web services,
  additive secret initializer, app volume and resource limits. `.dockerignore`
  must exclude web node_modules/.next while admitting actual web sources.
- [x] Run frontend tests, TypeScript and production build, full Python suite and
  `docker compose --profile app --profile serving --profile streaming config -q`;
  Expected: PASS without printing secrets. Build/start actual app services on the
  existing proof network, with port overrides preserved.
- [x] Inspect actual UI desktop/mobile screenshots; fix functional/layout errors;
  update architecture, diff check and commit
  `feat: add the campaign command center and local app runtime`.

## Task 6: Executable acceptance, restart and measured evidence

**Files:** Create `scripts/app-smoke-test.py`, `docs/runbooks/campaign-operations.md`,
`docs/architecture/mp06-decisions.md`, tests for proof assertions;
modify architecture, backlog and MP-06 spec/plan status.

- [x] Write RED proof assertions for source-position deduplication, provisional
  vs certified separation, persisted versions/alert identities and benchmark
  nearest-rank p95. Run proof unit tests; Expected: new assertions FAIL.
- [x] Implement the proof against an explicitly selected existing project; read
  actual Kafka/ClickHouse independently, configure demo budget/refund rule by HTTP,
  exercise filters/pacing/simulation, repeat evaluation and restart only new app
  services. Test fault paths in owned test relations/DB only; retain prior projects.
- [x] Run real Docker acceptance plus 20 warmup/100 uncached HTTP measurements each
  for dashboard/pacing/simulation; Expected: no failures, fixture p95 <=500ms,
  unchanged certified totals, identical persisted state after restart. Run browser
  checks on the production Docker UI; retain ignored evidence and disclose scale.
- [x] Run complete Python suite, source fixture validation, Compose/health checks
  and git diff --check/status. Expected: all required checks pass; existing opt-in
  integrations reported explicitly rather than counted as executed.
- [x] Request one fresh whole-branch review against base `a8843ae`; address Important
  findings via RED/GREEN regressions and full suite; record decisions/limitations.
- [x] Mark implemented/verified only where actual evidence exists, write reproducible
  start/demo/stop instructions and commit `feat: verify MP-06 campaign operations`.

## Self-review and execution handoff

Coverage: contracts/math (1), provisional projection (2), operational persistence
(3), analytical/API boundary (4), usable UI/runtime (5), independent proof (6).
The five review-focus failure modes have owning tests in tasks 1/2/4/5. Shared
types are dependency-free; Task 3's evaluation orchestration is wired after Task 4,
so persistence can be tested first without inventing a second analytical reader.
User chose existing-tree execution and delegated scope to backlog; continue inline
after builder review, without further preference/approval questions.
