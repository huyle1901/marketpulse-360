# MP-09 Quality, Observability and Privacy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Protect the Command Center and demonstrate trustworthy quality, telemetry and safe operations without changing MP01–08 analytical results.

**Architecture:** PostgreSQL owns users, opaque sessions and immutable operational evidence. FastAPI enforces roles and Origin; Next.js forwards cookies and presents role-aware workspaces. A bounded read-only collector exposes cached Prometheus metrics; Grafana is provisioned as code.

**Tech Stack:** Existing Python3.12/FastAPI/Next.js/ClickHouse/PostgreSQL; Prometheus3.14.0 and Grafana13.2.1-ubuntu.

**Spec:** `specs/09-quality-observability-privacy.md`

## Global Constraints

- Same source tree on `feat/mp09-quality-observability-privacy`; base `c25d1b3`; preserve untracked `work/`.
- No source replay/reseed, previous pipeline redesign, analytical deletion or credential commits.
- Runtime fails closed; no auth-disabled switch, startup account or hardcoded credential.
- Sessions use SHA256-only storage, eight-hour absolute TTL, HttpOnly SameSite=Strict cookies.
- Unsafe requests require exact configured Origin, including login and read-only POST simulation.
- Immutable audit decisions precede protected work; outcome recording is best effort and observable.
- Metrics use bounded labels; Kafka offsets include control records and never imply event counts.
- Retention deletes at most1000 expired sessions; all analytical/source/history data remains protected.
- Before each commit run relevant tests and `git diff --check`; fresh complete tests/fixture/Git checks before completion.

## Review Focus

- A role disabled or changed during a session must lose access immediately; concurrent edits cannot remove the last administrator (Task1 PG tests).
- A missing audit dependency must prevent protected effects; failure after a committed write must not claim rollback (Task1 middleware tests, Task3 counter).
- A same-count finance corruption or missing proof must fail or become unavailable, never pass (Task2 clone/proof tests).
- A Kafka/Flink outage must clear old values without inventing zero lag/event throughput (Task3 collector tests).
- Session expiry, delayed UI responses and viewer navigation must not reveal detail data or replace current state (Task5 browser tests).

## File Map

`security.py` owns pure password/session/policy contracts; `auth_store.py` and `sql/009_security.sql` own PG accounts/sessions/audit; `access.py` owns request enforcement; `auth_api.py` owns safe HTTP and CLI bootstrap. `operations.py`, `quality.py`, `ops_store.py` and `sql/009_operations.sql` own reports/lineage/probes/retention. `metrics.py`, `collector.py` and `platform_metrics.py` own bounded telemetry. `docker/observability/` owns provisioned infrastructure. `web/components/SessionGate.tsx`, login and operations pages own app access UX. Tests live beside existing `tests/test_app_*.py` and `web/tests/` conventions; proof scripts and runbook explain the authenticated demo.

## Task 1: Accounts, sessions, role policy and immutable audit

**Files:** Create security/auth_store/access/auth_api modules and security SQL, tests/test_app_security.py; modify api.py, runtime settings, old API test factories, app Docker command.

**Interfaces:** Produces `Principal(user_id,username,role)`, `AuthStore(store)` with initialize/create_user/login/resolve/logout/users/update_user/audit methods, `AccessControl(auth,origin,secure_cookie=False,metrics_token='')`. `create_app(analytics,store,access=None,operations=None)` denies application access by default. Tests explicitly inject trusted access. Auth endpoints are registered by `register_auth_routes(app,access)`.

- [x] Write pure hashing/canonical username/policy tests, failclosed API/Origin/denial-before-read/audit-failure tests, PG opt-in lockout/revocation/last-admin/concurrency tests; execute and observe RED.
- [x] Implement scrypt and bounded opaque sessions, PG migrations/triggers/throttles, serialized role changes, fixed route capabilities and decision/outcome audit. Register login/session/logout/user routes and explicit CLI password stdin. Disable raw access logging.
- [x] Adapt existing API tests with explicit trusted test principal. Run `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests`; expected all tests green, opt-in PG skipped until Docker proof. Inspect diff and commit focused auth behavior with architecture decision updates.

## Task 2: Certified quality, lineage, isolated probe and retention

**Files:** Create quality.py, operations.py, ops_store.py, operations SQL, tests/test_app_operations.py; modify runtime app and Compose read-only evidence mounts.

**Interfaces:** Consumes Task1 AccessControl/admin policy and AppStore. Produces `QualityService(analytics,ledger,state_root)`, `check(scope='live')`, `probe()`, `lineage()`; `Operations(quality,store,auth)` supplies status/reports/check/probe/clear/audit/users/retention handlers. Reports contain only bounded reasons/counts/digests/snapshots. `OpsStore` persists immutable reports plus independent probe acknowledgement state.

- [x] Write RED tests for missing evidence, full rowset corruption, source finance reconciliation, source binding/lineage, probe-owned cleanup and unchanged active database; bounded expired-only cleanup tests.
- [x] Implement readonly persisted certification validation, original source-finance/model checks and owned fixture clone corruption; no publishing/journal mutation. Register admin ops endpoints and session retention preview/cleanup.
- [x] Run focused tests then full Python suite, expected green; inspect diff and commit with architecture changes. Actual CH/PG proof belongs Task6.

## Task 3: Bounded request and platform telemetry

**Files:** Create metrics.py, collector.py, platform_metrics.py, tests/test_app_metrics.py; modify API middleware and Operations status.

**Interfaces:** Consumes Task1 fixed route templates and Task2 quality reports. Produces thread-safe `Metrics` request histogram/counters and bearer-protected rendered endpoint; `Collector` cached snapshots with source status/time and fixed-label samples. Poll functions return typed metric samples and safe JSON summaries; source failure omits previous samples. Collector HTTP serves cache only.

- [x] Write RED tests for histogram boundaries, unauthorized metrics token, no raw IDs/query labels, partial outage/stale clearing, offset-versus-event semantics, missing committed offsets and absent Flink jobs.
- [x] Implement API request/error/latency/process telemetry and bounded independent Kafka/Flink/CH/dependency/quality polls. Kafka reads watermarks/committed offsets without subscribing, committing or producing; Flink reads existing REST counters/checkpoints only. No job resubmit.
- [x] Run focused/full Python suite, expected green; inspect diff and commit telemetry plus architecture semantics.

## Task 4: Docker monitoring and safe secrets

**Files:** Create docker/observability Prometheus config/rules/Grafana provisioning/dashboard, setup helper and tests/test_ops_setup.py; modify compose.yaml, .env.example, .gitignore, Dockerfile.

**Interfaces:** Consumes Task3 collector module/API metrics bearer; Task1 cookie public origin. Produces observability profile services collector/prometheus/grafana, loopback49090/43001, internal-only collector. Compose secrets read from ignored .secrets; setup creates missing leaf secrets without overwriting existing data and rejects symlinks.

- [x] Write RED tests for setup permissions/idempotence/symlink refusal and monitoring config fixed labels/alerts/provisioning; run failing tests.
- [x] Implement pinned inspected images, 5s scrapes/evaluation/4s timeout/7d retention, Grafana auth and provisioned panels, separate failed-probe alert. Give API/collector readonly batch proof access; no privileged mounts.
- [x] Run setup/config tests and Compose quiet validation; expected green. Build changed images/new monitoring only, verify promtool and provisioning in Task6. Commit with architecture/deployment docs.

## Task 5: Sign-in, role-aware workspaces and admin operations

**Files:** Create SessionGate/auth context, login/operations pages and web/tests/auth-operations.spec.ts; modify layout, fixed proxy, existing pages/components and browser fixtures.

**Interfaces:** Consumes Task1 auth/session cookies and roles, Task2 admin JSON endpoints. Produces SessionGate/context `useSession()` with current principal and canOperate/canAdmin; Next proxy forwards Cookie/Origin only and relays Set-Cookie, no redirects or arbitrary routes. Operations actions guard stale requests and show authoritative errors.

- [x] Write RED browser tests for login/expiry/logout, viewer no detail requests/write controls, operator admin denial, delayed operations responses and failed actions; explicitly authenticate old browser fixtures.
- [x] Implement accessible login/gate/navigation and admin panels (health, lineage, reports/probe, audit, roles, retention). Gate existing journeys/profiles/editor/writes by role while preserving aggregate views. Protect direct API independently.
- [x] Run typecheck, build and all Playwright tests; expected green. Inspect desktop/mobile actual UI in Task6. Commit frontend with scope/architecture docs.

## Task 6: Real Docker verification, authenticated proof and final review

**Files:** Shared proof auth transport, prior smoke clients, new scripts/operations-smoke-test.py, docs/runbooks/operations.md, docs/architecture/mp09-decisions.md; update backlog/spec/architecture to verified status.

**Interfaces:** Consumes all previous tasks. Proof credentials come from private file/stdin, never argv plaintext; evidence under ignored .lakehouse/mp09-proof. Existing release identity/proof/finance and app business state are baseline invariants.

- [ ] Migrate prior clients to shared authenticated CookieJar transport with explicit Origin. Run actual PG security/audit/retention tests and CH isolated corruption proof; verify unchanged active release/sidecar/finance and no source replay.
- [ ] Build/recreate only changed app/new monitoring services. Create explicit private demo accounts via CLI, verify direct and proxy role/Origin/expiry matrix, telemetry/log/audit sanitization, Prometheus targets and provisioned Grafana. Fire separate probe alert, acknowledge and verify clear/history preservation.
- [ ] Inspect actual desktop/mobile login/admin/Grafana. Benchmark 20 warmups/100 samples with authenticated aggregate/lifecycle/ops p95<=500ms; time full quality check separately. Restart app/monitoring and check persistent roles/audit/config/session rules/MP06–08 results.
- [ ] Run complete Python/browser suites, meaningful integration checks, deterministic fixture validation, Compose/promtool/Grafana checks and `git diff --check`; expected all green. Record resources, commands, limits and recovery links. Commit evidence docs without credentials/artifacts.
- [ ] Dispatch one fresh whole-branch review using requesting-code-review on the most capable model. Re-grade and fix Critical/Important findings once using RED/GREEN plus complete suite; defer Minor explicitly. Record rulings and final checks, commit, remove only this plan's scratch workspace. No merge/push unless requested.
