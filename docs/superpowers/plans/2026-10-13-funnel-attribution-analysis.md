# MP-07 Funnel and Attribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans
> inline in this source tree; no worktree or implementer agents. Fresh design
> and final branch reviewers check the work.

**Goal:** Extend the Command Center with traceable channel/campaign funnel reach, three-model attribution comparison and representative order journeys.

**Architecture:** FastAPI reads existing certified Gold aggregates from a request-pinned ClickHouse release. An explicit Spark tool adds a separately validated immutable journey projection from that release's pinned Silver snapshot. Next.js polls/read-only drills through the existing same-origin proxy; PostgreSQL remains operational only.

**Tech Stack:** Existing Python 3.12+, FastAPI, Next.js/TypeScript, ClickHouse, Spark/Iceberg/Nessie/MinIO, psycopg and Compose pins. No new library.

**Spec:** `specs/07-funnel-and-attribution-analysis.md`.

## Global constraints

- Base `5b236b9`, pushed MP-06; stacked feature branch in the same source tree.
- UTC `[from_date,to_date)`, 1..366 days; existing platform/objective/market contracts.
- Analytics in ClickHouse; PostgreSQL only config/coordination. No runtime API Iceberg/source reads.
- Preserve MP-04 attribution policy, MP-05 publications/exports and MP-06 behavior.
- Stage reach sums are campaign-day session incidences, not global unique audiences or sequential drop-off.
- Money/counts cross JSON as exact decimal strings; allocated counts keep six-place scale.
- Attribution requires a single-currency release; global reconciliation precedes dimension filters.
- No raw PII/profile IDs in journey HTTP; no source reseeding, automatic deletes or new cloud services.
- New publisher is an explicit `app-tools` Compose tool, not a startup side effect.
- Architecture/spec update and relevant/full tests/diff check before each focused commit.

## Review focus

- A session spanning two campaigns/days must not become a falsely labeled unique cross-channel audience.
- Refunds inside the selected period may refer to an earlier purchase; eligibility stays tied to that purchase.
- A duplicated click whose canonical earliest touch is outside the window must not receive in-window credit.
- Missing/mixed-currency/mismatched release projections must fail visibly rather than fabricate a representative journey.
- A slower old selection or a release change between three HTTP reads must not display a mixed analytical page.

## Task 1: Exact analysis contracts and certified read service

**Files:** Create `src/marketpulse_app/analysis.py`, `tests/test_app_analysis.py`.
**Interfaces:** `AnalysisError(code: str)`; `AnalysisService(analytics: Analytics)`;
`funnel(filters: DashboardFilter, settings: Sequence[CampaignSetting]) -> dict[str,object]`;
`attribution(filters: DashboardFilter, settings: Sequence[CampaignSetting]) -> dict[str,object]`;
pure `summarize_funnel(rows: Sequence[Mapping[str,object]], dimensions: Sequence[Mapping[str,object]]) -> dict[str,object]` and
`summarize_attribution(rows, baseline, dimensions) -> dict[str,object]` use exact fields from spec.

- [ ] Write RED tests: duplicate campaign/day buckets summed with incidence label;
  stage skipping remains valid; unknown/unattributed channels; zero ratios;
  gross 9007199254740993 exact; three models conserve gross/refund/net;
  corrupted same-count money or missing model fails; filtered credit differs
  from source campaign revenue without false reconciliation failure;
  SQL parameters bind hostile values; all subqueries use one release; mixed
  campaigns/performance currencies rejected. Define literal fixture expectations.
- [ ] Run `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_app_analysis.py' -v`.
  Expected: missing analysis interfaces FAIL.
- [ ] Implement focused pure rollups, model definitions, request-pinned parameterized
  Gold queries and actual day-resolution provenance. No new attribution algorithm.
- [ ] Run focused/full Python suite. Expected: all tests pass (existing integrations opt-in).
- [ ] Update architecture, diff check and commit `feat: expose exact funnel and attribution analysis`.

## Task 2: Snapshot-bound privacy-safe journey projection

**Files:** Create `src/marketpulse_app/{journey_projection,journey_publish}.py`,
`docker/app/Dockerfile.journeys`, `tests/test_app_journey_projection.py`; modify Compose.
**Interfaces:** `JOURNEY_COLUMNS: Columns` for exact sanitized projection;
`project_event(row: Mapping[str,object], identity: str) -> dict[str,object]`;
`JourneyPublication(identity, proof_digest, database, silver_snapshot_id, version, row_count, checksum)`;
`JourneyJournal(client)` with `resolve(release: PublicationReservation)` and `publish(publication)`;
`copy_journeys(client, release, snapshot_id: int, rows: Iterable[Mapping[str,object]]) -> JourneyPublication`.
Publisher `main()` uses BatchLedger recorded receipt/bundle and existing Spark configuration.

- [ ] Write RED tests: no raw PII/profile fields; profile ref stable within release,
  different between releases; exact nullable money/time/opaque IDs; reject bot,
  denied consent/duplicate event; failed insert does not publish; conflicting
  metadata rejected; identical semantic journal rows collapse; source snapshot
  mismatch rejects; copy source/readback full checksum equals, including >2**53.
- [ ] Run focused tests. Expected: missing interfaces FAIL.
- [ ] Implement streamed snapshot projection, key validation, typed MergeTree copy
  with bounded chunks/disk-backed checksum, complete publication binding and
  existing-copy re-verification under PostgreSQL advisory lock 36007. Add globally
  canonical paid-touch view before window filters. Failed candidates stay isolated.
- [ ] Build explicit tool image, publish from the existing active receipt against
  real Spark/ClickHouse; verify rowset equality and retry reuse. Run focused/full
  suite and opt-in real projection fault/journal tests. Expected: PASS.
- [ ] Update architecture, diff check, commit `feat: publish pinned anonymized journey detail for analysis`.

## Task 3: Representative journeys and read-only HTTP boundary

**Files:** Create `src/marketpulse_app/{journeys,analysis_api}.py`,
`tests/test_app_journeys.py`, `tests/test_app_analysis_api.py`; modify `api.py` and proxy route allowlist.
**Interfaces:** `JourneyService(analytics: Analytics)` with
`sample(filters: DashboardFilter, settings: Sequence[CampaignSetting], model: str = 'last_touch', limit: int = 5) -> dict[str,object]`;
`register_analysis_routes(app: FastAPI, analytics: Analytics, store: AppStore) -> None`.

- [ ] Write RED tests for limit/model/date validation, no projection warning,
  old/mismatched receipt or conflicting projection rejection, paid touches before
  report dates, original purchase preceding refunds, canonical click outside
  window, selected-model credited campaign filtering (not source campaign),
  too-large context reports unavailable without partial credit, hashed HTTP refs,
  409/422/503 stable errors and no-store/one-release provenance. Use unchanged
  `build_attribution_daily` for bounded explanations and literal expectations.
- [ ] Run focused tests. Expected: new interfaces/routes FAIL.
- [ ] Implement bounded (100 candidates, 500 touches, 100 financial events) queries
  and safe sample DTOs. Register the three GET routes and specific AnalysisError
  handler without changing MP-06 errors. Proxy only fixed analysis GET paths.
- [ ] Run focused/full suite and real HTTP funnel/attribution/journey filters and
  refund windows against the actual sidecar. Expected: PASS, no raw profile/contact payloads.
- [ ] Update architecture, diff check, commit `feat: add representative journey drill-down APIs`.

## Task 4: Usable analysis page and race-safe navigation

**Files:** Create `web/app/analysis/{page,types}.tsx` (types may be `.ts`),
`web/tests/analysis.spec.ts`; modify existing navigation/CSS.
**Interfaces:** Three route DTOs from Tasks 1–3; model/campaign/channel drill actions
use the same selected interval/configured market. UI accepts refreshed reports
only when all release identities match and selection is still current.

- [ ] Write RED browser tests: actual navigation; stage-reach/incident labels;
  three model definitions and conservation operands; >2**53 exact display;
  channel→campaign→journey drill; unknown/no projection states; mobile/keyboard;
  delayed previous selection and mixed-release three-response batch rejected.
- [ ] Run frontend tests. Expected: absent page/actions FAIL.
- [ ] Build responsive labeled stage bars/tables, comparison cards, drill controls,
  full source/reconciliation details and honest sample timeline. Cancel old reads,
  preserve marked previous results on failure, never invent chart data.
- [ ] Run all MP-06/07 browser tests, TypeScript/build, full Python tests. Build/
  recreate only app API/web in proof project and inspect actual desktop/mobile UI.
  Expected: PASS and existing operations behavior retained.
- [ ] Update architecture, diff check, commit `feat: add funnel and attribution analysis workspace`.

## Task 5: Independent evidence, restart, review and runbook

**Files:** Create `scripts/analysis-smoke-test.py`, `tests/test_app_analysis_proof.py`,
`docs/runbooks/funnel-attribution.md`, `docs/architecture/mp07-decisions.md`;
modify backlog/spec/plan/architecture status.

- [ ] Write RED proof tests: exact keyed equality detects duplicate/missing keys;
  all model totals reconcile independently; refusal of filtered/global conflation.
- [ ] Implement explicit test-project proof: validated pinned MP-05 export oracles,
  sanitized Silver projection funnel/attribution oracles using existing Gold
  helpers, HTTP drill and representative timeline checks, unavailable/fault tests
  in owned candidates only, API/web restart, unchanged MP-06 source/finance/app state.
- [ ] Run actual publisher/retry and HTTP proof, 20 warmups/100 uncached samples
  per funnel/attribution/journey workload; require fixture p95 <=500 ms and disclose
  sample limits/counts/versions/resources. Retain ignored evidence; no capacity claim.
- [ ] Run full Python tests, fixture validation, Compose/diff checks, all browser
  tests/typecheck/build and real visual QA. Expected: all required checks pass.
- [ ] Request one fresh whole-branch review against 5b236b9; fix Important issues
  via RED/GREEN/full suite and rerun affected Docker proof. Record rulings/limits.
- [ ] Mark only observed acceptance complete; commit `feat: verify MP-07 funnel attribution and journey analysis`.

## Self-review / execution

All spec sections map to aggregate service (1), pinned sidecar (2), sample/HTTP (3),
usable UI (4), independent proof (5). Review Focus tests belong to Tasks 1/3/4.
Builder check clarified global click canonicalization before thirty-day filtering.
Public shared interfaces consume the same release/columns/filter contracts.
Continue inline using backlog defaults; user already requested implementation.
