# MP-09 — Quality, observability, privacy and operations

**Status:** Implemented, Docker-verified and final review fixes verified.
Known deferred minor details are in `docs/architecture/mp09-decisions.md`.
**Base:** complete MP-08 `c25d1b3`; same checkout, new branch
`feat/mp09-quality-observability-privacy`. Preserve the existing untracked `work/`.
Scope follows the user's standing backlog delegation and native execution.

## Outcome and boundaries

An operator can sign in, see trustworthy platform health/quality/lineage and
Grafana telemetry, investigate a failed reconciliation probe, inspect access audit
and apply safe operational retention. Protect the existing MP06–08 application
with PostgreSQL-backed accounts/sessions/roles. Keep ClickHouse analytical and
PostgreSQL operational. No source reseed/replay, earlier pipeline redesign,
external notifications, mandatory cloud/SSO, Internet-facing deployment claim or
analytical/source deletion in this story.

New mandatory local runtime is an `observability` Compose profile: a bounded
Python collector, Prometheus `v3.14.0`, Grafana `13.2.1-ubuntu`. Pin verified image
manifests/digests after Docker inspection. Reuse existing Python app dependencies;
stdlib hashing/session/HTTP/metrics primitives, no new service/library required.
App/tools use existing pins; no Kafka/Flink engine upgrade or JMX injection.

## Authentication, roles and sessions

Runtime fails closed: no auth-disabled environment switch, default account or
hardcoded credential. `create_app` accepts an explicit injectable access provider
for tests; missing provider denies application access. Runtime always installs
PostgreSQL-backed access. Existing API tests explicitly inject a trusted test
principal; browser regression mocks explicitly provide an authenticated session.
These test seams are not a deployment bypass.

App users are operator-created records in PostgreSQL, separate from source
customers. Explicit CLI creates a canonical username (lowercase ASCII3..64,
letters/digits/underscore/hyphen/dot), password12..128 characters read from stdin,
and role viewer/operator/admin. Store only salted scrypt hashes: n32768/r8/p1,
dklen32, random salt16bytes, maxmem64MiB; compare constant-time. Never print
passwords/tokens/connection strings. Unknown/disabled users get the same login
error and a dummy hash computation. Login throttles each normalized username's
hash at five failures/15minutes, persisted across API restart.

POST `/api/v1/auth/login` accepts exactly username/password. It issues a random
32-byte opaque session token in an HttpOnly SameSite=Strict cookie `mp_session`;
only SHA256(token) and user/expiry metadata are stored. Eight-hour absolute TTL,
no sliding expiry. Cookie Secure is configurable for HTTPS; local loopback HTTP
demo defaults false. No token in JSON/localStorage/URL. GET `/auth/session`
returns safe user ID/username/role or401. POST `/auth/logout` revokes the session
and expires the cookie. Disabled users/role edits take effect on the next request.

All POST/PUT/DELETE browser/API actions require Origin exactly equal to configured
`APP_PUBLIC_ORIGIN` (scheme/host/port); reject missing/null/foreign origin403.
SameSite alone is not a substitute. Web proxy forwards only Cookie, Origin and
Content-Type, relays Set-Cookie, blocks redirects/arbitrary routes/upstream input.
No CORS wildcard. Health live/ready remain unauthenticated minimal status only.
API docs/OpenAPI are admin-only. `/metrics` uses a separate constant-time Bearer
scrape secret, not a human session, and returns no business/profile identifiers.

Role matrix:

| Role | Allowed |
| --- | --- |
| viewer | Certified/provisional aggregate dashboards, funnel/attribution/cohorts, alerts/settings/pacing reads and read-only budget simulation |
| operator | Viewer plus journey/profile/timeline/segment reads, segment preview and normal budget/rule/segment writes/evaluation |
| admin | Operator plus ops health/quality/probe/lineage/audit/retention and account role/enable management |

Unknown application routes/methods are denied by default. Unauthorized401 and
insufficient-role403 occur before analytical reads/writes. UI sign-in/session gate
protects all existing pages, displays the role and logout, and handles expiry.
Viewer sees aggregate workspaces without enabled operational/detail actions;
server checks remain authoritative. Account creation/password changes are explicit
CLI operations, not public registration. Admin user-list/update endpoints use
optimistic versions and serialize role changes, preventing removal of the last
enabled administrator. Password reset and role/disable revoke affected sessions.

## Access audit and secret handling

Append-only PostgreSQL audit events record server UUID request ID/time, optional
app user ID, fixed role/capability/route template, decision and optional status.
No raw path/query/body, customer/profile references, username input, credentials,
headers or token hashes in audit/metrics/logs. Authorization decision is appended
before protected work; if that append fails, fail503 before side effects. Outcome
is a separate best-effort event; expose its failure counter instead of claiming a
write was rolled back. Read UI as attempts/decisions/outcomes, not inferred commit
history. Triggers prevent UPDATE/DELETE of audit and quality evidence.

Uvicorn access logging is disabled to avoid raw query logging. Structured logs
only contain allowlisted route/decision/request-ID/status metadata. Neither
exception strings nor arbitrary user values become labels. Metrics labels have
fixed route/status/method/service/topic/group/kind enums, never user IDs or URLs.

Explicit setup creates only missing secrets under ignored `.secrets/`, directory
0700. Host-only account-password files0600; service secret leaves0444 behind the
0700 parent so non-root mounted containers can read only their supplied secret.
Existing secrets/config are never overwritten; reject symlink targets. Exclude
`.secrets/` from Git and Docker build contexts. Metrics token and Grafana admin
password use Compose secrets; no plaintext in committed YAML. Grafana anonymous
access/signup disabled. Its own operator login is separate from app roles;
Prometheus/Flink/Dagster loopback tools are operator host surfaces, not an SSO
security boundary. App role enforcement covers both direct API and web proxy.

## Quality checks, lineage and safe corruption demonstration

A shared quality service resolves the active publication, ledger receipt and
certified artifact bundle from the existing read-only batch-state mount. Verify
release/proof/manifest binding and certified full typed rowsets/schema/count/key
invariants for five Gold/two reference relations. Compare source finance and all
three attribution models; equal row counts alone cannot establish correctness.
A report is passed/failed/unavailable, scoped live/probe, tied to release/proof
and source snapshots, with counts/checksums/reason codes only. Never copy business
rows into PostgreSQL. Missing dependencies/evidence are unavailable, not pass.

Collector performs live checks at most every60s; admin can trigger POST
`/ops/quality/check`. Store immutable reports and expose latest status/check time
as metrics. GET `/ops/quality` shows recent bounded reports. GET `/ops/lineage`
shows validated model/export digests, dbt invocation, receipt snapshots and exact
MP07 projection binding; no payload/customer identifiers or secret-bearing URLs.

POST `/ops/quality/probe` is admin-only, explicit and fixture-limited: max5000 total
certified rows. Create a fresh owned `mp09_probe_<uuid32>` ClickHouse database,
copy the certified relations, prove baseline equality, then alter gross/net by one
minor unit in a single copied finance row without changing row count or keys.
Check against the original certified expectations; record failed probe and exact
reconciliation/checksum evidence. Never modify/promote the active release or
journal. Finally drop only that exact newly-owned probe database. An active probe
flag drives a separate Prometheus demo alert, clearly distinguished from live
quality failure. POST `/ops/quality/probe/clear` acknowledges the demo condition;
it does not delete the immutable failed report. No external notification delivery.

## Observability and metric semantics

Prometheus scrapes API and collector every5s, timeout4s, evaluates alert rules every5s,
retains7days. Collector polls bounded targets in its background thread and serves
cached snapshots so scrape requests do not block on Kafka/Flink/quality. Include
observation times and per-source success; omit unavailable values instead of
inventing zero. Source failure clears stale samples or marks their age/availability.
JSON telemetry values are strings; Prometheus values are approximate operational
doubles, never certified financial results.

Required dashboard panels: API request rate and latency histogram/p95, Kafka log
end/offset advancement by fixed input/output topics, committed consumer lag for existing
`marketpulse-campaign-serving-v1` and `marketpulse-flink-v1`, Flink invalid-event rate and separate DLQ offset advancement,
Flink job/counters/checkpoint availability, business-event freshness separately
from arrival/scrape time, quality status/age, API/collector RSS+CPU, ClickHouse
tracked memory and Flink JVM heap/CPU where available. Missing offsets/job/target
are unknown/down, not zero lag or healthy. Historical fixture freshness is labeled
historical and may legitimately alert. No host/container-wide CPU claim is made
from process/JVM metrics; no Docker socket/privileged host exporter is mounted.

Read Flink2.2 metrics via its existing REST API; do not resubmit a job or replay
sources for telemetry. A stopped/missing Flink job remains visibly down. Kafka
metric consumers use no subscription, commits or publication. Fixed topic/group
allowlists bound cardinality. Collector also checks dependency reachability with
explicit HTTP/TCP semantics. Failure in one source must not erase other sources.

Provision Grafana datasource/dashboard as code. Alert rules cover target failure,
consumer lag, stale source/quality evidence, API errors/latency, live reconciliation
failure and the separate active failed probe. Kafka offset advancement includes
transaction/control records and is not business-event throughput. MP02 controlled
validation-case DLQ and Flink validation DLQ are labeled separately. Flink
operator counters supply stage-specific event rates; no cross-stage DLQ share is
reported without an exact matching denominator.
Grafana shows No data/Unavailable when evidence is missing. No invented samples.
Prometheus/Grafana/collector add only their own named volumes/services; ports bind
loopback (default49090/43001; collector internal only).

## Operations UI, retention and runbooks

`/operations` is admin-only and shows source status, links to Grafana/Prometheus,
quality reports/probe actions, validated lineage, bounded audit history, account
role/enable controls and retention policy/preview. Obsolete request/release/action
responses cannot replace current UI state. Actions show authoritative errors;
no implied fix on an unavailable dependency. Existing page navigation/login works
on desktop/mobile/keyboard, and expired sessions return to sign-in.

Retention controls are intentionally safe: eight-hour sessions plus admin-only
POST `/ops/retention/sessions` removes at most1000 expired sessions per call,
never active sessions/users/audit/revisions. GET `/ops/retention` returns policy,
expired count and protected publication/sidecar counts. All certified releases,
source objects/snapshots, active/pinned projections, immutable audit/quality/rule
history and earlier pipeline volumes stay protected; analytical deletion is
explicitly disabled. Prometheus7day expiration is its own time-series policy.
No destructive cleanup/reset of user data is required for acceptance. Runbooks
prepare concrete inventories/backups before any later approved source retention
change, rather than presenting unreviewed broad delete/reset commands.

Provide a unified demo-start/recovery guide linking existing deterministic replay,
Dagster backfill, checkpoint recovery and fixture validation, with correct Compose
profiles/ports and authenticated smoke-test migration. Authentication intentionally
changes old unauthenticated runtime behavior; existing pure analytics/business
semantics remain unchanged. Update prior proof clients to accept credentials or
provide a shared authenticated transport; do not leave prior proofs unusable.

## Acceptance

Spec/plan committed before code; fresh builder check, TDD contracts/security/
quality/collector/HTTP/UI and full Python/MP06–08 browser regressions. Real dedicated
PostgreSQL auth/lockout/session/revocation/audit/retention tests; actual CH same-count
corruption probe, exact hashes/financial mismatch and unchanged active data.
Authenticated direct API/web role matrix, CSRF, wrong/expired sessions, no secrets
in telemetry/log/audit, collector partial outage/no fake-zero cases.

Build only changed app/collector/new monitoring images. On retained proof project,
Prometheus targets scrape and provisioned Grafana panels use actual data; probe
alert fires then clears after acknowledgement, history remains. Inspect actual
app/login/admin and Grafana desktop/mobile UI. Benchmark authenticated requests
at fixture scale (20warmups/100samples, p95<=500ms for representative aggregate,
lifecycle and ops status; manual full quality check reported separately). Restart
API/web/monitoring verifies roles/session policy/audit/config and MP06–08 certified
results unchanged. No source replay/reseed or broad volume cleanup.

Document versions/resources/limits, source fixture validation, Compose/promtool/
Grafana provisioning checks, diff/Git state; ignored `.lakehouse/mp09-proof/`
evidence and a fresh whole-branch review/fix pass. Existing work/ remains untouched.

## Primary implementation references

Pinned tools follow [Prometheus download](https://prometheus.io/download/) and
[Grafana Docker distribution](https://grafana.com/grafana/download?edition=oss&platform=docker).
Configuration uses [Prometheus scrape/authorization configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)
and [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/).
Existing engine metrics use [Flink2.2 REST metrics](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/metrics/).
Password derivation uses [Python hashlib.scrypt](https://docs.python.org/3.12/library/hashlib.html#hashlib.scrypt).
