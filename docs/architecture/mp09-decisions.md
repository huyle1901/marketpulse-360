# MP-09 decisions

Spec: [quality, observability and privacy](../../specs/09-quality-observability-privacy.md).

Authentication is an application operational responsibility in PostgreSQL. The
API uses fixed capabilities, server-resolved roles and opaque eight-hour sessions;
Origin checks protect unsafe cookie requests. Every protected decision is written
before work. Outcome audit failure is observable and does not imply rollback.

Quality resolves existing persisted certification and receipt binding, checks full
typed rowsets and keys, then reconciles finance across the verified MP07 source
projection, campaign performance and all three attribution models. Missing proof
is unavailable; mismatched proof is failed. The probe creates and drops only a
fresh owned database and never publishes. Its alarm is separate from live quality.

Retention is session-only: at most1000 expired rows. Immutable history, all source
snapshots, certified releases and projection databases remain protected. Kafka
offset advancement includes control records; it is never labeled event count.

Collector polls run independently and scrapes read cached samples only. A failed
source clears its samples while preserving other source results; source age is
explicit. API metrics use server-defined templates and process CPU/RSS; no host
CPU inference or identifiers in labels. Kafka probes never subscribe/commit.

The observability profile adds collector, Prometheus and Grafana only. The API
and collector mount persisted batch evidence read-only. Compose secrets expose
only supplied leaf files; ignored host secret directory is0700. Grafana uses
its own operator login; app RBAC does not protect local operator tool ports.

Next.js forwards Cookie/Origin and relays Set-Cookie through an explicit route
allowlist. A global session gate handles expiry; viewers retain aggregate analysis
and cohorts while journey/profile/segment fetches require operator access. The
admin operations workspace guards stale refreshes and displays dependency errors.

## Docker verification before final review

Retained project `marketpulse-mp05-proof`: two Prometheus targets healthy,18 Grafana
panels provisioned with valid PromQL (15 have actual series; missing Flink metrics
remain unavailable). Login, admin Operations and Grafana inspected visually;
390px app viewport has no horizontal overflow.24 direct/proxy role checks,
anonymous/Origin/metrics enforcement and query/secret redaction passed.

Final pre-review proof:20warmups/100samples p95 dashboard35.573ms,
cohorts27.547ms, ops/status19.949ms; full quality152.937ms. Exact observed probe
checksum and gross/net+1 differ with identical row count. Alert fires/clears and
failed history persists. Restart preserves accounts/roles, active session, audit
marker, MP06–08 configuration/segments, release/proof and source-finance totals.
Evidence is ignored `.lakehouse/mp09-proof/final-acceptance.json`.

Five dedicated PG security integrations passed, including concurrent last-admin
protection and expired-only1000-row cleanup. Python395tests (40opt-in skipped),
27 browser tests, typecheck/build,615row/22file fixture validation, Compose quiet
validation and promtool9-rule verification passed. No earlier source replay,
publication, reseed or analytical deletion occurred. No credentials/artifacts or
pre-existing `work/` are staged. Tests remain explicit trusted-principal injection;
there is no runtime bypass.

## Final review and fix pass

Fresh independent review found no Critical and four Important issues. Each was
reproduced by a failing regression before its fix: session expiry invalidates an
auth generation and overlapping checks obey latest-request order; collector
validates sample shape/names/labels/finite values/unique identities before cache
acceptance; Flink JVM metrics aggregate total heap and mean available CPU across
up to four task managers, avoiding duplicate series; quality reports retain pinned
expected identity/proof and receipt snapshots before validation, then mark the
binding verified only after every check passes. Probe reuses the pinned session.

The collector reports unavailable when more than four task managers exceed its
explicit poll bound; missing manager measurements do not become partial totals.
Fixed gauges use kinds `heap_bytes_sum` and `cpu_load_mean`, not host CPU usage.

Deferred minors: a missing certification file is currently classified failed
rather than unavailable (still fail-closed, potentially a reconciliation alarm);
Operations monitoring links assume default43001/49090 and require direct
navigation for customized ports. These do not weaken authorization or mutate
certified data. Internet hardening and shared operator-tool SSO were reviewed as
out of scope under the explicit loopback-demo boundary; public deployment needs
its own security review. Same checkout was retained as explicitly requested;
branches continue to share untracked files, with user work/ preserved.

Post-review verification:398 Python tests green(40 opt-in skipped),29 browser tests,
typecheck/production build, changed Docker image builds, source validation and
Compose/promtool checks pass. Repeated retained-stack acceptance with roles,
probe alert fire/clear and restart passes. Final authenticated p95(20warmups/
100samples):dashboard39.857ms,cohorts27.398ms,ops/status22.487ms;full quality114.597ms.
`post-review-acceptance.json` preserves exact release/proof/snapshot/projection and
finance evidence. No Critical/Important review findings remain open. Two Minor
findings above are deferred explicitly. App and monitoring remain running;
branch is retained locally without merge, push or PR.

## GitGuardian PR review

PRs6–8 pass GitGuardian Security Checks. PR9 initially reports four Generic
Password occurrences in the original `ops_setup.py` filename-to-mode mapping.
The flagged line assigns filesystem modes0444/0600, not credentials. Comparing
ten current local credential values against422 unique committed blobs found no
matches; actual values remain in ignored local files. No credential values are
included in this record.

Setup now groups service/account filenames and selects permission bits separately,
retaining filenames, permissions, random generation, idempotence and symlink
refusal. Existing filesystem tests and the398-test Python suite pass(40 opt-in
skipped). Published history is preserved and alerts are not ignored; the new head
must be checked on GitHub before claiming scanner success.
