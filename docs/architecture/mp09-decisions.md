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
