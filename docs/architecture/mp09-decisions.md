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
