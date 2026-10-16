# MP-09 — Operations, quality and privacy

The app is at `http://localhost:43100` in the retained proof project; API48000,
Grafana43001, Prometheus49090. Standard Compose defaults remain web3100/API8000.
Set `APP_PUBLIC_ORIGIN` to the exact browser scheme/host/port. `localhost` and
`127.0.0.1` are different origins; no trailing slash. HTTPS deployments must set
`APP_COOKIE_SECURE=true`. This story verifies a loopback local demo, not Internet
hardening or SSO for operator tools.

## Prerequisites and start

Complete [batch serving](batch-serving.md), [campaign operations](campaign-operations.md),
[funnel/attribution](funnel-attribution.md) and [customer lifecycle](customer-lifecycle.md)
first. Reuse existing volumes and published data; no replay/reseed is necessary.
Secrets are explicit setup, never automatic startup accounts:

```bash
PYTHONPATH=src .venv/bin/python -m marketpulse_app.ops_setup
export APP_PUBLIC_ORIGIN=http://localhost:43100
export API_HOST_PORT=48000 WEB_HOST_PORT=43100
compose=(docker compose -p marketpulse-mp05-proof --profile '*')
"${compose[@]}" build api web
"${compose[@]}" start --wait app-postgres batch-postgres clickhouse kafka minio
"${compose[@]}" up -d --no-deps api web collector prometheus grafana
```

On first setup only, create real application accounts (not source customers):

```bash
"${compose[@]}" exec -T api python -m marketpulse_app.auth_api create mp09-admin --role admin < .secrets/admin_password
"${compose[@]}" exec -T api python -m marketpulse_app.auth_api create mp09-operator --role operator < .secrets/operator_password
"${compose[@]}" exec -T api python -m marketpulse_app.auth_api create mp09-viewer --role viewer < .secrets/viewer_password
```

Retrieve credentials privately from those local files. Do not paste passwords into
command arguments, chat, logs or screenshots. Grafana uses username `operator`
and `.secrets/grafana_password`, independently of application accounts. Setup
preserves existing secrets and refuses symlinks. `.secrets/` is0700; account leaves
0600, mounted service leaves0444 behind that parent. Docker context excludes them.
For deliberate password rotation use `auth_api password <username>` with stdin;
all affected sessions are revoked. Repeating account creation fails rather than
silently resetting a credential. There is no auth-disable environment switch.

## Demonstrate business trust

1. Sign in as viewer: aggregate campaign/funnel/attribution/cohorts work, journey
   and profile reads/writes are denied by API and absent from viewer UI.
2. Operator can inspect governed detail and manage app configuration/segments;
   admin alone can enter Operations and manage roles/enable flags. Role changes
   revoke sessions; optimistic versions prevent stale saves and the last enabled
   administrator is protected under concurrent edits.
3. Check live quality. Inspect full seven-relation checksums, source snapshot
   lineage and finance reconciliation against the verified governed projection.
4. Run isolated corruption probe. It changes gross/net by one minor unit in one
   disposable copied performance row. Counts/keys remain the same, checksums and
   finance differ. Inspect immutable report, exact expected/observed checksums and
   financial totals, and Grafana/Prometheus `ControlledQualityProbeFailed` alert.
   Acknowledge it: alert clears, report persists, live quality remains passed.
5. Inspect bounded access audit, retention preview and expired-session cleanup.
   Cleanup removes≤1000 expired sessions only. Users, active sessions, audit,
   quality, rules/segment revisions, all source snapshots/releases/projections
   remain protected. Analytical deletion is disabled.

Audit is authorization decision plus best-effort HTTP outcome, not inferred commit
history. If decision evidence cannot persist, protected work fails503 before
execution. If outcome append fails after work, response remains truthful and
`mp_audit_outcome_failures_total` increments. Tokens/passwords/raw URLs/query/body
and customer references never become metric labels or audit metadata. Uvicorn raw
access logging is disabled.

## Telemetry semantics and limits

Prometheus3.14.0 and Grafana13.2.1-ubuntu are pinned by inspected manifest digests
in Compose. Collector reuses the app image. Prometheus scrapes5s/timeout4s and
rules evaluate5s; retention7days. Each new service has a512MiB memory limit. Proof
Docker VM:15CPUs,16,747,331,584bytes memory. No Docker socket/host privilege.

The provisioned dashboard is `/d/marketpulse-operations`. API requests/histogram
and process CPU/RSS are measured; ClickHouse memory is its tracked memory, not
host usage. Kafka graphs show offset advance, including transaction/control
records; committed lag also uses offset units. MP02 controlled validation-case
DLQ and Flink validation DLQ are separate. Flink `invalid` counter rate measures
that operator's invalid events, not a cross-stage failure share. No denominator
is invented. Existing Flink REST supplies counters/checkpoints/JVM when present;
this verification deliberately leaves Flink/Dagster stopped and shows unavailable,
without resubmitting or replaying jobs. Historical fixture business freshness can
legitimately alert. Arrival, scrape, observation time and business-event time are
separate. Missing sources clear their cached samples; other sources continue.

Quality runs≤once/60s in collector or explicitly by admin. Checks read persisted
certification/dbt artifacts and full current rowsets. Missing evidence/dependency
is unavailable; changed evidence is failed. Probe bound is5000 total certified
rows. Only its freshly created exact `mp09_probe_<uuid>` database can be dropped.
The app/collector mount batch evidence read-only and access the operational ledger;
no source/Gold rows are stored in application PostgreSQL.

## Verify and benchmark

```bash
PYTHONPATH=src .venv/bin/python scripts/operations-smoke-test.py \
  --project marketpulse-mp05-proof --username mp09-admin \
  --password-file .secrets/admin_password --origin http://localhost:43100 \
  --roles-directory .secrets --restart --output .lakehouse/mp09-proof/final-acceptance.json
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests
npm --prefix web run typecheck
npm --prefix web test
npm --prefix web run build
PYTHONPATH=src .venv/bin/python -m marketpulse_mock validate \
  --input data/mock/fixtures/seed-360 --json
"${compose[@]}" config --quiet
"${compose[@]}" run --rm --no-deps --entrypoint promtool prometheus \
  check config /etc/prometheus/prometheus.yml
```

Old MP06–08 HTTP proof clients now require `--username`, `--password-file` and
`--origin`; they use CookieJar and prohibit redirects/unknown destinations. Use
operator/admin as their existing operational-write cases require. Their analytical
oracles and source boundaries remain unchanged. Do not rerun those proofs merely
to start MP09. Evidence belongs in ignored `.lakehouse/mp09-proof/`.

First measured restart proof (20warmups/100samples each): dashboard p95=60.463ms,
cohorts44.219ms, ops/status24.058ms; full manual quality133.752ms. Probe alert fired
and cleared, history persisted, all24 direct/proxy role checks passed. Five
security tests ran against dedicated `mp09_security_test` PostgreSQL, including
concurrent admin demotions, persistent throttle, expiry, bounded retention,
revocation and immutable audit. Full Python395tests (40opt-in skipped),27 browser
tests, typecheck/build, fixture validation and promtool passed before final review.
Later review fixes and final measured evidence are recorded in MP09 decisions.

Active identity remains `31ffb7c713c694b9082b0f1daad7a32d50ca76e9b842fcb988029d3da4fdef06`;
proof `564bf33845a443ac6ea4b159ba2d8d2172944d3bd5e82a22cdb74c1032c3c356`.
Gross27,160,000 − refund4,967,000 = net22,193,000VND, all three attribution models
unchanged. Snapshot/projection checksums are in ignored evidence and lineage.

## Recovery and later retention

Restart only failed app/monitoring services, inspect source status and observation
age, then check live quality. Preserve evidence before intervention. Use existing
[batch serving recovery](batch-serving.md) for receipt-pinned rebuild/backfill and
[Flink runbook](flink-local.md) for deterministic replay/checkpoint recovery;
these are separate operator actions, never triggered by opening Operations.

Before any future source retention change, inventory active/pinned releases,
sidecars, Iceberg snapshots, export/dbt proof objects and ledger mappings, take and
validate backups, and prepare an explicit deletion set for review. No broad volume
reset or analytical deletion command is part of this runbook.
