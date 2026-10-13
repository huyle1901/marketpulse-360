# Funnel and attribution workspace

Prerequisites: the [batch serving runbook](batch-serving.md) has already published
a certified MP-05 release, and [Campaign Operations](campaign-operations.md)
provides healthy app PostgreSQL/API/web. This workflow extends that existing
project. Do not regenerate sources or restart earlier pipelines.

## Build and publish once per certified release

Use the same Compose project and port overrides as the existing deployment.
For the retained proof project:

```sh
docker compose -p marketpulse-mp05-proof --profile app-tools build journey-publish
docker compose -p marketpulse-mp05-proof --profile app-tools run --rm --no-deps journey-publish
docker compose -p marketpulse-mp05-proof --profile app build api web
API_HOST_PORT=48000 WEB_HOST_PORT=43100 docker compose -p marketpulse-mp05-proof --profile app up -d --no-deps --wait api web
```

The publisher resolves the active certified release, reads its recorded exact
Silver snapshot and emits row count/checksum/binding metadata. Retry verifies the
existing immutable copy and reports `reused:true`. Failed candidates do not
publish; retain them for diagnosis. Never point a journal at partial data or
modify a certified release. Publication is explicit, not a startup side effect.
After every new certified release, run the publisher again. Until then, aggregate
analysis works and journeys show an unavailable message for that release.

## Use the workspace

Open <http://127.0.0.1:43100/analysis>, or use the Funnel & Attribution navigation
from Campaign Operations. Select a UTC interval with exclusive end date, paid
channel, campaign, objective, operator-configured market or currency. Click a
channel then a campaign to drill; All channels resets dimension filters while
retaining dates. Choose a sampling model for representative journey selection.
The campaign filter is credited campaign, not the source order campaign.

Stage bars show reach relative to sessions. Sums across campaign/day buckets are
incidences; they are not global distinct audience counts or sequential drop-off.
Three comparison cards and tables show first/last/linear allocated finance and
orders. Global reconciliation always precedes dimension filters. Inspect source
and freshness details for identity/proof/day boundary; historical data is labeled.

Journeys show original purchase time, inclusive thirty-day paid-click context,
financial recognition inside the report interval and all model allocations.
Touches outside the selected dates are labeled. Refund-only periods can have
negative net credit. Missing detail, scan bounds and excessive contexts are visible;
examples are not population estimates. Money is displayed in exact minor units.
Polling does not update underlying event freshness.

## Read-only API and errors

GET `/api/v1/analysis/funnel`, `/attribution` and `/journeys` on API port 48000;
the web uses the fixed same-origin allowlisted proxy. All responses are no-store.
Shared filters: from_date/to_date (1..366 UTC days), platform (google/meta/tiktok),
campaign_id, objective, configured market and currency. Journeys additionally
accept model first_touch/last_touch/linear and limit 1..10 (default 5).

422: invalid filter/date/model/limit. 409 unsupported_currency: currency-less
attribution Gold cannot safely serve a mixed-currency release. 409
reconciliation_failed: certified source/model amounts differ or a model is missing;
do not bypass this guard. 409 projection_mismatch: conflicting journey binding or
invalid order context. 503 dependency_unavailable: inspect dependencies without
exposing credentials. Missing sidecar returns an explicit unavailable journey
status, leaving aggregate APIs usable. UI keeps labeled last-good results on
failure and rejects mixed-release refreshes.

## Reproduce the read-only proof

```sh
PYTHONPATH=src .venv/bin/python scripts/analysis-smoke-test.py \
  --project marketpulse-mp05-proof \
  --api http://127.0.0.1:48000 --web http://127.0.0.1:43100 \
  --output .lakehouse/mp07-proof/evidence.json --restart
```

This validates receipt/certification/export bindings and object checksums, compares
independent exports to HTTP and recomputed governed projection Gold rows, checks
privacy/drill contexts, benchmarks 20 warmups/100 uncached samples per route and
restarts only API/web. It asserts unchanged MP-06 finance/settings/rules/alerts and
stable journey IDs/projection. Explicit test-owned `marketpulse-*-proof` project
and ignored output are required. No source or operational writes are made.
Use `MP07_CLICKHOUSE_TEST=1` with local test credentials for the opt-in projection
fault tests; those clean up only their owned `c…` candidates and test journal.

Observed proof/limits: [MP-07 decisions](../architecture/mp07-decisions.md).
At fixture scale p95 is below 500 ms; bounded samples do not establish capacity.
Old sidecars remain available for pinned reads. Cleanup/retention is a separate
operator decision, never an automatic task side effect.
