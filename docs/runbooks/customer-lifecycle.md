# Customer 360 and lifecycle workspace

Prerequisites: certified MP-05 release, the [MP-07 journey projection](funnel-attribution.md),
and healthy app PostgreSQL/ClickHouse. Reuse the existing project/volumes; do not
regenerate sources or replay earlier pipelines. Every new certified release needs
its own explicit MP-07 detail publication before profile/segment analysis.

## Start the updated app

For the retained proof project, start Docker if needed, then use its existing
storage/services and saved port bindings. No new MP-08 service is required.

```sh
docker compose -p marketpulse-mp05-proof --profile '*' start --wait clickhouse app-postgres batch-postgres minio kafka
docker compose -p marketpulse-mp05-proof --profile app --profile serving --profile streaming build api web
API_HOST_PORT=48000 WEB_HOST_PORT=43100 docker compose -p marketpulse-mp05-proof --profile app --profile serving --profile streaming up -d --no-deps --wait api web
```

API startup applies additive segment migrations under an advisory lock. It creates
no demo segments or analytical members. Worker/earlier pipelines need no change.
Keep app operational credentials in ignored local configuration.

## Use `/customers`

Open <http://127.0.0.1:43100/customers>, or Customer360 navigation from Operations/
Analysis. The header distinguishes historical governed analytics from activation;
shows data-bound as_of/currency and historical freshness. Inspect source details
for release/proof and Silver/checksum. Polling never updates underlying data age.

Click a cohort activity cell to select its exact cohort/type/activity-month
membership. Retention uses certified cell denominators; do not sum repeated cohort
size or interpret absent future cells as zero. Discovery filters include purpose,
profile type and explicit opportunity heuristic; pagination is stable.

RFM means recency since original purchase, original-order frequency and observed
net money after refunds. Null recency means no original purchase. All money is
exact minor units. High value requires orders≥1/net≥5,000,000; churn risk requires
orders≥1/recency≥30days; cart opportunity requires carts≥1/orders=0. These are
explainable rules, not predictions. Use custom segment thresholds where appropriate.

Inspect a profile to see categorical identity confidence, CRM consent/eligibility
reasons and a paginated governed timeline. Raw identifiers/contacts are hidden.
Latest certified analytics denial suppresses event detail. Unknown CRM/device/
session identity cannot qualify for activation. Confidence does not mean a person
was probabilistically verified. Consent is as of the certified snapshot.

Build a segment using purpose, optional cohort/activity/type and up to eight AND
predicates. Empty activation rules still enforce an identified CRM match and both
certified consent flags. Numeric values are canonical whole strings; only net may
be negative. Preview is read-only and returns an exact total plus at most20members;
the global exclusion count includes all profiles failing activation consent/identity.
No external delivery or member persistence occurs.

Save a named rule to get version1; edits require the latest version. Load a saved
segment, enter an older revision number and Load revision to inspect/preview it
read-only; Edit latest revision returns to editable head. Old revisions cannot be
changed, including direct database UPDATE/DELETE. Stale edit409 requires reloading.
A saved rule plus the same release/as_of produces the same evaluation identity;
a newer certified release may legitimately change membership.

## API and errors

GET `/api/v1/lifecycle/cohorts`, `/profiles`, `/profiles/{profile_ref}/timeline`,
`/segments`, `/segments/{id}/versions/{version}`; POST `/segments/preview` and
`/segments`; PUT `/segments/{id}`. Fixed same-origin proxy paths only; no arbitrary
SQL or upstream input. All responses no-store. Profile/timeline limits1..100,
offset0..100000; months are ISO first of month. Rule schema:

```json
{"schema_version":"mp08-segment-v1","purpose":"activation","cohort_month":null,"activity_month":null,"profile_type":null,"conditions":[{"field":"order_count","op":"gte","value":"1"}]}
```

Preview body `{"rule":<rule>}`; create/update additionally require name and strict
integer expected_version (0create, positiveupdate). Exact canonical digest is
order independent for conditions; unknown fields/operators/extra keys are rejected.
422invalid input;404unknown profile/segment revision;409stale version,
unsupported_currency, projection_mismatch or lifecycle_detail_unavailable;
503dependency failure. A missing sidecar returns visible unavailable discovery/
timeline and409preview; cohorts remain usable. Do not substitute zeros or bypass
consent/reconciliation. UI retains labeled previous results on failures and rejects
old draft/profile/release responses.

## Reproduce proof

Start the existing batch-code container for access to its immutable certified
artifact bundle, without starting/replaying a Dagster job:

```sh
docker compose -p marketpulse-mp05-proof --profile '*' start --wait batch-code
PYTHONPATH=src .venv/bin/python scripts/lifecycle-smoke-test.py \
  --project marketpulse-mp05-proof \
  --api http://127.0.0.1:48000 --web http://127.0.0.1:43100 \
  --output .lakehouse/mp08-proof/evidence.json --restart
```

The explicit test-owned proof validates receipt/certification/export object
bindings, compares independent profile/cohort/RFM/timeline/activation oracles,
creates and retains one named proof segment with two revisions, checks stale
writes and restarts only API/web. It verifies MP06–07 outputs/config remain and
benchmarks20warmups/100uncached samples/workload with fixture p95<=500ms. No source
mutation, reseed/replay or release promotion. The demonstration segment remains
operator data; cleanup is an explicit later decision.

Opt-in `MP08_CLICKHOUSE_TEST=1` runs real query and owned consent/refund/currency
fault cases with test credentials loaded through the existing local secret setup.
Those tests remove only newly owned `e…` candidate databases. PostgreSQL tests use
`MP08_POSTGRES_TEST=1`/`MP08_TEST_DSN` targeting **mp08_test only**, never the app DB;
create it once under the local app PostgreSQL role. API image already contains the
required driver and app code; a test can be sent to its Python stdin with the DSN
resolved internally, avoiding credentials in command arguments. Full Python/browser
checks are documented in the plan.

[Observed decisions/evidence](../architecture/mp08-decisions.md) record data bounds,
versions and resource measurements. Preserve earlier source/lakehouse/app volumes.
Production hardening and retention are separate MP-09 work.
