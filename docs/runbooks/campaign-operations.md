# MP-06 local campaign operations

MP-06 builds on the already verified MP-03 and MP-05 runtime. It never seeds source
events or silently runs a lakehouse/batch refresh. The Command Center displays
certified finance and provisional streaming activity separately.

## Build and start

Prerequisites: Docker Compose v2, a published MP-05 fixture release, Kafka with
the existing MP-03 Gold campaign topic, and the existing ignored `.env` credentials.
For a fresh environment first follow [batch-serving.md](batch-serving.md).

Initialize only a missing app password, build, then start the new app services:

```bash
PYTHONPATH=src python3 -m marketpulse_app.local_env --path .env
docker compose --profile app --profile serving --profile streaming build api web
docker compose --profile app --profile serving --profile streaming \
  up -d --wait app-postgres api campaign-worker web
```

Default web: http://localhost:3100, API/docs: http://localhost:8000/docs.
Dagster retains its port 3000. App PostgreSQL has no host port and uses its own
`app_metadata` volume. Startup creates schema only; budgets, market assignments,
rules and accounts are never generated. Health readiness checks access to app
PostgreSQL and ClickHouse; an unpublished release remains an explicit 409 on
analytical routes, and unavailable streaming is a dashboard warning.

Pins: Python 3.12.13, FastAPI 0.143.0, Uvicorn 0.54.0, Pydantic 2.14.1,
clickhouse-connect 1.10.0, psycopg 3.3.4, confluent-kafka 2.16.0,
Node 24.21.0, Next.js 16.4.0, React 19.3.0. Python dependencies are hash locked in
`docker/app/requirements.lock`; npm dependencies in `web/package-lock.json`.
Nonroot API/worker/web limits are 512 MiB each; app PostgreSQL is 256 MiB.

## Existing developer proof project

The source tree remains the single checkout at `/Users/huyle/projects/marketpulse-360`.
The proof adds only MP-06 services to `marketpulse-mp05-proof`, without recreating
MP-01–05 services or changing their volumes/ports. Build/start commands below use
`--no-deps` because Kafka and ClickHouse are already healthy there:

```bash
export API_HOST_PORT=48000 WEB_HOST_PORT=43100
docker compose -p marketpulse-mp05-proof --profile app --profile serving \
  --profile streaming build api web
docker compose -p marketpulse-mp05-proof --profile app --profile serving \
  --profile streaming up -d --no-deps --wait app-postgres api campaign-worker web
```

Web: http://localhost:43100. API: http://localhost:48000/api/v1/dashboard.
API docs: http://localhost:48000/docs. ClickHouse still uses 48123, Dagster 43000;
Kafka still uses 49192. When operating/recreating any MP-05 source/batch service,
preserve all the port exports from its runbook. Do not use plain default-project
`up` to operate this named proof stack.

## Four decision tasks

1. Open the Command Center. Its initial UTC interval comes from real published
   dates (`2026-01-01` to exclusive `2026-01-03` for this fixture). Filter by
   platform/campaign/objective and inspect certified spend/net ROAS. Expand the
   source/freshness panel to see Gold model, release identity and actual data day.
   Streaming activity shows its own event/processing/arrival times and unmapped
   lifecycle campaigns; historical source events remain historical after replay.
2. Select **Meta Traffic 2** (`b00db876-5a1b-55c9-8412-9441821fe3ef`). Assign
   market `VN`, period January 1–3 (exclusive), budget `1` minor VND unit. Save.
   This is a **Configured demo budget**, deliberately low; MP-01 did not inject
   an abnormal spend source. Configured market means this app assignment, not
   customer geography. Concurrent edits return version_conflict; reload before
   retrying so you can inspect the newer values.
3. Create a refund-share rule with threshold `0`, and a period-budget-overrun rule.
   Evaluate or wait for the worker's 5-second evaluation. Inspect exact operands,
   dates and release/rule/setting versions. This campaign's certified January 2
   refunds support the MP-01-derived alert. Repeated evaluation of unchanged
   versions/release/window creates no extra occurrence. Spend-spike requires a
   real preceding calendar day's nonzero baseline; missing data never fires it.
4. View pacing: this two-day period contains `91,461` spend; full-period forecast
   is the same amount. Use the scenario interval above, VND total `1000`, allocate
   `1000` to Meta Traffic 2 and zero to other campaigns. Expected net is `35,282`
   under its fixed historical net ROAS. The UI labels this a linear scenario,
   not a prediction, and leaves saved budgets unchanged. A changed scenario input
   discards its previous/pending result. Missing baseline yields unavailable.

Pacing uses contiguous certified closed dates, not intraday spend inferred from
daily reports. A gap followed by a later row suppresses projection; a missing tail
permits only an explicitly incomplete as-of projection. No rows/zero elapsed days
returns insufficient_data. Negative remaining budget/net forecasts are retained.
Money is never converted across currencies or rounded through JavaScript Number.

## Executable acceptance and restart

The proof is explicitly for the selected test-owned project. It creates/updates
the demo setting/rules above by HTTP; restarting only its four app services is
part of the test. Reruns save a new setting version and therefore may create new
version-bound occurrences; repeated evaluation *within* a version remains unique.
It does not reset app metadata. Its fault test resets only the literal
`mp06_live_test` ClickHouse database, and consumes with a separate Kafka group.

```bash
python3 scripts/app-smoke-test.py --project marketpulse-mp05-proof \
  --api http://127.0.0.1:48000 --output .lakehouse/mp06-proof/acceptance.json
```

The proof independently reads committed Kafka payloads, selects latest offsets,
compares every payload field/source position with HTTP results, injects lost
offset acknowledgement into the test projection and verifies unchanged first
receipt time on retry. It checks exact MP-05 totals, stale edits, filters, alerts,
pacing and simulation, then measures HTTP latency (20 warmups/100 uncached serial
requests per workload). It restarts app PostgreSQL/API/worker/web and compares
settings, rules, alert history, live payloads/receipt times and certified identity.

Observed initial proof: 22 source files/615 rows, 14 live keys; all four services
healthy; independent Kafka projection, lost-ack retry, restart persistence and
all three HTTP benchmarks passed. Final fixture p95: dashboard 26.41 ms, pacing 16.92 ms,
simulation 20.32 ms, no failures. A test-owned adapter-to-Analytics response
measurement was 58.44 ms, excluding HTTP and separate from historical event age.
These are fixture/client-inclusive observations, not capacity or event-to-dashboard claims. Nine browser behavior tests,
TypeScript and production builds pass; actual desktop/mobile screenshots show
no JavaScript errors or page overflow. Final review's six Important findings and
three traceability/proof findings were reproduced and fixed with regressions;
333 Python tests succeed (27 opt-in skips), with all three new real integration
cases separately exercised. Final rebuilt images passed refreshed acceptance.
Inspect the regenerated JSON for
current measurements/resources. Evidence and screenshots stay in ignored
`.lakehouse/mp06-proof/`.

Host regressions:

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -e '.[app,app-test]'
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v
PYTHONPATH=src .venv/bin/python -m marketpulse_mock validate --input data/mock/fixtures/seed-360
cd web
npm ci
npx playwright install chromium
npm test
npm run typecheck
npm run build
```

Opt-in PostgreSQL integrations require an explicitly dedicated database named
`mp06_test` (`MP06_POSTGRES_TEST=1`, secret `MP06_TEST_DSN` in local environment).
Only its test-owned `marketpulse_app` schema is reset. Never point them at the app
database. Live projection integrations use `MP06_CLICKHOUSE_TEST=1` and only
`mp06_live_test`; normal schema initialization does not delete data.

## Failure handling and stop

Missing credentials fail startup without echoing their values. Worker invalid
snapshot/source conflict stops without committing past the record; investigate
its contract/key/source identity before retrying. Recreated cluster/topic IDs
must not be mixed with persisted offsets. Explicit source reset/retention,
production authentication/RBAC/notifications and representative capacity remain
future work. The HTTP proxy only forwards fixed app routes; backend credentials
are never browser variables. No raw source payload/PII is returned in app errors.

```bash
docker compose -p marketpulse-mp05-proof --profile app --profile serving \
  --profile streaming stop web campaign-worker api app-postgres
```

Do not use `down -v`; existing app/source data remains available for the next demo.
