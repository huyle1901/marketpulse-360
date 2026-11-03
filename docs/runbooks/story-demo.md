# Story demo: certified history plus September streaming

An isolated Compose project (`marketpulse-story-demo`) that opens the Command
Center on January 2025 – August 2026 certified history and then streams
September 2026. It never touches `marketpulse-mp05-proof` containers or volumes.
Design: [continuous demo](../superpowers/specs/2026-10-29-marketing-dashboard-and-continuous-demo-design.md),
[modern command center](../superpowers/specs/2026-10-29-modern-marketing-command-center-design.md).

Always load the project, ports and resource sizes first:

```bash
source scripts/story-demo-env.sh   # project name, 43110 web, 48110 API, 3 GiB Flink, 5g batch Spark, 4 GiB ClickHouse
dc=(docker compose --profile '*')
```

## Varied world (v2 project)

The varied story world (plans 1–4 of the data-variety work: 13 cities, four channels including
Shopee Ads, Vietnam-time rhythm, organic/direct/email visits, the email lifecycle) is built in a
second Compose project, `marketpulse-story-demo-v2`, on the v1 ports + 100 (web 43210, API 48210,
ClickHouse 48213, Dagster 43220, Flink 48285). Its generated world lives in
`data/generated/story-demo-v2`, and its batch stack requires all four ad reports
(`BATCH_REQUIRED_PLATFORMS=google_ads,meta_ads,tiktok_ads,shopee_ads`).

```bash
source scripts/story-demo-v2-env.sh
dc=(docker compose --profile '*')
```

Before building v2, stop the v1 project without removing its volumes (this 24 GB machine cannot
hold both): `source scripts/story-demo-env.sh && docker compose --profile '*' stop`. The steps
below apply to v2 with `story-demo-v2` paths; measured values for v2 are recorded in the evidence
sections once it is certified.

## One-time preparation (measured 2026-10-29)

1. Generate the world (seed 360, 638 days, ignored under `data/generated/`):
   `PYTHONPATH=src .venv/bin/python -m marketpulse_mock generate --profile story-demo --seed 360 --output data/generated/story-demo`.
2. Start streaming/lakehouse services, submit Flink and register contracts as in
   [batch-serving.md](batch-serving.md), then replay the history window only:
   `"${dc[@]}" run --rm --no-deps replay kafka replay --input /app/data/generated/story-demo --bootstrap-servers kafka:29092 --from-date 2025-01-01 --to-date 2026-09-01 --no-sleep --include-dlq`.
3. If the Flink TaskManager ever fails, resume from its retained checkpoint; never reset offsets:
   `FLINK_RESTORE_PATH=file:///opt/flink/checkpoints/<job-id>/chk-<n> "${dc[@]}" run --rm --no-deps flink-job-submit`.
   Observed: the fixture-sized 1 GiB TaskManager ran out of heap at ~76% of the history. A 3 GiB TaskManager resumed from `chk-26` and drained the remaining 59,441 records.
4. Archive history ad/CRM files month by month. A single 608-day window exceeds the Spark driver because ingest parses rows in Python; windows are insert-only and idempotent:
   `"${dc[@]}" run --rm --no-deps spark-lakehouse ingest-files --run-id story-history-2025-01 --input /opt/marketpulse/data/generated/story-demo --from-date 2025-01-01 --to-date 2025-02-01` (repeat per month). Observed: 20 windows in 392 s, 1,012 rows per day.
5. Certify and publish one release over everything pinned in Bronze:
   `"${dc[@]}" up -d --no-deps --wait clickhouse batch-postgres batch-code dagster-webserver dagster-daemon`, then
   `"${dc[@]}" exec -T dagster-webserver dagster job launch -w /opt/marketpulse/workspace.yaml -j marketing_refresh_job --tags '{"dagster/partition":"2026-08-31"}'`.
   Observed: run `7d10fda0` succeeded in 302 s.
6. Publish journey detail for that release: `"${dc[@]}" run --rm --no-deps journey-publish` (observed 20.6 s, 247,789 governed events).
7. Start the app, then create accounts from the existing ignored `.secrets/` files (passwords travel over stdin only):
   `"${dc[@]}" up -d --no-deps --wait app-postgres api web campaign-worker`, then
   `"${dc[@]}" exec -T api python -m marketpulse_app.auth_api create mp09-admin --role admin < .secrets/admin_password`.

## Evidence (v2 varied world, release `109a2372…`, measured 2026-11-03)

v1 evidence for the first world (releases `5fae6827…`, `14a6b9ad…`, `669e1c2b…`) is kept in
git history. Every figure below was re-measured on v2 (`scripts/story-demo-v2-env.sh`).

| Step | Measured |
| --- | --- |
| Generate + validate | 47.8 s + 8.9 s; 2,576,737 rows in 6,342 files; manifest `f59d95e5…`, scenario `vietnam-retail`, 2025-01-01 → 2026-09-30 |
| History replay | 1,202,050 events (640 producer DLQ) in 32 s, `--no-sleep`; Flink lag 0, Flink validation DLQ 0 |
| Archive | 20 monthly windows in 384 s, 159 files each (4 master + 31 × CRM, Google, Meta, TikTok, Shopee) |
| Certify 2026-08-31 | `marketing_refresh_job` run `2d216b87` succeeded in 575 s with `BATCH_REQUIRED_PLATFORMS=google_ads,meta_ads,tiktok_ads,shopee_ads` |
| Journey detail | 1,164,717 governed events in 86 s (scan streamed from 64 hash partitions) |

### Oracle (`scripts/story_demo_oracle.py`): 314/314 checks equal

```bash
set -a; eval "$(grep '^CLICKHOUSE_PASSWORD=' .env)"; set +a
PYTHONPATH=src .venv/bin/python scripts/story_demo_oracle.py --root data/generated/story-demo-v2 \
  --clickhouse localhost:48213 --through 2026-08-31
```

| Measure | Certified | Generator files |
| --- | --- | --- |
| Impressions / clicks / spend, 4 platforms × 20 months (240 checks) | 2,614,669 / 227,830 / ₫1,339,776,177 | equal to ground truth |
| Spend share | Google 29.7 % · Meta 27.8 % · TikTok 23.8 % · Shopee 18.7 % | equal |
| Orders, gross, refunds (20 months each) | 32,774 · ₫73,488,628,000 · ₫4,329,091,000 (net ₫69,159,537,000) | governed computation equal; raw 33,858 · ₫75,966,922,000 |
| Revenue by city (13 cities + anonymous) | HCMC 18.6 % · Ha Noi 17.4 % · Thu Dau Mot 4.7 % … Ha Long 1.7 % | governed computation equal |

The raw−governed gap is consent only: 1,084 orders whose order event withholds analytics
consent are not certified (Silver `CONSENT_DENIED`; bot exclusion applies to web/app events
only, and late events stay eligible).

Read from Silver through Spark (not served): `email_sent` by type × month 60/60 equal to the
files (newsletter 115,536 · promo 36,814 · welcome 1,483); page-view sessions by traffic source
paid 219,770 · organic 43,886 · direct 29,581 · email 4,795, equal to the files after consent and
bot governance. Non-paid visits are 26.3 % of governed sessions.

### Page evidence (API, independent query)

| Page | API | Independent |
| --- | --- | --- |
| Insights: order values | 32,774 orders, median ₫1,797,000, P90 ₫4,497,000 | exact over consented order files |
| Insights: loyalty | net ₫69,159,537,000 | governed net |
| Insights: activity (GMT+7) | 20:00–22:00 sessions = 12.5 × 02:00–04:00; busiest Saturday 21:00 | — |
| Insights: channels | web 198,995 sessions, 11.9 % conversion; app 99,037, 8.5 % | — |
| Products | sports ₫17,016,573,000 · beauty ₫16,743,570,000 · electronics ₫14,666,775,000 · home ₫12,900,301,000 · fashion ₫12,161,409,000 | consented order items × master category, equal |
| Double day 2025-11-11 | 6,971 impressions (neighbours 4,000–4,600); Shopee 31.1 % of spend (neighbours 17–19 %) | — |
| Email | sent 7,917 (Jan 2025) → 9,350 (Aug 2026); recipients 1,163 → 1,929, rising every month | Silver email types equal |
| Cohorts | 748 cohort × month rows, cohorts from 2023 to 2026 | — |

### Numbers the presentation must update

Orders 11,124 → 32,774; gross ₫25.1 bn → ₫73.5 bn; net ₫23.2 bn → ₫69.2 bn; spend
₫668.6 m → ₫1.34 bn across four channels (Shopee new); journey events 247,789 → 1,164,717;
cities 5 → 13 (HCMC and Ha Noi now lead); email sends now grow instead of falling.

## Continue September

```bash
"${dc[@]}" run -d --name marketpulse-story-demo-v2-continuation --no-deps replay kafka replay \
  --input /app/data/generated/story-demo-v2 --bootstrap-servers kafka:29092 \
  --from-date 2026-09-01 --to-date 2026-10-01 --resume-state /app/state/story-demo-v2-september.json \
  --init-state --speed 20 --include-dlq --batch-size 20
```

`--speed 20` advances one business day in about 72 minutes. Omit `--init-state` to resume
after a stop. The cursor lives in the project's own `replay_state` volume and refuses to
continue after an ambiguous acknowledgement or a changed world; never reuse the v1 cursor
(`story-demo-september.json`, a different world identity). September streams to Live activity
as provisional data; certified views remain at 2026-08-31.

## Live View (measured 2026-11-03, v2)

Start the projection with the app: `"${dc[@]}" up -d --no-deps --wait app-postgres api web campaign-worker live-events-worker`.
On first start it reads the clean topic from the earliest offset (1,195,178 events), then follows the replay.
After a Flink restart, resubmit from the latest retained checkpoint (`FLINK_RESTORE_PATH=…/chk-<n>`); never reset offsets.

Independent check at replay time 2026-09-01 02:54:48 UTC against the generator files (dedup by event id,
consented, bot sessions excluded for web/app only):

| Measure | Live View | Source files |
| --- | --- | --- |
| Visitors / sessions today | 39 / 39 | 39 / 39 |
| Orders, gross, refunds (VND) | 6, 11,661,000, 0 | 6, 11,661,000, 0 |

## Known limits

- Journey sampling canonicalizes paid touches over the whole projection (~165 MB per
  candidate query on this history). Story-demo ClickHouse needs 4 GiB; the fixture default stays 2 GiB.
- The campaign worker replays all historical snapshots on first start (~260k), so Live activity
  fills about 20 minutes after the app starts.
