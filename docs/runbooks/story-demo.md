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

## Evidence (release `5fae6827…`)

| Check | Result |
| --- | --- |
| Certified days | 608, 2025-01-01 → 2026-08-31 |
| Impressions / clicks / spend | 1,216,000 / 91,328 / ₫668,639,347, exactly equal to generator ground truth |
| Orders, gross, net | 11,124 / ₫25,122,680,000 / ₫23,195,596,000 (governed: consent/bot exclusions; source ground truth has 11,530 orders) |
| Journey projection | 247,789 events; 11,124 `order_created`, equal to Gold; 6,539 email events |

## Insights evidence (Phase 1, release `5fae6827…`)

| Check | API | Independent query |
| --- | --- | --- |
| Heatmap orders / sessions | 11,124 / 87,592 | Gold orders 11,124; non-bot projection sessions 87,592 |
| Order-value bands | 11,124 orders, ₫25,122,680,000 | Gold orders and gross |
| Median / P90 order | ₫1,829,000 / ₫4,587,000 | exact `quantilesExact` |
| Loyalty tiers net | ₫23,195,596,000 | `customer_360` total |
| Web + app + unknown orders | 6,978 + 3,560 + 586 = 11,124 | projection `order_created` |

The oracle exposed a dropped `unknown` group (ClickHouse fills unmatched LEFT JOIN
columns with `''`); it was fixed before release with a regression test.

## Products evidence (Phase 2, release `14a6b9ad…`, refresh `c5a9b8d4`, 1,479 s)

| Check | Result |
| --- | --- |
| Cut-off | campaign performance and commerce both end at 2026-08-31, although Sep 1–3 events were already in Kafka |
| History unchanged | 11,124 orders, gross ₫25,122,680,000, 247,789 projection events (as before) |
| Commerce vs campaign finance | gross ₫25,122,680,000 and refunds ₫1,927,084,000, equal |
| Category gross vs generator files | beauty 5,602,931,000 · electronics 4,980,287,000 · fashion 4,271,169,000 · home 4,359,101,000 · sports 5,909,192,000 (equal) |
| Device gross | desktop 7,770,316,000 · mobile 8,061,260,000 · tablet 7,976,495,000 · unknown 1,314,609,000 |

Two earlier attempts failed closed and changed nothing:

- **`ee4ba7f6`:** Silver did not create `products` (a literal table list).
- **`59d10af8`:** Gold's malformed-items guard fired on a wrong Spark JSON schema.

Both were fixed with regression tests, including the opt-in `MP_SPARK_TEST`, which
compares the Spark commerce frame with the pure builder. Before re-certifying after
a continuation replay, let Flink reach zero lag so Silver can reconcile every raw
event.

## Continue September

```bash
"${dc[@]}" run -d --name marketpulse-story-demo-continuation --no-deps replay kafka replay \
  --input /app/data/generated/story-demo --bootstrap-servers kafka:29092 \
  --from-date 2026-09-01 --to-date 2026-10-01 --resume-state /app/state/story-demo-september.json \
  --init-state --speed 120 --include-dlq --batch-size 20
```

`--speed 120` advances one business day in about 12 minutes. Omit `--init-state` to resume
after a stop. The cursor lives in the `replay_state` volume and refuses to continue after an
ambiguous acknowledgement or a changed world. September streams to Live activity as
provisional data. Daily certification of September days and the automated
pause-at-day-boundary controller are not implemented yet; certified views remain at 2026-08-31.

## Known limits

- Journey sampling canonicalizes paid touches over the whole projection (~165 MB per
  candidate query on this history). Story-demo ClickHouse needs 4 GiB; the fixture default stays 2 GiB.
- The campaign worker replays all historical snapshots on first start (~260k), so Live activity
  fills about 20 minutes after the app starts.
