# Data variety 5/5: regenerate and recertify the story demo

Part of `2026-11-01-story-data-variety-design.md`. Runs once, after specs 1–4 are merged.

## Intent

The new scenario only reaches the Command Center after the story world is regenerated, its
history archived and certified, and the September replay restarted. The demo's recorded
evidence (runbook tables, presentation numbers) must then describe the new world.

## Steps

1. Generate `data/generated/story-demo` (seed 360, `story-demo`, `vietnam-retail`) and run the
   validator; record row counts, time range, checksums and seed in the manifest.
2. Prepare a fresh story-demo stack (see the open decision below), start streaming and
   lakehouse services, submit Flink, register contracts.
3. Replay the history window `2025-01-01 → 2026-09-01` without sleep; let Flink reach zero lag.
4. Archive ad (including `shopee_ads`) and CRM files month by month; run the certified refresh
   for 2026-08-31 with `BATCH_REQUIRED_PLATFORMS` including `shopee_ads`; publish journey detail.
5. Recreate the application accounts from `.secrets/`; start api, web and workers.
6. Initialise a new September replay cursor (the world identity changed) and resume at the
   agreed speed.
7. **Oracle**: compare certified totals with the generator files independently — orders,
   gross, net, spend by platform and month, revenue by city and region, email sends by type
   and month, non-paid share — and record them in `docs/runbooks/story-demo.md`, replacing the
   old evidence tables (kept in git history).
8. Walk every page in Chrome in light and dark and capture the new story in the runbook.

## Resolved after builder review

- **Oracle meaning** *(my call)*: ungoverned measures (impressions, clicks, spend by platform and
  month) must equal generator ground truth exactly. Governed measures (orders, revenue, city and
  region revenue) are compared with an independent governed computation (generator files joined
  with the certified projection and `customer_360`, as the Geography evidence did), and the raw
  versus governed gap is recorded with its reasons (consent, bots, identity).
- **Email type and non-paid checks** *(my call)*: read from Silver event payloads through Spark;
  no new serving column. "Non-paid share" means a share of sessions.
- **Replay speed**: 20, the user's latest choice.
- **Evidence** *(my call)*: every evidence table in the runbook is re-measured (including
  Insights, Products and Live View), not dropped.

## Decided by the user: a second project

The new world is built in a second Compose project, `marketpulse-story-demo-v2`, on its own
ports, while the current demo is kept for comparison. Because the new world is about three times
larger, the old demo's Flink, Spark and ClickHouse are stopped (not deleted) while v2 builds.
Once v2 passes the oracle and the walkthrough, `scripts/story-demo-env.sh` points to v2 (the old
values move to a `story-demo-v1-env.sh` for reference). Removing the old project's volumes is a
separate, later decision for the user.

## Acceptance

All certified-vs-generator checks equal; every page shows non-flat contrasts (city spread,
channel spend shares, hourly peaks, sale-day spikes, growing email, cohorts in 2025–2026);
Live View streams September in real time.

## Non-goals

No change to certification rules; the presentation slides are not edited automatically — the
runbook lists the numbers that changed so the slides can be updated by hand.
