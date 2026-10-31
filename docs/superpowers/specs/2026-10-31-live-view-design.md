# Live View — who is on the store right now and what they are buying

Status: approved for implementation 2026-10-31 (user asked to write the spec and build it).

## Intent

The Command Center already streams campaign/day snapshots, but the only live
page reads like an engineering ledger. Marketers expect a Shopify-style "Live
View": how many people are on the site right now, where they are in the buying
journey, what they are buying, and today's provisional sales — numbers that
move while the September replay runs. This makes the streaming half of the
architecture visible in a demo without touching certified finance.

## Goals

1. Show, per replay "now", active visitors (last 30 minutes), split by source
   (web, app) and device.
2. Show current shopper behaviour: of the sessions active in the last 30
   minutes, how many are browsing, viewing a product, holding a cart, checking
   out, or have purchased.
3. Show today's provisional totals: visitors, sessions, orders, gross, refunds,
   net and session conversion, per currency.
4. Show what is being bought today: top products by units ordered, add-to-carts
   and product views, labelled with names/categories when the certified release
   knows the product.
5. Show a per-minute activity series for the last 30 minutes and a feed of the
   latest events, with no personal identifiers.
6. Keep every number labelled provisional and separate from certified values.

## Non-goals

- Live ad-platform clicks, impressions or spend (they only arrive in daily
  report files; a live ad feed needs new generator data — a later phase).
- A visitor map (anonymous visitors have no city).
- Replacing certified pages or adding live numbers to certified totals.
- Predictions, alerts on live numbers, or a replay controller.

## Anti-goals

- Showing customer, anonymous, session or order identifiers to viewers.
- Measuring "now" by the wall clock: replayed data is dated September 2026, so
  a wall-clock window would always be empty.
- A second copy of Flink's deduplication or lateness logic in Python.

## Approaches considered

| Approach | Trade-off | Decision |
| --- | --- | --- |
| A. New Flink operators that emit visitor/behaviour/product aggregates | Most "streaming-native", but new Java state, new topics, checkpoint-compatible upgrades of the running job, and every new tile needs a Flink change | Rejected: slow to change, risky for the running job |
| B. Project Flink's already-clean event topic into ClickHouse and compute Live View with SQL at read time | One small worker and one table; reuses Flink's dedup/lateness verdicts; ClickHouse is the approved low-latency serving store; new tiles are SQL | **Chosen** |
| C. API reads Kafka directly per request | No storage, but slow, stateful per request and fragile | Rejected |

## Data flow

```
marketing.clean.customer-events.v1   (Flink: validated, deduplicated, late-classified)
        │  read_committed, consumer group marketpulse-live-events-v1
        ▼
live-events-worker  (new Compose service, app image, profile app)
        │  strict parse → batch insert (≤500 records or 1 s) → commit offsets
        ▼
ClickHouse marketpulse_live.events  (ReplacingMergeTree ORDER BY (partition, offset))
        ▼
GET /api/v1/live/view  (viewer) ──► Next.js /live "Live View"
```

- The worker records the Kafka cluster/topic identity in
  `marketpulse_live.event_source_identity` and refuses to continue if it
  changes (same guard as the campaign store).
- Offsets are committed only after a successful insert. A crash between insert
  and commit replays records; `ReplacingMergeTree` keyed by `(partition,
  offset)` plus `FINAL` reads make that idempotent.
- An invalid record stops the worker without committing (the existing
  fail-closed convention); the clean topic is produced by Flink, so this is a
  contract breach, not normal data.

### Stored row (one per clean event)

`partition, offset, received_at, event_id, event_name, source, event_time,
identity_key, session_id, campaign_key, consent_analytics,
eligible_for_aggregates, late_status, is_bot, device_type, product_id,
currency, gross_amount_minor, refund_amount_minor, items (Array(Tuple(product_id,
quantity)))`. Identifiers stay in ClickHouse for counting only; the API never
returns them.

Parser rules: `schema_version` 1; event names allowed per source (web/mobile:
page_view, view_item, add_to_cart, begin_checkout, purchase; commerce:
order_created, order_refunded; email: email_*); UTC timestamps; money as
non-negative integers; commerce money requires a 3-letter currency.

## Read model (`GET /api/v1/live/view`)

Eligible event = `eligible_for_aggregates AND consent_analytics` and its
session is not a bot session (any event in the session has `is_bot`). This
matches what certified analytics would admit, so Live View never shows
behaviour from people who declined analytics.

- **Anchor ("now")** = the latest eligible `event_time`. No eligible events →
  `status: "empty"`.
- **Active window** = `(anchor − 30 min, anchor]`.
- **Active visitors** = distinct `identity_key` with a web/mobile event in the
  window, plus splits by source and by `device_type`
  (desktop/mobile/tablet/unknown).
- **Behaviour** = sessions with a web/mobile event in the window, each placed
  in exactly one bucket by the furthest stage it reached up to the anchor:
  browsing (page_view) < viewing_product (view_item) < cart (add_to_cart) <
  checkout (begin_checkout) < purchased (purchase). Buckets sum to active
  sessions.
- **Today** = UTC day of the anchor, from 00:00 to the anchor: visitors,
  sessions, purchased sessions, conversion = purchased sessions ÷ sessions
  (exact decimal string, `null` when there are no sessions), and per currency
  orders, gross (order_created), refunds (order_refunded), net = gross −
  refunds. Money is integer minor units.
- **Per-minute series** = 30 one-minute buckets ending at the anchor minute,
  each with distinct visitors and orders; buckets with no events are 0 (the
  stream is complete up to the anchor).
- **Top products today** = up to 5 products ranked by units ordered, then
  add-to-carts, then views, then product id. Labels (name, category) come from
  the pinned certified release's `commerce_daily` when present; otherwise the
  product is shown as "Unlisted product".
- **Recent activity** = the 12 latest eligible events of kinds page_view,
  view_item, add_to_cart, begin_checkout, purchase, order_created,
  order_refunded: time, kind, source, device, product label, and amount for
  orders/refunds. No identifiers.
- **Freshness** = anchor, latest `received_at`, seconds since the latest
  receipt; `receiving` when a record arrived in the last 60 s.
- **Provenance** = source topic, source identity, stored rows, offset bounds.

All counts and money are strings (existing `wire` convention).

## UI

`/live` becomes **Live View** (navigation label unchanged: "Live activity").

1. Header with a pulsing "receiving" indicator, the anchor ("Replay time") and
   last receipt time.
2. KPI row: Visitors right now (with web/app split), Today's provisional net
   sales, Orders today, Conversion today.
3. Shopper behaviour strip: five tiles in journey order with counts and share.
4. Two small charts (one measure each, no dual axis): visitors per minute and
   orders per minute, last 30 minutes.
5. Top products today table and Recent activity feed side by side.
6. The existing campaign/day snapshot table moves below, under "By campaign
   (provisional)".

Every section carries "Provisional · not certified". Polling stays 5 s.

## Testing

- Parser: valid rows, each rejection rule.
- Store: schema creation, batch insert columns, identity guard, idempotent
  replays.
- Service: SQL shape (eligibility, window, FINAL) and assembly of every block
  from fake query results, empty state, no-currency days, missing release.
- API: route registered, viewer policy, web proxy allowlist.
- Playwright: Live View renders all blocks from a mocked API, empty state,
  phone width without horizontal scroll.
- End to end: story-demo stack with the September replay; Live View numbers
  move and match an independent count from the clean topic for one window.

## Left to the builder

Column ordering, SQL formulation, visual details within the existing shadcn
theme, and component split.
