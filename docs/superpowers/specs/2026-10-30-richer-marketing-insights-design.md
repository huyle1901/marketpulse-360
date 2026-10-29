# Richer marketing insights — phased design

Status: drafted 2026-10-30 at the user's request ("write the specs, then implement
each part"); awaiting the user's approval before Phase 1 starts. It builds on
[the modern command center](2026-10-29-modern-marketing-command-center-design.md).

## Intent

The redesigned UI is clearer but not yet impressive. The user wants new,
visually rich views backed by real data. Their preferred direction is to
surface fields that already exist in the generated sources before adding new
fields to the generator. The work is phased so each step can be verified on its
own and a failure never mixes several pipeline changes.

Success means each phase adds at least one view a marketer recognizes from
modern tools and that answers a business question in seconds. Every number
reconciles with an independent oracle.

## What the data already contains (verified 2026-10-30)

| Field | Where it lives today | Reaches ClickHouse? |
| --- | --- | --- |
| Event time (hour, weekday), source web/mobile/commerce/email | Journey projection `events` | Yes |
| Loyalty tier | `customers` reference relation (latest CRM) joined to `customer_360.customer_id` | Yes |
| Order value per order | Journey projection `gross_amount_minor` on `order_created` | Yes |
| Product items (product, quantity, unit price) → category, brand | `silver.customer_events.payload_json` plus `master/products` | No |
| Device type (desktop/mobile/tablet), market | `silver.customer_events.payload_json` | No |
| Customer city/country | `master/customers` (Bronze file rows only) | No |
| Ad creative format | `master/ads`; spend is reported per campaign, not per ad | Not attributable |

## Phases

### Phase 1 — Serving-only insights (no pipeline change)

New release-pinned, viewer-level reads over relations already certified and
published. Each read uses the existing projection consistency gate, and an
unpublished projection is unavailable, never zero.

- **When customers act:** a weekday × hour heatmap (UTC) of governed sessions
  started and original orders, with exact counts per cell and a busiest-slot
  callout.
- **Order value distribution:** a histogram of original order gross amounts in
  fixed, currency-safe bands, plus median and P90 computed exactly from integer
  amounts.
- **Revenue by loyalty tier:** customers, orders and net revenue per latest CRM
  tier, labeled *as of the certified release*. Anonymous profiles stay a
  separate "no CRM tier" group, never folded into a tier.
- **Web vs app:** sessions and orders by event source (web/mobile), from the
  projection.

UI: a new **Insights** workspace that leads with the heatmap, plus loyalty
cards on Customers.

### Phase 2 — Commerce and device Gold model (pipeline extension)

Add one Gold table, `gold.commerce_daily` (date × category × brand × device
type), built by Spark from the pinned Silver snapshot plus the archived
`master/products` rows:

- **Measures:** units, orders, gross and refund amounts, with refunds allocated
  to the original order's items.
- **Items:** parsed from `order_created` payloads; device comes from the
  purchase session's web/mobile events. A missing device is an explicit
  `unknown` value, not a guess.
- **Pipeline:** carried through export → isolated ClickHouse candidate → dbt
  model and tests (keys, accepted values, item gross reconciling exactly to
  certified order gross) → certification → publication.
- **Existing contracts:** the five MP-04 Gold contracts are unchanged; this is
  an additional certified relation.
- **UI:** a **Products** workspace with a category/brand treemap, top products,
  and conversion and AOV by device.

### Phase 3 — Geography (Silver contract extension)

Carry `city` and `country` from `master/customers` into `silver.customers` and
`customer_360`. This is an additive column change, with schema fingerprints and
evidence updated. The read is revenue, customers and orders per city for
identified customers; anonymous activity stays "location unknown".

UI: a **Geography** view with a Vietnam city bubble map using bundled
coordinates (no tile CDN), plus a ranked table.

### Phase 4 (optional, separate approval) — New generator fields

Candidates: traffic source (organic, social, referral, direct) and
promotion/discount codes. This regenerates the 638-day world and reruns the
full history, a measured multi-hour operation. It also changes MP-01 fixture
checksums, so it needs its own spec and approval.

## Constraints

- One phase per branch section and commit series. Each phase ends with focused
  RED/GREEN tests, full Python and browser suites, a production build, a
  story-demo deployment, and an independent oracle check against generator data.
- Certified and provisional values stay separate; money is exact (BigInt or
  Decimal); multi-currency data is never summed.
- No fabricated values, no seeded serving rows, no runtime CDN or paid service.
- Existing MP-01–10 contracts and the fixture stay unchanged in Phases 1–3,
  except for the additive Phase 3 columns.

## Decisions left to the builder

Exact histogram bands, colors, chart library primitives (Recharts or SVG), and
whether loyalty cards live on Customers or Insights.
