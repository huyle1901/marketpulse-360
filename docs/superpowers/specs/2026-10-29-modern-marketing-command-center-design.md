# Modern marketing command center — design addendum

Status: authorized by the user on 2026-10-29 ("please go ahead") after the
research summary below. It extends, and does not replace,
[the marketing dashboard and continuous demo design](2026-10-29-marketing-dashboard-and-continuous-demo-design.md).

## Intent

The user verified the first visual pass in the running app. They found it ugly
and could not understand it. Their goals:

1. Open the app to a populated history, January 2025 through August 2026, that
   then continues as streaming data from September 2026.
2. Make the experience look and read like a modern marketing product, with the
   shadcn/ui dashboard as the reference style.
3. Add more processing and more views where the data can support them.

Success means a marketer understands performance in seconds, in business
language, without reading engineering terms.

## Research (2026-10-29)

| Product | Pattern used | Applied here |
| --- | --- | --- |
| Triple Whale | Blended KPIs (revenue, spend, MER, CPA) with prior-period change; channel ROAS | KPI cards with change and sparkline; MER/CPA |
| Northbeam | KPI boxes above one main line graph; breakdowns by platform | Overview hierarchy |
| Klaviyo | Total vs marketing-attributed revenue; ranked flows/campaigns; per-recipient metrics | Email engagement view |
| GA4 / HubSpot | Summary cards that link to detail reports; acquisition and lifecycle views | Card-to-detail navigation; new vs returning customers |
| shadcn/ui dashboard-01 | Sidebar, section cards, interactive area chart with range tabs, data table | Visual system |

Root causes of the confusing first pass: engineering terms in primary text
("release identity", "campaign-day session incidences", "minor units",
"exclusive UTC"); only two days of fixture data, so charts are flat; no
prior-period change or sparklines; and a hand-written CSS approximation rather
than real shadcn/ui components.

## Approaches

- **A. Real shadcn/ui + Tailwind CSS v4 + Recharts (chosen).** Copy-in Radix-based
  components and the shadcn chart wrapper. This gives the reference look,
  accessible primitives and smooth charts. Cost: new dependencies and a styling
  migration of every page.
- B. Keep the hand-written CSS and add only Recharts. Smaller change, but it keeps
  the look the user rejected.
- C. Tremor components. Dashboard-focused, but a second design language and not
  the reference the user named.

## Decisions

- **Language:** English product labels, kept from the base design and suitable
  for the course report, but written in plain business words. Engineering terms,
  release IDs and exact minor-unit integers move into a "Data details"
  disclosure on each page. Correctness rules are unchanged: certified and
  provisional data stay separate, money uses exact BigInt, missing data is never
  zero, and nothing is added across currencies.
- **Pages:** Overview, Ad performance (new), Live activity, Journeys, Customers,
  Email (new), Data health. Existing role restrictions stay.
- **Overview:** KPI cards (net revenue, ad spend, MER = net revenue ÷ ad spend,
  orders, AOV, CPA), each with change against the equal prior period when
  coverage is complete, and a sparkline. An interactive area chart has
  7d/30d/90d/12m/all ranges and a metric selector, followed by revenue by
  channel, needs attention, and a top-campaigns table that opens the campaign
  panel.
- **Ad performance:** impressions, clicks, CTR, CPC, CPA and ROAS by channel and
  campaign, computed in the browser with BigInt from certified rows already
  served by `/dashboard`.
- **Email (new serving read):** sent, delivered, opened, clicked and unsubscribed
  counts by month and lifecycle campaign. These come from the governed event
  projection already published in ClickHouse (MP-07 sidecar), pinned to the
  release. Email-attributed revenue is out of scope: the certified attribution
  policy credits paid clicks only, and inventing an email model would violate it.
- **Customers (new serving read):** new versus returning ordering customers per
  month, from the same projection. A customer is new in the month of their first
  original `order_created`.
- **Product/category sales:** deferred. The projection has no product items.
  Adding it needs a new Silver/Gold/dbt/export contract, which is its own
  pipeline story.

## History completion (story-demo world)

The world is already generated (seed 360, 638 days) and its history was
published to the isolated `marketpulse-story-demo` Kafka. Flink failed with heap
OOM at about 76% of the web history (process size 1024m, heap state). Steps:

1. Make the TaskManager process size and container limit configurable,
   defaulting to today's values. The story-demo environment uses 3072m.
2. Allow `submit-job.sh` to restore from an explicit retained checkpoint path.
   Resume from `chk-26`; never reset offsets or drop state.
3. After Flink drains the history, range-ingest the history ad/CRM files into
   Bronze, then run one Dagster refresh/certify/publish for logical day
   2026-08-31. Silver/Gold cover everything pinned in Bronze. Then publish the
   journey projection.
4. Run the app in the story-demo project on localhost:43110 and verify it in a
   browser. Continue the September replay with the resumable cursor.

Long-running steps report measured elapsed time; nothing is claimed before it is
observed.

## Non-goals and anti-goals

- No fabricated chart values or seeded ClickHouse/PostgreSQL rows.
- No change to MP-01–10 business rules, and no reset of existing volumes.
- No jargon in primary text. A beautiful page that misstates money is a failure.
