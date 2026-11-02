# Data variety 2/5: channel economics and Shopee Ads

Part of `2026-11-01-story-data-variety-design.md`. Depends on spec 1's `Scenario`.

## Intent

Every channel takes exactly a third of spend and converts identically because impressions are
round-robin across campaigns and the click and funnel thresholds ignore the platform. The story
needs channels with character, and a fourth channel, Shopee Ads, that marketers in Vietnam use.

## Decisions (`vietnam-retail` only unless stated)

1. **Platforms**: google, meta, tiktok, shopee. Story-demo has 16 campaigns, four per platform,
   each with two ads as today. Shopee prefixes: campaign `S`, ad `SA`.
2. **Budget share**: spend splits by platform (Google 30, Meta 28, TikTok 24, Shopee 18) and,
   within a platform, by objective (conversions 1.3, traffic 1.0, awareness 0.7); impressions are
   drawn accordingly (see below). Spec 3 later raises Shopee's share on sale days through the
   same hook.
3. **Personas** (multipliers on cost per impression / click-through / purchase strength `m`):
   Google 1.4 / 0.8 / 1.3; Meta 1.0 / 1.0 / 1.0; TikTok 0.6 / 1.6 / 0.5; Shopee 1.0 / 1.1 / 1.6.
   How `m` shapes the funnel is defined below, so stage reach differs by channel.
4. **Shopee Ads report**: dataset `shopee_ads`, CSV, one row per campaign per day, in the shape
   of a Shopee Seller Centre export: `date` (`DD/MM/YYYY`), `ad_id` (platform campaign id),
   `impression`, `clicks`, `expense` (whole VND), `conversions`, `gmv` (whole VND). It
   reconciles with ground truth like the other vendor reports.
5. **Pipeline accepts `shopee` everywhere the platform list is fixed** (all profiles; `flat`
   simply never emits it):
   - generator: writers, validator, manifest counts;
   - lakehouse: `source_files` allowed datasets; Silver dataset→platform map and a Shopee
     parser (day-first date, whole-VND amounts);
   - batch: `REPORT_PLATFORMS`, `DAY_FIELDS` with a day-first parser, required platforms
     (story-demo compose sets `BATCH_REQUIRED_PLATFORMS` to include `shopee_ads`; the default
     stays the three original platforms so other stacks are unchanged);
   - app: platform filter contract; dbt `accepted_domains`.
6. **UI**: channel name "Shopee" and a four-channel palette plus unattributed grey, validated
   across **all pairs** in light and dark (donuts put any two channels side by side), used by
   Overview, Ads, Journey analysis, campaign table, attribution and scatter; platform selects
   list Shopee.

## Resolved after builder review

- **Weights are spend shares** *(my call)*: marketers set budgets, so 30/28/24/18 and the
  objective weights 1.3/1.0/0.7 are shares of **spend**. The impression draw divides each weight
  by its cost multiplier; TikTok therefore receives the most impressions and clicks, which is the
  "cheap clicks" story. Spec 3's sale-day hook doubles Shopee's spend share the same way.
- **Funnel scaling** *(my call)*: only the conditional rates of the last two steps scale:
  checkout|cart = 0.625 × √m and purchase|checkout = 0.48 × m, each capped at 0.9; view and
  cart steps keep today's cut-offs. Spec 3's device and sale-day multipliers multiply `m`.
- **Range-window source check** *(my call)*: `select_range_sources` requires the configured
  required platforms (`BATCH_REQUIRED_PLATFORMS`) on every day instead of every
  `REPORT_PLATFORMS` dataset. It stays fail-closed for what a stack declares; three-platform
  stacks keep their current requirement exactly.

## Tests

- Generator: platform spend shares within ±3 points of the weights over a 30-day window;
  TikTok click-through above Google's and purchase rate below it; Shopee report rows parse and
  reconcile with ground truth; `flat` fixture unchanged.
- Lakehouse/batch: a Shopee report row maps to `ad_spend_daily` with the right date and amount;
  day-first dates select the right partition; unknown dataset still rejected.
- App/dbt: `platform=shopee` accepted, other values rejected; accepted-domains test passes.
- Web: Shopee appears with its colour in each channel view; palette validator passes all pairs.

## Non-goals

No change to attribution windows or models; Shopee orders still land on the store (we model
Shopee *ads* sending traffic, not marketplace orders).
