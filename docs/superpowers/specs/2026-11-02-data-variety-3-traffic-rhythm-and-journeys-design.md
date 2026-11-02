# Data variety 3/5: traffic rhythm and journeys

Part of `2026-11-01-story-data-variety-design.md`. Depends on specs 1–2.

## Intent

Sessions are spread evenly through every day, so the weekday-hour heatmap is near-uniform and
nothing marks a sale. Every visit comes from a paid ad. Real shoppers have daily rhythms, sale
days and unpaid visits.

## Decisions (`vietnam-retail` only)

1. **Hour of day** (Vietnam time, GMT+7): event times are drawn from an hourly weight profile —
   quiet 01:00–06:00, rising from 08:00, a lunch peak 12:00–13:00, an evening peak
   20:00–22:00. Events within a day are generated in event-time order (times are drawn first,
   then sorted), preserving the replay's ordering guarantee.
2. **Day of week**: Saturday and Sunday ×1.15 daily traffic.
3. **Calendar** (traffic multipliers on the day's impressions): double days 1/1 … 12/12 ×1.8;
   Black Friday 2025-11-28 ×1.6; the 10 days before Tết (2025-01-29, 2026-02-17) ×1.4;
   Tết eve to the fourth day ×0.5. On double days Shopee's budget share doubles and its
   purchase multiplier rises ×1.5 (the hook from spec 2).
4. **Non-paid sessions**: 25 % of the day's sessions have no campaign and no click, split
   organic 60 % / direct 40 %, recorded as `traffic_source` on the landing `page_view`
   (`paid` on ad sessions). They follow the same funnel at 0.8× purchase probability and stay
   unattributed downstream. Email-referred visits arrive in spec 4.
5. **Device funnel**: mobile purchase probability ×0.85, desktop ×1.15, so device reports differ.

## Resolved after builder review

- **Event contracts** *(my call; architecture doc updated in the same PR)*: web and order event
  contracts stay `v1` and are widened compatibly — `campaign_id`, `platform_campaign_id`,
  `click_id` (web) and `attributed_platform`, `attributed_campaign_id` (orders) may be null when
  `traffic_source` is not `paid`. The Flink parser accepts them; campaign metrics skip events
  without a campaign key, as they already do; the registry compatibility check must pass. `flat` output is unchanged
  because it never emits nulls there.
- **Attribution** (answered by policy): a non-paid session never carries a click id; Gold may
  still credit an earlier paid click of the same profile inside the window — that is the
  last-paid-click policy, not a defect. Tests assert the session, not the Gold credit.
- **25 %** *(my call)*: organic plus direct sessions are 25 % of all sessions of the day
  (email-referred sessions from spec 4 are counted separately). The test band is 22–28 %.
- **Which calendar** *(my call)*: multipliers follow the **Vietnam calendar date of each hour**.
  The generator still works one UTC day at a time: the day's volume is the sum over its 24 hours
  of hourly weight × the multiplier of that hour's Vietnam date, and event times are drawn from
  the same weights. Vendor reports stay per UTC day. A double day therefore lifts 00:00–24:00
  Vietnam time across two UTC days.

## Tests

- Hourly: the share of sessions between 20:00 and 22:00 Vietnam time exceeds the share between
  02:00 and 04:00 by at least 4×.
- Calendar: a double day's sessions exceed the median of its surrounding ±7 days by at least
  1.5×; Tết days fall below 0.7×; Shopee's spend share on double days is higher than on others.
- Non-paid: 22–28 % of non-email sessions are organic or direct; none has a click id.
- Ordering: every generated day is non-decreasing in event time; determinism holds.

## Non-goals

No new event types; time zone handling downstream is unchanged (UTC storage, Vietnam display).
