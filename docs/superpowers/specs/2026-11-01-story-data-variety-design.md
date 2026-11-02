# Story-demo data variety

## Intent

The story demo should tell a marketing story with real contrasts. Today the generator spreads
everything evenly, so every page looks flat: five cities with near-equal revenue, three
channels at exactly 33.3 % of spend, a flat weekday-hour heatmap, no new customers during the
history, an email list that decays to nobody, and every session coming from a paid ad.
The user asked for more variety across geography, journeys, a Shopee Ads channel and email,
and accepted the proposed story. Decisions below marked *(my call)* were made by the builder.

## The story the data should tell

- Ho Chi Minh City and Ha Noi dominate; mid-size cities trail; the long tail is small.
- Google is expensive per click but converts well; TikTok buys cheap clicks that rarely buy;
  Meta sits in between; **Shopee Ads** converts best and surges on double-day sales.
- Shoppers browse at lunch and late evening (Vietnam time), more at weekends; double days
  (1/1 … 12/12), Black Friday and the run-up to Tết lift traffic, the Tết holiday dips it.
- New customers keep arriving during 2025–2026; some anonymous visitors come back.
- Not every visit is paid: organic, direct and email-referred sessions exist and stay
  unattributed under the paid-click attribution policy.
- The lifecycle email list grows with consented customers; weekly newsletters, sale-day promos
  and welcome emails have different open and click rates.

## Parts (one spec and one pull request each, in this order)

1. `2026-11-02-data-variety-1-geography-and-customers-design.md` — scenario plumbing, 13 weighted
   cities, customers joining through 2025–2026, returning visitors, 13-city map.
2. `2026-11-02-data-variety-2-channels-and-shopee-design.md` — budget shares, channel personas,
   Shopee Ads end to end.
3. `2026-11-02-data-variety-3-traffic-rhythm-and-journeys-design.md` — hourly rhythm in Vietnam
   time, weekends, sale calendar, organic and direct visits, device funnels.
4. `2026-11-02-data-variety-4-email-lifecycle-design.md` — growing audience, welcome, newsletter
   and promo emails, email-referred visits.
5. `2026-11-02-data-variety-5-regenerate-and-recertify-design.md` — regenerate once, certify,
   replay, record the new evidence.

Existing profiles other than `story-demo` keep today's output byte for byte (`flat` scenario).

## Approaches considered

- *Scenario-driven generator (chosen)*: explicit, documented weights and calendars produce a
  story the demo can narrate and the tests can assert.
- *More randomness only*: cheaper, but contrasts would be accidental and unexplainable.
- *Post-process the existing world*: avoids regeneration but breaks referential integrity
  between impressions, sessions, orders and reports.

## Non-goals

No change to attribution policy, certification, consent or bot rules. Other profiles
(fixture, smoke, demo, benchmark) keep their sizes; their committed fixtures are refreshed
only where the generator change requires it.

## Tests

Generator unit tests assert determinism, referential integrity, the city and platform weights
within tolerance, hourly and sale-day uplift, non-paid and returning sessions, email audience
growth and the Shopee report shape. Pipeline tests cover `shopee` in Silver, batch, app and dbt.
After regeneration, an oracle compares certified totals with the generator files.
