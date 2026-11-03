# Data variety 4/5: email lifecycle

Part of `2026-11-01-story-data-variety-design.md`. Depends on specs 1 (customers join over
time) and 3 (sale calendar).

## Intent

The Email page shows sends falling from 469 a month to 3 because the generator emails a fixed
2 % of marketing-consented customers chosen on day one, unsubscribes 1 % per send and never adds
anyone. A lifecycle programme should grow with the customer base and mix email types.

## Decisions (`vietnam-retail` only)

1. **Audience**: every marketing-consented customer from their `created_at`, until they
   unsubscribe.
2. **Email types** (`email_type` in the email event payload; `lifecycle_campaign_id` names the
   send):
   - `welcome` on the day a consented customer joins;
   - `newsletter` every Tuesday to the whole audience;
   - `promo` on double days and the 3 days before Tết to the whole audience.
3. **Unsubscribe**: 0.5 % per delivered email; an unsubscribed customer receives nothing later
   (today's guarantee stays).
4. **Email-referred visits**: each `email_clicked` starts a site session within 0–2 hours with
   `traffic_source = email`, no ad click, the customer's identity, and the normal funnel at
   1.2× purchase probability. The email itself is never credited with these orders; an earlier
   non-bot paid click within 7 days may be (last-paid-click policy).
5. **UI**: no change required; the Email page's monthly chart now grows. A per-type breakdown is
   a later, separate change (needs an API field).

## Resolved after builder review

- **Volume** *(my call)*: rates follow typical e-commerce benchmarks instead of the first draft,
  so email-referred visits stay a minority of traffic (about 6 % of sessions on early-2025 promo
  days and about 8 % across all story promo days, growing with the audience):
  welcome open 55 % / click 12 %, newsletter 28 % / 2.5 %, promo 32 % / 4.5 %.
- **Consent** (answered): consent rules do not change; "honoured" means an unsubscribe stops
  later sends and shows as `consent_marketing = false` in the next CRM snapshot. No new
  opt-in or opt-out behaviour is added.
- **One email per customer per day** *(my call)*: priority welcome > promo > newsletter.
  The welcome check applies to customers who join inside the window.

## Tests

- Audience grows: sends in the last month of the window exceed sends in the first month.
- Every consented customer who joins inside the window gets exactly one welcome on their join day; no email before
  `created_at` or after an unsubscribe.
- Type rates within ±5 points of the targets over a 90-day window; promos only on sale days;
  newsletters only on Tuesdays.
- Every email-referred session follows an `email_clicked` for the same customer within 2 hours
  and has no click id.

## Non-goals

No email revenue attribution; no change to the email Kafka contract beyond the optional
`email_type` payload field (validated as one of the three types when present).
