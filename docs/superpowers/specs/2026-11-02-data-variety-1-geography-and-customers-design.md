# Data variety 1/5: geography and customer growth

Part of `2026-11-01-story-data-variety-design.md` (overview). Order: 1 → 2 → 3 → 4 → 5.

## Intent

Geography looks flat because five cities receive customers uniformly, and cohort retention
is empty because every CRM customer was created 30–730 days *before* the history starts, so
nobody joins during 2025–2026. Anonymous devices never return, so their retention is trivial.
This spec makes the customer base grow, live in a realistic spread of cities, and come back.

## Scenario plumbing (shared by specs 1–4)

- `src/marketpulse_mock/config.py` gains a frozen `Scenario` attached to each profile.
  `story-demo` uses `vietnam-retail`; `fixture`, `smoke`, `demo` and `benchmark` use `flat`,
  which reproduces today's behaviour **byte for byte** (the committed `data/mock/fixtures/
  seed-360` manifest and every file checksum stay identical). This keeps every existing
  Silver, batch, replay and generator test valid.
- Scenario values are plain data (weights, multipliers, calendars) with one docstring each,
  so the story is readable in one place.

## Decisions (`vietnam-retail` only)

1. **Cities**: 13 Vietnamese cities with weights summing to 100 — Ho Chi Minh City 28,
   Ha Noi 24, Bien Hoa 6, Thu Dau Mot 6, Hai Phong 6, Da Nang 6, Can Tho 5, Nha Trang 4,
   Hue 3, Vung Tau 3, Quy Nhon 3, Buon Ma Thuot 3, Ha Long 3. A customer's city is a
   deterministic weighted draw.
2. **Customers**: story-demo grows from 1,000 to 3,000. 40 % were created in the two years
   before the history start; 60 % join during the generation window with a rising ramp
   (twice as many joins per day at the end as at the start).
3. **Integrity**: a customer appears in sessions, orders, CRM snapshots and email only on or
   after `created_at`. Cohorts therefore form every month of 2025–2026.
4. **Returning visitors**: 30 % of anonymous sessions reuse a device id from those seen in the
   previous 60 days (deterministic choice), so anonymous profiles span sessions and months.
5. **Web map** (`web/lib/cities.ts`, Geography page): coordinates, region (North, Central,
   South) and label side for all 13 cities. With 13 bubbles, callouts show for the six largest
   by the selected measure; the rest show their callout on hover or focus (Datawrapper hides
   colliding labels the same way). Region totals already follow `CITY_REGION`.

## Resolved after builder review

- **Activity over tenure** *(my call)*: identified sessions pick a joined customer with weight
  `1 / (1 + months_since_join / 6)`, so engagement decays with tenure and cohorts show a real
  drop-off; each new customer has a first session on their join day (an order is not forced).
- **Traffic grows with the base** *(my call)*: story-demo `impressions_per_day` rises from 2,000
  to 4,000 (before spec 3's calendar multipliers); the 70 % identified share stays.
- **CRM volume** *(my call)*: daily full snapshots stay; spec 5 re-measures the archive window
  and shortens it (e.g. half-month windows) if the Spark driver needs it.
- **Base branch**: built on `main` after the Geography PR (#23) merges, because callouts,
  region colours and `CITY_REGION` live there.

## Tests

- `flat` scenario: committed fixture manifest and file checksums unchanged.
- `vietnam-retail` (a short window, e.g. 30 days): city shares within ±3 points of the
  weights on 3,000 customers; no customer event before `created_at`; joins per day rise over
  the window; at least 20 % of anonymous sessions reuse an earlier device; same seed →
  identical output, other seed → different output.
- Web: all 13 cities plot; six callouts visible by default; a hidden callout appears on
  hover; region split includes every city.

## Non-goals

No new geography fields in the API; country stays VN; no change to identity resolution.
