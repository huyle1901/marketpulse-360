# Readable numbers and labels

Audit items 7–12 and 21 (`2026-11-01-ui-audit.md`). A marketer should read every figure
without decoding machine formats, and a rounded figure must not systematically understate.

## Problems

7. Compact money and percentages truncate: ₫3,457,576,000 reads ₫3.45B; 0.118542 reads 11.8%.
8. Journey analysis prints raw ratios ("0.848271 of sessions") and six-decimal allocated
   orders ("1.000000").
9. Raw technical times: ISO timestamps in journeys, an ISO "Last successful check", and
   "last event received 35,805s ago".
10. Journey analysis shows channels in lowercase while other pages say Google, Meta, TikTok.
11. Monospace currency figures raise and enlarge the ₫ sign.
12. Data health reports "Unavailable: lineage", an internal key.
21. Live activity totals split off a "No currency" group for campaign-days without orders.

## Decisions

- `web/lib/format.ts` owns every display rule:
  - `compactMoney` and `ratePercent` round half up (₫3.46B, 11.9%); a compact value that
    rounds to 1,000 of its unit carries to the next unit. Exact values stay in titles and
    exact tables.
  - `share(ratio)` shows a 0–1 ratio string as a percentage with one decimal.
  - `count(value)` drops trailing zeros from fixed-point counts ("1.000000" → "1",
    "0.250000" → "0.25").
  - `utcTime(iso)` → "31 Aug 2025, 00:41 UTC"; `clockTime(iso)` → "02:48:13 UTC".
  - `elapsed(seconds)` → "12 s", "4 min", "9 h 56 min", "2 d 3 h".
  - `channelName(key)` → Google, Meta, TikTok, Unattributed.
- Figures use the sans face with tabular numerals (`.figure`, `.live-figure`, `.live-mono`),
  so digits align without the monospace currency glyph.
- Data health names failed panels in words ("Could not load: validated lineage, audit").
- Live totals fold a currency-less campaign-day into its campaign's declared currency, as the
  per-campaign bars already do.

## Tests

Playwright assertions on Journey analysis (percent reach, trimmed counts, readable times,
capitalised channels), Live View (elapsed time, readable check time), Live activity (no
currency-less group), Data health (worded failure) and Overview (rounded compact money).

## Non-goals

No API or data change; exact values are unchanged and still one click away.
