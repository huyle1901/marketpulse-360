# Clear dates and times

Audit items 13 and 22–26 (`2026-11-01-ui-audit.md`). Marketers in Vietnam should read every
date and time at a glance, in one format, without decoding exclusive bounds or ISO strings.

## Problems

13. The weekday-hour heatmap and busiest slot are UTC only.
22. Date filters say "To (exclusive UTC)" and hold the day after the last day shown.
23. Four date formats appear (browser dd/mm/yyyy, ISO, "Tue, Sep 8, 2026", "31 Aug 2025, 00:18 UTC").
24. Machine timestamps with offsets, microseconds or nanoseconds are shown as text.
25. "Data through" says 2026-08-31 on Overview and 2026-09-01T00:00 on Customers and Analysis.
26. Cohort months are written as days.

## Decisions

- **One format per kind** (`web/lib/format.ts`):
  - a day: `dayLabel('2026-08-31')` → "31 Aug 2026";
  - a month: `monthName('2023-01-01')` → "Jan 2023";
  - a range of days, inclusive: `dayRange('2026-06-03','2026-09-01')` (exclusive end in) →
    "3 Jun – 31 Aug 2026";
  - an instant: `localTime(iso)` → "1 Nov 2026, 09:57 (GMT+7)"; `localTime(iso,{seconds:true})`
    adds seconds for audit rows; `localClock(iso)` → "09:57:05 (GMT+7)".
- **Instants are shown in Vietnam time** (Asia/Ho_Chi_Minh, GMT+7, no daylight saving):
  journeys, freshness, Live View clock and feed, Data health observations and audit rows.
- **Certified days stay UTC calendar days**, because Gold is partitioned by UTC day. They are
  shown as plain dates, and the date filter states the rule once: "Days are UTC calendar
  days (07:00–07:00 Vietnam time)". Live View "today" names its start the same way.
- **Date filters are inclusive.** The "To" field shows the last day included; the URL and API
  keep their exclusive `to_date`, so shared links and requests are unchanged
  (`to_date = To + 1 day`). Labels become "From" and "To".
- **"Data through"** is always the last included day, derived from the exclusive bound.
- **Heatmap** opens in Vietnam time with a "Time zone" switch (Vietnam time / UTC). Hour
  buckets shift by +7 hours and carry into the next weekday; the counts are unchanged.

## Tests

Playwright: heatmap default and switch, inclusive "To" on Overview, Ads, Analysis and Live
(the request still sends the exclusive bound), consistent day and instant formats on
Overview, Customers, Analysis, Live and Data health, and no ISO timestamp text in `main`.

## Non-goals

No API change; exact ISO values remain in technical provenance details where operators
need them.
