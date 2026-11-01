# Legend and colour correctness

Audit items 1–6 (`2026-11-01-ui-audit.md`). A legend is only correct when each mark can be
seen, no two marks look alike, and a colour means one thing per page.

## Problems

1. Loyalty "No CRM tier" uses the muted surface; slice and mark vanish on the card.
2. Loyalty Gold (amber) and Bronze (orange) are indistinguishable (ΔE 2.2 protan).
3. Blue against violet is below the normal-vision floor: Web/App on Insights, Mobile/Desktop
   on Products.
4. Products treemap: any two categories can touch, but the palette was checked only for
   legend neighbours; Electronics (blue) and Sports (violet) touch.
5. Email: Opened and Clicked change colour between the monthly lines and the funnel; the
   legend runs Clicked, Opened, Sent.
6. Live View: blue means "App" in the web/app split and "purchased" on the shop floor, so a
   shopper who bought on the web reads as an app shopper; the minute chart has no legend.

## Decisions

- Palettes are validated with the dataviz validator against **all pairs** whenever marks of
  every series can touch (donut, treemap, stacked bar), in light and dark separately.
- New tokens in `web/app/globals.css`, each with its own dark step:
  - `--tier-gold/silver/bronze/none`: amber, blue, magenta, green. "No CRM tier" gets a hue
    because every neutral grey either vanished on the card or collided with magenta.
  - `--source-web/app/unknown`: orange, magenta, grey. Used wherever Web and App appear
    (Insights, Live View), so a source has one colour across the product.
  - `--cat-1..5` become blue, orange, green, magenta, olive (light) and a tuned dark set that
    passes all pairs; Products maps electronics, fashion, home, sports, beauty to them.
- Products devices use the channel palette (blue, orange, green, grey) instead of blue/violet.
- Email funnel is one measure across steps, so it uses one colour; the monthly chart keeps
  Sent, Opened, Clicked colours with the legend in funnel order.
- Live View shop floor colours each dot by the session's source, matching the Web/App legend
  beside it; the purchased state is already named by its station. The API's `behaviour` rows
  gain `web` and `app` session counts (their sum is `sessions`; other sources stay uncoloured).
  The minute chart gets a legend: Visitors (live green) and Minute with an order (order blue).

## Tests

- `web/tests/legend-marks.ts`: every legend mark is visible against its card and distinct
  from its siblings; used on Insights, Products, Email and Live View in both themes.
- `tests/test_app_live_view.py`: behaviour rows carry `web` and `app` counts that sum with
  other sources to `sessions`.
- Story-demo check: Live View dots per source match a ClickHouse count of sessions by source.

## Non-goals

No change to how sessions, visitors or stages are counted.
