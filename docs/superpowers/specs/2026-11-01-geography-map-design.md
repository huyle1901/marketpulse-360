# Geography: a legible revenue map

Audit item 18 (`2026-11-01-ui-audit.md`) plus the user's request to make the page more
attractive and sensible, informed by how analytics products and cartographers present
location data.

## What we learned

- Shopify pairs a dot map (one colour per meaning) with a ranked location report; the map
  shows where, the ranking gives exact order.
- Cartography guidance (Axis Maps, proportional symbol literature): scale symbol **area**, not
  radius; use transparency and a stroke when symbols overlap; draw large symbols first; give
  a size legend with a few reference circles; never move symbols away from their place to
  avoid overlap. Choropleths suit rates over areas; our data is a handful of city totals, so
  proportional symbols fit.

## Problems today

The map is a 240 px blank rectangle with a lat/lon grid and 9 px labels; Ha Noi/Hai Phong and
Can Tho/Ho Chi Minh City overlap; there is no size scale; one measure only; the unknown share
is a large donut that repeats two numbers; the left card is half empty.

## Design

- **Basemap**: Vietnam's outline from Natural Earth 1:50m (public domain, via the
  `world-atlas` package), extracted once by `web/scripts/extract-vietnam.mjs` into
  `web/lib/vietnam.json` (no runtime download) and projected with `d3-geo` Mercator fitted to
  the frame. As Vietnamese maps must, the **Hoàng Sa** and **Trường Sa** archipelagos are drawn
  and labelled.
- **Symbols**: one circle per city at its coordinates, area proportional to the selected
  measure, translucent fill with a solid stroke, largest drawn first. Labels sit at the end
  of short leader lines on fixed sides so neighbours never collide; each label shows the city
  and its value.
- **Size legend**: three nested reference circles (the maximum, half and a quarter of it).
- **Measure switch**: Net revenue · Customers · Orders. The map, its accessible name and the
  ranking follow the measure; the ranking also shows customers, orders and net per customer.
- **Linked highlight**: hovering or focusing a city in the ranking or on the map highlights
  both.
- **Summary strip**: located net revenue and its share, top city, cities on the map, highest
  net per customer.
- **Regions**: North / Central / South share of located net revenue as one stacked bar with a
  marked legend (validated hues).
- **Coverage**: a single bar "known city vs location unknown" replaces the donut; the
  "Location unknown" card keeps profiles, orders and net revenue.
- Exact values stay in the "Exact city values" table.

## Revision: livelier symbols (after review)

The first symbols read as flat. Following Mapbox's blurred-circle halos, Flourish's
highlight/dim opacity and Datawrapper's haloed labels placed around the symbol:

- each bubble is a soft radial halo, a disc lit from the top left and a thin surface ring,
  so overlapping circles stay separable;
- colour names the region (North, Central, South), the same hues as the region bar and the
  ranking bars, so map, ranking and region split read as one system;
- labels are pill callouts with a region dot, city and value at the end of a leader line;
- bubbles grow in on load and the leading city carries a slow pulse ring; both stop under
  `prefers-reduced-motion`; hovering a city dims the others.

The basemap stays as approved (Viet Nam with Hoàng Sa and Trường Sa, no neighbouring countries).

## Tests

Playwright: the outline and both archipelago labels render; bubble radii follow the square
root of value; the size legend exists; the measure switch re-ranks cities and renames the
map; hover links ranking and bubble; region legend marks are visible and distinct; bubble and
ranking colours match the region legend; motion stops under reduced motion; existing
unknown-location, unavailable and phone-width tests still pass.

## Non-goals

No API change and no new geography data; cities without bundled coordinates are listed
under the map as today.
