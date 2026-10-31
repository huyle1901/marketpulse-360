# Light and dark themes with charts first

## Intent

Marketers read the Command Center at a glance. The earlier pages led with tables and mixed a
dark navy band with a white page. Every workspace now leads with a chart, keeps exact numbers
one click away, and reads well in both a light and a dark theme.

## Decisions

- **Themes.** `:root` holds the light tokens and `.dark` the dark ones (`web/app/globals.css`).
  The system preference is the default; the sidebar toggle stores a per-browser choice in
  `localStorage` (`mp-theme`), and an inline script applies it before paint. Legacy page CSS is
  imported in `@layer legacy` and uses tokens only, never hard-coded colours.
- **Palettes.** Channel colours (`--chart-1..4`: Google, Meta, TikTok, unattributed) and
  categorical colours (`--cat-1..5`) are fixed per entity, validated for colour-vision
  deficiency in each theme, and never reassigned by rank. Status colours (`--positive`,
  `--destructive`) are reserved for state and always carry a text label.
- **Chart forms** (`web/components/charts.tsx`): `Donut` for a few parts of a whole, with the
  total in the centre and a named legend list of marks, exact values and shares; `BarList` for
  ranked magnitudes (negatives are written as text, never drawn); `Legend` for every
  multi-series chart; `ExactValues` keeps each former table behind a `details` disclosure.
- **Page mapping.** Overview channel donut; Ads share bars, per-channel small multiples and a
  spend-versus-ROAS scatter; Products, Geography and Email ranked bars and donuts; Insights
  web-versus-app bars and a loyalty donut; Journey analysis sessions by channel with purchase
  rate; Customers profile-mix donut, ranked opportunity bars and a token heatmap; Live activity
  provisional net by campaign; Data health a labelled source-availability meter.
- **Kept as tables.** Record lists that carry controls or audit evidence stay tables: customer
  profiles (drill into a timeline), accounts (role and enable controls) and the access audit.

## Non-goals

No change to APIs, data contracts, certification or the provisional/certified separation.
