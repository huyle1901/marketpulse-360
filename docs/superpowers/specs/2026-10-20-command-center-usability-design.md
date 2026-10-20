# Command Center usability improvements

## Intent and authorization

The user requested implementation of all five recommendations after a desktop/mobile audit: unified navigation, a decision-oriented campaign overview, readable operations, persistent filters and usable tables, and visual Customer 360 exploration. This is a product usability extension after MP-10, not a reimplementation of MP-01–10. Implement sequentially in this checkout on `feat/command-center-usability`, based on merged MP-10 (`origin/main`, `8eeede7`). Preserve untracked `work/`.

## Constraints

- Keep FastAPI + Next.js; add no runtime dependencies.
- ClickHouse owns analytics; PostgreSQL owns accounts, budgets, rules, segments, and audit metadata.
- Preserve exact integer strings and BigInt money; never sum currencies together.
- Preserve certified/provisional separation, release provenance, stale-result labels, race guards, consent, and RBAC.
- Missing data stays unavailable; never invent historical points, prior periods, or cohort zeros.
- All mandatory services remain runnable through Docker Compose profiles.

## 1. One application shell

SessionGate owns one role-aware navigation with current-page indication, consistent names, brand and account actions. Remove duplicate workspace menus. Add a keyboard skip link. On narrow screens navigation wraps without document overflow. Add local section links on long pages and group campaign configuration in native expandable panels; overview remains primary. Existing forms/actions remain available with original permissions.

## 2. Campaign decisions

Extend the existing authenticated dashboard response (no new route) with `certified.daily_series`: one row per observed event date/currency, summing existing certified fields with exact wire integers. Include `certified.comparison` with equal-length immediately preceding UTC `[from,to)`, observed totals per currency, observed/expected day counts, and `status` complete/partial/unavailable. One pinned release serves current, previous, and coverage reads. Completeness is based on published daily source coverage before dimension filters. No percentage comparison is offered for partial/unavailable coverage or nonpositive previous values.

Render dependency-free SVG revenue/spend lines per currency, with an accessible exact-value table and explicit missing-day note. Draw only observed consecutive days; gaps break the line. Currency presentation uses currency-specific minor-unit precision while retaining exact raw amounts in details/forms. ROAS is displayed as a readable multiple with source precision available. Show the selected UTC interval, certified data-through, and up to three relevant alert explanations near the overview. Alert shortcuts link into the matching campaign selection; only alerts with matching current release, overlapping selected dates, and a campaign present in the displayed result enter the overview. Historical alert evidence remains accessible below.

## 3. Readable Operations & Trust

Replace default raw JSON panels with service health summaries, newest live quality result, probe state, validated lineage/snapshot counts, and session retention summaries. Technical JSON remains in expandable details. Unknown telemetry or absent reports stays unknown, not healthy. Quality history is collapsed as a group. Keep existing admin controls, errors, request-order protection, and audit evidence. Use normal full-width page flow instead of applying the two-column budget grid to the main element.

## 4. Filters and tables

Persist non-sensitive filter/model/page and local view preferences through allowlisted query parameters. Deep links, reload, and browser back/forward restore controls and request context. Each workspace keeps its own query state. Campaign rows link to Analysis with shared dates/campaign/dimensions. Add published-date-anchored last-day/last-seven-day presets, reset, clear selection context, and a copy-view-link action with success/failure feedback. Presets respect the existing 366-day API limit and do not imply missing history exists.

Campaign tables support name/ID search and exact numeric sorting; profile search/sort explicitly applies to the currently loaded page (the API remains bounded at 20). No claim of server-wide search. Analysis model and customer offsets/purpose remain shareable. Unknown query parameters are ignored, input enums normalized, invalid offsets reset; API validation remains authoritative for malformed dates and identifiers.

## 5. Customer exploration

Replace the default retention list with an accessible heatmap table: cohort/identity rows, observed month-index columns, retention percentage and active/original-cohort denominator in each observed cell, explicit unavailable markers elsewhere. Colors supplement text. Preserve the existing cohort drill action and raw source details; viewer users receive aggregate heatmap only without profile reads.

Keep profile detail in a focused labelled in-page panel adjacent to the profile list, with loading/error/consent states, profile summary, timeline, and close-and-return focus. A modeless panel permits switching profile without trapping focus or hiding filters. Saved segment builder receives a concise live rule summary and retains revision/digest/preview behavior. Do not infer persisted segment membership: existing preview is bounded and does not persist members.

## Verification and scope boundaries

Add failing Python tests for daily aggregation, prior-period coverage, filtered coverage, exact huge/negative values and same-release query pinning; then implement. Add failing browser behavior tests per improvement before implementation, retaining existing auth/consent/race coverage. Run full Python/browser suites, TypeScript/build, fixture validation, Compose configuration and diff checks. Build/recreate only app API/web in the retained Docker proof project, verify real authenticated results against the published release and capture desktop/mobile screenshots of all four workspaces. No data generation, pipeline refresh, budget/rule edits, analytical deletion, or account changes are needed for acceptance.

Implementation calls delegated to the builder: exact spacing, SVG colors, panel copy, component decomposition, sort defaults, and which native details start open. All five requested improvements ship; committing locally does not merge or publish them.
