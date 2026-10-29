# Geography Phase 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox syntax.

**Goal:** Certify customer city/country and serve a **Geography** workspace: revenue, customers and orders per city, with anonymous activity shown separately as "location unknown".

**Architecture:** Add `country` and `city` (non-null, from `master/customers`) to `silver.customers`, which is already exported as the `customers` serving relation. Geography joins `customer_360` to `customers` at serving time, the same pattern as Phase 1 loyalty, so Gold stays unchanged. The dbt `required_fields` gate lists the new columns. The UI plots bubbles at real city coordinates bundled in the app; there is no external map or geodata download.

**Spec:** `docs/superpowers/specs/2026-10-30-richer-marketing-insights-design.md` (Phase 3)

## Global Constraints

- Branch `feat/geography-phase-3`, stacked on `feat/commerce-phase-2`.
- Changes are additive to `silver.customers`. MP-01 data and the fixture are unchanged.
- City totals reconcile exactly with `customer_360` net. Profiles that are not customers, or have no CRM row, form a separate `location unknown` group and are never assigned a city.
- An older release whose `customers` relation lacks `city` returns `unavailable`.

## Review Focus

1. **Release without the `city` column:** `unavailable`, not a SQL error. Test in Task 2.
2. **Customer without a CRM row:** counted under location unknown. Test in Task 2.
3. **City outside the bundled coordinate table:** listed in the table, and not drawn at an invented position. Test in Task 3.
4. **Negative city net:** the bubble size uses absolute positive revenue only; a negative city is listed but not drawn. Test in Task 3.
5. **Mixed currency:** existing guard. Test in Task 2.

### Task 1: `silver.customers` country and city

- [ ] RED: the contract test lists `country, city` after `consent_marketing`; the pure `build_customers` fixture rows carry each master customer's city; dbt `required_fields` for `customers` includes both (schema.yml).
- [ ] GREEN: contract, pure builder, Spark master parse and join, and schema.yml. Full suite plus `dbt parse` in the batch image. Commit `feat: certify customer country and city in silver customers`.

### Task 2: Geography read

`InsightsService.geography() -> {status, currency, cities:[{country, city, customers, orders, net_revenue_minor, net_per_customer_minor|None}], unknown:{profiles, orders, net_revenue_minor}, total_net_revenue_minor, provenance}`, via `GET /api/v1/insights/geography` (viewer; policy and proxy). It fails closed with `reconciliation_failed` unless cities plus unknown equal the `customer_360` net.

- [ ] RED/GREEN Python tests for Review Focus 1, 2 and 5 plus the route. Commit `feat: serve certified revenue by customer city`.

### Task 3: Geography UI

A `/geography` page with an SVG bubble plot at bundled coordinates (`lib/cities.ts`: name → lat/lon for the five generator cities). Bubble area is proportional to positive net, and each bubble carries a direct label. A ranked table and a location-unknown card follow. Nav item `Geography`.

- [ ] RED/GREEN Playwright tests for Review Focus 3 and 4, a phone fit, and unavailable. Full browser suite and build. Commit `feat: add geography workspace`.

### Task 4: Story-demo certification, oracle, docs, PR

- [ ] Rebuild; Flink to zero lag; refresh 2026-08-31; publish the journey projection.
- [ ] Oracle: per-city net equals the generator master city of each `customer_360` customer (from the export), summed independently.
- [ ] Docs; all suites; push; open the PR against `feat/commerce-phase-2`; stop Docker.
