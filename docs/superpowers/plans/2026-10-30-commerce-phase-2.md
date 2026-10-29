# Commerce Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox syntax.

**Goal:** Certify product and device commerce data end to end, then serve it in a **Products** workspace: a category/brand treemap, top products, and revenue, orders and AOV by device.

**Architecture:** Gold reads only pinned Silver (the MP-04 rule), so products enter as a new `silver.products` table parsed from the archived `master/products` Bronze rows, the same way `silver.campaigns` is built. A new `gold.commerce_daily` comes from the governed `silver.customer_events` used by campaign performance. Export picks it up automatically (every `gold.*` contract). dbt builds and tests it against campaign-performance finance; certification and publication carry it like the other Gold relations.

**Tech Stack:** PySpark/Iceberg, dbt-clickhouse, Dagster, FastAPI, Next.js, Recharts.

**Spec:** `docs/superpowers/specs/2026-10-30-richer-marketing-insights-design.md` (Phase 2)

## Global Constraints

- Branch `feat/commerce-phase-2`, stacked on `feat/insights-phase-1`; the PR targets that branch.
- The existing five Gold and five Silver contracts are unchanged; the two new tables are additive. The MP-01 generator and fixture are unchanged.
- Exact integers everywhere. Refunds are allocated to the original order's items in proportion to item gross, using integer division; each remainder goes in ascending `product_id` order.
- A missing device is the explicit value `unknown`. Device comes from the governed web/mobile `purchase` event that carries the same `order_id`.

## Review Focus

1. **Item gross ≠ order `gross_amount_minor`:** the Gold build must fail before writing (no silent mismatch). Test in Task 2.
2. **Order refunded on a later day:** the refund lands on the refund date's rows, with the original items and device. Test in Task 2.
3. **Unknown `product_id` in an order:** fail closed (no orphan category). Test in Task 2.
4. **Release without `commerce_daily`** (an older generation): the Products page shows unavailable, not an error crash. Test in Task 4.
5. **Mixed currency:** the existing guard applies with 409 and is never summed. Test in Task 4.

---

### Task 1: `silver.products`

Files: `src/marketpulse_lakehouse/contracts.py` (new contract, key `product_id`; columns `product_id, sku, name, category, brand, unit_price_minor:long, currency, source_file, source_row_number:long`), `src/marketpulse_lakehouse/silver.py` (pure `build_products(source_rows)` beside `build_campaigns`; Spark parse in `_build_dimension_frames` that rejects a duplicate `product_id`; returned as `products`), and the six-table Silver set in `gold.build_gold`, `verify.py` and `cli.py`. Tests: the Silver and contract tests, with counts updated to 14 contracts.

- [ ] RED: `test_build_products_parses_master_rows_and_rejects_duplicates` (fixture master/products yields 100 rows with a category in the MP-01 enum; a duplicate id raises `ValueError('duplicate product_id')`), plus contract-count tests now expecting the products table.
- [ ] GREEN, then the full Python suite. Commit `feat: add silver products from archived master data`.

### Task 2: `gold.commerce_daily`

Contract grain `(event_date, product_id, device_type, currency)`. Columns: `product_name, category, brand, orders:long, units:long, refunds:long, gross_revenue_minor:long, refund_amount_minor:long, net_revenue_minor:long`. Files: `contracts.py`; `gold.py` (pure `build_commerce_daily(events, products) -> tuple[dict]` mirroring the other pure builders, plus Spark `_commerce_frame(events, products)` registered in `build_gold`'s frames and `_validate_gold_frames`). Test: `tests/test_lakehouse_gold.py`.

- [ ] RED tests:
  - `test_commerce_daily_allocates_refunds_to_items_on_refund_date` (2 items 600/400, refund 500 → 300/200 on the refund date, with the original device);
  - `test_commerce_daily_rejects_item_gross_mismatch_and_unknown_product`;
  - `test_commerce_daily_totals_equal_campaign_performance_finance` (fixture: gross/refund/net sums equal the governed totals 27,160,000 / 4,967,000 / 22,193,000);
  - device `unknown` when no purchase event carries the order.
- [ ] GREEN, then the full Python suite. Commit `feat: build certified commerce daily gold model`.

### Task 3: dbt, Dagster and certification

Files: `dbt/marketpulse_serving/models/sources.yml` (`raw_commerce_daily`), `models/commerce_daily.sql` (order by the grain), `models/schema.yml` (not-null on the grain, accepted `device_type` ∈ {desktop, mobile, tablet, unknown}, accepted category enum), `tests/commerce_conservation.sql` (per currency, commerce gross/refund/net equal `campaign_performance_daily`), and `src/marketpulse_batch/definitions.py` (`certified_release` deps add `commerce_daily`). Update every test that pins relation or dbt-test counts.

- [ ] RED: definitions and certification tests expect `commerce_daily` among the certified relations; dbt parse lists the new model and test.
- [ ] GREEN, then the full Python suite and `dbt parse`. Commit `feat: certify and publish commerce daily serving relation`.

### Task 4: Serving read and Products UI

`InsightsService.products()` → `GET /api/v1/insights/products` (viewer). It returns `{status, currency, categories:[{category, net_revenue_minor, gross_revenue_minor, orders, units, brands:[{brand,…}]}], top_products:[{product_id, product_name, category, brand, units, orders, net_revenue_minor}] (top 10 by net), devices:[{device_type, orders, gross_revenue_minor, net_revenue_minor, aov_minor|None}], provenance}`. A missing relation gives `status: 'unavailable'`. It reconciles to `campaign_performance_daily` gross, and otherwise fails closed with `reconciliation_failed`. The UI is a `/products` page with a Recharts treemap (category, then brand, sized by net revenue and labeled), a top-products table, and device cards.

- [ ] RED Python and Playwright tests (including Review Focus 4 and 5).
- [ ] GREEN, then the full suites and build. Commit `feat: add products workspace from certified commerce data`.

### Task 5: Story-demo certification, oracles, docs and PR

- [ ] Rebuild the batch, app and journey images. Rerun the Dagster refresh for 2026-08-31; the new model digest gives a new release. Republish the journey projection.
- [ ] Oracles:
  - commerce gross equals certified gross;
  - per-category gross equals the generator `orders` files plus `master/products`, restricted to `order_created` events present in the projection (matched by order UUID);
  - the device total equals all orders.
- [ ] Update the architecture, runbook and backlog; run all suites; push; open the PR. Stop Docker afterwards.
