from __future__ import annotations

import json
import unittest

from marketpulse_lakehouse.contracts import TABLE_CONTRACTS
from datetime import UTC, date, datetime
from decimal import Decimal

from marketpulse_lakehouse.gold import (
    _exact_division_expression,
    _validate_gold_observations,
    build_attribution_daily,
    build_campaign_performance_daily,
    build_cohort_retention,
    build_customer_360,
    build_funnel_daily,
)


def _event(
    event_id: str,
    event_name: str,
    when: str,
    *,
    profile: str = "customer:c-1",
    session: str = "session-1",
    campaign: str | None = "campaign-a",
    click: str | None = None,
    order: str | None = None,
    gross: int | None = None,
    refund: int | None = None,
    bot: bool = False,
) -> dict[str, object]:
    instant = datetime.fromisoformat(when.replace("Z", "+00:00")).astimezone(UTC)
    return {
        "event_id": event_id,
        "event_name": event_name,
        "event_time": instant,
        "event_date": instant.date(),
        "source": "commerce" if order else "web",
        "profile_key": profile,
        "session_id": session,
        "campaign_id": campaign,
        "click_id": click,
        "is_bot_session": bot,
        "order_id": order,
        "gross_amount_minor": gross,
        "refund_amount_minor": refund,
        "currency": "VND" if order else None,
    }


class GoldModelTests(unittest.TestCase):
    def test_spark_linear_allocation_uses_integral_division(self) -> None:
        self.assertEqual(
            _exact_division_expression("gross_minor"),
            "`gross_minor` DIV `touch_count`",
        )
        with self.assertRaisesRegex(ValueError, "unsupported"):
            _exact_division_expression("unsafe")

    def test_gold_validation_fails_before_write_on_schema_nulls_or_duplicate_keys(self) -> None:
        types = (
            ("event_date", "date"),
            ("campaign_id", "string"),
            ("sessions", "long"),
            ("page_view_sessions", "long"),
            ("view_item_sessions", "long"),
            ("add_to_cart_sessions", "long"),
            ("checkout_sessions", "long"),
            ("purchase_sessions", "long"),
        )
        _validate_gold_observations(
            "funnel_daily",
            actual_types=types,
            has_null_required=False,
            has_duplicate_key=False,
        )
        with self.assertRaisesRegex(ValueError, "schema mismatch"):
            _validate_gold_observations(
                "funnel_daily",
                actual_types=types[:-1],
                has_null_required=False,
                has_duplicate_key=False,
            )
        with self.assertRaisesRegex(ValueError, "null required"):
            _validate_gold_observations(
                "funnel_daily",
                actual_types=types,
                has_null_required=True,
                has_duplicate_key=False,
            )
        with self.assertRaisesRegex(ValueError, "duplicate keys"):
            _validate_gold_observations(
                "funnel_daily",
                actual_types=types,
                has_null_required=False,
                has_duplicate_key=True,
            )

    def test_funnel_counts_nonsequential_distinct_sessions_and_excludes_bots(self) -> None:
        events = [
            _event("e-1", "page_view", "2026-01-02T01:00:00Z"),
            _event("e-2", "add_to_cart", "2026-01-02T01:01:00Z"),
            _event("e-3", "page_view", "2026-01-02T02:00:00Z", session="s-2"),
            _event(
                "e-4",
                "purchase",
                "2026-01-02T02:01:00Z",
                session="s-2",
            ),
            _event(
                "e-5",
                "page_view",
                "2026-01-02T03:00:00Z",
                session="bot",
                bot=True,
            ),
        ]

        rows = build_funnel_daily(events)

        self.assertEqual(
            rows,
            (
                {
                    "event_date": date(2026, 1, 2),
                    "campaign_id": "campaign-a",
                    "sessions": 2,
                    "page_view_sessions": 2,
                    "view_item_sessions": 0,
                    "add_to_cart_sessions": 1,
                    "checkout_sessions": 0,
                    "purchase_sessions": 1,
                },
            ),
        )

    def test_first_last_linear_and_unattributed_refund_reuse_order_touches(self) -> None:
        events = [
            _event(
                "touch-a",
                "page_view",
                "2026-01-01T01:00:00Z",
                campaign="campaign-a",
                click="click-a",
            ),
            _event(
                "touch-b",
                "page_view",
                "2026-01-02T01:00:00Z",
                campaign="campaign-b",
                click="click-b",
            ),
            _event(
                "order-1-event",
                "order_created",
                "2026-01-03T01:00:00Z",
                order="order-1",
                gross=101,
            ),
            _event(
                "refund-1-event",
                "order_refunded",
                "2026-01-04T01:00:00Z",
                campaign=None,
                order="order-1",
                gross=101,
                refund=11,
            ),
            _event(
                "order-2-event",
                "order_created",
                "2026-01-03T02:00:00Z",
                profile="anonymous:no-touch",
                session="s-2",
                campaign=None,
                order="order-2",
                gross=10,
            ),
        ]

        rows = build_attribution_daily(events)
        indexed = {
            (row["model"], row["event_date"], row["campaign_id"]): row
            for row in rows
        }

        self.assertEqual(
            indexed[("first_touch", date(2026, 1, 3), "campaign-a")][
                "allocated_gross_minor"
            ],
            101,
        )
        self.assertEqual(
            indexed[("last_touch", date(2026, 1, 3), "campaign-b")][
                "allocated_gross_minor"
            ],
            101,
        )
        self.assertEqual(
            indexed[("linear", date(2026, 1, 3), "campaign-a")][
                "allocated_gross_minor"
            ],
            51,
        )
        self.assertEqual(
            indexed[("linear", date(2026, 1, 3), "campaign-b")][
                "allocated_gross_minor"
            ],
            50,
        )
        self.assertEqual(
            indexed[("linear", date(2026, 1, 4), "campaign-a")][
                "allocated_refund_minor"
            ],
            6,
        )
        self.assertEqual(
            indexed[("linear", date(2026, 1, 4), "campaign-b")][
                "allocated_refund_minor"
            ],
            5,
        )
        for model in ("first_touch", "last_touch", "linear"):
            unattributed = indexed[(model, date(2026, 1, 3), "__unattributed__")]
            self.assertFalse(unattributed["is_attributed"])
            self.assertEqual(unattributed["allocated_gross_minor"], 10)
            self.assertEqual(
                sum(row["allocated_gross_minor"] for row in rows if row["model"] == model),
                111,
            )
            self.assertEqual(
                sum(row["allocated_order_count"] for row in rows if row["model"] == model),
                Decimal("2.000000"),
            )
        self.assertEqual(
            indexed[("linear", date(2026, 1, 3), "campaign-a")][
                "allocated_order_count"
            ],
            Decimal("0.500000"),
        )

    def test_customer_360_and_cohort_keep_anonymous_profiles_without_contact_fields(self) -> None:
        events = [
            _event("known-1", "page_view", "2026-01-02T01:00:00Z"),
            _event(
                "known-order",
                "order_created",
                "2026-02-02T01:00:00Z",
                order="known-order",
                gross=20,
            ),
            _event(
                "known-refund",
                "order_refunded",
                "2026-02-03T01:00:00Z",
                order="known-order",
                gross=20,
                refund=7,
            ),
            _event(
                "anon-1",
                "page_view",
                "2026-02-03T01:00:00Z",
                profile="anonymous:a-1",
                session="anon-session",
                campaign="campaign-b",
            ),
        ]
        customers = [
            {
                "customer_id": "c-1",
                "created_at": datetime(2025, 12, 1, tzinfo=UTC),
                "consent_analytics": True,
                "consent_marketing": False,
            }
        ]

        profiles = build_customer_360(events, customers)
        retention = build_cohort_retention(events, profiles)

        by_key = {row["profile_key"]: row for row in profiles}
        self.assertEqual(by_key["customer:c-1"]["cohort_month"], date(2025, 12, 1))
        self.assertEqual(by_key["customer:c-1"]["gross_revenue_minor"], 20)
        self.assertEqual(by_key["customer:c-1"]["net_revenue_minor"], 13)
        self.assertEqual(by_key["anonymous:a-1"]["profile_type"], "anonymous")
        self.assertEqual(
            by_key["anonymous:a-1"]["acquisition_campaign_id"], "campaign-b"
        )
        self.assertNotIn("email", by_key["customer:c-1"])
        known_months = {
            (row["activity_month"], row["active_profiles"], row["retained_rate"])
            for row in retention
            if row["profile_type"] == "customer"
        }
        self.assertIn((date(2026, 1, 1), 1, Decimal("1.000000")), known_months)
        self.assertIn((date(2026, 2, 1), 1, Decimal("1.000000")), known_months)

    def test_campaign_performance_full_outer_join_and_zero_spend_roas(self) -> None:
        spend = [
            {
                "spend_date": date(2026, 1, 2),
                "campaign_id": "campaign-a",
                "platform": "google",
                "impressions": 100,
                "clicks": 10,
                "conversions": 2,
                "spend_minor": 25,
                "conversion_value_minor": 60,
                "currency": "VND",
            }
        ]
        funnel = build_funnel_daily(
            [_event("e", "page_view", "2026-01-02T01:00:00Z")]
        )
        events = [
            _event(
                "order",
                "order_created",
                "2026-01-02T02:00:00Z",
                order="order-1",
                gross=50,
            )
        ]
        attribution = build_attribution_daily(events)
        campaigns = [
            {
                "campaign_id": "campaign-a",
                "platform": "google",
                "name": "Search",
                "currency": "VND",
            }
        ]

        rows = build_campaign_performance_daily(
            spend, funnel, events, attribution, campaigns
        )
        indexed = {(row["event_date"], row["campaign_id"]): row for row in rows}

        paid = indexed[(date(2026, 1, 2), "campaign-a")]
        self.assertEqual(paid["source_roas"], Decimal("2.000000"))
        unattributed = indexed[(date(2026, 1, 2), "__unattributed__")]
        self.assertEqual(unattributed["first_touch_gross_minor"], 50)
        self.assertIsNone(unattributed["source_roas"])


if __name__ == "__main__":
    unittest.main()


class GoldSilverInputTests(unittest.TestCase):
    def test_gold_requires_every_silver_contract_including_products(self) -> None:
        from marketpulse_lakehouse.gold import build_gold
        five = {f"silver.{name}": 1 for name in ("customer_events", "customers", "campaigns", "ad_spend_daily", "quality_summary")}
        with self.assertRaisesRegex(ValueError, "silver.products"):
            build_gold(None, silver_snapshots=five, run_id="r")


def _commerce(event_id: str, name: str, when: str, order: str, items: list[tuple[str, int, int]], *, gross: int | None = None, refund: int | None = None) -> dict[str, object]:
    total = sum(quantity * price for _, quantity, price in items)
    event = _event(event_id, name, when, order=order, gross=total if gross is None and name == "order_created" else gross,
                   refund=refund if name == "order_refunded" else 0)
    event["payload_json"] = json.dumps({"order_id": order, "items": [{"product_id": p, "quantity": q, "unit_price_minor": u} for p, q, u in items]})
    return event


def _purchase(event_id: str, when: str, order: str, device: str) -> dict[str, object]:
    event = _event(event_id, "purchase", when)
    event["order_id"] = order
    event["payload_json"] = json.dumps({"order_id": order, "device_type": device, "is_bot": False})
    return event


PRODUCTS = [{"product_id": "p-1", "name": "Phone", "category": "electronics", "brand": "Nova"},
            {"product_id": "p-2", "name": "Lamp", "category": "home", "brand": "Lumen"}]


class CommerceDailyTests(unittest.TestCase):
    def test_commerce_daily_allocates_refunds_to_items_on_refund_date(self) -> None:
        from marketpulse_lakehouse.gold import build_commerce_daily
        events = [_purchase("w", "2026-01-02T01:00:00Z", "o-1", "mobile"),
                  _commerce("c", "order_created", "2026-01-02T01:00:01Z", "o-1", [("p-1", 2, 300), ("p-2", 1, 400)]),
                  _commerce("r", "order_refunded", "2026-01-05T09:00:00Z", "o-1", [("p-1", 2, 300), ("p-2", 1, 400)], refund=501)]
        rows = {(r["event_date"].isoformat(), r["product_id"]): r for r in build_commerce_daily(events, PRODUCTS)}
        phone, lamp = rows[("2026-01-02", "p-1")], rows[("2026-01-02", "p-2")]
        self.assertEqual((phone["device_type"], phone["units"], phone["orders"], phone["gross_revenue_minor"], phone["category"]), ("mobile", 2, 1, 600, "electronics"))
        self.assertEqual((lamp["gross_revenue_minor"], lamp["refund_amount_minor"]), (400, 0))
        refund_phone, refund_lamp = rows[("2026-01-05", "p-1")], rows[("2026-01-05", "p-2")]
        # 501 * 600/1000 = 300 r6, 501 * 400/1000 = 200 r4: one remaining minor unit goes to the lowest product_id.
        self.assertEqual((refund_phone["refund_amount_minor"], refund_lamp["refund_amount_minor"]), (301, 200))
        self.assertEqual((refund_phone["refunds"], refund_phone["gross_revenue_minor"], refund_phone["net_revenue_minor"], refund_phone["device_type"]), (1, 0, -301, "mobile"))
        self.assertEqual(tuple(phone), tuple(c.name for c in TABLE_CONTRACTS["gold.commerce_daily"].columns))

    def test_commerce_daily_rejects_item_gross_mismatch_and_unknown_product(self) -> None:
        from marketpulse_lakehouse.gold import build_commerce_daily
        with self.assertRaisesRegex(ValueError, "item gross"):
            build_commerce_daily([_commerce("c", "order_created", "2026-01-02T01:00:00Z", "o-1", [("p-1", 1, 300)], gross=999)], PRODUCTS)
        with self.assertRaisesRegex(ValueError, "unknown product"):
            build_commerce_daily([_commerce("c", "order_created", "2026-01-02T01:00:00Z", "o-1", [("p-9", 1, 300)])], PRODUCTS)

    def test_commerce_daily_totals_equal_campaign_performance_and_missing_device_is_unknown(self) -> None:
        from marketpulse_lakehouse.gold import build_campaign_performance_daily, build_commerce_daily, build_funnel_daily
        events = [_commerce("c1", "order_created", "2026-01-02T01:00:00Z", "o-1", [("p-1", 3, 333), ("p-2", 1, 7)]),
                  _commerce("c2", "order_created", "2026-01-03T01:00:00Z", "o-2", [("p-2", 5, 100)]),
                  _commerce("r1", "order_refunded", "2026-01-04T01:00:00Z", "o-1", [("p-1", 3, 333), ("p-2", 1, 7)], refund=999)]
        rows = build_commerce_daily(events, PRODUCTS)
        self.assertTrue(all(r["device_type"] == "unknown" for r in rows))
        performance = build_campaign_performance_daily([], build_funnel_daily([]), events, [], [{"campaign_id": "campaign-a", "platform": "google", "currency": "VND"}])
        for field in ("gross_revenue_minor", "refund_amount_minor", "net_revenue_minor"):
            self.assertEqual(sum(r[field] for r in rows), sum(r[field] for r in performance), field)
