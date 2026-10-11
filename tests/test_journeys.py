from __future__ import annotations

import unittest
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from marketpulse_mock.config import GenerationConfig, ScaleProfile, resolve_config
from marketpulse_mock.journeys import PaidTouch, iter_day_records, last_paid_touch
from marketpulse_mock.masters import generate_master_data


class AttributionTests(unittest.TestCase):
    def test_last_paid_touch_selects_latest_click_inside_seven_day_window(self) -> None:
        purchase_time = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
        touches = (
            PaidTouch("old", "campaign-old", "google", purchase_time - timedelta(days=8)),
            PaidTouch("meta", "campaign-meta", "meta", purchase_time - timedelta(days=2)),
            PaidTouch("tiktok", "campaign-tiktok", "tiktok", purchase_time - timedelta(hours=2)),
        )

        selected = last_paid_touch(touches, purchase_time)

        self.assertIsNotNone(selected)
        self.assertEqual("tiktok", selected.click_id)

    def test_last_paid_touch_rejects_future_and_expired_clicks(self) -> None:
        purchase_time = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
        touches = (
            PaidTouch("expired", "campaign", "google", purchase_time - timedelta(days=8)),
            PaidTouch("future", "campaign", "meta", purchase_time + timedelta(seconds=1)),
        )

        self.assertIsNone(last_paid_touch(touches, purchase_time))


class JourneyGenerationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = resolve_config(
            "fixture", seed=360, output_dir=Path("unused"), start_date=date(2026, 1, 1)
        )
        cls.masters = generate_master_data(cls.config)
        cls.records = list(iter_day_records(cls.config, cls.masters, 0))

    def test_purchase_and_order_created_share_identity_and_amount(self) -> None:
        web_purchases = {
            record.payload["payload"]["order_id"]: record.payload
            for record in self.records
            if record.dataset == "web_events"
            and record.payload["event_name"] == "purchase"
        }
        order_events = {
            record.payload["payload"]["order_id"]: record.payload
            for record in self.records
            if record.dataset == "orders"
            and record.payload["event_name"] == "order_created"
        }

        self.assertTrue(web_purchases)
        self.assertEqual(set(web_purchases), set(order_events))
        for order_id, web_event in web_purchases.items():
            order_event = order_events[order_id]
            self.assertEqual(web_event["customer_id"], order_event["customer_id"])
            self.assertEqual(web_event["session_id"], order_event["session_id"])
            self.assertEqual(
                web_event["payload"]["amount_minor"],
                order_event["payload"]["gross_amount_minor"],
            )

    def test_paid_click_ids_and_campaign_ids_cross_source_boundaries(self) -> None:
        campaigns = {campaign.campaign_id for campaign in self.masters.campaigns}
        clicked_events = [
            record.payload
            for record in self.records
            if record.dataset == "web_events" and record.payload["click_id"]
        ]
        attributed_orders = [
            record.payload
            for record in self.records
            if record.dataset == "orders"
            and record.payload["event_name"] == "order_created"
        ]

        self.assertTrue(clicked_events)
        self.assertTrue(attributed_orders)
        self.assertTrue(
            all(event["campaign_id"] in campaigns for event in clicked_events)
        )
        self.assertTrue(
            all(order["payload"]["attributed_campaign_id"] in campaigns for order in attributed_orders)
        )

    def test_vendor_reports_reconcile_with_ground_truth(self) -> None:
        truth = {
            record.payload["campaign_id"]: record.payload
            for record in self.records
            if record.dataset == "ground_truth"
        }
        campaigns_by_vendor = {
            campaign.platform_campaign_id: campaign
            for campaign in self.masters.campaigns
        }

        reports = [
            record
            for record in self.records
            if record.dataset in {"google_ads", "meta_ads", "tiktok_ads"}
        ]
        self.assertEqual(len(self.masters.campaigns), len(reports))
        for report in reports:
            campaign = campaigns_by_vendor[report.payload["campaign_id"]]
            expected = truth[campaign.campaign_id]
            self.assertEqual(expected["impressions"], int(report.payload["impressions"]))
            self.assertEqual(expected["clicks"], int(report.payload["clicks"]))
            if report.dataset == "google_ads":
                self.assertEqual(expected["orders"], report.payload["conversions"])
                self.assertEqual(
                    expected["gross_revenue_minor"],
                    report.payload["conversions_value"],
                )
            elif report.dataset == "meta_ads":
                self.assertEqual(
                    expected["orders"], int(report.payload["actions"][0]["value"])
                )
                self.assertEqual(
                    expected["gross_revenue_minor"],
                    int(report.payload["action_values"][0]["value"]),
                )
            else:
                self.assertEqual(expected["orders"], report.payload["conversion"])
                self.assertEqual(
                    expected["gross_revenue_minor"],
                    int(report.payload["total_purchase_value"]),
                )

    def test_refunds_reduce_net_revenue_without_erasing_orders(self) -> None:
        truth_records = [
            record.payload
            for record in self.records
            if record.dataset == "ground_truth"
        ]
        refund_events = [
            record
            for record in self.records
            if record.dataset == "orders"
            and record.payload["event_name"] == "order_refunded"
        ]

        self.assertTrue(refund_events)
        self.assertGreater(sum(row["orders"] for row in truth_records), 0)
        self.assertGreater(sum(row["refunds_minor"] for row in truth_records), 0)
        for row in truth_records:
            self.assertEqual(
                row["gross_revenue_minor"] - row["refunds_minor"],
                row["net_revenue_minor"],
            )

    def test_all_records_stay_in_the_requested_utc_partition(self) -> None:
        self.assertTrue(self.records)
        self.assertTrue(
            all(record.event_time.date() == self.config.start_date for record in self.records)
        )
        self.assertTrue(
            all(record.event_time.tzinfo is timezone.utc for record in self.records)
        )

    def test_anomaly_counts_are_nonzero_and_deterministic(self) -> None:
        repeated = list(iter_day_records(self.config, self.masters, 0))

        def counts(records):
            event_ids = [
                record.payload["event_id"]
                for record in records
                if record.dataset in {"web_events", "orders", "email_events"}
            ]
            return {
                "duplicates": sum(count - 1 for count in Counter(event_ids).values()),
                "late": sum(
                    record.payload.get("is_late", False)
                    for record in records
                    if record.dataset in {"web_events", "orders", "email_events"}
                ),
                "quality": sum(record.dataset == "quality_cases" for record in records),
                "bots": sum(
                    record.dataset == "web_events"
                    and record.payload["payload"].get("is_bot", False)
                    for record in records
                ),
                "refunds": sum(
                    record.dataset == "orders"
                    and record.payload["event_name"] == "order_refunded"
                    for record in records
                ),
            }

        first_counts = counts(self.records)
        self.assertEqual(first_counts, counts(repeated))
        self.assertTrue(all(value > 0 for value in first_counts.values()))

    def test_every_campaign_has_a_report_even_when_click_count_is_zero(self) -> None:
        profile = ScaleProfile(
            name="tiny",
            days=1,
            customers=3,
            campaigns=3,
            products=3,
            impressions_per_day=1,
            rows_per_file=10,
        )
        config = GenerationConfig(
            profile=profile,
            seed=360,
            output_dir=Path("unused"),
            start_date=date(2026, 1, 1),
            rows_per_file=10,
        )
        records = list(iter_day_records(config, generate_master_data(config), 0))
        reports = [
            record
            for record in records
            if record.dataset in {"google_ads", "meta_ads", "tiktok_ads"}
        ]

        self.assertEqual(3, len(reports))
        self.assertGreaterEqual(
            sum(int(report.payload["clicks"]) == 0 for report in reports), 2
        )


if __name__ == "__main__":
    unittest.main()
