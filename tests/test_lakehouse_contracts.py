from __future__ import annotations

import unittest

from marketpulse_lakehouse.contracts import TABLE_CONTRACTS


class LakehouseContractTests(unittest.TestCase):
    def test_all_thirteen_table_names_are_stable(self) -> None:
        self.assertEqual(
            tuple(TABLE_CONTRACTS),
            (
                "bronze.kafka_records",
                "bronze.source_files",
                "bronze.source_file_rows",
                "silver.customer_events",
                "silver.customers",
                "silver.campaigns",
                "silver.products",
                "silver.ad_spend_daily",
                "silver.quality_summary",
                "gold.funnel_daily",
                "gold.attribution_daily",
                "gold.customer_360",
                "gold.cohort_retention",
                "gold.campaign_performance_daily",
            ),
        )

    def test_ordered_columns_and_partitions_are_contractual(self) -> None:
        expected = {
            "bronze.kafka_records": (
                ("ingest_id", "source_topic", "source_partition", "source_offset", "kafka_timestamp", "kafka_timestamp_type", "key_base64", "key_sha256", "headers_json", "payload_base64", "payload_text", "payload_sha256", "ingestion_run_id", "ingestion_manifest_sha256", "ingested_at"),
                ("ingest_id",),
                ("days(kafka_timestamp)",),
            ),
            "bronze.source_files": (
                ("file_sha256", "source_dataset", "source_file", "source_object_uri", "compressed_size_bytes", "manifest_sha256", "ingestion_run_id", "archived_at"),
                ("file_sha256",),
                ("source_dataset",),
            ),
            "bronze.source_file_rows": (
                ("ingest_id", "source_dataset", "source_file", "source_object_uri", "row_number", "source_date", "file_sha256", "payload_json", "payload_sha256", "ingestion_run_id", "ingested_at"),
                ("ingest_id",),
                ("source_dataset", "source_date"),
            ),
            "silver.customer_events": (
                ("event_id", "event_name", "event_time", "ingested_at", "event_date", "source", "channel", "customer_id", "anonymous_id", "profile_key", "session_id", "campaign_id", "campaign_key", "click_id", "consent_analytics", "late_status", "is_bot_session", "order_id", "gross_amount_minor", "refund_amount_minor", "currency", "payload_json", "source_payload_sha256", "lineage_topic", "lineage_partition", "lineage_offset"),
                ("event_id",),
                ("months(event_date)",),
            ),
            "silver.customers": (
                ("customer_id", "created_at", "snapshot_date", "loyalty_tier", "lifetime_value_minor", "last_order_at", "consent_analytics", "consent_marketing", "source_file", "source_row_number"),
                ("customer_id",),
                (),
            ),
            "silver.campaigns": (
                ("campaign_id", "platform", "platform_campaign_id", "name", "objective", "status", "start_date", "end_date", "daily_budget_minor", "currency", "ad_count", "source_file", "source_row_number"),
                ("campaign_id",),
                (),
            ),
            "silver.products": (
                ("product_id", "sku", "name", "category", "brand", "unit_price_minor", "currency", "source_file", "source_row_number"),
                ("product_id",),
                (),
            ),
            "silver.ad_spend_daily": (
                ("spend_key", "spend_date", "campaign_id", "platform", "platform_campaign_id", "impressions", "clicks", "conversions", "spend_minor", "conversion_value_minor", "currency", "source_file", "source_row_number"),
                ("spend_key",),
                ("months(spend_date)",),
            ),
            "silver.quality_summary": (
                ("event_date", "source_topic", "reason_code", "record_count", "distinct_event_count", "run_id"),
                ("event_date", "source_topic", "reason_code"),
                ("months(event_date)",),
            ),
            "gold.funnel_daily": (
                ("event_date", "campaign_id", "sessions", "page_view_sessions", "view_item_sessions", "add_to_cart_sessions", "checkout_sessions", "purchase_sessions"),
                ("event_date", "campaign_id"),
                ("months(event_date)",),
            ),
            "gold.attribution_daily": (
                ("model", "event_date", "campaign_id", "is_attributed", "allocated_order_count", "allocated_gross_minor", "allocated_refund_minor", "allocated_net_minor"),
                ("model", "event_date", "campaign_id"),
                ("months(event_date)",),
            ),
            "gold.customer_360": (
                ("profile_key", "customer_id", "anonymous_id", "profile_type", "first_activity_at", "last_activity_at", "event_count", "session_count", "order_count", "refund_count", "gross_revenue_minor", "refund_amount_minor", "net_revenue_minor", "acquisition_campaign_id", "cohort_month", "consent_analytics", "consent_marketing"),
                ("profile_key",),
                (),
            ),
            "gold.cohort_retention": (
                ("cohort_month", "activity_month", "profile_type", "month_number", "cohort_size", "active_profiles", "retained_rate"),
                ("cohort_month", "activity_month", "profile_type"),
                ("months(cohort_month)",),
            ),
            "gold.campaign_performance_daily": (
                ("event_date", "campaign_id", "platform", "currency", "impressions", "clicks", "vendor_conversions", "spend_minor", "sessions", "page_view_sessions", "view_item_sessions", "add_to_cart_sessions", "checkout_sessions", "purchase_sessions", "orders", "refunds", "gross_revenue_minor", "refund_amount_minor", "net_revenue_minor", "first_touch_gross_minor", "first_touch_refund_minor", "first_touch_net_minor", "last_touch_gross_minor", "last_touch_refund_minor", "last_touch_net_minor", "linear_gross_minor", "linear_refund_minor", "linear_net_minor", "source_roas", "first_touch_roas", "last_touch_roas", "linear_roas"),
                ("event_date", "campaign_id"),
                ("months(event_date)",),
            ),
        }

        actual = {
            name: (
                tuple(column.name for column in contract.columns),
                contract.key_fields,
                contract.partition_by,
            )
            for name, contract in TABLE_CONTRACTS.items()
        }
        self.assertEqual(actual, expected)

    def test_operational_columns_are_excluded_from_business_checksums(self) -> None:
        kafka = TABLE_CONTRACTS["bronze.kafka_records"]
        self.assertEqual(
            kafka.business_columns,
            tuple(
                name
                for name in (column.name for column in kafka.columns)
                if name not in {"ingestion_run_id", "ingestion_manifest_sha256", "ingested_at"}
            ),
        )


if __name__ == "__main__":
    unittest.main()
