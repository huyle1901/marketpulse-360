from __future__ import annotations

import hashlib
import json
import unittest
from datetime import UTC, date, datetime
from pathlib import Path

from marketpulse_lakehouse.contracts import TABLE_CONTRACTS
from marketpulse_lakehouse.silver import (
    build_ad_spend_daily,
    build_campaigns,
    build_products,
    build_customers,
    govern_customer_events,
    parse_clean_bronze_record,
    recover_raw_click_id,
    parse_dlq_bronze_record,
    reconcile_kafka_outcomes,
    validate_namespace,
)
from marketpulse_lakehouse.source_files import (
    build_source_archive_plan,
    normalize_source_file,
)


ROOT = Path(__file__).resolve().parents[1]


def _clean_value(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": 1,
        "event_id": "00000000-0000-5000-8000-000000000001",
        "event_name": "page_view",
        "event_time": "2026-01-02T03:04:05Z",
        "ingested_at": "2026-01-02T03:04:06Z",
        "processed_at": "2026-01-02T03:04:07Z",
        "source": "web",
        "channel": "web",
        "customer_id": None,
        "anonymous_id": "anon-1",
        "identity_key": "anon-1",
        "session_id": "session-1",
        "campaign_id": "campaign-1",
        "campaign_key": "campaign-1",
        "consent_analytics": True,
        "late_status": "on_time",
        "eligible_for_aggregates": True,
        "payload": {"click_id": "click-1", "is_bot": False},
        "lineage": {
            "topic": "marketing.raw.web-events.v1",
            "partition": 0,
            "offset": 1,
            "record_timestamp": 1767323045000,
            "record_key": "anon-1",
        },
    }
    value.update(overrides)
    return value


def _bronze(value: dict[str, object], *, offset: int = 10) -> dict[str, object]:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return {
        "source_topic": "marketing.clean.customer-events.v1",
        "source_partition": 1,
        "source_offset": offset,
        "payload_text": payload,
        "payload_sha256": hashlib.sha256(payload.encode()).hexdigest(),
    }


def _source_row(
    dataset: str,
    payload: dict[str, object],
    *,
    source_file: str,
    row_number: int = 1,
    source_date: date | None = None,
) -> dict[str, object]:
    return {
        "source_dataset": dataset,
        "source_file": source_file,
        "row_number": row_number,
        "source_date": source_date,
        "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":")),
    }


class SilverEventTests(unittest.TestCase):
    def test_click_id_is_recovered_from_original_envelope_with_event_identity_check(self) -> None:
        event_id = "00000000-0000-5000-8000-000000000001"
        self.assertEqual(
            recover_raw_click_id(
                json.dumps({"event_id": event_id, "click_id": "gclid-1", "payload": {}}),
                event_id,
            ),
            "gclid-1",
        )
        self.assertIsNone(recover_raw_click_id(
            json.dumps({"event_id": event_id, "click_id": None}), event_id
        ))
        with self.assertRaisesRegex(ValueError, "raw event identity"):
            recover_raw_click_id(json.dumps({"event_id": "different"}), event_id)
        with self.assertRaisesRegex(ValueError, "click_id"):
            recover_raw_click_id(json.dumps({"event_id": event_id, "click_id": 7}), event_id)

    def test_dlq_contract_and_target_namespace_fail_closed(self) -> None:
        dlq = {
            "schema_version": 1,
            "error_id": "00000000-0000-5000-8000-000000000032",
            "detected_at": "2026-01-02T03:04:08Z",
            "processor": "marketpulse-flink",
            "source_topic": "marketing.raw.web-events.v1",
            "source_partition": 0,
            "source_offset": 2,
            "source_key": None,
            "reason_code": "MALFORMED_JSON",
            "failed_value_base64": "bm90LWpzb24=",
            "failed_value_utf8": "not-json",
        }
        record = {
            "source_topic": "marketing.dlq.flink-validation.v1",
            "source_partition": 1,
            "source_offset": 5,
            "payload_text": json.dumps(dlq),
        }

        parsed = parse_dlq_bronze_record(record)
        self.assertEqual(parsed.source_offset, 2)
        self.assertEqual(parsed.reason_code, "MALFORMED_JSON")
        tombstone = {
            **dlq,
            "source_key": "",
            "failed_value_base64": "",
            "failed_value_utf8": None,
        }
        self.assertEqual(
            parse_dlq_bronze_record(
                {**record, "payload_text": json.dumps(tombstone)}
            ).source_partition,
            0,
        )
        for mutation in (
            {"reason_code": None},
            {"schema_version": 2},
            {"failed_value_base64": "%%%"},
            {"processor": "other"},
            {"source_partition": "0"},
            {"source_offset": "2"},
        ):
            invalid = {**record, "payload_text": json.dumps({**dlq, **mutation})}
            with self.assertRaises(ValueError):
                parse_dlq_bronze_record(invalid)
        self.assertEqual(validate_namespace("rebuild_run_1"), "rebuild_run_1")
        for namespace in ("", "1silver", "bad-name", "bad`name"):
            with self.assertRaisesRegex(ValueError, "SQL identifier"):
                validate_namespace(namespace)

    def test_raw_clean_dlq_reconciliation_accounts_for_duplicate_and_invalid_positions(self) -> None:
        event_id = "00000000-0000-5000-8000-000000000031"
        raw_value = {"event_id": event_id, "event_time": "2026-01-02T03:04:05Z"}
        raw = [
            {
                "source_topic": "marketing.raw.web-events.v1",
                "source_partition": 0,
                "source_offset": offset,
                "payload_text": json.dumps(raw_value) if offset < 2 else "not-json",
            }
            for offset in range(3)
        ]
        clean = _clean_value(
            event_id=event_id,
            lineage={
                "topic": "marketing.raw.web-events.v1",
                "partition": 0,
                "offset": 0,
                "record_timestamp": 1767323045000,
                "record_key": "anon-1",
            },
        )
        dlq = {
            "schema_version": 1,
            "error_id": "00000000-0000-5000-8000-000000000032",
            "detected_at": "2026-01-02T03:04:08Z",
            "processor": "marketpulse-flink",
            "source_topic": "marketing.raw.web-events.v1",
            "source_partition": 0,
            "source_offset": 2,
            "source_key": None,
            "reason_code": "MALFORMED_JSON",
            "failed_value_base64": "bm90LWpzb24=",
            "failed_value_utf8": "not-json",
        }
        dlq_text = json.dumps(dlq, sort_keys=True, separators=(",", ":"))
        records = [
            *raw,
            _bronze(clean),
            {
                "source_topic": "marketing.dlq.flink-validation.v1",
                "source_partition": 1,
                "source_offset": 5,
                "payload_text": dlq_text,
            },
        ]

        result = reconcile_kafka_outcomes(records)

        self.assertEqual(result.raw_records, 3)
        self.assertEqual(result.clean_records, 1)
        self.assertEqual(result.dlq_records, 1)
        self.assertEqual(result.duplicate_raw_records, 1)
        self.assertEqual(result.dlq_reason_counts, {"MALFORMED_JSON": 1})

        orphan = {
            "source_topic": "marketing.raw.order-events.v1",
            "source_partition": 0,
            "source_offset": 9,
            "payload_text": json.dumps(
                {"event_id": "00000000-0000-5000-8000-000000000039"}
            ),
        }
        with self.assertRaisesRegex(ValueError, "unreconciled raw position"):
            reconcile_kafka_outcomes([*records, orphan])

    def test_complete_clean_schema_is_parsed_and_extra_or_invalid_fields_fail(self) -> None:
        parsed = parse_clean_bronze_record(_bronze(_clean_value()))
        self.assertEqual(parsed.event_id, "00000000-0000-5000-8000-000000000001")
        self.assertEqual(parsed.event_time, datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
        self.assertEqual(parsed.lineage_offset, 1)

        extra = _clean_value(unexpected=True)
        with self.assertRaisesRegex(ValueError, "additional field"):
            parse_clean_bronze_record(_bronze(extra))
        invalid = _clean_value(consent_analytics="true")
        with self.assertRaisesRegex(ValueError, "consent_analytics"):
            parse_clean_bronze_record(_bronze(invalid))

        reserved = _clean_value(campaign_id="__unattributed__")
        with self.assertRaisesRegex(ValueError, "reserved attribution sentinel"):
            parse_clean_bronze_record(_bronze(reserved))

    def test_total_order_deduplicates_replays_and_reports_payload_conflicts(self) -> None:
        event_id = "00000000-0000-5000-8000-000000000011"
        first = _clean_value(event_id=event_id)
        repeat = _clean_value(
            event_id=event_id,
            processed_at="2026-01-02T04:00:00Z",
            lineage={
                **first["lineage"],  # type: ignore[arg-type]
                "offset": 2,
                "record_timestamp": 1767323046000,
            },
        )
        conflicting = _clean_value(
            event_id=event_id,
            event_name="view_item",
            lineage={
                **first["lineage"],  # type: ignore[arg-type]
                "offset": 3,
                "record_timestamp": None,
            },
        )

        plan = govern_customer_events(
            [_bronze(conflicting, offset=13), _bronze(repeat, offset=12), _bronze(first, offset=11)],
            run_id="silver-1",
        )

        self.assertEqual(len(plan.rows), 1)
        self.assertEqual(plan.rows[0]["event_name"], "page_view")
        counts = {item["reason_code"]: item["record_count"] for item in plan.quality}
        self.assertEqual(counts["DUPLICATE_EVENT"], 2)
        self.assertEqual(counts["EVENT_ID_PAYLOAD_CONFLICT"], 1)
        self.assertEqual(counts["ACCEPTED"], 1)

    def test_identity_consent_late_and_whole_bot_session_rules_are_deterministic(self) -> None:
        anonymous = _clean_value(
            event_id="00000000-0000-5000-8000-000000000021",
            anonymous_id="anon-stitch",
        )
        known = _clean_value(
            event_id="00000000-0000-5000-8000-000000000022",
            customer_id="customer-1",
            anonymous_id="anon-stitch",
            event_name="view_item",
            lineage={**anonymous["lineage"], "offset": 2},  # type: ignore[arg-type]
        )
        bot = _clean_value(
            event_id="00000000-0000-5000-8000-000000000023",
            session_id="bot-session",
            anonymous_id="bot-anon",
            payload={"is_bot": True},
            lineage={**anonymous["lineage"], "offset": 3},  # type: ignore[arg-type]
        )
        bot_peer = _clean_value(
            event_id="00000000-0000-5000-8000-000000000024",
            session_id="bot-session",
            anonymous_id="bot-anon",
            event_name="view_item",
            payload={"is_bot": False},
            lineage={**anonymous["lineage"], "offset": 4},  # type: ignore[arg-type]
        )
        denied = _clean_value(
            event_id="00000000-0000-5000-8000-000000000025",
            session_id="denied-session",
            consent_analytics=False,
            lineage={**anonymous["lineage"], "offset": 5},  # type: ignore[arg-type]
        )
        late = _clean_value(
            event_id="00000000-0000-5000-8000-000000000026",
            session_id="late-session",
            late_status="watermark_late",
            eligible_for_aggregates=False,
            lineage={**anonymous["lineage"], "offset": 6},  # type: ignore[arg-type]
        )

        plan = govern_customer_events(
            [
                _bronze(value, offset=index)
                for index, value in enumerate(
                    [anonymous, known, bot, bot_peer, denied, late], start=20
                )
            ],
            run_id="silver-2",
        )

        self.assertEqual(len(plan.rows), 2)
        self.assertEqual({row["profile_key"] for row in plan.rows}, {"customer:customer-1"})
        reasons = {item["reason_code"]: item["record_count"] for item in plan.quality}
        self.assertEqual(reasons["BOT_EXCLUDED"], 2)
        self.assertEqual(reasons["CONSENT_DENIED"], 1)
        self.assertEqual(reasons["LATE_INELIGIBLE"], 1)
        self.assertEqual(reasons["ACCEPTED"], 2)


class SilverDimensionTests(unittest.TestCase):
    def test_build_products_parses_master_rows_and_rejects_duplicates(self) -> None:
        plan = build_source_archive_plan(ROOT / "data/mock/fixtures/seed-360")
        rows = [row for source in plan.source_files if source.source_dataset == "master/products"
                for row in normalize_source_file(source, run_id="silver-fixture", ingested_at=datetime(2026, 1, 3, tzinfo=UTC))]
        products = build_products(rows)
        self.assertEqual(len(products), 50)  # fixture manifest: master/products = 50
        self.assertEqual([p["product_id"] for p in products], sorted(p["product_id"] for p in products))
        self.assertTrue({p["category"] for p in products} <= {"electronics", "home", "beauty", "fashion", "sports"})
        self.assertTrue(all(type(p["unit_price_minor"]) is int and p["unit_price_minor"] > 0 and p["currency"] == "VND" for p in products))
        self.assertEqual(tuple(products[0]), tuple(column.name for column in TABLE_CONTRACTS["silver.products"].columns))
        with self.assertRaisesRegex(ValueError, "duplicate product_id"):
            build_products([*rows, rows[0]])
        broken = dict(rows[0]); broken["payload_json"] = json.dumps({**json.loads(str(rows[0]["payload_json"])), "category": "toys"})
        with self.assertRaisesRegex(ValueError, "category"):
            build_products([broken])

    def test_fixture_dimension_counts_and_spend_totals_are_literal(self) -> None:
        plan = build_source_archive_plan(ROOT / "data/mock/fixtures/seed-360")
        source_rows = [
            row
            for source in plan.source_files
            for row in normalize_source_file(
                source,
                run_id="silver-fixture",
                ingested_at=datetime(2026, 1, 3, tzinfo=UTC),
            )
        ]

        customers = build_customers(source_rows)
        campaigns = build_campaigns(source_rows)
        spend = build_ad_spend_daily(source_rows, campaigns)

        self.assertEqual(len(customers), 100)
        self.assertEqual(len(campaigns), 6)
        self.assertEqual(len(spend), 12)
        self.assertEqual(sum(int(row["spend_minor"]) for row in spend), 551_737)
        self.assertEqual(
            sum(int(row["conversion_value_minor"]) for row in spend), 29_718_000
        )
        self.assertEqual(sum(int(row["impressions"]) for row in spend), 1_000)
        self.assertEqual(sum(int(row["clicks"]) for row in spend), 73)

    def test_latest_crm_snapshot_joins_created_at_and_omits_contact_fields(self) -> None:
        rows = [
            _source_row(
                "master/customers",
                {
                    "customer_id": "customer-1",
                    "created_at": "2024-01-02T00:00:00Z",
                    "email": "fake@example.test",
                    "phone_token": "synthetic-1",
                    "country": "VN",
                    "city": "HCM",
                    "loyalty_tier": "bronze",
                    "consent_analytics": "true",
                    "consent_marketing": "true",
                },
                source_file="master/customers.csv.gz",
            ),
            _source_row(
                "crm",
                {
                    "snapshot_date": "2026-01-01",
                    "customer_id": "customer-1",
                    "email": "fake@example.test",
                    "loyalty_tier": "silver",
                    "lifetime_value_minor": "100",
                    "last_order_at": "",
                    "consent_analytics": "true",
                    "consent_marketing": "true",
                },
                source_file="crm/date=2026-01-01/part.csv.gz",
                source_date=date(2026, 1, 1),
            ),
            _source_row(
                "crm",
                {
                    "snapshot_date": "2026-01-02",
                    "customer_id": "customer-1",
                    "email": "fake@example.test",
                    "loyalty_tier": "gold",
                    "lifetime_value_minor": "250",
                    "last_order_at": "2026-01-02T01:00:00Z",
                    "consent_analytics": "false",
                    "consent_marketing": "true",
                },
                source_file="crm/date=2026-01-02/part.csv.gz",
                source_date=date(2026, 1, 2),
            ),
        ]

        customers = build_customers(rows)

        self.assertEqual(len(customers), 1)
        self.assertEqual(customers[0]["loyalty_tier"], "gold")
        self.assertEqual(customers[0]["lifetime_value_minor"], 250)
        self.assertFalse(customers[0]["consent_analytics"])
        self.assertNotIn("email", customers[0])
        self.assertNotIn("phone_token", customers[0])

    def test_campaigns_count_ads_and_map_unique_platform_ids(self) -> None:
        rows = [
            _source_row(
                "master/campaigns",
                {
                    "campaign_id": "campaign-1",
                    "platform": "google",
                    "platform_campaign_id": "G-1",
                    "name": "Search",
                    "objective": "traffic",
                    "status": "active",
                    "start_date": "2026-01-01",
                    "end_date": "2026-01-31",
                    "daily_budget_minor": "1000",
                    "currency": "VND",
                },
                source_file="master/campaigns.csv.gz",
            ),
            *[
                _source_row(
                    "master/ads",
                    {"ad_id": f"ad-{index}", "campaign_id": "campaign-1"},
                    source_file="master/ads.csv.gz",
                    row_number=index,
                )
                for index in (1, 2)
            ],
        ]

        campaigns = build_campaigns(rows)
        self.assertEqual(campaigns[0]["ad_count"], 2)
        self.assertEqual(campaigns[0]["platform_campaign_id"], "G-1")

        duplicate = dict(rows[0])
        duplicate["payload_json"] = json.dumps(
            {**json.loads(str(rows[0]["payload_json"])), "campaign_id": "campaign-2"}
        )
        with self.assertRaisesRegex(ValueError, "platform_campaign_id"):
            build_campaigns([*rows, duplicate])

        reserved = dict(rows[0])
        reserved["payload_json"] = json.dumps(
            {
                **json.loads(str(rows[0]["payload_json"])),
                "campaign_id": "__unattributed__",
            }
        )
        with self.assertRaisesRegex(ValueError, "reserved attribution sentinel"):
            build_campaigns([reserved])

    def test_source_specific_spend_conversion_is_exact_and_currency_fails_closed(self) -> None:
        campaigns = [
            {
                "campaign_id": "internal-google",
                "platform": "google",
                "platform_campaign_id": "G-1",
                "currency": "VND",
            },
            {
                "campaign_id": "internal-meta",
                "platform": "meta",
                "platform_campaign_id": "M-1",
                "currency": "VND",
            },
            {
                "campaign_id": "internal-tiktok",
                "platform": "tiktok",
                "platform_campaign_id": "T-1",
                "currency": "VND",
            },
        ]
        rows = [
            _source_row(
                "google_ads",
                {
                    "segments_date": "2026-01-02",
                    "campaign_id": "G-1",
                    "impressions": "10",
                    "clicks": "2",
                    "cost_micros": "1500000",
                    "conversions": "1",
                    "conversions_value": "7",
                },
                source_file="google.csv.gz",
                source_date=date(2026, 1, 2),
            ),
            _source_row(
                "meta_ads",
                {
                    "date_start": "2026-01-02",
                    "date_stop": "2026-01-02",
                    "campaign_id": "M-1",
                    "impressions": "20",
                    "clicks": "3",
                    "spend": "2.5",
                    "actions": [{"action_type": "purchase", "value": "2"}],
                    "action_values": [{"action_type": "purchase", "value": "9.5"}],
                },
                source_file="meta.jsonl.gz",
                source_date=date(2026, 1, 2),
            ),
            _source_row(
                "tiktok_ads",
                {
                    "stat_time_day": "2026-01-02",
                    "campaign_id": "T-1",
                    "impressions": "30",
                    "clicks": "4",
                    "spend": "3.5",
                    "conversion": "3",
                    "total_purchase_value": "11.5",
                },
                source_file="tiktok.csv.gz",
                source_date=date(2026, 1, 2),
            ),
        ]

        spend = build_ad_spend_daily(rows, campaigns)

        by_platform = {row["platform"]: row for row in spend}
        self.assertEqual(by_platform["google"]["spend_minor"], 2)
        self.assertEqual(by_platform["meta"]["spend_minor"], 3)
        self.assertEqual(by_platform["tiktok"]["spend_minor"], 4)
        self.assertEqual(by_platform["meta"]["conversion_value_minor"], 10)
        bad_campaigns = [{**campaigns[0], "currency": "USD"}]
        with self.assertRaisesRegex(ValueError, "currency"):
            build_ad_spend_daily(rows[:1], bad_campaigns)


if __name__ == "__main__":
    unittest.main()


class SilverTableCreationTests(unittest.TestCase):
    def test_every_silver_contract_is_created_in_a_preparation_namespace(self) -> None:
        from marketpulse_lakehouse.silver import _ensure_silver_tables

        class FakeSpark:
            def __init__(self) -> None:
                self.statements: list[str] = []

            def sql(self, statement: str) -> None:
                self.statements.append(statement)

        spark = FakeSpark()
        _ensure_silver_tables(spark, "mp_batch_x_1")
        for name in (name for name in TABLE_CONTRACTS if name.startswith("silver.")):
            table = name.split(".", 1)[1]
            self.assertTrue(any(f"`mp_batch_x_1`.`{table}`" in s for s in spark.statements), table)
