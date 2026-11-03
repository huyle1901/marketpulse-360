from __future__ import annotations

import copy
import gzip
import json
import unittest
from pathlib import Path

from marketpulse_mock.flink_smoke_verify import _schema_errors
from marketpulse_mock.kafka_contracts import (
    ALL_TOPIC_CONTRACTS,
    FLINK_TOPIC_CONTRACTS,
    SOURCE_TOPIC_CONTRACTS,
    TOPIC_CONTRACTS,
    load_contract_schema,
    record_key,
)


class KafkaContractTests(unittest.TestCase):
    def test_source_and_flink_contract_sets_are_explicit_and_complete(self) -> None:
        self.assertIs(TOPIC_CONTRACTS, SOURCE_TOPIC_CONTRACTS)
        self.assertEqual(4, len(SOURCE_TOPIC_CONTRACTS))
        self.assertEqual(
            {
                "clean_customer_events": (
                    "marketing.clean.customer-events.v1",
                    "customer-events-v1",
                    6,
                    604_800_000,
                    "delete",
                ),
                "sessions": (
                    "marketing.clean.sessions.v1",
                    "sessions-v1",
                    6,
                    604_800_000,
                    "compact,delete",
                ),
                "funnel_metrics": (
                    "marketing.gold.funnel-metrics.v1",
                    "funnel-metrics-v1",
                    3,
                    2_592_000_000,
                    "compact,delete",
                ),
                "revenue_metrics": (
                    "marketing.gold.revenue-metrics.v1",
                    "revenue-metrics-v1",
                    3,
                    2_592_000_000,
                    "compact,delete",
                ),
                "campaign_metrics": (
                    "marketing.gold.campaign-metrics.v1",
                    "campaign-metrics-v1",
                    3,
                    2_592_000_000,
                    "compact,delete",
                ),
                "flink_validation_dlq": (
                    "marketing.dlq.flink-validation.v1",
                    "flink-validation-dlq-v1",
                    6,
                    604_800_000,
                    "delete",
                ),
            },
            {
                dataset: (
                    contract.topic,
                    contract.artifact_id,
                    contract.partitions,
                    contract.retention_ms,
                    contract.cleanup_policy,
                )
                for dataset, contract in FLINK_TOPIC_CONTRACTS.items()
            },
        )
        self.assertEqual(10, len(ALL_TOPIC_CONTRACTS))

    def test_topic_names_artifacts_and_key_fallbacks_are_stable(self) -> None:
        self.assertEqual(
            {
                "web_events": (
                    "marketing.raw.web-events.v1",
                    "web-events-v1",
                ),
                "orders": (
                    "marketing.raw.order-events.v1",
                    "order-events-v1",
                ),
                "email_events": (
                    "marketing.raw.email-events.v1",
                    "email-events-v1",
                ),
                "quality_cases": (
                    "marketing.dlq.validation.v1",
                    "validation-dlq-v1",
                ),
            },
            {
                dataset: (contract.topic, contract.artifact_id)
                for dataset, contract in TOPIC_CONTRACTS.items()
            },
        )
        self.assertEqual(
            "customer-1",
            record_key(
                "web_events",
                {"customer_id": "customer-1", "session_id": "session-1"},
            ),
        )
        self.assertEqual(
            "session-1",
            record_key(
                "web_events",
                {"customer_id": None, "session_id": "session-1"},
            ),
        )
        self.assertEqual(
            "order-1",
            record_key(
                "orders",
                {
                    "customer_id": None,
                    "session_id": "session-1",
                    "payload": {"order_id": "order-1"},
                },
            ),
        )
        self.assertEqual(
            "message-1",
            record_key(
                "email_events",
                {"customer_id": None, "payload": {"message_id": "message-1"}},
            ),
        )

    def test_missing_partition_identity_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "partition key"):
            record_key("web_events", {"customer_id": None, "session_id": None})
        with self.assertRaisesRegex(ValueError, "partition key"):
            record_key(
                "orders",
                {"customer_id": None, "session_id": "not-an-order", "payload": {}},
            )
        with self.assertRaisesRegex(ValueError, "partition key"):
            record_key(
                "email_events",
                {"customer_id": None, "session_id": "not-a-message", "payload": {}},
            )

    def test_every_contract_loads_a_json_schema_with_required_identity(self) -> None:
        contract_dir = Path("contracts/kafka")
        for dataset, contract in ALL_TOPIC_CONTRACTS.items():
            schema = load_contract_schema(contract_dir, contract)
            self.assertEqual("https://json-schema.org/draft/2020-12/schema", schema["$schema"])
            self.assertEqual("object", schema["type"])
            required = set(schema["required"])
            if dataset == "quality_cases":
                self.assertIn("case_id", required)
                self.assertIn("reason_code", required)
            elif dataset == "flink_validation_dlq":
                self.assertIn("error_id", required)
                self.assertIn("failed_value_base64", required)
            elif dataset == "sessions":
                self.assertIn("session_id", required)
                self.assertEqual(
                    ["open", "closed"], schema["properties"]["status"]["enum"]
                )
            elif dataset.endswith("_metrics"):
                self.assertIn("metric_key", required)
                self.assertIn("campaign_id", required)
            else:
                self.assertIn("event_id", required)
                self.assertIn("event_time", required)
            json.dumps(schema)

        clean = load_contract_schema(
            contract_dir, FLINK_TOPIC_CONTRACTS["clean_customer_events"]
        )
        self.assertEqual(
            ["on_time", "source_marked_late", "watermark_late"],
            clean["properties"]["late_status"]["enum"],
        )
        dlq = load_contract_schema(
            contract_dir, FLINK_TOPIC_CONTRACTS["flink_validation_dlq"]
        )
        self.assertEqual(
            {
                "MALFORMED_JSON",
                "UNSUPPORTED_SCHEMA_VERSION",
                "TOPIC_SOURCE_MISMATCH",
                "MISSING_REQUIRED_FIELD",
                "INVALID_FIELD_TYPE",
                "INVALID_EVENT_ID",
                "INVALID_TIMESTAMP",
                "INVALID_EVENT_NAME",
                "MISSING_IDENTITY",
                "INVALID_PAYLOAD",
            },
            set(dlq["properties"]["reason_code"]["enum"]),
        )


FIXTURE = Path("data/mock/fixtures/seed-360")


def _first_fixture_row(dataset: str, event_name: str) -> dict[str, object]:
    for path in sorted((FIXTURE / dataset).glob("date=*/*.jsonl.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            for line in stream:
                row = json.loads(line)
                if row["event_name"] == event_name:
                    return row
    raise AssertionError(f"no {event_name} in the {dataset} fixture")


def _non_paid(row: dict[str, object], traffic_source: str) -> dict[str, object]:
    """Return ``row`` as a non-paid visit: no campaign, no click."""

    value = copy.deepcopy(row)
    value["campaign_id"] = None
    value["platform_campaign_id"] = None
    value["click_id"] = None
    payload = value["payload"]
    assert isinstance(payload, dict)
    payload["traffic_source"] = traffic_source
    return value


class WidenedSourceContractTests(unittest.TestCase):
    """v1 web and order contracts accept non-paid visits without a campaign."""

    @classmethod
    def setUpClass(cls) -> None:
        contract_dir = Path("contracts/kafka")
        cls.web = load_contract_schema(contract_dir, SOURCE_TOPIC_CONTRACTS["web_events"])
        cls.orders = load_contract_schema(contract_dir, SOURCE_TOPIC_CONTRACTS["orders"])

    def test_fixture_and_organic_web_events_validate(self) -> None:
        page_view = _first_fixture_row("web_events", "page_view")
        self.assertEqual([], _schema_errors(page_view, self.web))
        for source in ("organic", "direct", "email"):
            with self.subTest(source):
                self.assertEqual(
                    [], _schema_errors(_non_paid(page_view, source), self.web)
                )

    def test_fixture_and_unattributed_organic_orders_validate(self) -> None:
        order = _first_fixture_row("orders", "order_created")
        self.assertEqual([], _schema_errors(order, self.orders))
        organic = _non_paid(order, "organic")
        payload = organic["payload"]
        assert isinstance(payload, dict)
        payload["attributed_platform"] = None
        payload["attributed_campaign_id"] = None
        self.assertEqual([], _schema_errors(organic, self.orders))
        paid = copy.deepcopy(order)
        paid["payload"]["traffic_source"] = "paid"
        self.assertEqual([], _schema_errors(paid, self.orders))

    def test_widened_fields_stay_typed(self) -> None:
        page_view = _first_fixture_row("web_events", "page_view")
        for field in ("campaign_id", "platform_campaign_id", "click_id"):
            with self.subTest(field):
                bad = copy.deepcopy(page_view)
                bad[field] = 7
                self.assertNotEqual([], _schema_errors(bad, self.web))
        order = _first_fixture_row("orders", "order_created")
        for field in ("attributed_platform", "attributed_campaign_id"):
            with self.subTest(field):
                bad = copy.deepcopy(order)
                bad["payload"][field] = 7
                self.assertNotEqual([], _schema_errors(bad, self.orders))
        bad = copy.deepcopy(order)
        bad["payload"]["traffic_source"] = "social"
        self.assertNotEqual([], _schema_errors(bad, self.orders))
        for schema in (self.web, self.orders):
            self.assertEqual({"const": 1}, schema["properties"]["schema_version"])


if __name__ == "__main__":
    unittest.main()
