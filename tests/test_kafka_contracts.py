from __future__ import annotations

import json
import unittest
from pathlib import Path

from marketpulse_mock.kafka_contracts import (
    TOPIC_CONTRACTS,
    load_contract_schema,
    record_key,
)


class KafkaContractTests(unittest.TestCase):
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

    def test_every_contract_loads_a_json_schema_with_required_identity(self) -> None:
        contract_dir = Path("contracts/kafka")
        for dataset, contract in TOPIC_CONTRACTS.items():
            schema = load_contract_schema(contract_dir, contract)
            self.assertEqual("https://json-schema.org/draft/2020-12/schema", schema["$schema"])
            self.assertEqual("object", schema["type"])
            required = set(schema["required"])
            if dataset == "quality_cases":
                self.assertIn("case_id", required)
                self.assertIn("reason_code", required)
            else:
                self.assertIn("event_id", required)
                self.assertIn("event_time", required)
            json.dumps(schema)


if __name__ == "__main__":
    unittest.main()
