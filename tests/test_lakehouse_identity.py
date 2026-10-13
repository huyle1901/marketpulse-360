from __future__ import annotations

import unittest

from marketpulse_lakehouse.identity import (
    file_ingest_id,
    kafka_ingest_id,
    resolve_profile_key,
)


class LakehouseIdentityTests(unittest.TestCase):
    def test_kafka_ingest_id_uses_canonical_position(self) -> None:
        self.assertEqual(
            kafka_ingest_id("marketing.raw.web-events.v1", 0, 42),
            "c967be1b52f84d3077f40fe1728295e2d0d255ad65b80d2432b05b080ee5878e",
        )

    def test_file_ingest_id_uses_one_based_row_number(self) -> None:
        self.assertEqual(
            file_ingest_id("abc123", 7),
            "4411527fd7d80819d44e68b335a7a2e68e0a9c8493aaead527dcc93bd2faaa09",
        )

    def test_invalid_positions_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "partition"):
            kafka_ingest_id("topic", -1, 0)
        with self.assertRaisesRegex(ValueError, "offset"):
            kafka_ingest_id("topic", 0, -1)
        with self.assertRaisesRegex(ValueError, "row_number"):
            file_ingest_id("abc123", 0)

    def test_profile_key_prefers_customer_then_stitched_then_anonymous(self) -> None:
        self.assertEqual(
            resolve_profile_key("customer-1", "anonymous-1", "session-1"),
            "customer:customer-1",
        )
        self.assertEqual(
            resolve_profile_key(
                None,
                "anonymous-1",
                "session-1",
                stitched_customer_id="customer-2",
            ),
            "customer:customer-2",
        )
        self.assertEqual(
            resolve_profile_key(None, "anonymous-1", "session-1"),
            "anonymous:anonymous-1",
        )
        self.assertEqual(
            resolve_profile_key(None, None, "session-1"),
            "session:session-1",
        )

    def test_missing_profile_identity_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "profile identity"):
            resolve_profile_key(None, None, None)


if __name__ == "__main__":
    unittest.main()
