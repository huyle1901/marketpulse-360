from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

from marketpulse_lakehouse.evidence import (
    EVIDENCE_SCHEMA_VERSION,
    TableEvidence,
    canonical_row_checksum,
    compare_table_evidence,
    schema_fingerprint,
)


class LakehouseEvidenceTests(unittest.TestCase):
    def test_canonical_checksum_ignores_row_and_mapping_order(self) -> None:
        columns = ("id", "when", "ratio", "payload")
        rows_a = [
            {
                "id": "b",
                "when": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
                "ratio": Decimal("1.250000"),
                "payload": {"z": 2, "a": 1},
            },
            {
                "id": "a",
                "when": date(2026, 1, 1),
                "ratio": Decimal("0.000000"),
                "payload": None,
            },
        ]
        rows_b = [
            {"payload": None, "ratio": Decimal("0.000000"), "when": date(2026, 1, 1), "id": "a"},
            {"payload": {"a": 1, "z": 2}, "ratio": Decimal("1.250000"), "when": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc), "id": "b"},
        ]
        self.assertEqual(
            canonical_row_checksum(rows_a, columns),
            canonical_row_checksum(rows_b, columns),
        )

    def test_canonical_checksum_changes_when_a_business_value_changes(self) -> None:
        self.assertNotEqual(
            canonical_row_checksum([{"id": "a", "amount": 1}], ("id", "amount")),
            canonical_row_checksum([{"id": "a", "amount": 2}], ("id", "amount")),
        )

    def test_schema_fingerprint_includes_order_type_and_nullability(self) -> None:
        base = (("id", "string", False), ("amount", "long", True))
        self.assertNotEqual(schema_fingerprint(base), schema_fingerprint(tuple(reversed(base))))
        self.assertNotEqual(
            schema_fingerprint(base),
            schema_fingerprint((("id", "string", False), ("amount", "integer", True))),
        )

    def test_evidence_comparison_reports_each_business_mismatch(self) -> None:
        expected = TableEvidence(
            table="gold.funnel_daily",
            snapshot_id=10,
            schema_fingerprint="schema-a",
            row_count=1,
            checksum="checksum-a",
            reconciliations={"net_revenue_minor": 100},
        )
        actual = TableEvidence(
            table="gold.funnel_daily",
            snapshot_id=11,
            schema_fingerprint="schema-b",
            row_count=2,
            checksum="checksum-b",
            reconciliations={"net_revenue_minor": 99},
        )
        self.assertEqual(EVIDENCE_SCHEMA_VERSION, 1)
        self.assertEqual(
            compare_table_evidence(expected, actual),
            (
                "gold.funnel_daily: schema_fingerprint expected schema-a, actual schema-b",
                "gold.funnel_daily: row_count expected 1, actual 2",
                "gold.funnel_daily: checksum expected checksum-a, actual checksum-b",
                "gold.funnel_daily: reconciliation net_revenue_minor expected 100, actual 99",
            ),
        )

    def test_snapshot_id_is_operational_not_a_rebuild_mismatch(self) -> None:
        expected = TableEvidence("gold.funnel_daily", 10, "schema", 1, "sum", {})
        rebuilt = TableEvidence("rebuild_demo.gold_funnel_daily", 99, "schema", 1, "sum", {})
        self.assertEqual(compare_table_evidence(expected, rebuilt), ())


if __name__ == "__main__":
    unittest.main()
