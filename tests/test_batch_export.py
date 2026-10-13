from __future__ import annotations

import unittest
import tempfile
from pathlib import Path
from datetime import UTC, date, datetime
from decimal import Decimal

from marketpulse_batch.canonical import canonical_row, clickhouse_type, rowset_checksum
from marketpulse_batch.export import json_rows


class BatchExportTests(unittest.TestCase):
    def test_money_exact_above_float_precision(self) -> None:
        columns = (("amount", "long", False),)
        self.assertEqual(canonical_row(columns, {"amount": 9007199254740993}), b'["9007199254740993"]')
        self.assertEqual(canonical_row(columns, {"amount": -1}), b'["-1"]')
        with self.assertRaises(ValueError):
            canonical_row(columns, {"amount": 9007199254740992.0})
        with self.assertRaises(ValueError):
            canonical_row(columns, {"amount": 2**63})

    def test_nullable_decimal_date_roundtrip(self) -> None:
        columns = (("ratio", "decimal(18,6)", True), ("day", "date", False), ("id", "string", False))
        self.assertEqual(canonical_row(columns, {"ratio": Decimal("1.230000"), "day": date(2026, 1, 1), "id": "001"}),
                         b'["1.230000","2026-01-01","001"]')
        self.assertEqual(canonical_row(columns, {"ratio": None, "day": "2026-01-01", "id": ""}),
                         b'[null,"2026-01-01",""]')
        for value in (Decimal("NaN"), Decimal("1.0000001"), 1.23):
            with self.subTest(value=value), self.assertRaises(ValueError):
                canonical_row(columns, {"ratio": value, "day": "2026-01-01", "id": "001"})

    def test_unknown_columns_fail(self) -> None:
        for row in ({}, {"id": "x", "extra": "x"}, {"id": None}):
            with self.subTest(row=row), self.assertRaises(ValueError):
                canonical_row((("id", "string", False),), row)

    def test_type_mapping(self) -> None:
        for source, expected in (("long", "Int64"), ("int", "Int32"), ("date", "Date32"),
                                 ("timestamp", "DateTime64(6,'UTC')"), ("boolean", "Bool"),
                                 ("decimal(18,6)", "Decimal(18,6)")):
            self.assertEqual(clickhouse_type(source, False), expected)
            self.assertEqual(clickhouse_type(source, True), f"Nullable({expected})")
        with self.assertRaises(ValueError):
            clickhouse_type("array(string)", False)

    def test_timestamp_utc_and_boolean(self) -> None:
        columns = (("at", "timestamp", False), ("yes", "boolean", False))
        self.assertEqual(canonical_row(columns, {"at": datetime(2026, 1, 1, tzinfo=UTC), "yes": True}),
                         b'["2026-01-01T00:00:00.000000Z",true]')
        with self.assertRaises(ValueError):
            canonical_row(columns, {"at": "2026-01-01T00:00:00", "yes": True})

    def test_rowset_checksum_order_independent_and_duplicate_sensitive(self) -> None:
        columns = (("id", "string", False),)
        first = rowset_checksum(columns, iter(({"id": "a"}, {"id": "b"})))
        self.assertEqual(first, rowset_checksum(columns, iter(({"id": "b"}, {"id": "a"}))))
        self.assertNotEqual(first, rowset_checksum(columns, iter(({"id": "a"}, {"id": "a"}))))
        self.assertNotEqual(first, rowset_checksum(columns, iter(({"id": "a"}, {"id": "b"}, {"id": "b"}))))
        self.assertEqual(first[0], 2)

    def test_duplicate_json_fields_and_oversized_rows_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "part.json"
            for content in (b'{"id":"a","id":"b"}\n', b'{"id":"' + b'a' * (1024 * 1024) + b'"}\n'):
                path.write_bytes(content)
                with self.subTest(size=len(content)), self.assertRaises(ValueError):
                    list(json_rows(path))
