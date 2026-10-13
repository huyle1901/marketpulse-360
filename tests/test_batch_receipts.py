from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import date
from pathlib import Path

from marketpulse_batch.contracts import BatchSelection, PreparationReceipt
from marketpulse_batch.receipts import preparation_identity, read_receipt, write_receipt


class BatchReceiptTests(unittest.TestCase):
    def receipt(self) -> PreparationReceipt:
        selection = BatchSelection(date(2026, 1, 1))
        bronze = {"bronze.kafka_records": 101, "bronze.source_files": 102}
        identity = preparation_identity(selection, bronze, "a" * 64)
        return PreparationReceipt(identity, selection.logical_day, "a" * 64, bronze,
                                  {"gold.funnel_daily": 201}, f"mp_batch_{identity}_1", "b" * 64)

    def test_identity_order_independent(self) -> None:
        selection = BatchSelection(date(2026, 1, 1))
        first = preparation_identity(selection, {"bronze.a": 1, "bronze.b": 2}, "a" * 64)
        self.assertEqual(first, preparation_identity(selection, {"bronze.b": 2, "bronze.a": 1}, "a" * 64))
        self.assertNotEqual(first, preparation_identity(selection, {"bronze.a": 2, "bronze.b": 2}, "a" * 64))
        self.assertNotEqual(first, preparation_identity(selection, {"bronze.a": 1, "bronze.b": 2}, "b" * 64))
        self.assertEqual(len(first), 64)

    def test_identity_includes_required_platforms(self) -> None:
        selection = BatchSelection(date(2026, 1, 1))
        other = BatchSelection(date(2026, 1, 1), ("google_ads",))
        self.assertNotEqual(preparation_identity(selection, {"bronze.a": 1}, "a" * 64),
                            preparation_identity(other, {"bronze.a": 1}, "a" * 64))

    def test_receipt_roundtrip_and_idempotent_write(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "receipt.json"
            receipt = self.receipt()
            write_receipt(receipt, path)
            original = path.read_bytes()
            write_receipt(receipt, path)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(read_receipt(path), receipt)

    def test_conflicting_receipt_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "receipt.json"
            receipt = self.receipt()
            write_receipt(receipt, path)
            with self.assertRaisesRegex(ValueError, "conflict"):
                write_receipt(replace(receipt, proof_digest="c" * 64), path)
            self.assertEqual(read_receipt(path).proof_digest, "b" * 64)

    def test_caller_cannot_mutate_pinned_snapshot_map(self) -> None:
        receipt = self.receipt()
        with self.assertRaises(TypeError):
            receipt.bronze_snapshots["bronze.kafka_records"] = 999

    def test_invalid_snapshot_and_namespace_rejected(self) -> None:
        receipt = self.receipt()
        for changes in ({"bronze_snapshots": {"bronze.a": True}},
                        {"output_snapshots": {"gold.a": -1}},
                        {"namespace": "default"}, {"identity": "../oops"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(receipt, **changes)

    def test_read_rejects_unknown_fields_and_malformed_json(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "receipt.json"
            write_receipt(self.receipt(), path)
            payload = json.loads(path.read_text())
            payload["unknown"] = 1
            path.write_text(json.dumps(payload))
            with self.assertRaises(ValueError):
                read_receipt(path)
            path.write_text("{")
            with self.assertRaises(ValueError):
                read_receipt(path)
