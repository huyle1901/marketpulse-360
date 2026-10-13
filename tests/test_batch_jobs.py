from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

from marketpulse_batch.contracts import BatchSelection, PreparationReceipt
from marketpulse_batch.receipts import preparation_identity, write_receipt
from marketpulse_batch.jobs import resolve_rebuild_receipts, model_digest, ordered_asset_outputs


class BackfillContractTests(unittest.TestCase):
    def test_reused_dbt_outputs_follow_dependencies(self) -> None:
        self.assertEqual(ordered_asset_outputs({"dashboard": "dashboard", "base": "base"},
                                                {"dashboard": {"base"}, "base": {"external_raw"}}),
                         ("base", "dashboard"))
        with self.assertRaises(ValueError):
            ordered_asset_outputs({"a": "a", "b": "b"}, {"a": {"b"}, "b": {"a"}})

    def receipt(self, day: date) -> PreparationReceipt:
        bronze = {"bronze.kafka_records": 101}
        identity = preparation_identity(BatchSelection(day), bronze, "a" * 64)
        return PreparationReceipt(identity, day, "a" * 64, bronze,
                                  {"gold.funnel_daily": 201}, f"mp_batch_{identity}_1", "b" * 64)

    def test_missing_backfill_mapping_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, "mapping"):
            resolve_rebuild_receipts((date(2026, 1, 1),), {})

    def test_wrong_day_receipt_fails(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "receipt.json"
            write_receipt(self.receipt(date(2026, 1, 2)), path)
            with self.assertRaisesRegex(ValueError, "day"):
                resolve_rebuild_receipts((date(2026, 1, 1),), {"2026-01-01": path.as_uri()})

    def test_receipt_retry_does_not_read_heads(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "receipt.json"
            write_receipt(self.receipt(date(2026, 1, 1)), path)
            rows = resolve_rebuild_receipts((date(2026, 1, 1),), {"2026-01-01": path.as_uri()})
            self.assertEqual(dict(rows[0].bronze_snapshots), {"bronze.kafka_records": 101})
            self.assertEqual(dict(rows[0].output_snapshots), {"gold.funnel_daily": 201})

    def test_ambiguous_mapping_rejected(self) -> None:
        with self.assertRaises(ValueError):
            resolve_rebuild_receipts((date(2026, 1, 1),), {"2026-01-01": ["first", "second"]})

    def test_model_digest_excludes_docs_but_includes_dbt(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)
            (path / "src/marketpulse_lakehouse").mkdir(parents=True)
            (path / "src/marketpulse_lakehouse/gold.py").write_text("transform revision one")
            (path / "dbt/marketpulse_serving/models").mkdir(parents=True)
            (path / "dbt/marketpulse_serving/models/model.sql").write_text("select 1")
            original = model_digest(path)
            (path / "README.md").write_text("documentation change")
            self.assertEqual(model_digest(path), original)
            (path / "dbt/marketpulse_serving/models/model.sql").write_text("select 2")
            self.assertNotEqual(model_digest(path), original)
