from __future__ import annotations

import unittest
from datetime import date

from marketpulse_app.contracts import PLATFORMS
from marketpulse_batch.contracts import REPORT_PLATFORMS, BatchSelection
from marketpulse_batch.receipts import preparation_identity
from marketpulse_batch.selection import DAY_FIELDS
from marketpulse_lakehouse.silver import VENDOR_DATASETS
from marketpulse_lakehouse.source_files import ALLOWED_SOURCE_DATASETS
from marketpulse_mock.writers import CSV_FIELDS, JSONL_DATASETS


class PlatformListTests(unittest.TestCase):
    def test_every_layer_lists_the_same_vendor_reports(self) -> None:
        vendors = set(VENDOR_DATASETS)
        self.assertEqual(vendors, set(REPORT_PLATFORMS))
        self.assertEqual(vendors, set(DAY_FIELDS) - {"crm"})
        self.assertLessEqual(vendors, ALLOWED_SOURCE_DATASETS)
        self.assertEqual(set(VENDOR_DATASETS.values()), set(PLATFORMS))
        for dataset in vendors:
            self.assertTrue(dataset in CSV_FIELDS or dataset in JSONL_DATASETS, dataset)

    def test_default_receipt_identity_matches_the_three_platform_baseline(self) -> None:
        # Digest computed at b9f7265, before configurable required platforms existed.
        identity = preparation_identity(
            BatchSelection(date(2026, 1, 1)),
            {"bronze.kafka_records": 3, "bronze.source_files": 2, "bronze.source_file_rows": 1},
            "a" * 64,
        )
        self.assertEqual("696ae745f0a1bdc1d2bcce6c7cd8fc60fd86ffa89921cfd93835e5512aea0310", identity)


if __name__ == "__main__":
    unittest.main()
