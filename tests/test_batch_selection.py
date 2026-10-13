from __future__ import annotations

import gzip
import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

from marketpulse_batch.contracts import BatchSelection
from marketpulse_batch.selection import select_daily_sources
from marketpulse_lakehouse.source_files import (
    SourceArchivePlan, build_source_archive_plan, normalize_source_file,
)

FIXTURE = Path(__file__).resolve().parents[1] / "data/mock/fixtures/seed-360"
DAY = date(2026, 1, 1)


class BatchSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = build_source_archive_plan(FIXTURE)

    def test_daily_selection_preserves_master_and_lineage(self) -> None:
        selected = select_daily_sources(self.plan, BatchSelection(DAY))
        self.assertEqual(len(selected.source_files), 8)
        self.assertEqual(len(selected.archive_objects), 9)
        self.assertEqual(selected.manifest_sha256, self.plan.manifest_sha256)
        self.assertEqual({f.source_dataset for f in selected.source_files}, {
            "master/ads", "master/campaigns", "master/customers", "master/products",
            "crm", "google_ads", "meta_ads", "tiktok_ads",
        })
        self.assertTrue(all(f.source_date in (None, DAY) for f in selected.source_files))
        source = next(f for f in selected.source_files if f.source_dataset == "crm")
        self.assertIs(source, next(f for f in self.plan.source_files if f.source_file == source.source_file))
        rows = normalize_source_file(source, run_id="original", ingested_at=datetime(2026, 1, 3, tzinfo=UTC))
        self.assertEqual(rows[0]["row_number"], 1)
        self.assertEqual(rows[0]["source_file"], "crm/date=2026-01-01/part-00000.csv.gz")

    def test_missing_required_platform_rejected(self) -> None:
        files = tuple(f for f in self.plan.source_files if f.source_dataset != "tiktok_ads")
        with self.assertRaisesRegex(ValueError, "tiktok_ads"):
            select_daily_sources(replace(self.plan, source_files=files), BatchSelection(DAY))

    def test_unknown_or_duplicate_platform_rejected(self) -> None:
        for platforms in (("other",), ("google_ads", "google_ads"), ()):
            with self.subTest(platforms=platforms), self.assertRaises(ValueError):
                BatchSelection(DAY, platforms)

    def test_non_date_day_rejected(self) -> None:
        with self.assertRaises(ValueError):
            BatchSelection(datetime(2026, 1, 1, tzinfo=UTC))

    def test_missing_partition_rejected(self) -> None:
        files = tuple(replace(f, source_date=None) if f.source_dataset == "crm" else f for f in self.plan.source_files)
        with self.assertRaisesRegex(ValueError, "partition"):
            select_daily_sources(replace(self.plan, source_files=files), BatchSelection(DAY))

    def _with_crm(self, header: str, rows: str, expected_rows: int, root: Path) -> SourceArchivePlan:
        original = next(f for f in self.plan.source_files if f.source_dataset == "crm" and f.source_date == DAY)
        content = gzip.compress((header + "\n" + rows).encode(), mtime=0)
        path = root / "crm.csv.gz"
        path.write_bytes(content)
        source = replace(original, local_path=path, file_sha256=hashlib.sha256(content).hexdigest(),
                         compressed_size_bytes=len(content), row_count=expected_rows)
        return replace(self.plan, source_files=tuple(source if f is original else f for f in self.plan.source_files))

    def test_row_partition_mismatch_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            plan = self._with_crm("snapshot_date,customer_id", "2026-01-01,c1\n2026-01-02,c2\n", 2, Path(root))
            with self.assertRaisesRegex(ValueError, "date"):
                select_daily_sources(plan, BatchSelection(DAY))

    def test_empty_crm_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            plan = self._with_crm("snapshot_date,customer_id", "", 0, Path(root))
            with self.assertRaisesRegex(ValueError, "crm"):
                select_daily_sources(plan, BatchSelection(DAY))

    def test_missing_row_date_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            plan = self._with_crm("customer_id", "c1\n", 1, Path(root))
            with self.assertRaisesRegex(ValueError, "date"):
                select_daily_sources(plan, BatchSelection(DAY))

    def test_default_ingest_unchanged(self) -> None:
        # All-file archive planning still includes both dates without batch selection.
        self.assertEqual(len(self.plan.source_files), 12)
        self.assertEqual({f.source_date for f in self.plan.source_files}, {None, DAY, date(2026, 1, 2)})

