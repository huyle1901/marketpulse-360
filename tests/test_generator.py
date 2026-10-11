from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from marketpulse_mock.config import GenerationConfig, ScaleProfile, resolve_config
from marketpulse_mock.generator import generate_dataset
from marketpulse_mock.validator import validate_dataset


EXPECTED_DATASETS = {
    "master/customers",
    "master/products",
    "master/campaigns",
    "master/ads",
    "google_ads",
    "meta_ads",
    "tiktok_ads",
    "web_events",
    "orders",
    "crm",
    "email_events",
    "quality_cases",
    "ground_truth",
}


class DatasetGenerationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary_directory = tempfile.TemporaryDirectory()
        root = Path(cls.temporary_directory.name)
        cls.run_one = root / "run-one"
        cls.run_two = root / "run-two"
        cls.run_other_seed = root / "run-other-seed"
        cls.manifest_one = generate_dataset(
            resolve_config("fixture", 360, cls.run_one)
        )
        cls.manifest_two = generate_dataset(
            resolve_config("fixture", 360, cls.run_two)
        )
        cls.manifest_other_seed = generate_dataset(
            resolve_config("fixture", 361, cls.run_other_seed)
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary_directory.cleanup()

    def test_same_seed_produces_identical_manifest_and_file_hashes(self) -> None:
        self.assertEqual(self.manifest_one.to_dict(), self.manifest_two.to_dict())
        self.assertEqual(
            (self.run_one / "manifest.json").read_bytes(),
            (self.run_two / "manifest.json").read_bytes(),
        )

    def test_different_seed_changes_master_and_event_files(self) -> None:
        hashes_one = {
            stat.relative_path: stat.sha256 for stat in self.manifest_one.files
        }
        hashes_other = {
            stat.relative_path: stat.sha256
            for stat in self.manifest_other_seed.files
        }

        self.assertNotEqual(
            hashes_one["master/customers.csv.gz"],
            hashes_other["master/customers.csv.gz"],
        )
        web_path = next(path for path in hashes_one if path.startswith("web_events/"))
        self.assertNotEqual(hashes_one[web_path], hashes_other[web_path])

    def test_manifest_contains_all_sources_and_file_row_totals(self) -> None:
        self.assertEqual(EXPECTED_DATASETS, set(self.manifest_one.datasets))
        for dataset, row_count in self.manifest_one.datasets.items():
            file_rows = sum(
                stat.row_count
                for stat in self.manifest_one.files
                if stat.dataset == dataset
            )
            self.assertEqual(row_count, file_rows, dataset)

    def test_manifest_records_nonzero_anomalies_and_reconciled_totals(self) -> None:
        self.assertTrue(
            all(value > 0 for value in self.manifest_one.anomalies.values())
        )
        totals = self.manifest_one.totals
        self.assertEqual(
            totals["gross_revenue_minor"] - totals["refunds_minor"],
            totals["net_revenue_minor"],
        )
        self.assertGreater(totals["impressions"], totals["clicks"])
        self.assertGreater(totals["clicks"], totals["orders"])

    def test_manifest_file_is_valid_json_and_contains_no_absolute_paths(self) -> None:
        payload = json.loads((self.run_one / "manifest.json").read_text())
        self.assertTrue(payload["synthetic"])
        self.assertEqual(360, payload["seed"])
        self.assertEqual("2026-01-01", payload["start_date"])
        self.assertTrue(
            all(not Path(item["relative_path"]).is_absolute() for item in payload["files"])
        )

    def test_generation_refuses_a_nonempty_directory_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "existing"
            output.mkdir()
            marker = output / "keep.txt"
            marker.write_text("keep")

            with self.assertRaisesRegex(FileExistsError, "not empty"):
                generate_dataset(resolve_config("fixture", 360, output))

            self.assertEqual("keep", marker.read_text())

    def test_explicit_overwrite_replaces_only_the_exact_output_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            parent = Path(temporary_directory)
            output = parent / "replace-me"
            sibling = parent / "keep-me.txt"
            output.mkdir()
            (output / "old.txt").write_text("old")
            sibling.write_text("keep")

            manifest = generate_dataset(
                resolve_config("fixture", 360, output), overwrite=True
            )

            self.assertFalse((output / "old.txt").exists())
            self.assertTrue((output / "manifest.json").exists())
            self.assertEqual("keep", sibling.read_text())
            self.assertEqual(360, manifest.seed)

    def test_overwrite_refuses_symbolic_link_without_touching_target(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            parent = Path(temporary_directory)
            target = parent / "real-output"
            target.mkdir()
            marker = target / "keep.txt"
            marker.write_text("keep")
            output_link = parent / "output-link"
            output_link.symlink_to(target, target_is_directory=True)

            with self.assertRaisesRegex(ValueError, "symbolic link"):
                generate_dataset(
                    resolve_config("fixture", 360, output_link),
                    overwrite=True,
                )

            self.assertTrue(output_link.is_symlink())
            self.assertEqual("keep", marker.read_text())

    def test_overwrite_refuses_an_ancestor_of_the_working_directory(self) -> None:
        protected_ancestor = Path.cwd().parent
        with patch("marketpulse_mock.generator.shutil.rmtree") as remove_tree:
            with self.assertRaisesRegex(ValueError, "unsafe overwrite target"):
                generate_dataset(
                    resolve_config("fixture", 360, protected_ancestor),
                    overwrite=True,
                )

        remove_tree.assert_not_called()

    def test_small_rotation_limit_bounds_every_part(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "rotated"
            manifest = generate_dataset(
                resolve_config("fixture", 360, output, rows_per_file=7)
            )

        self.assertTrue(all(stat.row_count <= 7 for stat in manifest.files))
        self.assertGreater(
            sum(stat.dataset == "web_events" for stat in manifest.files), 2
        )

    def test_dense_profile_keeps_overlapping_journeys_in_event_time_order(self) -> None:
        profile = ScaleProfile(
            name="dense",
            days=1,
            customers=50,
            campaigns=3,
            products=10,
            impressions_per_day=3_000,
            rows_per_file=10_000,
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "dense"
            config = GenerationConfig(
                profile=profile,
                seed=360,
                output_dir=output,
                start_date=date(2026, 1, 1),
                rows_per_file=profile.rows_per_file,
            )

            generate_dataset(config)
            report = validate_dataset(output)

        self.assertTrue(report.valid, report.errors)


if __name__ == "__main__":
    unittest.main()
