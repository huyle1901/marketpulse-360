from __future__ import annotations

import gzip
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from typing import Callable

from marketpulse_mock.config import resolve_config
from marketpulse_mock.generator import generate_dataset
from marketpulse_mock.validator import validate_dataset


class DatasetValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name) / "fixture"
        generate_dataset(resolve_config("fixture", 360, self.root))

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_generated_fixture_is_valid(self) -> None:
        report = validate_dataset(self.root)

        self.assertTrue(report.valid, report.errors)
        self.assertEqual(0, len(report.errors))
        self.assertGreater(report.files_checked, 0)
        self.assertGreater(report.rows_checked, 0)

    def test_checksum_tampering_names_the_modified_file(self) -> None:
        target = next((self.root / "web_events").rglob("*.jsonl.gz"))
        with target.open("ab") as stream:
            stream.write(b"tampered")

        report = validate_dataset(self.root)

        self.assertFalse(report.valid)
        self.assertTrue(
            any("checksum mismatch" in error and target.name in error for error in report.errors),
            report.errors,
        )

    def test_missing_campaign_foreign_key_is_detected(self) -> None:
        self._mutate_first_json_record(
            "web_events", lambda row: row.__setitem__("campaign_id", "missing-campaign")
        )

        report = validate_dataset(self.root)

        self.assertFalse(report.valid)
        self.assertTrue(
            any("unknown campaign_id missing-campaign" in error for error in report.errors),
            report.errors,
        )

    def test_broken_web_purchase_amount_is_detected(self) -> None:
        def mutate(row: dict[str, object]) -> None:
            if row.get("event_name") == "purchase":
                payload = row["payload"]
                assert isinstance(payload, dict)
                payload["amount_minor"] = int(payload["amount_minor"]) + 1

        self._mutate_first_json_record("web_events", mutate, event_name="purchase")

        report = validate_dataset(self.root)

        self.assertFalse(report.valid)
        self.assertTrue(
            any("purchase/order mismatch" in error for error in report.errors),
            report.errors,
        )

    def test_ground_truth_and_vendor_aggregate_mismatch_is_detected(self) -> None:
        def mutate(row: dict[str, object]) -> None:
            row["clicks"] = int(row["clicks"]) + 1

        self._mutate_first_json_record("ground_truth", mutate)

        report = validate_dataset(self.root)

        self.assertFalse(report.valid)
        self.assertTrue(
            any("vendor/ground-truth mismatch" in error for error in report.errors),
            report.errors,
        )

    def _mutate_first_json_record(
        self,
        dataset: str,
        mutate: Callable[[dict[str, object]], None],
        event_name: str | None = None,
    ) -> None:
        target = next((self.root / dataset).rglob("*.jsonl.gz"))
        with gzip.open(target, "rt", encoding="utf-8") as stream:
            rows = [json.loads(line) for line in stream]
        for row in rows:
            if event_name is None or row.get("event_name") == event_name:
                mutate(row)
                break
        else:
            self.fail(f"no matching row in {target}")

        raw = target.open("wb")
        compressed = gzip.GzipFile(
            filename="", mode="wb", fileobj=raw, compresslevel=6, mtime=0
        )
        text = io.TextIOWrapper(compressed, encoding="utf-8", newline="")
        for row in rows:
            text.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
            text.write("\n")
        text.close()
        raw.close()
        self._refresh_manifest_file_stat(target)

    def _refresh_manifest_file_stat(self, target: Path) -> None:
        manifest_path = self.root / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        relative_path = target.relative_to(self.root).as_posix()
        content = target.read_bytes()
        for item in manifest["files"]:
            if item["relative_path"] == relative_path:
                item["byte_size"] = len(content)
                item["sha256"] = hashlib.sha256(content).hexdigest()
                break
        else:
            self.fail(f"manifest has no entry for {relative_path}")
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )


if __name__ == "__main__":
    unittest.main()

