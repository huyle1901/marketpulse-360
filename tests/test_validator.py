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

    def test_vendor_spend_mismatch_is_detected(self) -> None:
        target = next((self.root / "google_ads").rglob("*.csv.gz"))
        with gzip.open(target, "rt", encoding="utf-8", newline="") as stream:
            header = stream.readline()
            rows = [line for line in stream]
        columns = header.rstrip("\n").split(",")
        values = rows[0].rstrip("\n").split(",")
        cost_index = columns.index("cost_micros")
        values[cost_index] = str(int(values[cost_index]) + 1_000_000)
        rows[0] = ",".join(values) + "\n"
        raw = target.open("wb")
        compressed = gzip.GzipFile(
            filename="", mode="wb", fileobj=raw, compresslevel=6, mtime=0
        )
        text = io.TextIOWrapper(compressed, encoding="utf-8", newline="")
        text.write(header)
        text.writelines(rows)
        text.close()
        raw.close()
        self._refresh_manifest_file_stat(target)

        report = validate_dataset(self.root)

        self.assertFalse(report.valid)
        self.assertTrue(
            any("vendor/ground-truth mismatch" in error for error in report.errors),
            report.errors,
        )

    def test_manifest_anomaly_count_mismatch_is_detected(self) -> None:
        manifest_path = self.root / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["anomalies"]["late_events"] += 1
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        report = validate_dataset(self.root)

        self.assertFalse(report.valid)
        self.assertTrue(
            any("anomaly-count mismatch" in error for error in report.errors),
            report.errors,
        )

    def test_invalid_event_source_and_boolean_type_are_detected(self) -> None:
        def mutate(row: dict[str, object]) -> None:
            row["source"] = "telepathy"
            row["consent_analytics"] = "true"

        self._mutate_first_json_record("web_events", mutate)

        report = validate_dataset(self.root)

        self.assertFalse(report.valid)
        self.assertTrue(any("invalid source" in error for error in report.errors))
        self.assertTrue(any("consent_analytics must be boolean" in error for error in report.errors))

    def test_event_time_must_be_utc_and_match_its_partition(self) -> None:
        def mutate(row: dict[str, object]) -> None:
            row["event_time"] = "2030-01-01T00:00:00+07:00"

        self._mutate_first_json_record("web_events", mutate)

        report = validate_dataset(self.root)

        self.assertFalse(report.valid)
        self.assertTrue(any("event_time must be UTC" in error for error in report.errors))
        self.assertTrue(any("partition date mismatch" in error for error in report.errors))

    def test_manifest_schema_versions_are_verified(self) -> None:
        manifest_path = self.root / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["schema_versions"]["web_events"] = 999
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        report = validate_dataset(self.root)

        self.assertFalse(report.valid)
        self.assertTrue(any("schema_versions mismatch" in error for error in report.errors))

    def test_manifest_file_time_bounds_are_verified(self) -> None:
        manifest_path = self.root / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        web_file = next(
            item for item in manifest["files"] if item["dataset"] == "web_events"
        )
        web_file["max_event_time"] = "2099-01-01T00:00:00Z"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        report = validate_dataset(self.root)

        self.assertFalse(report.valid)
        self.assertTrue(any("time-bound mismatch" in error for error in report.errors))

    def test_negative_refund_amount_is_detected(self) -> None:
        def mutate(row: dict[str, object]) -> None:
            if row.get("event_name") == "order_refunded":
                payload = row["payload"]
                assert isinstance(payload, dict)
                payload["refund_amount_minor"] = -1

        self._mutate_first_json_record(
            "orders", mutate, event_name="order_refunded"
        )

        report = validate_dataset(self.root)

        self.assertFalse(report.valid)
        self.assertTrue(any("invalid refund amount" in error for error in report.errors))

    def test_broken_funnel_stage_is_detected(self) -> None:
        def mutate(row: dict[str, object]) -> None:
            row["event_name"] = "page_view"

        self._mutate_first_json_record(
            "web_events", mutate, event_name="add_to_cart"
        )

        report = validate_dataset(self.root)

        self.assertFalse(report.valid)
        self.assertTrue(any("broken funnel" in error for error in report.errors))

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
