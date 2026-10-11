from __future__ import annotations

import json
import gzip
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

from marketpulse_mock.replay_reader import iter_replay_records
from marketpulse_mock.validator import ValidationReport


FIXTURE = Path("data/mock/fixtures/seed-360")


class ReplayReaderTests(unittest.TestCase):
    def test_fixture_records_are_merged_in_event_time_order(self) -> None:
        records = list(iter_replay_records(FIXTURE))

        self.assertEqual(
            Counter({"web_events": 199, "orders": 14, "email_events": 6}),
            Counter(record.dataset for record in records),
        )
        self.assertEqual(
            sorted(record.event_time for record in records),
            [record.event_time for record in records],
        )
        self.assertTrue(all(record.key for record in records))
        first_payload = json.loads(records[0].value)
        self.assertEqual(first_payload["event_id"], records[0].event_id)
        self.assertNotIn(b" ", records[0].value)
        self.assertEqual(
            {"dataset", "schema-version", "event-id", "source-file"},
            {name for name, _ in records[0].headers},
        )

    def test_quality_cases_are_optionally_mapped_to_dlq_records(self) -> None:
        without_dlq = list(iter_replay_records(FIXTURE))
        with_dlq = list(iter_replay_records(FIXTURE, include_dlq=True))
        dlq = [record for record in with_dlq if record.dataset == "quality_cases"]

        self.assertEqual(len(without_dlq) + 4, len(with_dlq))
        self.assertEqual(4, len(dlq))
        payload = json.loads(dlq[0].value)
        self.assertEqual(dlq[0].key, payload["case_id"])
        self.assertEqual("web_events", payload["source_dataset"])
        self.assertEqual("marketing.raw.web-events.v1", payload["source_topic"])
        self.assertIn(
            payload["reason_code"],
            {
                "MISSING_REQUIRED_FIELD",
                "UNSUPPORTED_SCHEMA_VERSION",
                "ORPHAN_CLICK_ID",
            },
        )

    def test_manifest_file_order_does_not_change_equal_time_order(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            copied = Path(temporary_directory) / "fixture"
            import shutil

            shutil.copytree(FIXTURE, copied)
            manifest_path = copied / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["files"].reverse()
            manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

            original = [
                (record.event_time, record.dataset, record.event_id)
                for record in iter_replay_records(FIXTURE, include_dlq=True)
            ]
            reordered = [
                (record.event_time, record.dataset, record.event_id)
                for record in iter_replay_records(copied, include_dlq=True)
            ]

        self.assertEqual(original, reordered)

    def test_invalid_dataset_is_rejected_before_any_record_is_yielded(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            copied = Path(temporary_directory) / "fixture"
            import shutil

            shutil.copytree(FIXTURE, copied)
            target = next((copied / "web_events").rglob("*.jsonl.gz"))
            with target.open("ab") as stream:
                stream.write(b"tampered")

            iterator = iter_replay_records(copied)
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                next(iterator)

    def test_rotated_equal_timestamp_parts_use_path_tie_breaking(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            paths = []
            for part, event_id in ((1, "later-path"), (0, "earlier-path")):
                relative = f"web_events/date=2026-01-01/part-{part:05d}.jsonl.gz"
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                row = {
                    "event_id": event_id,
                    "event_time": "2026-01-01T00:00:00Z",
                    "customer_id": "customer-1",
                    "session_id": "session-1",
                }
                with gzip.open(path, "wt", encoding="utf-8") as stream:
                    stream.write(json.dumps(row) + "\n")
                paths.append(relative)
            (root / "manifest.json").write_text(
                json.dumps(
                    {
                        "files": [
                            {"dataset": "web_events", "relative_path": path}
                            for path in paths
                        ]
                    }
                )
            )

            with patch(
                "marketpulse_mock.replay_reader.validate_dataset",
                return_value=ValidationReport((), 2, 2),
            ):
                result = list(iter_replay_records(root))

        self.assertEqual(["earlier-path", "later-path"], [row.event_id for row in result])


if __name__ == "__main__":
    unittest.main()
