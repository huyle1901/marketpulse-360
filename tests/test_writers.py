from __future__ import annotations

import gzip
import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from marketpulse_mock.journeys import SourceRecord
from marketpulse_mock.writers import DatasetWriter


class DatasetWriterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def _web_record(self, ordinal: int) -> SourceRecord:
        event_time = self.start + timedelta(seconds=ordinal)
        return SourceRecord(
            dataset="web_events",
            event_time=event_time,
            payload={
                "event_id": f"event-{ordinal}",
                "event_name": "page_view",
                "event_time": event_time.isoformat().replace("+00:00", "Z"),
                "ingested_at": event_time.isoformat().replace("+00:00", "Z"),
                "source": "web",
                "schema_version": 1,
                "customer_id": None,
                "anonymous_id": "anonymous-1",
                "session_id": "session-1",
                "campaign_id": "campaign-1",
                "platform_campaign_id": "G-1",
                "click_id": "gclid-1",
                "consent_analytics": True,
                "is_late": False,
                "payload": {"page_url": "https://example.test"},
            },
        )

    def test_jsonl_writer_rotates_and_preserves_key_order(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = DatasetWriter(Path(temporary_directory), rows_per_file=2)
            for ordinal in range(3):
                writer.write(self._web_record(ordinal))
            stats = writer.close()

            relative_paths = [stat.relative_path for stat in stats]
            self.assertEqual(
                [
                    "web_events/date=2026-01-01/part-00000.jsonl.gz",
                    "web_events/date=2026-01-01/part-00001.jsonl.gz",
                ],
                relative_paths,
            )
            self.assertEqual([2, 1], [stat.row_count for stat in stats])
            with gzip.open(
                Path(temporary_directory) / relative_paths[0], "rt", encoding="utf-8"
            ) as stream:
                first_line = stream.readline().strip()
            self.assertTrue(first_line.startswith('{"event_id":"event-0","event_name"'))

    def test_csv_writer_uses_the_version_one_header(self) -> None:
        report = SourceRecord(
            dataset="google_ads",
            event_time=self.start,
            payload={
                "segments_date": "2026-01-01",
                "campaign_id": "G-1",
                "impressions": 10,
                "clicks": 2,
                "cost_micros": 10_000_000,
                "conversions": 1,
                "conversions_value": 500_000,
            },
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = DatasetWriter(Path(temporary_directory), rows_per_file=10)
            writer.write(report)
            stat = writer.close()[0]
            with gzip.open(
                Path(temporary_directory) / stat.relative_path, "rt", encoding="utf-8"
            ) as stream:
                lines = stream.read().splitlines()

        self.assertEqual(
            "segments_date,campaign_id,impressions,clicks,cost_micros,conversions,conversions_value",
            lines[0],
        )
        self.assertEqual("2026-01-01,G-1,10,2,10000000,1,500000", lines[1])

    def test_partition_date_comes_from_utc_event_time(self) -> None:
        record = self._web_record(0)
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = DatasetWriter(Path(temporary_directory), rows_per_file=10)
            writer.write(record)
            relative_path = writer.close()[0].relative_path

        self.assertIn("date=2026-01-01", relative_path)

    def test_writer_rejects_decreasing_event_time_within_a_dataset_partition(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = DatasetWriter(Path(temporary_directory), rows_per_file=10)
            writer.write(self._web_record(2))
            with self.assertRaisesRegex(ValueError, "nondecreasing event_time"):
                writer.write(self._web_record(1))
            writer.close()

    def test_file_stats_hash_the_final_compressed_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            writer = DatasetWriter(root, rows_per_file=10)
            writer.write(self._web_record(0))
            stat = writer.close()[0]

            expected_hash = hashlib.sha256(
                (root / stat.relative_path).read_bytes()
            ).hexdigest()

        self.assertEqual(expected_hash, stat.sha256)
        self.assertGreater(stat.byte_size, 0)

    def test_same_records_produce_identical_compressed_bytes(self) -> None:
        hashes: list[str] = []
        for _ in range(2):
            with tempfile.TemporaryDirectory() as temporary_directory:
                writer = DatasetWriter(Path(temporary_directory), rows_per_file=10)
                writer.write(self._web_record(0))
                writer.write(self._web_record(1))
                hashes.append(writer.close()[0].sha256)

        self.assertEqual(hashes[0], hashes[1])

    def test_master_files_are_not_date_partitioned(self) -> None:
        record = SourceRecord(
            dataset="master/customers",
            event_time=self.start,
            payload={
                "customer_id": "customer-1",
                "email": "customer-1@marketpulse.example.test",
                "phone_token": "synthetic-0000000001",
                "created_at": "2025-01-01T00:00:00Z",
                "country": "VN",
                "city": "Da Nang",
                "loyalty_tier": "silver",
                "consent_analytics": True,
                "consent_marketing": True,
            },
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = DatasetWriter(Path(temporary_directory), rows_per_file=10)
            writer.write(record)
            relative_path = writer.close()[0].relative_path

        self.assertEqual("master/customers.csv.gz", relative_path)

    def test_csv_booleans_use_lowercase_contract_values(self) -> None:
        record = SourceRecord(
            dataset="master/customers",
            event_time=self.start,
            payload={
                "customer_id": "customer-1",
                "email": "customer-1@marketpulse.example.test",
                "phone_token": "synthetic-0000000001",
                "created_at": "2025-01-01T00:00:00Z",
                "country": "VN",
                "city": "Da Nang",
                "loyalty_tier": "silver",
                "consent_analytics": True,
                "consent_marketing": False,
            },
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = DatasetWriter(Path(temporary_directory), rows_per_file=10)
            writer.write(record)
            relative_path = writer.close()[0].relative_path
            with gzip.open(
                Path(temporary_directory) / relative_path, "rt", encoding="utf-8"
            ) as stream:
                row = stream.read().splitlines()[1]

        self.assertTrue(row.endswith(",true,false"), row)

    def test_close_does_not_leak_the_underlying_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = DatasetWriter(Path(temporary_directory), rows_per_file=10)
            writer.write(self._web_record(0))
            raw_stream = next(iter(writer._parts.values())).raw_stream
            writer.close()

        self.assertTrue(raw_stream.closed)


if __name__ == "__main__":
    unittest.main()
