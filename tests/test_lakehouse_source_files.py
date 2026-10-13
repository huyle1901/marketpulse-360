from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from datetime import UTC, date, datetime
from pathlib import Path

from marketpulse_lakehouse.identity import file_ingest_id
from marketpulse_lakehouse.source_files import (
    archive_source_objects,
    build_source_archive_plan,
    create_s3_client,
    normalize_source_file,
    source_file_record,
)


ROOT = Path(__file__).resolve().parents[1]


def _gzip_csv(rows: list[dict[str, str]], fieldnames: list[str]) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as compressed:
        text = io.TextIOWrapper(compressed, encoding="utf-8", newline="")
        writer = csv.DictWriter(text, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        text.flush()
    return buffer.getvalue()


def _gzip_jsonl(rows: list[dict[str, object]]) -> bytes:
    content = "".join(json.dumps(row) + "\n" for row in rows).encode()
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as compressed:
        compressed.write(content)
    return buffer.getvalue()


class SourceFileIngestionTests(unittest.TestCase):
    def test_s3_client_requires_explicit_nonblank_password_before_connecting(self) -> None:
        boto = SimpleNamespace(client=lambda *args, **kwargs: object())
        for env in ({}, {"MINIO_ROOT_PASSWORD": ""}, {"MINIO_ROOT_PASSWORD": "   "}):
            with self.subTest(env=env), patch.dict("os.environ", env, clear=True), \
                    patch.dict("sys.modules", {"boto3": boto}):
                with self.assertRaisesRegex(ValueError, "MINIO_ROOT_PASSWORD"):
                    create_s3_client()

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_manifest(self, files: list[dict[str, object]]) -> bytes:
        content = json.dumps({"files": files}, indent=2, sort_keys=True).encode() + b"\n"
        (self.root / "manifest.json").write_bytes(content)
        return content

    def _write_source(
        self,
        relative_path: str,
        dataset: str,
        format_name: str,
        content: bytes,
        row_count: int,
    ) -> dict[str, object]:
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return {
            "dataset": dataset,
            "relative_path": relative_path,
            "format": format_name,
            "row_count": row_count,
            "byte_size": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
        }

    def test_plan_archives_exact_manifest_and_only_allowlisted_compressed_files(self) -> None:
        crm = _gzip_csv(
            [{"customer_id": "c-1", "tier": ""}],
            ["customer_id", "tier"],
        )
        web = _gzip_jsonl([{"event_id": "event-1"}])
        crm_item = self._write_source(
            "crm/date=2026-01-02/part-00000.csv.gz", "crm", "csv", crm, 1
        )
        web_item = self._write_source(
            "web_events/date=2026-01-02/part-00000.jsonl.gz",
            "web_events",
            "jsonl",
            web,
            1,
        )
        manifest_bytes = self._write_manifest([crm_item, web_item])

        plan = build_source_archive_plan(self.root)

        self.assertEqual(plan.manifest_sha256, hashlib.sha256(manifest_bytes).hexdigest())
        self.assertEqual(
            [item.source_dataset for item in plan.archive_objects],
            ["_manifest", "crm"],
        )
        self.assertEqual(plan.archive_objects[0].content, manifest_bytes)
        self.assertEqual(plan.archive_objects[1].content, crm)
        digest = hashlib.sha256(crm).hexdigest()
        self.assertEqual(
            plan.archive_objects[1].source_object_uri,
            "s3://marketpulse-lakehouse/bronze/source-files/"
            f"sha256={digest}/part-00000.csv.gz",
        )

    def test_csv_and_jsonl_rows_are_canonical_one_based_and_dated(self) -> None:
        csv_bytes = _gzip_csv(
            [
                {"customer_id": "c-1", "tier": "gold"},
                {"customer_id": "c-2", "tier": ""},
            ],
            ["customer_id", "tier"],
        )
        jsonl_bytes = _gzip_jsonl([{"z": 1, "a": "á"}])
        csv_item = self._write_source(
            "crm/date=2026-01-02/part-00000.csv.gz", "crm", "csv", csv_bytes, 2
        )
        jsonl_item = self._write_source(
            "meta_ads/date=2026-01-03/part-00000.jsonl.gz",
            "meta_ads",
            "jsonl",
            jsonl_bytes,
            1,
        )
        self._write_manifest([csv_item, jsonl_item])
        plan = build_source_archive_plan(self.root)
        ingested_at = datetime(2026, 1, 4, tzinfo=UTC)

        crm_rows = normalize_source_file(
            plan.source_files[0], run_id="run-1", ingested_at=ingested_at
        )
        meta_rows = normalize_source_file(
            plan.source_files[1], run_id="run-1", ingested_at=ingested_at
        )

        csv_digest = hashlib.sha256(csv_bytes).hexdigest()
        self.assertEqual([row["row_number"] for row in crm_rows], [1, 2])
        self.assertEqual(crm_rows[0]["ingest_id"], file_ingest_id(csv_digest, 1))
        self.assertEqual(crm_rows[0]["payload_json"], '{"customer_id":"c-1","tier":"gold"}')
        self.assertEqual(crm_rows[1]["payload_json"], '{"customer_id":"c-2","tier":null}')
        self.assertEqual(crm_rows[0]["source_date"], date(2026, 1, 2))
        self.assertEqual(meta_rows[0]["payload_json"], '{"a":"á","z":1}')
        self.assertEqual(meta_rows[0]["source_date"], date(2026, 1, 3))
        for row in (*crm_rows, *meta_rows):
            self.assertEqual(
                row["payload_sha256"],
                hashlib.sha256(row["payload_json"].encode()).hexdigest(),
            )
            self.assertEqual(row["ingestion_run_id"], "run-1")
            self.assertEqual(row["ingested_at"], ingested_at)

    def test_master_rows_have_null_source_date_and_repeat_stable_identity(self) -> None:
        content = _gzip_csv([{"campaign_id": "campaign-1"}], ["campaign_id"])
        item = self._write_source(
            "master/campaigns.csv.gz", "master/campaigns", "csv", content, 1
        )
        self._write_manifest([item])

        first = build_source_archive_plan(self.root)
        second = build_source_archive_plan(self.root)
        first_rows = normalize_source_file(
            first.source_files[0],
            run_id="first",
            ingested_at=datetime(2026, 1, 2, tzinfo=UTC),
        )
        second_rows = normalize_source_file(
            second.source_files[0],
            run_id="second",
            ingested_at=datetime(2026, 1, 3, tzinfo=UTC),
        )

        self.assertIsNone(first_rows[0]["source_date"])
        self.assertEqual(first_rows[0]["ingest_id"], second_rows[0]["ingest_id"])
        self.assertEqual(first.archive_objects, second.archive_objects)

    def test_file_changed_after_planning_is_rejected_before_normalization(self) -> None:
        original = _gzip_csv([{"campaign_id": "campaign-1"}], ["campaign_id"])
        item = self._write_source(
            "master/campaigns.csv.gz", "master/campaigns", "csv", original, 1
        )
        self._write_manifest([item])
        plan = build_source_archive_plan(self.root)
        changed = _gzip_csv([{"campaign_id": "campaign-2"}], ["campaign_id"])
        (self.root / "master/campaigns.csv.gz").write_bytes(changed)

        with self.assertRaisesRegex(ValueError, "changed after planning"):
            normalize_source_file(
                plan.source_files[0],
                run_id="run",
                ingested_at=datetime(2026, 1, 2, tzinfo=UTC),
            )

    def test_source_file_record_uses_manifest_lineage_and_operational_values(self) -> None:
        content = _gzip_jsonl([{"campaign_id": "campaign-1"}])
        item = self._write_source(
            "tiktok_ads/date=2026-01-02/part.jsonl.gz",
            "tiktok_ads",
            "jsonl",
            content,
            1,
        )
        self._write_manifest([item])
        plan = build_source_archive_plan(self.root)
        archived_at = datetime(2026, 1, 3, tzinfo=UTC)

        record = source_file_record(
            plan.source_files[0],
            manifest_sha256=plan.manifest_sha256,
            run_id="run-2",
            archived_at=archived_at,
        )

        self.assertEqual(record["file_sha256"], hashlib.sha256(content).hexdigest())
        self.assertEqual(record["compressed_size_bytes"], len(content))
        self.assertEqual(record["manifest_sha256"], plan.manifest_sha256)
        self.assertEqual(record["ingestion_run_id"], "run-2")
        self.assertEqual(record["archived_at"], archived_at)

    def test_archive_upload_is_exact_idempotent_and_rejects_object_collision(self) -> None:
        content = _gzip_jsonl([{"campaign_id": "campaign-1"}])
        item = self._write_source(
            "tiktok_ads/date=2026-01-02/part.jsonl.gz",
            "tiktok_ads",
            "jsonl",
            content,
            1,
        )
        self._write_manifest([item])
        plan = build_source_archive_plan(self.root)

        class NotFound(Exception):
            response = {"Error": {"Code": "404"}}

        class MemoryS3:
            def __init__(self) -> None:
                self.objects: dict[tuple[str, str], tuple[bytes, dict[str, str]]] = {}

            def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
                try:
                    body, metadata = self.objects[(Bucket, Key)]
                except KeyError as error:
                    raise NotFound from error
                return {
                    "ContentLength": len(body),
                    "Metadata": metadata,
                    "Body": io.BytesIO(body),
                }

            def upload_file(
                self,
                Filename: str,
                Bucket: str,
                Key: str,
                ExtraArgs: dict[str, object],
            ) -> None:
                metadata = dict(ExtraArgs["Metadata"])  # type: ignore[arg-type]
                self.objects[(Bucket, Key)] = (Path(Filename).read_bytes(), metadata)

        client = MemoryS3()
        self.assertEqual(archive_source_objects(plan, client=client), (2, 0))
        self.assertEqual(archive_source_objects(plan, client=client), (0, 2))
        self.assertEqual(
            {body for body, _metadata in client.objects.values()},
            {plan.archive_objects[0].content, content},
        )
        first_key = next(iter(client.objects))
        body, _metadata = client.objects[first_key]
        client.objects[first_key] = (body, {"sha256": "0" * 64})
        with self.assertRaisesRegex(ValueError, "SOURCE_OBJECT_COLLISION"):
            archive_source_objects(plan, client=client)

        client.objects[first_key] = (
            b"x" * len(body),
            {"sha256": hashlib.sha256(body).hexdigest()},
        )
        with self.assertRaisesRegex(ValueError, "SOURCE_OBJECT_COLLISION"):
            archive_source_objects(plan, client=client)

    def test_archive_upload_uses_a_stable_copy_when_source_mutates_during_upload(self) -> None:
        content = _gzip_jsonl([{"campaign_id": "campaign-1"}])
        item = self._write_source(
            "tiktok_ads/date=2026-01-02/part.jsonl.gz",
            "tiktok_ads",
            "jsonl",
            content,
            1,
        )
        self._write_manifest([item])
        plan = build_source_archive_plan(self.root)

        class NotFound(Exception):
            response = {"Error": {"Code": "404"}}

        class MutatingS3:
            def __init__(self) -> None:
                self.objects: dict[tuple[str, str], tuple[bytes, dict[str, str]]] = {}

            def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
                try:
                    body, metadata = self.objects[(Bucket, Key)]
                except KeyError as error:
                    raise NotFound from error
                return {
                    "ContentLength": len(body),
                    "Metadata": metadata,
                    "Body": io.BytesIO(body),
                }

            def upload_file(
                self,
                Filename: str,
                Bucket: str,
                Key: str,
                ExtraArgs: dict[str, object],
            ) -> None:
                # The original input can change after staging without changing the upload.
                if Key.endswith("part.jsonl.gz"):
                    plan.archive_objects[-1].local_path.write_bytes(b"mutated")
                metadata = dict(ExtraArgs["Metadata"])  # type: ignore[arg-type]
                self.objects[(Bucket, Key)] = (Path(Filename).read_bytes(), metadata)

        client = MutatingS3()
        self.assertEqual(archive_source_objects(plan, client=client), (2, 0))
        uploaded = {body for body, _metadata in client.objects.values()}
        self.assertIn(content, uploaded)

    def test_plan_rejects_same_digest_with_conflicting_file_lineage(self) -> None:
        content = _gzip_csv([{"customer_id": "c-1"}], ["customer_id"])
        first = self._write_source(
            "crm/date=2026-01-01/part.csv.gz", "crm", "csv", content, 1
        )
        second = self._write_source(
            "crm/date=2026-01-02/part.csv.gz", "crm", "csv", content, 1
        )
        self._write_manifest([first, second])

        with self.assertRaisesRegex(ValueError, "duplicate content digest"):
            build_source_archive_plan(self.root)

    def test_committed_fixture_has_twelve_business_files_and_380_rows(self) -> None:
        plan = build_source_archive_plan(ROOT / "data/mock/fixtures/seed-360")
        ingested_at = datetime(2026, 1, 4, tzinfo=UTC)

        rows = [
            row
            for source in plan.source_files
            for row in normalize_source_file(
                source, run_id="fixture-proof", ingested_at=ingested_at
            )
        ]

        self.assertEqual(len(plan.archive_objects), 13)
        self.assertEqual(len(plan.source_files), 12)
        self.assertEqual(len(rows), 380)
        self.assertEqual(
            {source.source_dataset for source in plan.source_files},
            {
                "crm",
                "google_ads",
                "meta_ads",
                "tiktok_ads",
                "master/ads",
                "master/campaigns",
                "master/customers",
                "master/products",
            },
        )

    def test_checksum_path_and_row_count_errors_fail_closed(self) -> None:
        content = _gzip_jsonl([{"a": 1}])
        item = self._write_source(
            "meta_ads/date=2026-01-02/part.jsonl.gz",
            "meta_ads",
            "jsonl",
            content,
            1,
        )
        item["sha256"] = "0" * 64
        self._write_manifest([item])
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            build_source_archive_plan(self.root)

        item["sha256"] = hashlib.sha256(content).hexdigest()
        item["relative_path"] = "../outside.jsonl.gz"
        self._write_manifest([item])
        with self.assertRaisesRegex(ValueError, "relative path"):
            build_source_archive_plan(self.root)

        item["relative_path"] = "meta_ads/date=2026-01-02/part.jsonl.gz"
        item["row_count"] = 2
        self._write_manifest([item])
        plan = build_source_archive_plan(self.root)
        with self.assertRaisesRegex(ValueError, "row-count mismatch"):
            normalize_source_file(
                plan.source_files[0],
                run_id="run",
                ingested_at=datetime(2026, 1, 2, tzinfo=UTC),
            )


if __name__ == "__main__":
    unittest.main()
