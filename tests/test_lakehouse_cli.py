from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from marketpulse_lakehouse.cli import build_parser, main


class LakehouseCliTests(unittest.TestCase):
    def test_parser_exposes_the_four_bronze_commands_and_required_arguments(self) -> None:
        parser = build_parser()

        self.assertEqual(parser.parse_args(["bootstrap"]).command, "bootstrap")
        capture = parser.parse_args(
            ["capture-offsets", "--run-id", "run-1", "--output", "offsets.json"]
        )
        self.assertEqual(capture.bootstrap_servers, "kafka:29092")
        ingest_kafka = parser.parse_args(
            [
                "ingest-kafka",
                "--run-id",
                "run-1",
                "--offset-manifest",
                "offsets.json",
            ]
        )
        self.assertEqual(ingest_kafka.offset_manifest, Path("offsets.json"))
        ingest_files = parser.parse_args(
            ["ingest-files", "--run-id", "run-1", "--input", "/data/fixture"]
        )
        self.assertEqual(ingest_files.input, Path("/data/fixture"))
        build_silver = parser.parse_args(
            [
                "build-silver",
                "--run-id",
                "run-1",
                "--bronze-snapshots",
                "snapshots.json",
            ]
        )
        self.assertEqual(build_silver.bronze_snapshots, Path("snapshots.json"))
        self.assertEqual(build_silver.target_namespace, "silver")
        build_gold = parser.parse_args(
            [
                "build-gold",
                "--run-id",
                "run-1",
                "--silver-snapshots",
                "silver.json",
            ]
        )
        self.assertEqual(build_gold.silver_snapshots, Path("silver.json"))
        self.assertEqual(build_gold.source_namespace, "silver")
        self.assertEqual(build_gold.target_namespace, "gold")
        write_evidence = parser.parse_args(
            [
                "write-evidence",
                "--run-id",
                "run-1",
                "--bronze-snapshots",
                "bronze.json",
                "--silver-snapshots",
                "silver.json",
                "--gold-snapshots",
                "gold.json",
                "--later-silver-snapshots",
                "later-silver.json",
                "--later-gold-snapshots",
                "later-gold.json",
                "--offset-manifest",
                "offsets.json",
                "--file-manifest",
                "manifest.json",
                "--output",
                "evidence.json",
            ]
        )
        self.assertEqual(write_evidence.output, Path("evidence.json"))
        self.assertEqual(
            parser.parse_args(["verify", "--evidence", "evidence.json"]).evidence,
            Path("evidence.json"),
        )
        self.assertEqual(
            parser.parse_args(
                ["backfill-proof", "--evidence", "evidence.json"]
            ).command,
            "backfill-proof",
        )

    def test_main_dispatches_and_prints_one_machine_readable_summary(self) -> None:
        expected = {"command": "bootstrap", "tables": 13}
        output = io.StringIO()
        with patch("marketpulse_lakehouse.cli._run_bootstrap", return_value=expected) as run:
            with redirect_stdout(output):
                exit_code = main(["bootstrap"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(output.getvalue()), expected)
        run.assert_called_once_with()

    def test_capture_writes_manifest_atomically_and_reports_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output_path = Path(temporary) / "offsets.json"
            result = {
                "command": "capture-offsets",
                "manifest_json": '{"version":1}',
                "manifest_sha256": "abc",
            }
            output = io.StringIO()
            with patch(
                "marketpulse_lakehouse.cli._run_capture_offsets", return_value=result
            ):
                with redirect_stdout(output):
                    exit_code = main(
                        [
                            "capture-offsets",
                            "--run-id",
                            "run-1",
                            "--output",
                            str(output_path),
                        ]
                    )

            self.assertEqual(exit_code, 0)
            self.assertEqual(output_path.read_text(), '{"version":1}\n')
            summary = json.loads(output.getvalue())
            self.assertNotIn("manifest_json", summary)
            self.assertEqual(summary["manifest_sha256"], "abc")

    def test_runtime_error_returns_two_without_a_success_summary(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch(
            "marketpulse_lakehouse.cli._run_ingest_files",
            side_effect=ValueError("checksum mismatch"),
        ):
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = main(
                    ["ingest-files", "--run-id", "run-1", "--input", "/data"]
                )

        self.assertEqual(exit_code, 2)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("checksum mismatch", stderr.getvalue())

    def test_build_silver_reads_explicit_snapshot_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            snapshot_path = Path(temporary) / "snapshots.json"
            snapshot_path.write_text(
                json.dumps(
                    {
                        "bronze.kafka_records": 101,
                        "bronze.source_files": 151,
                        "bronze.source_file_rows": 202,
                    }
                )
            )
            expected = {"command": "build-silver", "counts": {}}
            output = io.StringIO()
            with patch(
                "marketpulse_lakehouse.cli._run_build_silver", return_value=expected
            ) as run:
                with redirect_stdout(output):
                    exit_code = main(
                        [
                            "build-silver",
                            "--run-id",
                            "run-1",
                            "--bronze-snapshots",
                            str(snapshot_path),
                        ]
                    )

            self.assertEqual(exit_code, 0)
            self.assertEqual(json.loads(output.getvalue()), expected)
            run.assert_called_once_with(
                run_id="run-1",
                bronze_snapshots=snapshot_path,
                target_namespace="silver",
            )

    def test_build_gold_reads_explicit_snapshot_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            snapshot_path = Path(temporary) / "silver.json"
            snapshot_path.write_text(
                json.dumps(
                    {
                        "silver.customer_events": 1,
                        "silver.customers": 2,
                        "silver.campaigns": 3,
                        "silver.ad_spend_daily": 4,
                        "silver.quality_summary": 5,
                    }
                )
            )
            expected = {"command": "build-gold", "counts": {}}
            output = io.StringIO()
            with patch(
                "marketpulse_lakehouse.cli._run_build_gold", return_value=expected
            ) as run:
                with redirect_stdout(output):
                    exit_code = main(
                        [
                            "build-gold",
                            "--run-id",
                            "run-1",
                            "--silver-snapshots",
                            str(snapshot_path),
                        ]
                    )

            self.assertEqual(exit_code, 0)
            self.assertEqual(json.loads(output.getvalue()), expected)
            run.assert_called_once_with(
                run_id="run-1",
                silver_snapshots=snapshot_path,
                source_namespace="silver",
                target_namespace="gold",
            )


if __name__ == "__main__":
    unittest.main()
