from __future__ import annotations

import os
import unittest
from dataclasses import replace


@unittest.skipUnless(os.environ.get("BATCH_TEST_DAGSTER"), "opt-in Dagster runtime")
class DagsterDefinitionTests(unittest.TestCase):
    def test_source_history_backfill_ingests_without_receipts(self) -> None:
        import tempfile
        from pathlib import Path
        from types import SimpleNamespace
        from unittest.mock import patch
        from marketpulse_batch.definitions import _source
        with tempfile.TemporaryDirectory() as root:
            context = SimpleNamespace(job_name="source_history_job", run_id="new-history",
                run=SimpleNamespace(tags={"dagster/backfill": "new-history"}), partition_key="2026-01-01",
                resources=SimpleNamespace(batch_settings=SimpleNamespace(state_path=Path(root), source_root=Path(root))),
                asset_key=SimpleNamespace(path=["ad_reports"]), add_output_metadata=lambda _: None)
            def ingest(settings, operation, payload):
                self.assertEqual(operation, "ingest_files")
                self.assertEqual(payload["selection"]["logical_day"], "2026-01-01")
                return {"inserted_rows": 7}
            with patch("marketpulse_batch.definitions.run_spark_operation", side_effect=ingest):
                uri = _source(context, ["google_ads"])
            import json
            self.assertEqual(json.loads(Path(uri.removeprefix("file://")).read_text()), {"inserted_rows": 7})

    def test_daily_backfill_stages_share_one_partition_per_run(self) -> None:
        from dagster import BackfillPolicy
        from marketpulse_batch.definitions import ALL_ASSETS
        for asset in ALL_ASSETS:
            if asset.partitions_def is not None:
                with self.subTest(asset=asset.node_def.name):
                    self.assertEqual(asset.backfill_policy, BackfillPolicy.multi_run(max_partitions_per_run=1))

    def test_dbt_retry_loads_persisted_upstream_candidate(self) -> None:
        import json
        import tempfile
        from pathlib import Path
        from types import SimpleNamespace
        from marketpulse_batch.definitions import marketing_dbt
        from marketpulse_batch.settings import load_settings
        from marketpulse_batch.ledger import BatchLedger
        from marketpulse_batch.resources import clickhouse_client
        from marketpulse_batch.publication import resolve_active
        settings = load_settings(os.environ)
        client = clickhouse_client(settings)
        try:
            release = resolve_active(client)
        finally:
            client.close()
        ledger = BatchLedger(settings.ledger_dsn)
        _, receipt_uri = ledger.get_receipt(release.identity)
        bundle = ledger.get_certified_bundle(release.identity)
        with tempfile.TemporaryDirectory() as root:
            candidate = Path(root) / "previous-run" / "candidate.json"
            candidate.parent.mkdir()
            candidate.write_text(json.dumps({"receipt_uri": receipt_uri, "manifest_uri": bundle.export_manifest_uri,
                                             "database": bundle.database, "certification_uri": bundle.certification_uri}))
            context = SimpleNamespace(run_id="different-reexecution-id", job_name="marketing_refresh_job",
                run=SimpleNamespace(tags={}, parent_run_id="previous-run"),
                instance=SimpleNamespace(get_run_by_id=lambda _: SimpleNamespace(parent_run_id=None)),
                resources=SimpleNamespace(batch_settings=replace(settings, state_path=Path(root))))
            old_directory = Path(root) / "dagster-runs" / "previous-run"
            old_directory.mkdir(parents=True)
            candidate = candidate.rename(old_directory / "candidate.json")
            outputs = list(marketing_dbt.op.compute_fn.decorated_fn(context))
            self.assertEqual(len(outputs), 8)
            result = json.loads((candidate.parent / "dbt_result.json").read_text())
            self.assertTrue(Path(result["target_path"]).is_dir())

    def test_backfill_no_publish(self) -> None:
        from types import SimpleNamespace
        from marketpulse_batch.definitions import active_release
        metadata = {}
        context = SimpleNamespace(job_name="historical_rebuild_job", run=SimpleNamespace(tags={}),
                                  add_output_metadata=metadata.update)
        # No resources exist on this context: a publication attempt would fail.
        self.assertEqual(active_release.op.compute_fn.decorated_fn(context, "file:///owned/certification.json"),
                         "file:///owned/certification.json")
        self.assertEqual(metadata["publication"], "skipped_historical_rebuild")

    def test_downstream_retry_pins_ancestry_over_later_partition_materialization(self) -> None:
        import json
        import tempfile
        from pathlib import Path
        from types import SimpleNamespace
        from marketpulse_batch.definitions import certified_release, active_release
        from marketpulse_batch.settings import load_settings
        from marketpulse_batch.ledger import BatchLedger
        from marketpulse_batch.resources import clickhouse_client
        from marketpulse_batch.publication import resolve_active
        from marketpulse_batch.certification import read_certification
        from marketpulse_batch.jobs import local_receipt_path
        settings = load_settings(os.environ)
        client = clickhouse_client(settings)
        try:
            release = resolve_active(client)
        finally:
            client.close()
        ledger = BatchLedger(settings.ledger_dsn)
        _, receipt_uri = ledger.get_receipt(release.identity)
        bundle = ledger.get_certified_bundle(release.identity)
        cert = read_certification(local_receipt_path(bundle.certification_uri))
        with tempfile.TemporaryDirectory() as root:
            old = Path(root) / "dagster-runs" / "parent-cert"
            old.mkdir(parents=True)
            (old / "candidate.json").write_text(json.dumps({"receipt_uri": receipt_uri,
                "manifest_uri": bundle.export_manifest_uri, "database": bundle.database,
                "certification_uri": bundle.certification_uri}))
            (old / "dbt_result.json").write_text(json.dumps({"target_path": str(cert.proof_path.parent)}))
            unrelated = Path(root) / "later-candidate.json"
            unrelated.write_text("{}")
            context = SimpleNamespace(run_id="retry-cert", job_name="marketing_refresh_job",
                run=SimpleNamespace(tags={}, parent_run_id="parent-cert"),
                instance=SimpleNamespace(get_run_by_id=lambda _: SimpleNamespace(parent_run_id=None)),
                resources=SimpleNamespace(batch_settings=replace(settings, state_path=Path(root))),
                add_output_metadata=lambda _: None)
            result = certified_release.op.compute_fn.decorated_fn(context, unrelated.as_uri())
            self.assertEqual(result, bundle.certification_uri)
            publication_uri = active_release.op.compute_fn.decorated_fn(context, "file:///unrelated/certification.json")
            publication = json.loads(local_receipt_path(publication_uri).read_text())
            self.assertEqual(publication["identity"], release.identity)
            self.assertEqual(publication["generation"], release.generation)

    def test_native_backfill_tag_also_blocks_publication(self) -> None:
        from types import SimpleNamespace
        from dagster._core.storage.tags import BACKFILL_ID_TAG
        from marketpulse_batch.definitions import active_release
        context = SimpleNamespace(job_name="marketing_refresh_job", run=SimpleNamespace(tags={BACKFILL_ID_TAG: "fixture"}),
                                  add_output_metadata=lambda _: None)
        self.assertEqual(active_release.op.compute_fn.decorated_fn(context, "file:///owned/certification.json"),
                         "file:///owned/certification.json")

    def test_dagster_notifications_use_explicit_compatible_driver(self) -> None:
        from dagster import DagsterInstance
        with DagsterInstance.get() as instance:
            self.assertEqual(instance.event_log_storage._engine.dialect.driver, "psycopg2")

    def test_marketing_graph_and_stopped_schedule_load(self) -> None:
        from dagster import Definitions, DefaultScheduleStatus
        from marketpulse_batch.definitions import defs
        Definitions.validate_loadable(defs)
        keys = {key.to_user_string() for key in defs.resolve_asset_graph().get_all_asset_keys()}
        self.assertTrue({"ad_reports", "crm_snapshots", "master_data", "historical_file_manifest",
                         "bounded_kafka", "pinned_bronze", "governed_silver", "marketing_gold",
                         "snapshot_export", "certified_release", "active_release", "campaign_dashboard"} <= keys)
        self.assertEqual(defs.resolve_schedule_def("daily_marketing_refresh").default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(defs.resolve_schedule_def("daily_marketing_refresh").cron_schedule, "0 2 * * *")
        for job in ("marketing_refresh_job", "historical_rebuild_job", "source_history_job"):
            self.assertIsNotNone(defs.resolve_job_def(job))
