from __future__ import annotations

import os
import unittest


@unittest.skipUnless(os.environ.get("BATCH_TEST_DAGSTER"), "opt-in Dagster runtime")
class DagsterDefinitionTests(unittest.TestCase):
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
