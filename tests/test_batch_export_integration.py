from __future__ import annotations

import hashlib
import os
import unittest
import uuid
from datetime import date

from marketpulse_batch.contracts import BatchSelection, PreparationReceipt
from marketpulse_batch.export import EXPORTED_RELATIONS, export_snapshots
from marketpulse_batch.copying import load_candidate
from marketpulse_batch.receipts import preparation_identity


@unittest.skipUnless(os.environ.get("BATCH_TEST_SPARK"), "opt-in existing MP-04 proof catalog")
class SnapshotExportIntegrationTests(unittest.TestCase):
    def test_export_snapshot_not_head(self) -> None:
        import clickhouse_connect
        from marketpulse_batch.settings import load_settings
        from marketpulse_lakehouse.session import create_spark_session
        from marketpulse_lakehouse.source_files import create_s3_client
        from marketpulse_lakehouse.silver import _ensure_silver_tables
        from marketpulse_lakehouse.gold import _ensure_gold_tables, _current_snapshot
        settings = load_settings(os.environ)
        spark = create_spark_session("mp05-pinned-export-proof")
        try:
            bronze = {f"bronze.{name}": _current_snapshot(spark, "bronze", name)
                      for name in ("kafka_records", "source_files", "source_file_rows")}
            model_digest = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
            identity = preparation_identity(BatchSelection(date(2026, 1, 1)), bronze, model_digest)
            namespace = f"mp_batch_{identity}_1"
            _ensure_silver_tables(spark, namespace)
            _ensure_gold_tables(spark, namespace)
            snapshots = {}
            for name in EXPORTED_RELATIONS:
                model = name.split(".")[1]
                spark.sql(f"INSERT INTO lakehouse.{namespace}.{model} SELECT * FROM lakehouse.{name}")
                snapshots[name] = _current_snapshot(spark, namespace, model)
            receipt = PreparationReceipt(identity, date(2026, 1, 1), model_digest, bronze,
                                         snapshots, namespace, "b" * 64)
            expected = spark.sql("SELECT count(*) FROM lakehouse.gold.funnel_daily").first()[0]
            # Mutate only our owned test namespace AFTER pinning its earlier snapshot.
            spark.sql(f"INSERT INTO lakehouse.{namespace}.funnel_daily SELECT * FROM lakehouse.gold.funnel_daily")
            manifest = export_snapshots(spark, receipt,
                                        f"s3://marketpulse-lakehouse/batch-exports/{identity}/integration")
            funnel = next(r for r in manifest.relations if r.name == "gold.funnel_daily")
            self.assertEqual(funnel.row_count, expected)
            self.assertEqual(spark.sql(f"SELECT count(*) FROM lakehouse.{namespace}.funnel_daily").first()[0], expected * 2)
            client = clickhouse_connect.get_client(host=settings.clickhouse_host, port=settings.clickhouse_port,
                                                   username=settings.clickhouse_user, password=settings.clickhouse_password)
            candidate = load_candidate(client, manifest, attempt=1, s3=create_s3_client())
            self.assertEqual(client.query(f"SELECT count() FROM {candidate}.raw_funnel_daily").first_row[0], expected)
            self.assertEqual(len(manifest.relations), 7)
            print(f"Verified all seven snapshot/export/ClickHouse rowsets; pinned funnel rows={expected}")
            if os.environ.get("BATCH_TEST_DBT"):
                from pathlib import Path
                from marketpulse_batch.dbt_runner import run_dbt_build
                from marketpulse_batch.certification import certify_candidate, verify_certification_current
                artifact_root = Path(settings.state_path) / f"dbt-proof-{identity}"
                print(f"dbt proof artifacts: {artifact_root}", flush=True)
                artifacts = run_dbt_build(candidate, settings=settings, artifact_root=artifact_root)
                certification = certify_candidate(client, receipt, manifest, candidate, artifacts)
                self.assertEqual(len(certification.checked_relations), 7)
                verify_certification_current(client, receipt, certification)
                # Same count, altered amount: full-row equality must reject the change.
                client.command(f"ALTER TABLE {candidate}.campaign_performance_daily UPDATE net_revenue_minor=net_revenue_minor+1 WHERE 1",
                               settings={"mutations_sync": 2})
                with self.assertRaisesRegex(ValueError, "rowset"):
                    certify_candidate(client, receipt, manifest, candidate, artifacts)
                with self.assertRaisesRegex(ValueError, "rowset"):
                    verify_certification_current(client, receipt, certification)
                bad_candidate = load_candidate(client, manifest, attempt=2, s3=create_s3_client())
                bad_fields = ",".join("'missing-test-campaign' AS campaign_id" if name == "campaign_id" else f"`{name}`"
                                      for name, _, _ in next(r for r in manifest.relations if r.name == "gold.campaign_performance_daily").columns)
                # MergeTree keys cannot be UPDATEd; append a deliberately bad test row.
                client.command(f"INSERT INTO {bad_candidate}.raw_campaign_performance_daily SELECT {bad_fields} "
                               f"FROM {bad_candidate}.raw_campaign_performance_daily LIMIT 1")
                bad_artifacts = Path(settings.state_path) / f"dbt-bad-proof-{identity}"
                with self.assertRaisesRegex(ValueError, "dbt build failed"):
                    run_dbt_build(bad_candidate, settings=settings, artifact_root=bad_artifacts)
                with self.assertRaisesRegex(ValueError, "dbt"):
                    certify_candidate(client, receipt, manifest, bad_candidate, bad_artifacts / "target")
                print("Verified real dbt build, financial gates, dashboard oracle and same-count mutation rejection", flush=True)
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)
