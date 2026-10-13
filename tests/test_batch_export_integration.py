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
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)
