from __future__ import annotations

import unittest
import uuid
from unittest.mock import patch

from marketpulse_lakehouse.session import (
    create_spark_session,
    run_catalog_probe,
    spark_configuration,
)


class _RecordingBuilder:
    def __init__(self) -> None:
        self.app_name: str | None = None
        self.master_value: str | None = None
        self.settings: dict[str, str] = {}
        self.created = object()

    def appName(self, value: str) -> "_RecordingBuilder":
        self.app_name = value
        return self

    def master(self, value: str) -> "_RecordingBuilder":
        self.master_value = value
        return self

    def config(self, key: str, value: str) -> "_RecordingBuilder":
        self.settings[key] = value
        return self

    def getOrCreate(self) -> object:
        return self.created


class _Result:
    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        self._rows = rows or []

    def collect(self) -> list[dict[str, object]]:
        return self._rows


class _ProbeSpark:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.stopped = False

    def sql(self, query: str) -> _Result:
        self.queries.append(query)
        if ".refs" in query:
            return _Result([{"snapshot_id": 8675309}])
        if "SELECT id, value" in query:
            return _Result([{"id": 1, "value": "survives-restart"}])
        return _Result()

    def stop(self) -> None:
        self.stopped = True


class LakehouseSessionTests(unittest.TestCase):
    def test_missing_empty_or_blank_password_is_rejected_without_disclosing_values(self) -> None:
        for env in ({}, {"MINIO_ROOT_PASSWORD": ""}, {"MINIO_ROOT_PASSWORD": "   "}):
            with self.subTest(env=env):
                with self.assertRaisesRegex(ValueError, "MINIO_ROOT_PASSWORD"):
                    spark_configuration(env=env)

    def test_default_configuration_uses_native_nessie_s3_and_utc(self) -> None:
        password = uuid.uuid4().hex
        config = spark_configuration("lakehouse", "main", env={"MINIO_ROOT_PASSWORD": password})
        self.assertEqual(
            config,
            {
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.lakehouse.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
                "spark.sql.catalog.lakehouse.uri": "http://nessie:19120/api/v2",
                "spark.sql.catalog.lakehouse.ref": "main",
                "spark.sql.catalog.lakehouse.authentication.type": "NONE",
                "spark.sql.catalog.lakehouse.warehouse": "s3://marketpulse-lakehouse/warehouse",
                "spark.sql.catalog.lakehouse.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                "spark.sql.catalog.lakehouse.s3.endpoint": "http://minio:9000",
                "spark.sql.catalog.lakehouse.s3.path-style-access": "true",
                "spark.sql.catalog.lakehouse.s3.region": "us-east-1",
                "spark.sql.catalog.lakehouse.s3.access-key-id": "marketpulse",
                "spark.sql.catalog.lakehouse.s3.secret-access-key": password,
                "spark.sql.session.timeZone": "UTC",
                "spark.sql.shuffle.partitions": "2",
                "spark.sql.adaptive.enabled": "false",
            },
        )

    def test_environment_overrides_endpoints_credentials_and_runtime(self) -> None:
        config = spark_configuration(
            "analytics",
            "experiment",
            env={
                "NESSIE_URI": "http://catalog:19120/api/v2",
                "ICEBERG_WAREHOUSE": "s3://custom/warehouse",
                "MINIO_ENDPOINT": "http://object-store:9000",
                "MINIO_REGION": "eu-west-1",
                "MINIO_ROOT_USER": "user",
                "MINIO_ROOT_PASSWORD": "secret",
                "SPARK_SQL_SESSION_TIMEZONE": "Asia/Ho_Chi_Minh",
                "SPARK_SQL_SHUFFLE_PARTITIONS": "8",
            },
        )
        self.assertEqual(config["spark.sql.catalog.analytics.ref"], "experiment")
        self.assertEqual(config["spark.sql.catalog.analytics.uri"], "http://catalog:19120/api/v2")
        self.assertEqual(config["spark.sql.catalog.analytics.warehouse"], "s3://custom/warehouse")
        self.assertEqual(config["spark.sql.catalog.analytics.s3.endpoint"], "http://object-store:9000")
        self.assertEqual(config["spark.sql.catalog.analytics.s3.region"], "eu-west-1")
        self.assertEqual(config["spark.sql.catalog.analytics.s3.access-key-id"], "user")
        self.assertEqual(config["spark.sql.catalog.analytics.s3.secret-access-key"], "secret")
        self.assertEqual(config["spark.sql.session.timeZone"], "Asia/Ho_Chi_Minh")
        self.assertEqual(config["spark.sql.shuffle.partitions"], "8")

    def test_create_session_applies_app_master_and_every_setting(self) -> None:
        builder = _RecordingBuilder()
        env = {"SPARK_MASTER": "local[2]", "MINIO_ROOT_PASSWORD": uuid.uuid4().hex}
        created = create_spark_session(
            "marketpulse-probe",
            builder=builder,
            env=env,
        )
        self.assertIs(created, builder.created)
        self.assertEqual(builder.app_name, "marketpulse-probe")
        self.assertEqual(builder.master_value, "local[2]")
        self.assertEqual(
            builder.settings,
            spark_configuration("lakehouse", "main", env=env),
        )

    def test_default_ref_uses_environment_and_explicit_ref_takes_precedence(self) -> None:
        env = {"NESSIE_REF": "experiment", "MINIO_ROOT_PASSWORD": uuid.uuid4().hex}
        from_environment = spark_configuration(env=env)
        explicit = spark_configuration(ref="main", env=env)

        self.assertEqual(from_environment["spark.sql.catalog.lakehouse.ref"], "experiment")
        self.assertEqual(explicit["spark.sql.catalog.lakehouse.ref"], "main")

    def test_catalog_probe_reports_the_current_snapshot_id(self) -> None:
        spark = _ProbeSpark()
        with patch("marketpulse_lakehouse.session.create_spark_session", return_value=spark):
            result = run_catalog_probe("read")

        self.assertEqual(
            result,
            {"mode": "read", "rows": 1, "snapshot_id": 8675309, "value": "survives-restart"},
        )
        self.assertTrue(any(".refs" in query for query in spark.queries))
        self.assertTrue(spark.stopped)

    def test_invalid_catalog_ref_and_shuffle_partition_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "catalog_name"):
            spark_configuration("bad.name", "main", env={})
        with self.assertRaisesRegex(ValueError, "ref"):
            spark_configuration("lakehouse", "", env={})
        with self.assertRaisesRegex(ValueError, "SPARK_SQL_SHUFFLE_PARTITIONS"):
            spark_configuration("lakehouse", "main", env={"SPARK_SQL_SHUFFLE_PARTITIONS": "0"})


if __name__ == "__main__":
    unittest.main()
