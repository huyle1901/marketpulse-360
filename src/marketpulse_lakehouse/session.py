"""Spark session configuration and a minimal catalog persistence probe."""

from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Mapping
from typing import Any, Protocol

from .credentials import minio_password


class SparkBuilder(Protocol):
    def appName(self, value: str) -> "SparkBuilder": ...

    def master(self, value: str) -> "SparkBuilder": ...

    def config(self, key: str, value: str) -> "SparkBuilder": ...

    def getOrCreate(self) -> Any: ...


_CATALOG_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def spark_configuration(
    catalog_name: str = "lakehouse",
    ref: str | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    values = os.environ if env is None else env
    resolved_ref = values.get("NESSIE_REF", "main") if ref is None else ref
    if not _CATALOG_NAME.fullmatch(catalog_name):
        raise ValueError("catalog_name must be a SQL identifier")
    if not resolved_ref or any(character.isspace() for character in resolved_ref):
        raise ValueError("ref must not be empty or contain whitespace")

    shuffle_partitions = values.get("SPARK_SQL_SHUFFLE_PARTITIONS", "2")
    try:
        if int(shuffle_partitions) < 1:
            raise ValueError
    except ValueError as error:
        raise ValueError("SPARK_SQL_SHUFFLE_PARTITIONS must be positive") from error

    prefix = f"spark.sql.catalog.{catalog_name}"
    return {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
        f"{prefix}.uri": values.get("NESSIE_URI", "http://nessie:19120/api/v2"),
        f"{prefix}.ref": resolved_ref,
        f"{prefix}.authentication.type": "NONE",
        f"{prefix}.warehouse": values.get(
            "ICEBERG_WAREHOUSE", "s3://marketpulse-lakehouse/warehouse"
        ),
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.s3.endpoint": values.get("MINIO_ENDPOINT", "http://minio:9000"),
        f"{prefix}.s3.path-style-access": "true",
        f"{prefix}.s3.region": values.get("MINIO_REGION", "us-east-1"),
        f"{prefix}.s3.access-key-id": values.get("MINIO_ROOT_USER", "marketpulse"),
        f"{prefix}.s3.secret-access-key": minio_password(values),
        "spark.sql.session.timeZone": values.get("SPARK_SQL_SESSION_TIMEZONE", "UTC"),
        "spark.sql.shuffle.partitions": shuffle_partitions,
        "spark.sql.adaptive.enabled": "false",
    }


def create_spark_session(
    app_name: str,
    catalog_name: str = "lakehouse",
    ref: str | None = None,
    *,
    builder: SparkBuilder | None = None,
    env: Mapping[str, str] | None = None,
) -> Any:
    if not app_name:
        raise ValueError("app_name must not be empty")
    values = os.environ if env is None else env
    if builder is None:
        from pyspark.sql import SparkSession

        builder = SparkSession.builder

    configured = builder.appName(app_name).master(values.get("SPARK_MASTER", "local[2]"))
    for key, value in spark_configuration(catalog_name, ref, env=values).items():
        configured = configured.config(key, value)
    return configured.getOrCreate()


def run_catalog_probe(mode: str) -> dict[str, object]:
    spark = create_spark_session("marketpulse-catalog-probe")
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.system_probe")
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS lakehouse.system_probe.catalog_persistence (
                id INT NOT NULL,
                value STRING NOT NULL
            ) USING iceberg
            """
        )
        if mode == "write":
            spark.sql(
                """
                MERGE INTO lakehouse.system_probe.catalog_persistence target
                USING (SELECT 1 AS id, 'survives-restart' AS value) source
                ON target.id = source.id
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        rows = spark.sql(
            "SELECT id, value FROM lakehouse.system_probe.catalog_persistence ORDER BY id"
        ).collect()
        observed = [(int(row["id"]), str(row["value"])) for row in rows]
        if observed != [(1, "survives-restart")]:
            raise RuntimeError(f"catalog probe mismatch: {observed}")
        snapshots = spark.sql(
            """
            SELECT snapshot_id
            FROM lakehouse.system_probe.catalog_persistence.refs
            WHERE name = 'main'
            """
        ).collect()
        if len(snapshots) != 1:
            raise RuntimeError(f"catalog snapshot probe mismatch: {snapshots}")
        return {
            "mode": mode,
            "rows": len(observed),
            "snapshot_id": int(snapshots[0]["snapshot_id"]),
            "value": observed[0][1],
        }
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MarketPulse lakehouse runtime probe")
    parser.add_argument("mode", choices=("probe-write", "probe-read"))
    args = parser.parse_args(argv)
    result = run_catalog_probe(args.mode.removeprefix("probe-"))
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
