"""Bounded local Spark process runner; credentials never enter command arguments."""

from __future__ import annotations

import json
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any

from .settings import BatchSettings


def run_spark_operation(settings: BatchSettings, operation: str, payload: dict[str, object]) -> dict[str, Any]:
    if operation not in {"ingest_files", "bounded_kafka", "pin_bronze", "silver", "gold", "export"}:
        raise ValueError("invalid bounded Spark operation")
    directory = settings.state_path / "commands" / uuid.uuid4().hex
    directory.mkdir(parents=True, exist_ok=False)
    input_path, output_path = directory / "input.json", directory / "result.json"
    input_path.write_text(json.dumps(payload, sort_keys=True))
    env = os.environ | {"MINIO_ENDPOINT": settings.minio_endpoint, "MINIO_ROOT_USER": settings.minio_user,
                        "MINIO_ROOT_PASSWORD": settings.minio_password, "NESSIE_URI": settings.nessie_uri,
                        "AWS_REGION": "us-east-1", "SPARK_MASTER": "local[2]", "SPARK_SQL_SHUFFLE_PARTITIONS": "2"}
    result = subprocess.run(["/opt/spark/bin/spark-submit", "--driver-memory", "2g",
                             "/opt/marketpulse/src/marketpulse_batch/engine.py", "--operation", operation,
                             "--input", str(input_path), "--output", str(output_path)],
                            env=env, capture_output=True, text=True, timeout=1800)
    log = result.stdout + result.stderr
    for secret in (settings.minio_password, settings.clickhouse_password, settings.ledger_password):
        log = log.replace(secret, "[REDACTED]")
    (directory / "operation.log").write_text(log)
    if result.returncode or not output_path.is_file():
        raise ValueError(f"bounded Spark {operation} failed; inspect owned redacted operation.log")
    value = json.loads(output_path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError("Spark result must be an object")
    return value


def clickhouse_client(settings: BatchSettings) -> Any:
    import clickhouse_connect
    return clickhouse_connect.get_client(host=settings.clickhouse_host, port=settings.clickhouse_port,
                                         username=settings.clickhouse_user, password=settings.clickhouse_password)
