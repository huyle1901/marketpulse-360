"""Bounded Spark subprocess entrypoint; uses MP-04 transforms unchanged."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime, date
from pathlib import Path

from marketpulse_batch.contracts import BatchSelection, PreparationReceipt, snapshot_map
from marketpulse_batch.canonical import rowset_checksum
from marketpulse_batch.export import export_snapshots
from marketpulse_batch.receipts import read_receipt
from marketpulse_lakehouse.bronze import bootstrap_lakehouse, capture_offset_manifest, ingest_kafka_manifest, current_snapshot_id
from marketpulse_lakehouse.source_files import ingest_source_files
from marketpulse_lakehouse.session import create_spark_session
from marketpulse_lakehouse.silver import build_silver
from marketpulse_lakehouse.gold import build_gold
from marketpulse_lakehouse.contracts import TABLE_CONTRACTS
from marketpulse_lakehouse.verify import validate_physical_schema


def execute(spark, operation: str, payload: dict[str, object]) -> dict[str, object]:
    run_id = payload.get("run_id", "batch")
    if operation == "ingest_files":
        selected = payload.get("selection")
        selection = BatchSelection(date.fromisoformat(selected["logical_day"]), tuple(selected["required_platforms"])) if selected else None
        return ingest_source_files(spark, Path(payload["source_root"]), run_id=run_id,
                                   ingested_at=datetime.now(UTC), selection=selection,
                                   datasets=tuple(payload["datasets"]) if payload.get("datasets") else None)
    if operation == "bounded_kafka":
        manifest = capture_offset_manifest(spark, bootstrap_servers=payload["bootstrap_servers"], captured_at=datetime.now(UTC))
        result = ingest_kafka_manifest(spark, manifest, bootstrap_servers=payload["bootstrap_servers"],
                                      run_id=run_id, ingested_at=datetime.now(UTC))
        result["offset_manifest"] = json.loads(manifest.to_json())
        result["offset_manifest_sha256"] = manifest.sha256
        return result
    if operation == "pin_bronze":
        snapshots = {f"bronze.{name}": current_snapshot_id(spark, f"bronze.{name}")
                     for name in ("kafka_records", "source_files", "source_file_rows")}
        if any(value is None for value in snapshots.values()):
            raise ValueError("Bronze inputs are empty; ingest source files and replay marketing events first")
        return {"bronze_snapshots": snapshots}
    if operation == "silver":
        bronze = snapshot_map(payload["bronze_snapshots"], "bronze")
        namespace = payload["namespace"]
        # New attempts must never overwrite even an incomplete earlier namespace.
        namespaces = {row[0] for row in spark.sql("SHOW NAMESPACES IN lakehouse").collect()}
        if namespace in namespaces:
            raise ValueError("owned preparation namespace already exists")
        return build_silver(spark, kafka_snapshot_id=bronze["bronze.kafka_records"],
                            source_rows_snapshot_id=bronze["bronze.source_file_rows"],
                            run_id=run_id, target_namespace=namespace)
    if operation == "gold":
        namespace = payload["namespace"]
        result = build_gold(spark, silver_snapshots=payload["silver_snapshots"], run_id=run_id,
                            source_namespace=namespace, target_namespace=namespace)
        outputs = payload["silver_snapshots"] | {f"gold.{name}": value for name, value in result["snapshots"].items()}
        proof_tables = {}
        for name, snapshot in sorted(outputs.items()):
            model = name.split(".")[1]
            frame = spark.read.format("iceberg").option("snapshot-id", str(snapshot)).load(f"lakehouse.{namespace}.{model}")
            validate_physical_schema(frame.schema, name)
            columns = tuple((c.name, c.iceberg_type, c.nullable) for c in TABLE_CONTRACTS[name].columns if not c.operational)
            def rows():
                for row in frame.select(*(c[0] for c in columns)).repartition(32).toLocalIterator(prefetchPartitions=False):
                    values = row.asDict()
                    for key, kind, _ in columns:
                        if kind == "timestamp" and isinstance(values[key], datetime) and values[key].tzinfo is None:
                            values[key] = values[key].replace(tzinfo=UTC)
                    yield values
            count, checksum = rowset_checksum(columns, rows())
            proof_tables[name] = {"columns": columns, "row_count": count, "checksum": checksum}
        proof = {"version": 1, "identity": payload["identity"], "model_digest": payload["model_digest"],
                 "bronze_snapshots": payload["bronze_snapshots"], "tables": proof_tables}
        content = json.dumps(proof, sort_keys=True, separators=(",", ":")).encode()
        return {"output_snapshots": outputs, "proof": proof, "proof_digest": hashlib.sha256(content).hexdigest()}
    if operation == "export":
        receipt = read_receipt(Path(payload["receipt_path"]))
        return export_snapshots(spark, receipt, payload["output_prefix"]).payload()
    raise ValueError("unknown bounded Spark batch operation")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation", choices=("ingest_files", "bounded_kafka", "pin_bronze", "silver", "gold", "export"), required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = json.loads(args.input.read_bytes())
    spark = create_spark_session(f"marketpulse-batch-{args.operation}")
    try:
        bootstrap_lakehouse(spark)
        result = execute(spark, args.operation, payload)
        args.output.write_text(json.dumps(result, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
