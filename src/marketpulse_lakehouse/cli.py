"""Command-line entry point for bounded lakehouse operations."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Sequence

from marketpulse_lakehouse.bronze import (
    KafkaOffsetManifest,
    bootstrap_lakehouse,
    capture_offset_manifest,
    ingest_kafka_manifest,
)
from marketpulse_lakehouse.contracts import TABLE_CONTRACTS
from marketpulse_lakehouse.gold import build_gold
from marketpulse_lakehouse.session import create_spark_session, run_catalog_probe
from marketpulse_lakehouse.silver import build_silver
from marketpulse_lakehouse.source_files import ingest_source_files
from marketpulse_lakehouse.verify import (
    backfill_proof,
    create_evidence,
    load_evidence,
    verify_evidence,
    write_evidence,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="marketpulse-lakehouse")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("bootstrap")
    subparsers.add_parser("probe-write")
    subparsers.add_parser("probe-read")

    capture = subparsers.add_parser("capture-offsets")
    capture.add_argument("--run-id", required=True)
    capture.add_argument("--output", type=Path, required=True)
    capture.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    )

    kafka = subparsers.add_parser("ingest-kafka")
    kafka.add_argument("--run-id", required=True)
    kafka.add_argument("--offset-manifest", type=Path, required=True)
    kafka.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    )

    files = subparsers.add_parser("ingest-files")
    files.add_argument("--run-id", required=True)
    files.add_argument("--input", type=Path, required=True)
    files.add_argument("--from-date", type=date.fromisoformat)
    files.add_argument("--to-date", type=date.fromisoformat)

    silver = subparsers.add_parser("build-silver")
    silver.add_argument("--run-id", required=True)
    silver.add_argument("--bronze-snapshots", type=Path, required=True)
    silver.add_argument("--target-namespace", default="silver")

    gold = subparsers.add_parser("build-gold")
    gold.add_argument("--run-id", required=True)
    gold.add_argument("--silver-snapshots", type=Path, required=True)
    gold.add_argument("--source-namespace", default="silver")
    gold.add_argument("--target-namespace", default="gold")

    evidence = subparsers.add_parser("write-evidence")
    evidence.add_argument("--run-id", required=True)
    evidence.add_argument("--bronze-snapshots", type=Path, required=True)
    evidence.add_argument("--silver-snapshots", type=Path, required=True)
    evidence.add_argument("--gold-snapshots", type=Path, required=True)
    evidence.add_argument("--later-silver-snapshots", type=Path, required=True)
    evidence.add_argument("--later-gold-snapshots", type=Path, required=True)
    evidence.add_argument("--offset-manifest", type=Path, required=True)
    evidence.add_argument("--file-manifest", type=Path, required=True)
    evidence.add_argument("--output", type=Path, required=True)
    evidence.add_argument("--silver-namespace", default="silver")
    evidence.add_argument("--gold-namespace", default="gold")

    verify = subparsers.add_parser("verify")
    verify.add_argument("--evidence", type=Path, required=True)

    backfill = subparsers.add_parser("backfill-proof")
    backfill.add_argument("--evidence", type=Path, required=True)
    return parser


def _with_session(app_name: str, operation: object) -> dict[str, object]:
    spark = create_spark_session(app_name)
    try:
        bootstrap_lakehouse(spark)
        return operation(spark)  # type: ignore[operator]
    finally:
        spark.stop()


def _run_bootstrap() -> dict[str, object]:
    spark = create_spark_session("marketpulse-lakehouse-bootstrap")
    try:
        statements = bootstrap_lakehouse(spark)
        return {"command": "bootstrap", "tables": len(statements) - 3}
    finally:
        spark.stop()


def _run_capture_offsets(
    *, run_id: str, bootstrap_servers: str
) -> dict[str, object]:
    def operation(spark: object) -> dict[str, object]:
        manifest = capture_offset_manifest(
            spark,
            bootstrap_servers=bootstrap_servers,
            captured_at=datetime.now(UTC),
        )
        return {
            "command": "capture-offsets",
            "run_id": run_id,
            "manifest_json": manifest.to_json(),
            "manifest_sha256": manifest.sha256,
            "ranges": len(manifest.ranges),
        }

    return _with_session("marketpulse-capture-offsets", operation)


def _run_ingest_kafka(
    *, run_id: str, bootstrap_servers: str, offset_manifest: Path
) -> dict[str, object]:
    manifest = KafkaOffsetManifest.from_json(offset_manifest.read_text(encoding="utf-8"))

    def operation(spark: object) -> dict[str, object]:
        return ingest_kafka_manifest(
            spark,
            manifest,
            bootstrap_servers=bootstrap_servers,
            run_id=run_id,
            ingested_at=datetime.now(UTC),
        )

    return _with_session("marketpulse-ingest-kafka", operation)


def _run_ingest_files(*, run_id: str, input_path: Path, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
    def operation(spark: object) -> dict[str, object]:
        return ingest_source_files(
            spark,
            input_path,
            run_id=run_id,
            ingested_at=datetime.now(UTC),
            from_date=from_date,
            to_date=to_date,
        )

    return _with_session("marketpulse-ingest-files", operation)


def _run_build_silver(
    *, run_id: str, bronze_snapshots: Path, target_namespace: str
) -> dict[str, object]:
    raw = json.loads(bronze_snapshots.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Bronze snapshots must be a JSON object")
    required = {
        "bronze.kafka_records",
        "bronze.source_files",
        "bronze.source_file_rows",
    }
    if set(raw) != required or any(
        not isinstance(raw[name], int) or isinstance(raw[name], bool) or raw[name] < 1
        for name in required
    ):
        raise ValueError("Bronze snapshots must contain three positive integer IDs")

    def operation(spark: object) -> dict[str, object]:
        return build_silver(
            spark,
            kafka_snapshot_id=int(raw["bronze.kafka_records"]),
            source_rows_snapshot_id=int(raw["bronze.source_file_rows"]),
            run_id=run_id,
            target_namespace=target_namespace,
        )

    return _with_session("marketpulse-build-silver", operation)


def _run_build_gold(
    *,
    run_id: str,
    silver_snapshots: Path,
    source_namespace: str,
    target_namespace: str,
) -> dict[str, object]:
    raw = json.loads(silver_snapshots.read_text(encoding="utf-8"))
    required = {name for name in TABLE_CONTRACTS if name.startswith("silver.")}
    if not isinstance(raw, dict) or set(raw) != required or any(
        not isinstance(raw[name], int) or isinstance(raw[name], bool) or raw[name] < 1
        for name in required
    ):
        raise ValueError("Silver snapshots must contain positive integer IDs for: " + ", ".join(sorted(required)))

    def operation(spark: object) -> dict[str, object]:
        return build_gold(
            spark,
            silver_snapshots={name: int(raw[name]) for name in required},
            run_id=run_id,
            source_namespace=source_namespace,
            target_namespace=target_namespace,
        )

    return _with_session("marketpulse-build-gold", operation)


def _read_snapshot_ids(path: Path, required: set[str]) -> dict[str, int]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or set(value) != required or any(
        not isinstance(value[name], int)
        or isinstance(value[name], bool)
        or value[name] < 1
        for name in required
    ):
        raise ValueError(f"{path.name} does not contain the required snapshot IDs")
    return {name: int(value[name]) for name in required}


def _run_write_evidence(
    *,
    run_id: str,
    bronze_snapshots: Path,
    silver_snapshots: Path,
    gold_snapshots: Path,
    later_silver_snapshots: Path,
    later_gold_snapshots: Path,
    offset_manifest: Path,
    file_manifest: Path,
    output: Path,
    silver_namespace: str,
    gold_namespace: str,
) -> dict[str, object]:
    bronze = _read_snapshot_ids(
        bronze_snapshots,
        {
            "bronze.kafka_records",
            "bronze.source_files",
            "bronze.source_file_rows",
        },
    )
    silver = _read_snapshot_ids(
        silver_snapshots,
        {name for name in TABLE_CONTRACTS if name.startswith("silver.")},
    )
    gold = _read_snapshot_ids(
        gold_snapshots,
        {name for name in TABLE_CONTRACTS if name.startswith("gold.")},
    )
    later_silver = _read_snapshot_ids(later_silver_snapshots, set(silver))
    later_gold = _read_snapshot_ids(later_gold_snapshots, set(gold))
    offset_manifest_bytes = offset_manifest.read_bytes()
    file_manifest_bytes = file_manifest.read_bytes()

    def operation(spark: object) -> dict[str, object]:
        manifest = create_evidence(
            spark,
            run_id=run_id,
            bronze_snapshots=bronze,
            silver_snapshots=silver,
            gold_snapshots=gold,
            later_silver_snapshots=later_silver,
            later_gold_snapshots=later_gold,
            offset_manifest=offset_manifest_bytes,
            file_manifest=file_manifest_bytes,
            silver_namespace=silver_namespace,
            gold_namespace=gold_namespace,
        )
        write_evidence(output, manifest)
        return {
            "command": "write-evidence",
            "run_id": run_id,
            "output": str(output),
            "tables": len(manifest["tables"]),  # type: ignore[arg-type]
        }

    return _with_session("marketpulse-write-evidence", operation)


def _run_verify(*, evidence: Path) -> dict[str, object]:
    manifest = load_evidence(evidence)
    return _with_session(
        "marketpulse-verify-evidence",
        lambda spark: verify_evidence(spark, manifest),
    )


def _run_backfill_proof(*, evidence: Path) -> dict[str, object]:
    manifest = load_evidence(evidence)

    def operation(spark: object) -> dict[str, object]:
        result = backfill_proof(spark, manifest)
        rebuilt = result.pop("rebuilt_tables")
        assert isinstance(rebuilt, dict)
        manifest["backfill"] = {
            "namespace": result["namespace"],
            "matches": True,
            "tables": {
                name: {
                    "snapshot_id": entry["snapshot_id"],
                    "checksum": entry["checksum"],
                    "matches": True,
                }
                for name, entry in rebuilt.items()
            },
        }
        write_evidence(evidence, manifest)
        return result

    return _with_session("marketpulse-backfill-proof", operation)


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(content + "\n", encoding="utf-8")
    temporary.replace(path)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "bootstrap":
            result = _run_bootstrap()
        elif args.command in {"probe-write", "probe-read"}:
            result = run_catalog_probe(args.command.removeprefix("probe-"))
        elif args.command == "capture-offsets":
            result = _run_capture_offsets(
                run_id=args.run_id,
                bootstrap_servers=args.bootstrap_servers,
            )
            _atomic_write(args.output, str(result["manifest_json"]))
            result = {key: value for key, value in result.items() if key != "manifest_json"}
        elif args.command == "ingest-kafka":
            result = _run_ingest_kafka(
                run_id=args.run_id,
                bootstrap_servers=args.bootstrap_servers,
                offset_manifest=args.offset_manifest,
            )
        elif args.command == "ingest-files":
            if (args.from_date is None) != (args.to_date is None):
                raise ValueError("ingest-files requires both --from-date and --to-date")
            result = _run_ingest_files(run_id=args.run_id, input_path=args.input, from_date=args.from_date, to_date=args.to_date)
        elif args.command == "build-silver":
            result = _run_build_silver(
                run_id=args.run_id,
                bronze_snapshots=args.bronze_snapshots,
                target_namespace=args.target_namespace,
            )
        elif args.command == "build-gold":
            result = _run_build_gold(
                run_id=args.run_id,
                silver_snapshots=args.silver_snapshots,
                source_namespace=args.source_namespace,
                target_namespace=args.target_namespace,
            )
        elif args.command == "write-evidence":
            result = _run_write_evidence(
                run_id=args.run_id,
                bronze_snapshots=args.bronze_snapshots,
                silver_snapshots=args.silver_snapshots,
                gold_snapshots=args.gold_snapshots,
                later_silver_snapshots=args.later_silver_snapshots,
                later_gold_snapshots=args.later_gold_snapshots,
                offset_manifest=args.offset_manifest,
                file_manifest=args.file_manifest,
                output=args.output,
                silver_namespace=args.silver_namespace,
                gold_namespace=args.gold_namespace,
            )
        elif args.command == "verify":
            result = _run_verify(evidence=args.evidence)
        elif args.command == "backfill-proof":
            result = _run_backfill_proof(evidence=args.evidence)
        else:  # argparse prevents this branch
            raise ValueError(f"unsupported command: {args.command}")
        print(json.dumps(result, sort_keys=True, default=str))
        return 0
    except (OSError, RuntimeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
