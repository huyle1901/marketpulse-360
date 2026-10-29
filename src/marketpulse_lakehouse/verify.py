"""Lakehouse evidence creation, historical verification, and isolated rebuild proof."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from .contracts import TABLE_CONTRACTS, contract_schema_columns, spark_schema_columns
from .evidence import (
    CHECKSUM_ALGORITHM,
    EVIDENCE_SCHEMA_VERSION,
    TableEvidence,
    canonical_row_bytes,
    checksum_sorted_encoded_rows,
    compare_table_evidence,
    schema_fingerprint,
)
from .bronze import KafkaOffsetManifest
from .gold import _event_amount, build_gold
from .silver import build_silver, validate_namespace
from .source_files import ALLOWED_SOURCE_DATASETS


SILVER_TABLES = tuple(name for name in TABLE_CONTRACTS if name.startswith("silver."))
GOLD_TABLES = tuple(name for name in TABLE_CONTRACTS if name.startswith("gold."))
EVIDENCE_TABLES = SILVER_TABLES + GOLD_TABLES
BRONZE_TABLES = (
    "bronze.kafka_records",
    "bronze.source_files",
    "bronze.source_file_rows",
)


def rebuild_namespace(run_id: str, identity: str = "") -> str:
    normalized = re.sub(r"[^a-z0-9_]+", "_", run_id.lower()).strip("_")
    if not normalized:
        raise ValueError("run_id cannot form a rebuild namespace")
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]
    return validate_namespace(f"rebuild_{normalized[:95]}_{digest}")


def validate_snapshot_ancestry(
    parents: Mapping[int, int | None], *, baseline: int, later: int, head: int
) -> None:
    if later != head:
        raise ValueError("later snapshot is not the current table head")
    if baseline == later:
        raise ValueError("baseline and later snapshots must be distinct")
    current: int | None = later
    visited: set[int] = set()
    while current is not None and current != baseline:
        if current in visited:
            raise ValueError("Iceberg snapshot ancestry contains a cycle")
        visited.add(current)
        if current not in parents:
            raise ValueError("Iceberg snapshot ancestry is incomplete")
        current = parents[current]
    if current != baseline:
        raise ValueError("baseline snapshot is not an ancestor of later snapshot")


def evidence_table_from_mapping(value: Mapping[str, object]) -> TableEvidence:
    required = {
        "table", "physical_table", "snapshot_id", "later_snapshot_id",
        "schema_fingerprint", "row_count", "checksum_algorithm",
        "business_columns", "checksum", "reconciliations",
    }
    if set(value) != required:
        raise ValueError("evidence table fields do not match schema v1")
    for name in ("table", "physical_table", "schema_fingerprint", "checksum"):
        if not isinstance(value[name], str) or not value[name]:
            raise ValueError(f"evidence table {name} must be a non-empty string")
    if value["checksum_algorithm"] != CHECKSUM_ALGORITHM:
        raise ValueError("evidence checksum algorithm is unsupported")
    for name in ("snapshot_id", "later_snapshot_id", "row_count"):
        if not isinstance(value[name], int) or isinstance(value[name], bool) or value[name] < 0:
            raise ValueError(f"evidence table {name} must be a non-negative integer")
    columns = value["business_columns"]
    if not isinstance(columns, list) or not columns or not all(
        isinstance(item, str) and item for item in columns
    ):
        raise ValueError("business_columns must be a non-empty string list")
    reconciliations = value["reconciliations"]
    if not isinstance(reconciliations, dict) or not all(
        isinstance(name, str) and isinstance(amount, int) and not isinstance(amount, bool)
        for name, amount in reconciliations.items()
    ):
        raise ValueError("reconciliations must map names to integers")
    return TableEvidence(
        table=str(value["table"]), snapshot_id=int(value["snapshot_id"]),
        schema_fingerprint=str(value["schema_fingerprint"]),
        row_count=int(value["row_count"]), checksum=str(value["checksum"]),
        reconciliations={str(key): int(item) for key, item in reconciliations.items()},
    )


def _snapshot_mapping(value: object, names: tuple[str, ...], label: str) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != set(names) or any(
        not isinstance(item, int) or isinstance(item, bool) or item < 1
        for item in value.values()
    ):
        raise ValueError(f"evidence {label} snapshots are incomplete")
    return {str(name): int(item) for name, item in value.items()}


def _provenance_bytes(value: object, label: str) -> bytes:
    if not isinstance(value, dict) or set(value) != {"base64", "sha256", "size_bytes"}:
        raise ValueError(f"evidence {label} provenance is invalid")
    if not isinstance(value["base64"], str) or not isinstance(value["sha256"], str):
        raise ValueError(f"evidence {label} provenance is invalid")
    size = value["size_bytes"]
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        raise ValueError(f"evidence {label} provenance is invalid")
    try:
        payload = base64.b64decode(value["base64"], validate=True)
    except (ValueError, TypeError) as error:
        raise ValueError(f"evidence {label} provenance base64 is invalid") from error
    if len(payload) != size or hashlib.sha256(payload).hexdigest() != value["sha256"]:
        raise ValueError(f"evidence {label} provenance hash mismatch")
    return payload


def _provenance(value: bytes) -> dict[str, object]:
    return {
        "base64": base64.b64encode(value).decode("ascii"),
        "sha256": hashlib.sha256(value).hexdigest(),
        "size_bytes": len(value),
    }


def _parse_file_manifest(content: bytes) -> tuple[tuple[str, str, str, int], ...]:
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"evidence file manifest is malformed: {error}") from error
    if not isinstance(value, dict) or not isinstance(value.get("files"), list):
        raise ValueError("evidence file manifest must contain a files list")
    entries: list[tuple[str, str, str, int]] = []
    seen: set[str] = set()
    for item in value["files"]:
        if not isinstance(item, dict):
            raise ValueError("evidence file manifest entry must be an object")
        dataset = item.get("dataset")
        relative_path = item.get("relative_path")
        checksum = item.get("sha256")
        row_count = item.get("row_count")
        if dataset not in ALLOWED_SOURCE_DATASETS:
            continue
        if (
            not isinstance(relative_path, str)
            or not relative_path
            or not isinstance(checksum, str)
            or len(checksum) != 64
            or not isinstance(row_count, int)
            or isinstance(row_count, bool)
            or row_count < 0
        ):
            raise ValueError("evidence file manifest entry is invalid")
        if relative_path in seen:
            raise ValueError(f"evidence file manifest path is duplicated: {relative_path}")
        seen.add(relative_path)
        entries.append((str(dataset), relative_path, checksum, row_count))
    return tuple(sorted(entries))


def parse_input_provenance(
    inputs: Mapping[str, object],
) -> tuple[KafkaOffsetManifest, tuple[tuple[str, str, str, int], ...], bytes]:
    offset_bytes = _provenance_bytes(inputs.get("offset_manifest"), "offset manifest")
    file_bytes = _provenance_bytes(inputs.get("file_manifest"), "file manifest")
    try:
        offset_manifest = KafkaOffsetManifest.from_json(offset_bytes.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise ValueError(f"evidence offset manifest is malformed: {error}") from error
    return offset_manifest, _parse_file_manifest(file_bytes), file_bytes


def validate_input_provenance(
    spark: Any,
    *,
    run_id: str,
    inputs: Mapping[str, object],
) -> None:
    """Bind embedded source manifests to the exact Bronze snapshot rows."""

    from pyspark.sql import functions as F

    offset_manifest, expected_files, file_manifest_bytes = parse_input_provenance(inputs)
    bronze = _snapshot_mapping(inputs["bronze_snapshots"], BRONZE_TABLES, "Bronze")
    kafka_rows = (
        spark.sql(
            "SELECT source_topic, source_partition, source_offset, ingestion_run_id, "
            "ingestion_manifest_sha256 "
            "FROM lakehouse.bronze.kafka_records "
            f"VERSION AS OF {bronze['bronze.kafka_records']}"
        )
        .filter(F.col("ingestion_run_id") == run_id)
        .groupBy("source_topic", "source_partition", "ingestion_manifest_sha256")
        .agg(
            F.min("source_offset").alias("minimum"),
            F.max("source_offset").alias("maximum"),
            F.count("source_offset").alias("records"),
            F.countDistinct("source_offset").alias("distinct_records"),
        )
        .collect()
    )
    validate_offset_manifest_hashes(
        offset_manifest,
        tuple(row["ingestion_manifest_sha256"] for row in kafka_rows),
    )
    ranges = {
        (item.topic, item.partition): item for item in offset_manifest.ranges
    }
    for row in kafka_rows:
        position = (str(row["source_topic"]), int(row["source_partition"]))
        item = ranges.get(position)
        if item is None or not (
            item.start_offset <= int(row["minimum"]) <= int(row["maximum"]) < item.end_offset
        ) or row["records"] != row["distinct_records"]:
            raise ValueError("offset manifest range does not match Bronze ingestion provenance")
    source_rows = (
        spark.sql(
            "SELECT source_dataset, source_file, file_sha256, ingestion_run_id "
            "FROM lakehouse.bronze.source_file_rows "
            f"VERSION AS OF {bronze['bronze.source_file_rows']}"
        )
        .filter(F.col("ingestion_run_id") == run_id)
        .groupBy("source_dataset", "source_file", "file_sha256")
        .count()
        .collect()
    )
    file_rows = (
        spark.sql(
            "SELECT source_dataset, source_file, file_sha256, manifest_sha256, ingestion_run_id "
            "FROM lakehouse.bronze.source_files "
            f"VERSION AS OF {bronze['bronze.source_files']}"
        )
        .filter(F.col("ingestion_run_id") == run_id)
        .select("source_dataset", "source_file", "file_sha256", "manifest_sha256")
        .collect()
    )
    validate_provenance_observations(
        offset_manifest=offset_manifest,
        expected_files=expected_files,
        # Transaction markers and aborted values occupy positions but are invisible
        # to read_committed. The immutable manifest hash binds the exact bounds;
        # only aggregate endpoints are transferred to the Python driver.
        kafka_rows=tuple(
            position
            for row in kafka_rows
            for position in {
                (str(row["source_topic"]), int(row["source_partition"]), int(row["minimum"])),
                (str(row["source_topic"]), int(row["source_partition"]), int(row["maximum"])),
            }
        ),
        source_rows=tuple(
            (
                str(row["source_dataset"]), str(row["source_file"]),
                str(row["file_sha256"]), int(row["count"]),
            ) for row in source_rows
        ),
        source_files=tuple(
            (
                str(row["source_dataset"]), str(row["source_file"]),
                str(row["file_sha256"]), str(row["manifest_sha256"]),
            ) for row in file_rows
        ),
        file_manifest_sha256=hashlib.sha256(file_manifest_bytes).hexdigest(),
    )


def validate_provenance_observations(
    *,
    offset_manifest: KafkaOffsetManifest,
    expected_files: tuple[tuple[str, str, str, int], ...],
    kafka_rows: tuple[tuple[str, int, int], ...],
    source_rows: tuple[tuple[str, str, str, int], ...],
    source_files: tuple[tuple[str, str, str, str], ...],
    file_manifest_sha256: str,
) -> None:
    from collections import defaultdict

    observed_offsets: dict[tuple[str, int], list[int]] = defaultdict(list)
    for topic, partition, offset in kafka_rows:
        observed_offsets[(topic, partition)].append(offset)
    expected_ranges = {
        (item.topic, item.partition): (item.start_offset, item.end_offset)
        for item in offset_manifest.ranges
    }
    if not set(observed_offsets).issubset(expected_ranges):
        raise ValueError("offset manifest topic-partitions do not match Bronze provenance")
    for position, offsets in observed_offsets.items():
        start, end = expected_ranges[position]
        if len(offsets) != len(set(offsets)):
            raise ValueError(f"offset manifest range does not match Bronze provenance: {position}")
        if any(offset < start or offset >= end for offset in offsets):
            raise ValueError(f"offset manifest contains out-of-range Bronze rows: {position}")
    if tuple(sorted(source_rows)) != expected_files:
        raise ValueError("file manifest rows do not match Bronze provenance")
    expected_identities = {(item[0], item[1], item[2]) for item in expected_files}
    observed_identities = {(item[0], item[1], item[2]) for item in source_files}
    if observed_identities != expected_identities or any(
        item[3] != file_manifest_sha256 for item in source_files
    ):
        raise ValueError("file manifest metadata does not match Bronze provenance")


def validate_offset_manifest_hashes(
    manifest: KafkaOffsetManifest, stored_hashes: tuple[object, ...]
) -> None:
    if not stored_hashes or any(value != manifest.sha256 for value in stored_hashes):
        raise ValueError("offset manifest hash does not match Bronze ingestion provenance")


def _revenue_mapping(value: object, label: str) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != {"gross", "refund", "net"} or any(
        not isinstance(item, int) or isinstance(item, bool) for item in value.values()
    ):
        raise ValueError(f"{label} revenue is invalid")
    result = {name: int(value[name]) for name in ("gross", "refund", "net")}
    if result["net"] != result["gross"] - result["refund"]:
        raise ValueError(f"{label} revenue net does not reconcile")
    return result


def validate_evidence_manifest(manifest: Mapping[str, object]) -> None:
    if manifest.get("schema_version") != EVIDENCE_SCHEMA_VERSION:
        raise ValueError("evidence schema_version must be 1")
    if manifest.get("checksum_algorithm") != CHECKSUM_ALGORITHM:
        raise ValueError("evidence checksum algorithm is unsupported")
    if not isinstance(manifest.get("run_id"), str) or not manifest["run_id"]:
        raise ValueError("evidence run_id must be non-empty")
    inputs = manifest.get("inputs")
    required_inputs = {
        "bronze_snapshots", "silver_snapshots", "gold_snapshots",
        "later_silver_snapshots", "later_gold_snapshots",
        "offset_manifest", "file_manifest",
    }
    if not isinstance(inputs, dict) or set(inputs) != required_inputs:
        raise ValueError("evidence inputs are incomplete")
    _snapshot_mapping(inputs["bronze_snapshots"], BRONZE_TABLES, "Bronze")
    baseline = {
        **_snapshot_mapping(inputs["silver_snapshots"], SILVER_TABLES, "Silver"),
        **_snapshot_mapping(inputs["gold_snapshots"], GOLD_TABLES, "Gold"),
    }
    later = {
        **_snapshot_mapping(inputs["later_silver_snapshots"], SILVER_TABLES, "later Silver"),
        **_snapshot_mapping(inputs["later_gold_snapshots"], GOLD_TABLES, "later Gold"),
    }
    parse_input_provenance(inputs)
    tables = manifest.get("tables")
    if not isinstance(tables, dict) or set(tables) != set(EVIDENCE_TABLES):
        raise ValueError("evidence table coverage is incomplete")
    for name in EVIDENCE_TABLES:
        entry = tables[name]
        if not isinstance(entry, dict) or entry.get("table") != name:
            raise ValueError(f"evidence table identity mismatch: {name}")
        evidence_table_from_mapping(entry)
        if entry["snapshot_id"] == entry["later_snapshot_id"]:
            raise ValueError(f"baseline and later snapshots must be distinct: {name}")
        if entry["snapshot_id"] != baseline[name] or entry["later_snapshot_id"] != later[name]:
            raise ValueError(f"evidence snapshot map mismatch: {name}")
    quality = manifest.get("quality")
    if not isinstance(quality, dict) or not all(
        isinstance(key, str) and isinstance(value, int) and not isinstance(value, bool)
        for key, value in quality.items()
    ):
        raise ValueError("evidence quality is invalid")
    revenue = manifest.get("revenue")
    if not isinstance(revenue, dict) or set(revenue) != {"source", "governed", "attribution"}:
        raise ValueError("evidence revenue must be an object")
    source = _revenue_mapping(revenue.get("source"), "source")
    governed = _revenue_mapping(revenue.get("governed"), "governed")
    silver_reconciliation = tables["silver.customer_events"]["reconciliations"]
    if (
        silver_reconciliation.get("gross_amount_minor") != governed["gross"]
        or silver_reconciliation.get("refund_amount_minor") != governed["refund"]
    ):
        raise ValueError("Silver financial reconciliation differs from governed revenue")
    attribution = revenue.get("attribution")
    if not isinstance(attribution, dict) or set(attribution) != {
        "first_touch", "last_touch", "linear"
    }:
        raise ValueError("evidence attribution coverage is incomplete")
    for model, values in attribution.items():
        if _revenue_mapping(values, str(model)) != governed:
            raise ValueError(f"{model} attribution does not reconcile to governed revenue")
    if source["net"] != source["gross"] - source["refund"]:
        raise ValueError("source revenue net does not reconcile")


def compare_manifest_tables(
    expected: Mapping[str, Mapping[str, object]],
    actual: Mapping[str, Mapping[str, object]],
) -> tuple[str, ...]:
    if set(expected) != set(actual):
        return ("table coverage differs between original and rebuild",)
    mismatches: list[str] = []
    for name in sorted(expected):
        mismatches.extend(compare_table_evidence(
            evidence_table_from_mapping(expected[name]),
            evidence_table_from_mapping(actual[name]),
        ))
    return tuple(mismatches)


def validate_physical_schema(schema: object, logical_name: str) -> tuple[tuple[str, str, bool], ...]:
    actual = spark_schema_columns(schema)
    expected = contract_schema_columns(TABLE_CONTRACTS[logical_name])
    if actual != expected:
        raise ValueError(
            f"physical schema mismatch: {logical_name}; expected {expected}, actual {actual}"
        )
    return actual


def validate_recomputed_blocks(
    manifest: Mapping[str, object],
    *,
    quality: Mapping[str, int],
    revenue: Mapping[str, object],
) -> tuple[str, ...]:
    mismatches: list[str] = []
    if dict(quality) != manifest.get("quality"):
        mismatches.append("quality evidence differs from recomputed baseline")
    if dict(revenue) != manifest.get("revenue"):
        mismatches.append("revenue evidence differs from recomputed baseline")
    return tuple(mismatches)


def _physical(namespace: str, logical_name: str) -> str:
    return f"lakehouse.`{validate_namespace(namespace)}`.`{logical_name.split('.', 1)[1]}`"


def _normalized_row(row: Any) -> dict[str, object]:
    result = row.asDict(recursive=True)
    for key, value in result.items():
        if isinstance(value, datetime) and value.tzinfo is None:
            result[key] = value.replace(tzinfo=UTC)
    return result


def _canonical_checksum(frame: Any, columns: tuple[str, ...]) -> str:
    spark = frame.sparkSession

    def encode(row: Any) -> tuple[str]:
        return (canonical_row_bytes(_normalized_row(row), columns).decode("utf-8"),)

    encoded = spark.createDataFrame(frame.select(*columns).rdd.map(encode), "encoded string")
    ordered: Iterable[bytes] = (
        str(row["encoded"]).encode("utf-8")
        for row in encoded.orderBy("encoded").toLocalIterator()
    )
    return checksum_sorted_encoded_rows(ordered)


def _sum_columns(frame: Any, names: tuple[str, ...]) -> dict[str, int]:
    from pyspark.sql import functions as F

    existing = [name for name in names if name in frame.columns]
    if not existing:
        return {}
    financial_events = {
        "gross_amount_minor": "order_created",
        "refund_amount_minor": "order_refunded",
    }
    row = frame.agg(*[
        F.sum(
            _event_amount(financial_events[name], name)
            if "event_name" in frame.columns and name in financial_events
            else F.coalesce(F.col(name), F.lit(0))
        ).cast("long").alias(name)
        for name in existing
    ]).collect()[0]
    return {name: int(row[name] or 0) for name in existing}


def _snapshot_state(spark: Any, table: str) -> tuple[dict[int, int | None], int]:
    rows = spark.sql(f"SELECT snapshot_id, parent_id FROM {table}.snapshots").collect()
    parents = {
        int(row["snapshot_id"]): int(row["parent_id"]) if row["parent_id"] is not None else None
        for row in rows
    }
    refs = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").collect()
    if len(refs) != 1:
        raise ValueError(f"Iceberg main ref is missing or ambiguous: {table}")
    return parents, int(refs[0]["snapshot_id"])


def collect_table_evidence(
    spark: Any, *, logical_name: str, namespace: str, snapshot_id: int,
    later_snapshot_id: int, require_history: bool,
) -> dict[str, object]:
    contract = TABLE_CONTRACTS[logical_name]
    table = _physical(namespace, logical_name)
    parents, head = _snapshot_state(spark, table)
    if require_history:
        validate_snapshot_ancestry(parents, baseline=snapshot_id, later=later_snapshot_id, head=head)
    elif snapshot_id not in parents or later_snapshot_id != snapshot_id:
        raise ValueError(f"rebuild snapshot identity is invalid: {logical_name}")
    frame = spark.sql(f"SELECT * FROM {table} VERSION AS OF {int(snapshot_id)}")
    actual_schema = validate_physical_schema(frame.schema, logical_name)
    business_columns = contract.business_columns
    reconciliations = _sum_columns(frame, tuple(
        name for name in (
            "record_count", "distinct_event_count", "gross_amount_minor",
            "refund_amount_minor", "gross_revenue_minor", "net_revenue_minor",
            "allocated_gross_minor", "allocated_refund_minor", "allocated_net_minor",
        ) if name in frame.columns
    ))
    return {
        "table": logical_name,
        "physical_table": f"{namespace}.{logical_name.split('.', 1)[1]}",
        "snapshot_id": snapshot_id,
        "later_snapshot_id": later_snapshot_id,
        "schema_fingerprint": schema_fingerprint(actual_schema),
        "row_count": int(frame.count()),
        "checksum_algorithm": CHECKSUM_ALGORITHM,
        "business_columns": list(business_columns),
        "checksum": _canonical_checksum(frame, business_columns),
        "reconciliations": reconciliations,
    }


def _revenue_from_events(frame: Any) -> dict[str, int]:
    from pyspark.sql import functions as F

    row = frame.agg(
        F.sum(_event_amount("order_created", "gross_amount_minor")).cast("long").alias("gross"),
        F.sum(_event_amount("order_refunded", "refund_amount_minor")).cast("long").alias("refund"),
    ).collect()[0]
    gross, refund = int(row["gross"] or 0), int(row["refund"] or 0)
    return {"gross": gross, "refund": refund, "net": gross - refund}


def _source_revenue(spark: Any, snapshot_id: int) -> dict[str, int]:
    from pyspark.sql import functions as F

    rows = spark.sql(
        "SELECT source_topic, payload_text FROM lakehouse.bronze.kafka_records "
        f"VERSION AS OF {int(snapshot_id)}"
    ).filter(F.col("source_topic") == "marketing.clean.customer-events.v1")
    parsed = rows.select(
        F.get_json_object("payload_text", "$.event_name").alias("event_name"),
        F.get_json_object("payload_text", "$.payload.gross_amount_minor").cast("long").alias("gross_amount_minor"),
        F.get_json_object("payload_text", "$.payload.refund_amount_minor").cast("long").alias("refund_amount_minor"),
    ).filter(F.col("event_name").isin("order_created", "order_refunded"))
    return _revenue_from_events(parsed)


def _attribution_revenue(frame: Any) -> dict[str, dict[str, int]]:
    from pyspark.sql import functions as F

    rows = frame.groupBy("model").agg(
        F.sum("allocated_gross_minor").cast("long").alias("gross"),
        F.sum("allocated_refund_minor").cast("long").alias("refund"),
        F.sum("allocated_net_minor").cast("long").alias("net"),
    ).collect()
    result = {
        str(row["model"]): {
            "gross": int(row["gross"] or 0), "refund": int(row["refund"] or 0),
            "net": int(row["net"] or 0),
        } for row in rows
    }
    for model in ("first_touch", "last_touch", "linear"):
        result.setdefault(model, {"gross": 0, "refund": 0, "net": 0})
    return result


def _quality(spark: Any, namespace: str, snapshot_id: int) -> dict[str, int]:
    rows = spark.sql(
        "SELECT reason_code, SUM(record_count) AS records FROM "
        f"{_physical(namespace, 'silver.quality_summary')} VERSION AS OF {snapshot_id} "
        "GROUP BY reason_code"
    ).collect()
    return {str(row["reason_code"]): int(row["records"]) for row in rows}


def _metrics(
    spark: Any, *, bronze_snapshots: Mapping[str, int],
    silver_snapshots: Mapping[str, int], gold_snapshots: Mapping[str, int],
    silver_namespace: str, gold_namespace: str,
) -> tuple[dict[str, int], dict[str, object]]:
    events = spark.sql(
        f"SELECT * FROM {_physical(silver_namespace, 'silver.customer_events')} "
        f"VERSION AS OF {silver_snapshots['silver.customer_events']}"
    )
    attribution = spark.sql(
        f"SELECT * FROM {_physical(gold_namespace, 'gold.attribution_daily')} "
        f"VERSION AS OF {gold_snapshots['gold.attribution_daily']}"
    )
    quality = _quality(spark, silver_namespace, silver_snapshots["silver.quality_summary"])
    revenue: dict[str, object] = {
        "source": _source_revenue(spark, bronze_snapshots["bronze.kafka_records"]),
        "governed": _revenue_from_events(events),
        "attribution": _attribution_revenue(attribution),
    }
    return quality, revenue


def create_evidence(
    spark: Any, *, run_id: str, bronze_snapshots: Mapping[str, int],
    silver_snapshots: Mapping[str, int], gold_snapshots: Mapping[str, int],
    later_silver_snapshots: Mapping[str, int], later_gold_snapshots: Mapping[str, int],
    offset_manifest: bytes, file_manifest: bytes,
    silver_namespace: str = "silver", gold_namespace: str = "gold",
) -> dict[str, object]:
    if not run_id:
        raise ValueError("run_id must not be empty")
    baseline = {**silver_snapshots, **gold_snapshots}
    later = {**later_silver_snapshots, **later_gold_snapshots}
    if set(baseline) != set(EVIDENCE_TABLES) or set(later) != set(EVIDENCE_TABLES):
        raise ValueError("Silver/Gold snapshot coverage is incomplete")
    provenance_inputs = {
        "bronze_snapshots": dict(bronze_snapshots),
        "offset_manifest": _provenance(offset_manifest),
        "file_manifest": _provenance(file_manifest),
    }
    validate_input_provenance(spark, run_id=run_id, inputs=provenance_inputs)
    tables = {
        name: collect_table_evidence(
            spark, logical_name=name,
            namespace=silver_namespace if name.startswith("silver.") else gold_namespace,
            snapshot_id=int(baseline[name]), later_snapshot_id=int(later[name]),
            require_history=True,
        ) for name in EVIDENCE_TABLES
    }
    quality, revenue = _metrics(
        spark, bronze_snapshots=bronze_snapshots, silver_snapshots=silver_snapshots,
        gold_snapshots=gold_snapshots, silver_namespace=silver_namespace,
        gold_namespace=gold_namespace,
    )
    manifest: dict[str, object] = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "checksum_algorithm": CHECKSUM_ALGORITHM,
        "run_id": run_id,
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "git_commit": os.environ.get("GIT_COMMIT", "unknown"),
        "catalog_ref": os.environ.get("NESSIE_REF", "main"),
        "software": {"spark": spark.version, "python": "3.12"},
        "inputs": {
            "bronze_snapshots": dict(bronze_snapshots),
            "silver_snapshots": dict(silver_snapshots),
            "gold_snapshots": dict(gold_snapshots),
            "later_silver_snapshots": dict(later_silver_snapshots),
            "later_gold_snapshots": dict(later_gold_snapshots),
            "offset_manifest": provenance_inputs["offset_manifest"],
            "file_manifest": provenance_inputs["file_manifest"],
        },
        "tables": tables, "quality": quality, "revenue": revenue, "backfill": None,
    }
    validate_evidence_manifest(manifest)
    return manifest


def verify_evidence(spark: Any, manifest: Mapping[str, object]) -> dict[str, object]:
    validate_evidence_manifest(manifest)
    inputs, tables = manifest["inputs"], manifest["tables"]
    assert isinstance(inputs, dict) and isinstance(tables, dict)
    bronze = _snapshot_mapping(inputs["bronze_snapshots"], BRONZE_TABLES, "Bronze")
    silver = _snapshot_mapping(inputs["silver_snapshots"], SILVER_TABLES, "Silver")
    gold = _snapshot_mapping(inputs["gold_snapshots"], GOLD_TABLES, "Gold")
    validate_input_provenance(
        spark, run_id=str(manifest["run_id"]), inputs=inputs
    )
    actual: dict[str, Mapping[str, object]] = {}
    namespaces: dict[str, str] = {}
    for name in EVIDENCE_TABLES:
        expected = tables[name]
        assert isinstance(expected, dict)
        namespace, _table = str(expected["physical_table"]).split(".", 1)
        namespaces[name] = namespace
        actual[name] = collect_table_evidence(
            spark, logical_name=name, namespace=namespace,
            snapshot_id=int(expected["snapshot_id"]),
            later_snapshot_id=int(expected["later_snapshot_id"]), require_history=True,
        )
    mismatches = list(compare_manifest_tables(tables, actual))  # type: ignore[arg-type]
    quality, revenue = _metrics(
        spark, bronze_snapshots=bronze, silver_snapshots=silver, gold_snapshots=gold,
        silver_namespace=namespaces["silver.customer_events"],
        gold_namespace=namespaces["gold.attribution_daily"],
    )
    mismatches.extend(validate_recomputed_blocks(manifest, quality=quality, revenue=revenue))
    if mismatches:
        raise ValueError("; ".join(mismatches))
    return {"command": "verify", "run_id": manifest["run_id"], "tables": len(actual), "verified": True}


def _current_head(spark: Any, namespace: str, logical_name: str) -> int:
    _parents, head = _snapshot_state(spark, _physical(namespace, logical_name))
    return head


def _namespace_exists(spark: Any, namespace: str) -> bool:
    return namespace in {str(row[0]) for row in spark.sql("SHOW NAMESPACES IN lakehouse").collect()}


def _evidence_identity(manifest: Mapping[str, object]) -> str:
    payload = json.dumps(
        {"run_id": manifest["run_id"], "inputs": manifest["inputs"]},
        sort_keys=True, separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def backfill_proof(spark: Any, manifest: Mapping[str, object]) -> dict[str, object]:
    validate_evidence_manifest(manifest)
    run_id = str(manifest["run_id"])
    namespace = rebuild_namespace(run_id, _evidence_identity(manifest))
    tables, inputs = manifest["tables"], manifest["inputs"]
    assert isinstance(tables, dict) and isinstance(inputs, dict)
    original_locations = {
        name: str(tables[name]["physical_table"]).split(".", 1)[0]  # type: ignore[index]
        for name in EVIDENCE_TABLES
    }
    if namespace in set(original_locations.values()):
        raise ValueError("rebuild namespace collides with an original namespace")
    if _namespace_exists(spark, namespace):
        raise ValueError(f"rebuild namespace already exists: {namespace}")
    original_heads = {
        name: _current_head(spark, original_locations[name], name) for name in EVIDENCE_TABLES
    }
    bronze = _snapshot_mapping(inputs["bronze_snapshots"], BRONZE_TABLES, "Bronze")
    silver_result = build_silver(
        spark, kafka_snapshot_id=bronze["bronze.kafka_records"],
        source_rows_snapshot_id=bronze["bronze.source_file_rows"],
        run_id=f"{run_id}-rebuild", target_namespace=namespace,
    )
    silver_ids = {f"silver.{name}": int(value) for name, value in silver_result["snapshots"].items()}
    gold_result = build_gold(
        spark, silver_snapshots=silver_ids, run_id=f"{run_id}-rebuild",
        source_namespace=namespace, target_namespace=namespace,
    )
    gold_ids = {f"gold.{name}": int(value) for name, value in gold_result["snapshots"].items()}
    rebuilt: dict[str, Mapping[str, object]] = {}
    for name, snapshot in (silver_ids | gold_ids).items():
        rebuilt[name] = collect_table_evidence(
            spark, logical_name=name, namespace=namespace, snapshot_id=snapshot,
            later_snapshot_id=snapshot, require_history=False,
        )
    mismatches = list(compare_manifest_tables(tables, rebuilt))  # type: ignore[arg-type]
    after_heads = {
        name: _current_head(spark, original_locations[name], name) for name in EVIDENCE_TABLES
    }
    if after_heads != original_heads:
        mismatches.append("original table heads changed during isolated rebuild")
    if mismatches:
        raise ValueError("; ".join(mismatches))
    return {
        "command": "backfill-proof", "run_id": run_id, "namespace": namespace,
        "tables": len(rebuilt), "matches": True, "rebuilt_tables": rebuilt,
    }


def load_evidence(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("evidence must be a JSON object")
    validate_evidence_manifest(value)
    return value


def write_evidence(path: Path, manifest: Mapping[str, object]) -> None:
    validate_evidence_manifest(manifest)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)
