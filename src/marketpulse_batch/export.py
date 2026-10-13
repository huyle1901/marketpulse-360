"""Snapshot-pinned, typed exports from Iceberg to owned MinIO object prefixes."""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
from dataclasses import dataclass
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

from marketpulse_lakehouse.contracts import TABLE_CONTRACTS
from marketpulse_lakehouse.verify import validate_physical_schema
from .canonical import Columns, canonical_row, rowset_checksum
from .contracts import PreparationReceipt, validate_digest

EXPORTED_RELATIONS = tuple(sorted([
    name for name in TABLE_CONTRACTS if name.startswith("gold.")
] + ["silver.campaigns", "silver.customers"]))


def object_location(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError("export objects require a plain S3 URI")
    key = parsed.path.lstrip("/")
    if not key or ".." in PurePosixPath(key).parts or "//" in key:
        raise ValueError("invalid export object key")
    return parsed.netloc, key


def business_schema(name: str) -> Columns:
    return tuple((c.name, c.iceberg_type, c.nullable) for c in TABLE_CONTRACTS[name].columns if not c.operational)


@dataclass(frozen=True, slots=True)
class ExportObject:
    uri: str
    sha256: str
    byte_size: int
    row_count: int

    def __post_init__(self) -> None:
        object_location(self.uri)
        validate_digest(self.sha256, "object sha256")
        if any(type(n) is not int or n < 0 for n in (self.byte_size, self.row_count)):
            raise ValueError("invalid export object counts")


@dataclass(frozen=True, slots=True)
class ExportRelation:
    name: str
    snapshot_id: int
    columns: Columns
    objects: tuple[ExportObject, ...]
    row_count: int
    business_checksum: str

    def __post_init__(self) -> None:
        if self.name not in EXPORTED_RELATIONS or tuple(self.columns) != business_schema(self.name):
            raise ValueError("export relation/schema does not match Gold/reference contract")
        validate_digest(self.business_checksum, "business checksum")
        if type(self.snapshot_id) is not int or self.snapshot_id <= 0:
            raise ValueError("invalid export source snapshot")
        if type(self.row_count) is not int or self.row_count < 0:
            raise ValueError("invalid export relation count")
        if self.row_count != sum(obj.row_count for obj in self.objects):
            raise ValueError("export object counts do not match relation")
        object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(self, "objects", tuple(self.objects))


@dataclass(frozen=True, slots=True)
class ExportManifest:
    identity: str
    relations: tuple[ExportRelation, ...]

    def __post_init__(self) -> None:
        validate_digest(self.identity, "identity")
        if tuple(sorted(r.name for r in self.relations)) != EXPORTED_RELATIONS:
            raise ValueError("export requires exactly five Gold and two reference relations")
        seen = set()
        for relation in self.relations:
            for obj in relation.objects:
                _, key = object_location(obj.uri)
                if not key.startswith(f"batch-exports/{self.identity}/") or obj.uri in seen:
                    raise ValueError("export object is outside this identity or duplicated")
                seen.add(obj.uri)
        object.__setattr__(self, "relations", tuple(sorted(self.relations, key=lambda r: r.name)))

    def payload(self) -> dict[str, object]:
        return {"version": 1, "identity": self.identity, "relations": [
            {"name": r.name, "snapshot_id": r.snapshot_id, "columns": r.columns,
             "row_count": r.row_count, "business_checksum": r.business_checksum,
             "objects": [{"uri": o.uri, "sha256": o.sha256, "byte_size": o.byte_size,
                          "row_count": o.row_count} for o in r.objects]} for r in self.relations
        ]}

    @property
    def digest(self) -> str:
        return hashlib.sha256(json.dumps(self.payload(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _unique_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate export JSON field")
        result[name] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError("non-finite export JSON value")


def json_rows(path: Path) -> Iterator[dict[str, object]]:
    with path.open("rb") as stream:
        while line := stream.readline(1024 * 1024 + 1):
            if len(line) > 1024 * 1024:
                raise ValueError("export row exceeds bounded copy size")
            try:
                row = json.loads(line, parse_float=Decimal, parse_constant=_reject_constant,
                                 object_pairs_hook=_unique_fields)
            except (ValueError, UnicodeDecodeError) as error:
                raise ValueError("malformed export JSON row") from error
            if not isinstance(row, dict):
                raise ValueError("export JSON row must be an object")
            yield row


def export_snapshots(spark: Any, receipt: PreparationReceipt, output_prefix: str, *, s3: Any = None) -> ExportManifest:
    bucket, prefix = object_location(output_prefix)
    if re.fullmatch(rf"batch-exports/{receipt.identity}/[a-zA-Z0-9_-]+", prefix) is None:
        raise ValueError("export prefix must be a fresh owned identity/attempt prefix")
    if not set(EXPORTED_RELATIONS) <= set(receipt.output_snapshots):
        raise ValueError("receipt lacks required Gold/reference snapshots")
    if s3 is None:
        from marketpulse_lakehouse.source_files import create_s3_client
        s3 = create_s3_client()
    relations = []
    with tempfile.TemporaryDirectory(prefix="marketpulse-export-") as root:
        for name in EXPORTED_RELATIONS:
            snapshot = receipt.output_snapshots[name]
            model = name.split(".")[1]
            frame = spark.read.format("iceberg").option("snapshot-id", str(snapshot)).load(
                f"lakehouse.{receipt.namespace}.{model}"
            )
            validate_physical_schema(frame.schema, name)
            columns = business_schema(name)
            business = frame.select(*(c[0] for c in columns))

            def source_rows():
                for row in business.repartition(32).toLocalIterator(prefetchPartitions=False):
                    values = row.asDict()
                    # Spark's Python timestamp representation is naive in the UTC session.
                    for field, kind, _ in columns:
                        if kind == "timestamp" and isinstance(values[field], datetime) and values[field].tzinfo is None:
                            values[field] = values[field].replace(tzinfo=UTC)
                    yield values

            source_count, source_checksum = rowset_checksum(columns, source_rows())
            directory = Path(root) / model
            (business.repartition(32).write.mode("errorifexists")
             .option("ignoreNullFields", "false").option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
             .option("maxRecordsPerFile", 50000).json(str(directory)))
            local_files = sorted(directory.glob("part-*.json"))

            def exported_rows():
                for path in local_files:
                    yield from json_rows(path)

            count, checksum = rowset_checksum(columns, exported_rows())
            if (count, checksum) != (source_count, source_checksum):
                raise ValueError(f"snapshot/export business mismatch: {name}")
            objects = []
            for path in local_files:
                digest = hashlib.sha256()
                rows = 0
                with path.open("rb") as stream:
                    for line in stream:
                        digest.update(line)
                        rows += 1
                key = f"{prefix}/{name}/{path.name}"
                # Conditional put: never overwrite a previous export object.
                with path.open("rb") as stream:
                    s3.put_object(Bucket=bucket, Key=key, Body=stream, IfNoneMatch="*")
                objects.append(ExportObject(f"s3://{bucket}/{key}", digest.hexdigest(), path.stat().st_size, rows))
            relations.append(ExportRelation(name, snapshot, columns, tuple(objects), count, checksum))
    manifest = ExportManifest(receipt.identity, tuple(relations))
    s3.put_object(Bucket=bucket, Key=f"{prefix}/manifest.json",
                  Body=json.dumps(manifest.payload(), sort_keys=True).encode(), IfNoneMatch="*")
    return manifest
