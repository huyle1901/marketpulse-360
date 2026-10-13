"""Isolated ClickHouse copy attempts; partial databases are never reused."""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path
from typing import Any

from marketpulse_lakehouse.contracts import TABLE_CONTRACTS
from .canonical import canonical_row, clickhouse_type, rowset_checksum
from .export import ExportManifest, json_rows, object_location


def _download(s3: Any, obj: Any, path: Path, columns: Any) -> None:
    bucket, key = object_location(obj.uri)
    stream = s3.get_object(Bucket=bucket, Key=key)["Body"]
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("wb") as target:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
                if size > obj.byte_size:
                    raise ValueError("export object size mismatch")
                target.write(chunk)
    finally:
        stream.close()
    if size != obj.byte_size or digest.hexdigest() != obj.sha256:
        raise ValueError("export object hash/size mismatch")
    count = 0
    for count, row in enumerate(json_rows(path), start=1):
        canonical_row(columns, row)
    if count != obj.row_count:
        raise ValueError("export object row-count mismatch")


def load_candidate(client: Any, manifest: ExportManifest, *, attempt: int, s3: Any) -> str:
    if type(attempt) is not int or attempt < 1:
        raise ValueError("copy attempt must be a positive integer")
    database = f"mp_release_{manifest.identity}_{attempt}"
    exists = client.query("SELECT count() FROM system.databases WHERE name={database:String}",
                          parameters={"database": database}).first_row[0]
    if exists:
        raise ValueError("candidate database already exists; use a fresh attempt")
    # No IF NOT EXISTS: racing callers must not share an attempt database.
    client.command(f"CREATE DATABASE {database} ENGINE=Atomic")
    with tempfile.TemporaryDirectory(prefix="marketpulse-copy-") as root:
        for relation in manifest.relations:
            model = relation.name.split(".")[1]
            columns = ",".join(f"`{name}` {clickhouse_type(kind, nullable)}" for name, kind, nullable in relation.columns)
            order = ",".join(f"`{name}`" for name in TABLE_CONTRACTS[relation.name].key_fields)
            client.command(f"CREATE TABLE {database}.raw_{model} ({columns}) ENGINE=MergeTree ORDER BY ({order})")
            for index, obj in enumerate(relation.objects):
                path = Path(root) / f"object-{index}.json"
                _download(s3, obj, path, relation.columns)
                with path.open("rb") as stream:
                    batch = bytearray()
                    for line in stream:
                        if len(line) > 1024 * 1024:
                            raise ValueError("export row exceeds bounded copy size")
                        batch.extend(line)
                        if len(batch) >= 1024 * 1024:
                            client.raw_insert(f"{database}.raw_{model}", insert_block=bytes(batch), fmt="JSONEachRow")
                            batch.clear()
                    if batch:
                        client.raw_insert(f"{database}.raw_{model}", insert_block=bytes(batch), fmt="JSONEachRow")
                path.unlink()
            names = [name for name, _, _ in relation.columns]
            with client.query_rows_stream(f"SELECT * FROM {database}.raw_{model}", query_tz="UTC", tz_mode="aware") as rows:
                count, checksum = rowset_checksum(relation.columns, (dict(zip(names, row)) for row in rows))
            if (count, checksum) != (relation.row_count, relation.business_checksum):
                raise ValueError(f"candidate business rowset mismatch: {relation.name}")
    return database
