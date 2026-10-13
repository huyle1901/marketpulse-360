"""Fail-closed planning and normalization for immutable Bronze source files."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Iterator, Protocol

from .bronze import _spark_schema, current_snapshot_id, merge_insert_only
from .contracts import TABLE_CONTRACTS
from .credentials import minio_password
from .identity import file_ingest_id


ALLOWED_SOURCE_DATASETS = frozenset(
    {
        "crm",
        "google_ads",
        "meta_ads",
        "tiktok_ads",
        "master/ads",
        "master/campaigns",
        "master/customers",
        "master/products",
    }
)
DEFAULT_SOURCE_ARCHIVE_PREFIX = (
    "s3://marketpulse-lakehouse/bronze/source-files"
)


@dataclass(frozen=True, slots=True)
class SourceArchiveObject:
    source_dataset: str
    source_file: str
    source_object_uri: str
    file_sha256: str
    local_path: Path
    compressed_size_bytes: int
    format_name: str
    row_count: int
    source_date: date | None

    @property
    def content(self) -> bytes:
        """Read exact bytes on demand; production plans do not retain file bodies."""

        return self.local_path.read_bytes()


@dataclass(frozen=True, slots=True)
class SourceArchivePlan:
    manifest_sha256: str
    archive_objects: tuple[SourceArchiveObject, ...]
    source_files: tuple[SourceArchiveObject, ...]


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _object_uri(prefix: str, digest: str, basename: str) -> str:
    return f"{prefix.rstrip('/')}/sha256={digest}/{basename}"


def _source_date(relative_path: str) -> date | None:
    partitions = [
        part.removeprefix("date=")
        for part in PurePosixPath(relative_path).parts
        if part.startswith("date=")
    ]
    if not partitions:
        return None
    if len(partitions) != 1:
        raise ValueError(f"source file has multiple date partitions: {relative_path}")
    try:
        return date.fromisoformat(partitions[0])
    except ValueError as error:
        raise ValueError(f"invalid source date partition: {relative_path}") from error


def _safe_source_path(root: Path, relative_path: str) -> Path:
    relative = PurePosixPath(relative_path)
    if not relative_path or relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"invalid relative path: {relative_path}")
    root_resolved = root.resolve()
    path = (root / Path(*relative.parts)).resolve()
    if not path.is_relative_to(root_resolved):
        raise ValueError(f"relative path escapes dataset root: {relative_path}")
    return path


def build_source_archive_plan(
    root: Path,
    *,
    archive_prefix: str = DEFAULT_SOURCE_ARCHIVE_PREFIX,
) -> SourceArchivePlan:
    """Validate an input manifest completely before returning archive work."""

    root = Path(root)
    manifest_path = root / "manifest.json"
    try:
        manifest_content = manifest_path.read_bytes()
        manifest = json.loads(manifest_content)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"invalid manifest: {error}") from error
    if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), list):
        raise ValueError("manifest must contain a files list")

    manifest_digest = _sha256(manifest_content)
    manifest_object = SourceArchiveObject(
        source_dataset="_manifest",
        source_file="manifest.json",
        source_object_uri=_object_uri(
            archive_prefix, manifest_digest, "manifest.json"
        ),
        file_sha256=manifest_digest,
        local_path=manifest_path,
        compressed_size_bytes=len(manifest_content),
        format_name="json",
        row_count=0,
        source_date=None,
    )

    seen_paths: set[str] = set()
    seen_digests: dict[str, str] = {}
    source_files: list[SourceArchiveObject] = []
    for index, raw_item in enumerate(manifest["files"]):
        if not isinstance(raw_item, dict):
            raise ValueError(f"manifest file entry {index} must be an object")
        dataset = raw_item.get("dataset")
        relative_path = raw_item.get("relative_path")
        if not isinstance(dataset, str) or not isinstance(relative_path, str):
            raise ValueError(f"manifest file entry {index} has invalid identity")
        if relative_path in seen_paths:
            raise ValueError(f"duplicate manifest relative path: {relative_path}")
        seen_paths.add(relative_path)
        if dataset not in ALLOWED_SOURCE_DATASETS:
            continue

        format_name = raw_item.get("format")
        row_count = raw_item.get("row_count")
        expected_size = raw_item.get("byte_size")
        expected_digest = raw_item.get("sha256")
        if format_name not in {"csv", "jsonl"}:
            raise ValueError(f"unsupported source format: {relative_path}")
        if not isinstance(row_count, int) or row_count < 0:
            raise ValueError(f"invalid row count: {relative_path}")
        if not isinstance(expected_size, int) or expected_size < 0:
            raise ValueError(f"invalid byte size: {relative_path}")
        if not isinstance(expected_digest, str) or len(expected_digest) != 64:
            raise ValueError(f"invalid checksum: {relative_path}")
        if not relative_path.endswith(f".{format_name}.gz"):
            raise ValueError(f"source file extension mismatch: {relative_path}")

        path = _safe_source_path(root, relative_path)
        try:
            actual_size = path.stat().st_size
            actual_digest = _sha256_path(path)
        except OSError as error:
            raise ValueError(f"cannot read source file: {relative_path}") from error
        if actual_digest != expected_digest:
            raise ValueError(f"checksum mismatch: {relative_path}")
        if actual_size != expected_size:
            raise ValueError(f"byte-size mismatch: {relative_path}")
        previous_path = seen_digests.get(actual_digest)
        if previous_path is not None and previous_path != relative_path:
            raise ValueError(
                "duplicate content digest has conflicting file lineage: "
                f"{previous_path} and {relative_path}"
            )
        seen_digests[actual_digest] = relative_path

        source_files.append(
            SourceArchiveObject(
                source_dataset=dataset,
                source_file=relative_path,
                source_object_uri=_object_uri(
                    archive_prefix, actual_digest, PurePosixPath(relative_path).name
                ),
                file_sha256=actual_digest,
                local_path=path,
                compressed_size_bytes=actual_size,
                format_name=format_name,
                row_count=row_count,
                source_date=_source_date(relative_path),
            )
        )

    files = tuple(source_files)
    return SourceArchivePlan(
        manifest_sha256=manifest_digest,
        archive_objects=(manifest_object, *files),
        source_files=files,
    )


def _parsed_rows(source: SourceArchiveObject) -> list[dict[str, Any]]:
    try:
        with gzip.open(source.local_path, mode="rt", encoding="utf-8", newline="") as stream:
            if source.format_name == "csv":
                reader = csv.DictReader(stream)
                if not reader.fieldnames or len(reader.fieldnames) != len(set(reader.fieldnames)):
                    raise ValueError(f"invalid CSV header: {source.source_file}")
                rows: list[dict[str, Any]] = []
                for row in reader:
                    if None in row or any(value is None for value in row.values()):
                        raise ValueError(f"invalid CSV row shape: {source.source_file}")
                    rows.append(
                        {key: None if value == "" else value for key, value in row.items()}
                    )
                return rows

            rows = []
            for line_number, line in enumerate(stream, start=1):
                if not line.strip():
                    raise ValueError(
                        f"blank JSONL row {line_number}: {source.source_file}"
                    )
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError(
                        f"JSONL row {line_number} is not an object: {source.source_file}"
                    )
                rows.append(value)
            return rows
    except (OSError, EOFError, gzip.BadGzipFile, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot parse source file: {source.source_file}") from error


def verify_source_archive_object(source: SourceArchiveObject) -> None:
    """Detect local mutations between planning, archival, and parsing."""

    try:
        current_size = source.local_path.stat().st_size
        current_digest = _sha256_path(source.local_path)
    except OSError as error:
        raise ValueError(
            f"source file changed after planning: {source.source_file}"
        ) from error
    if (
        current_size != source.compressed_size_bytes
        or current_digest != source.file_sha256
    ):
        raise ValueError(f"source file changed after planning: {source.source_file}")


def normalize_source_file(
    source: SourceArchiveObject,
    *,
    run_id: str,
    ingested_at: datetime,
) -> tuple[dict[str, object], ...]:
    if source.source_dataset not in ALLOWED_SOURCE_DATASETS:
        raise ValueError("only allowlisted compressed source files can be normalized")
    if not run_id:
        raise ValueError("run_id must not be empty")
    verify_source_archive_object(source)
    rows = _parsed_rows(source)
    if len(rows) != source.row_count:
        raise ValueError(
            f"row-count mismatch: {source.source_file}; "
            f"manifest={source.row_count} actual={len(rows)}"
        )

    normalized: list[dict[str, object]] = []
    for row_number, row in enumerate(rows, start=1):
        try:
            payload_json = json.dumps(
                row,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as error:
            raise ValueError(
                f"source row is not canonical JSON: {source.source_file}:{row_number}"
            ) from error
        normalized.append(
            {
                "ingest_id": file_ingest_id(source.file_sha256, row_number),
                "source_dataset": source.source_dataset,
                "source_file": source.source_file,
                "source_object_uri": source.source_object_uri,
                "row_number": row_number,
                "source_date": source.source_date,
                "file_sha256": source.file_sha256,
                "payload_json": payload_json,
                "payload_sha256": _sha256(payload_json.encode()),
                "ingestion_run_id": run_id,
                "ingested_at": ingested_at,
            }
        )
    return tuple(normalized)


def source_file_record(
    source: SourceArchiveObject,
    *,
    manifest_sha256: str,
    run_id: str,
    archived_at: datetime,
) -> dict[str, object]:
    if not manifest_sha256 or not run_id:
        raise ValueError("manifest_sha256 and run_id must not be empty")
    verify_source_archive_object(source)
    return {
        "file_sha256": source.file_sha256,
        "source_dataset": source.source_dataset,
        "source_file": source.source_file,
        "source_object_uri": source.source_object_uri,
        "compressed_size_bytes": source.compressed_size_bytes,
        "manifest_sha256": manifest_sha256,
        "ingestion_run_id": run_id,
        "archived_at": archived_at,
    }


class S3Client(Protocol):
    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]: ...

    def upload_file(
        self,
        Filename: str,
        Bucket: str,
        Key: str,
        ExtraArgs: dict[str, Any],
    ) -> None: ...


def _s3_parts(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"source object URI is not S3: {uri}")
    bucket, separator, key = uri.removeprefix("s3://").partition("/")
    if not bucket or not separator or not key:
        raise ValueError(f"invalid S3 object URI: {uri}")
    return bucket, key


def create_s3_client() -> S3Client:
    password = minio_password()
    try:
        import boto3
    except ImportError as error:
        raise RuntimeError("S3 support is not installed in the lakehouse image") from error
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "marketpulse"),
        aws_secret_access_key=password,
        region_name=os.environ.get("MINIO_REGION", "us-east-1"),
    )


@contextmanager
def _staged_source_object(source: SourceArchiveObject) -> Iterator[Path]:
    """Copy and hash an input once so later upload cannot race source mutation."""

    staged_path: Path | None = None
    digest = hashlib.sha256()
    size = 0
    try:
        with source.local_path.open("rb") as input_stream:
            with tempfile.NamedTemporaryFile(prefix="marketpulse-bronze-", delete=False) as staged:
                staged_path = Path(staged.name)
                while chunk := input_stream.read(1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
                    staged.write(chunk)
                staged.flush()
                os.fsync(staged.fileno())
        if size != source.compressed_size_bytes or digest.hexdigest() != source.file_sha256:
            raise ValueError(f"source file changed after planning: {source.source_file}")
        yield staged_path
    except OSError as error:
        raise ValueError(f"cannot stage source file: {source.source_file}") from error
    finally:
        if staged_path is not None:
            staged_path.unlink(missing_ok=True)


def _verified_remote_object(
    response: dict[str, Any], source: SourceArchiveObject
) -> bool:
    metadata = {
        str(name).lower(): str(value)
        for name, value in response.get("Metadata", {}).items()
    }
    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        return False
    digest = hashlib.sha256()
    size = 0
    try:
        while chunk := body.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()
    return (
        int(response.get("ContentLength", -1)) == source.compressed_size_bytes
        and size == source.compressed_size_bytes
        and metadata.get("sha256") == source.file_sha256
        and digest.hexdigest() == source.file_sha256
    )


def archive_source_objects(
    plan: SourceArchivePlan,
    *,
    client: S3Client | None = None,
) -> tuple[int, int]:
    """Upload exact bytes by content hash, rejecting an incompatible existing key."""

    s3 = create_s3_client() if client is None else client
    uploaded = existing = 0
    for source in plan.archive_objects:
        bucket, key = _s3_parts(source.source_object_uri)
        with _staged_source_object(source) as staged_path:
            try:
                observed = s3.get_object(Bucket=bucket, Key=key)
            except Exception as error:  # boto's optional exception type is lazy-loaded
                response = getattr(error, "response", {})
                code = str(response.get("Error", {}).get("Code", ""))
                if code not in {"404", "NoSuchKey", "NotFound"}:
                    raise
                s3.upload_file(
                    str(staged_path),
                    bucket,
                    key,
                    ExtraArgs={"Metadata": {"sha256": source.file_sha256}},
                )
                observed = s3.get_object(Bucket=bucket, Key=key)
                if not _verified_remote_object(observed, source):
                    raise ValueError(
                        f"SOURCE_OBJECT_COLLISION: {source.source_object_uri}"
                    )
                uploaded += 1
                continue
            if not _verified_remote_object(observed, source):
                raise ValueError(f"SOURCE_OBJECT_COLLISION: {source.source_object_uri}")
            existing += 1
    return uploaded, existing


def ingest_source_files(
    spark: Any,
    root: Path,
    *,
    run_id: str,
    ingested_at: datetime,
    client: S3Client | None = None,
) -> dict[str, object]:
    if not run_id:
        raise ValueError("run_id must not be empty")
    plan = build_source_archive_plan(root)
    uploaded, existing_objects = archive_source_objects(plan, client=client)
    file_records = tuple(
        source_file_record(
            source,
            manifest_sha256=plan.manifest_sha256,
            run_id=run_id,
            archived_at=ingested_at,
        )
        for source in plan.source_files
    )
    row_records = tuple(
        row
        for source in plan.source_files
        for row in normalize_source_file(
            source,
            run_id=run_id,
            ingested_at=ingested_at,
        )
    )
    files_df = spark.createDataFrame(
        file_records, schema=_spark_schema(TABLE_CONTRACTS["bronze.source_files"])
    )
    rows_df = spark.createDataFrame(
        row_records, schema=_spark_schema(TABLE_CONTRACTS["bronze.source_file_rows"])
    )
    inserted_files, noop_files = merge_insert_only(
        spark, table_name="bronze.source_files", records=files_df
    )
    inserted_rows, noop_rows = merge_insert_only(
        spark, table_name="bronze.source_file_rows", records=rows_df
    )
    return {
        "command": "ingest-files",
        "run_id": run_id,
        "manifest_sha256": plan.manifest_sha256,
        "archive_objects": len(plan.archive_objects),
        "uploaded_objects": uploaded,
        "existing_objects": existing_objects,
        "source_files": len(file_records),
        "source_rows": len(row_records),
        "inserted_files": inserted_files,
        "noop_files": noop_files,
        "inserted_rows": inserted_rows,
        "noop_rows": noop_rows,
        "source_files_snapshot_id": current_snapshot_id(spark, "bronze.source_files"),
        "source_rows_snapshot_id": current_snapshot_id(
            spark, "bronze.source_file_rows"
        ),
    }
