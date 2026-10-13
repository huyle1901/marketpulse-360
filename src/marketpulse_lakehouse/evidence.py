"""Canonical business-row evidence and rebuild comparison."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Iterable, Mapping, Sequence


EVIDENCE_SCHEMA_VERSION = 1
CHECKSUM_ALGORITHM = "canonical-json-rowset-v1"


@dataclass(frozen=True, slots=True)
class TableEvidence:
    table: str
    snapshot_id: int
    schema_fingerprint: str
    row_count: int
    checksum: str
    reconciliations: Mapping[str, int]


def schema_fingerprint(columns: Sequence[tuple[str, str, bool]]) -> str:
    payload = [
        {"name": name, "type": column_type, "nullable": nullable}
        for name, column_type, nullable in columns
    ]
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


def canonical_row_checksum(
    rows: Iterable[Mapping[str, object]],
    columns: Sequence[str],
) -> str:
    encoded_rows = sorted(
        _canonical_json([_normalize(row.get(column)) for column in columns])
        for row in rows
    )
    digest = hashlib.sha256()
    for encoded in encoded_rows:
        digest.update(len(encoded).to_bytes(8, byteorder="big", signed=False))
        digest.update(encoded)
    return digest.hexdigest()


def canonical_row_bytes(
    row: Mapping[str, object], columns: Sequence[str]
) -> bytes:
    """Encode one projected business row using the v1 checksum contract."""

    return _canonical_json([_normalize(row.get(column)) for column in columns])


def checksum_sorted_encoded_rows(rows: Iterable[bytes]) -> str:
    """Hash already lexicographically sorted canonical rows without buffering."""

    digest = hashlib.sha256()
    previous: bytes | None = None
    for encoded in rows:
        if previous is not None and encoded < previous:
            raise ValueError("canonical rows are not sorted")
        digest.update(len(encoded).to_bytes(8, byteorder="big", signed=False))
        digest.update(encoded)
        previous = encoded
    return digest.hexdigest()


def compare_table_evidence(
    expected: TableEvidence,
    actual: TableEvidence,
) -> tuple[str, ...]:
    mismatches: list[str] = []
    label = expected.table
    for field_name in ("schema_fingerprint", "row_count", "checksum"):
        expected_value = getattr(expected, field_name)
        actual_value = getattr(actual, field_name)
        if expected_value != actual_value:
            mismatches.append(
                f"{label}: {field_name} expected {expected_value}, actual {actual_value}"
            )
    all_names = sorted(set(expected.reconciliations) | set(actual.reconciliations))
    for name in all_names:
        expected_value = expected.reconciliations.get(name)
        actual_value = actual.reconciliations.get(name)
        if expected_value != actual_value:
            mismatches.append(
                f"{label}: reconciliation {name} expected {expected_value}, "
                f"actual {actual_value}"
            )
    return tuple(mismatches)


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _normalize(value: object) -> object:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("canonical timestamps must be timezone-aware")
        utc_value = value.astimezone(timezone.utc)
        return utc_value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, Mapping):
        return {str(key): _normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")
