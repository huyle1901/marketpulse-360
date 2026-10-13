"""Stable ingestion and governed profile identities."""

from __future__ import annotations

import hashlib
import json


def _canonical_digest(parts: list[object]) -> str:
    encoded = json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def kafka_ingest_id(topic: str, partition: int, offset: int) -> str:
    if not topic:
        raise ValueError("topic must not be empty")
    if partition < 0:
        raise ValueError("partition must be non-negative")
    if offset < 0:
        raise ValueError("offset must be non-negative")
    return _canonical_digest([topic, partition, offset])


def file_ingest_id(file_sha256: str, row_number: int) -> str:
    if not file_sha256:
        raise ValueError("file_sha256 must not be empty")
    if row_number < 1:
        raise ValueError("row_number must be one-based")
    return _canonical_digest([file_sha256, row_number])


def resolve_profile_key(
    customer_id: str | None,
    anonymous_id: str | None,
    session_id: str | None,
    *,
    stitched_customer_id: str | None = None,
) -> str:
    if customer_id:
        return f"customer:{customer_id}"
    if stitched_customer_id:
        return f"customer:{stitched_customer_id}"
    if anonymous_id:
        return f"anonymous:{anonymous_id}"
    if session_id:
        return f"session:{session_id}"
    raise ValueError("profile identity is missing")
