"""Stable identifier and integer derivation helpers."""

from __future__ import annotations

import hashlib
import uuid


def stable_id(seed: int, entity: str, ordinal: int) -> str:
    """Return a deterministic UUIDv5 for a seeded entity ordinal."""

    return str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"marketpulse-{seed}:{entity}:{ordinal}")
    )


def stable_int(seed: int, *parts: object, modulo: int) -> int:
    """Derive a deterministic integer without relying on PRNG implementation details."""

    if modulo <= 0:
        raise ValueError("modulo must be positive")
    material = ":".join([str(seed), *(str(part) for part in parts)]).encode()
    return int.from_bytes(hashlib.sha256(material).digest()[:8], "big") % modulo

