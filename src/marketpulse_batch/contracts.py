"""Dependency-free, immutable batch input and snapshot contracts."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType

REPORT_PLATFORMS = ("google_ads", "meta_ads", "tiktok_ads")


def validate_digest(value: str, name: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


def snapshot_map(value: Mapping[str, int], layer: str) -> Mapping[str, int]:
    if not isinstance(value, Mapping) or not value:
        raise ValueError(f"{layer} snapshots must be nonempty")
    result = {}
    for key, snapshot in value.items():
        if not isinstance(key, str) or re.fullmatch(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", key) is None:
            raise ValueError("invalid snapshot table key")
        if type(snapshot) is not int or snapshot <= 0:
            raise ValueError("snapshot IDs must be positive integers")
        if layer == "bronze" and not key.startswith("bronze."):
            raise ValueError("input snapshots must be Bronze")
        if layer == "output" and not key.startswith(("silver.", "gold.")):
            raise ValueError("output snapshots must be Silver/Gold")
        result[key] = snapshot
    return MappingProxyType(dict(sorted(result.items())))


@dataclass(frozen=True, slots=True)
class BatchSelection:
    logical_day: date
    required_platforms: tuple[str, ...] = REPORT_PLATFORMS

    def __post_init__(self) -> None:
        if type(self.logical_day) is not date:
            raise ValueError("logical_day must be a UTC calendar date")
        platforms = tuple(self.required_platforms)
        if (not platforms or any(p not in REPORT_PLATFORMS for p in platforms)
                or len(platforms) != len(set(platforms))):
            raise ValueError("required_platforms must be distinct supported platforms")
        object.__setattr__(self, "required_platforms", tuple(sorted(platforms)))


@dataclass(frozen=True, slots=True)
class PreparationReceipt:
    identity: str
    logical_day: date
    model_digest: str
    bronze_snapshots: Mapping[str, int]
    output_snapshots: Mapping[str, int]
    namespace: str
    proof_digest: str
    required_platforms: tuple[str, ...] = REPORT_PLATFORMS

    def __post_init__(self) -> None:
        for name in ("identity", "model_digest", "proof_digest"):
            validate_digest(getattr(self, name), name)
        selection = BatchSelection(self.logical_day, self.required_platforms)
        object.__setattr__(self, "required_platforms", selection.required_platforms)
        object.__setattr__(self, "bronze_snapshots", snapshot_map(self.bronze_snapshots, "bronze"))
        object.__setattr__(self, "output_snapshots", snapshot_map(self.output_snapshots, "output"))
        if not isinstance(self.namespace, str) or re.fullmatch(
            rf"mp_batch_{self.identity}_[1-9][0-9]*", self.namespace
        ) is None:
            raise ValueError("namespace must belong to this preparation identity")
