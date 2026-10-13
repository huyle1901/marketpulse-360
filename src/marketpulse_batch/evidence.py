"""Fail-closed validation of measured serving and restart evidence."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from .queries import WORKLOADS

STABLE_FIELDS = ("identity", "database", "generation", "proof_digest", "export_digest", "model_digest",
                 "source_snapshots", "source_row_counts", "financial_totals", "query_interval")


def validate_evidence(before: Mapping[str, Any], after: Mapping[str, Any] | None = None) -> None:
    for report in (before,) if after is None else (before, after):
        if report.get("accepted") is not True or report.get("query_cache") != "disabled" or report.get("percentile_method") != "nearest_rank":
            raise ValueError("missing accepted uncached benchmark evidence")
        if any(not report.get(field) for field in STABLE_FIELDS):
            raise ValueError("incomplete release evidence")
        workloads = report.get("workloads", {})
        if set(workloads) != set(WORKLOADS):
            raise ValueError("six measured workloads required")
        for name, result in workloads.items():
            p95 = result.get("p95_ms")
            if (result.get("warmups") != 20 or result.get("samples") != 100 or
                    result.get("successful_samples") != 100 or result.get("failures") != 0 or
                    result.get("correctness_checked") is not True or
                    not isinstance(p95, (int, float)) or not math.isfinite(p95) or not 0 <= p95 <= 500):
                raise ValueError(f"workload acceptance failed: {name}")
    if after is not None:
        for field in STABLE_FIELDS:
            if before[field] != after[field]:
                raise ValueError(f"restart changed {field}")
