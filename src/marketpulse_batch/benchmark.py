"""Client-inclusive serial benchmarks; correctness is separate from timing."""

from __future__ import annotations

import json
import math
import os
import platform
import time
from pathlib import Path
from datetime import UTC, date, datetime
from decimal import Decimal
from collections.abc import Mapping, Sequence
from typing import Any

from .queries import QueryRequest, run_query
from .ledger import PublicationReservation


def resource_metadata() -> dict[str, object]:
    result: dict[str, object] = {"visible_cpu_count": os.cpu_count() or 1, "platform": platform.platform()}
    for name, path in (("container_memory_limit_bytes", "/sys/fs/cgroup/memory.max"),
                       ("cpu_quota", "/sys/fs/cgroup/cpu.max")):
        try:
            value = Path(path).read_text().strip()
            result[name] = int(value) if value.isdigit() else value
        except OSError:
            result[name] = None
    return result


def nearest_rank(values: Sequence[float], percentile: float) -> float:
    if not values or not 0 < percentile <= 1:
        raise ValueError("percentiles require nonempty samples and 0<p<=1")
    return sorted(values)[math.ceil(percentile * len(values)) - 1]


def normalized_rows(rows: list[dict[str, object]]) -> str:
    def encode(value):
        if isinstance(value, datetime):
            return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, Decimal):
            return format(value, "f")
        raise TypeError("unsupported benchmark answer type")
    return json.dumps(rows, sort_keys=True, default=encode, separators=(",", ":"))


def benchmark_queries(client: Any, release: PublicationReservation, requests: Sequence[QueryRequest], *,
                       warmups: int = 20, samples: int = 100,
                       expected: Mapping[str, list[dict[str, object]]] | None = None) -> dict[str, object]:
    if type(warmups) is not int or warmups < 0 or type(samples) is not int or samples < 1 or not requests:
        raise ValueError("invalid benchmark sample configuration")
    if len({request.workload for request in requests}) != len(requests):
        raise ValueError("benchmark workloads must be distinct")
    results = {}
    for request in requests:
        if expected is not None and request.workload not in expected:
            raise ValueError("missing independent workload oracle")
        for _ in range(warmups):
            run_query(client, release, request)
        timings = []
        failures = 0
        row_count = 0
        for _ in range(samples):
            started = time.perf_counter()
            try:
                rows = run_query(client, release, request)
                elapsed = (time.perf_counter() - started) * 1000
                if expected is not None and normalized_rows(rows) != normalized_rows(expected[request.workload]):
                    raise ValueError("query answer differs from independent oracle")
                row_count = len(rows)
                timings.append(elapsed)
            except Exception:
                failures += 1
        results[request.workload] = {"warmups": warmups, "samples": samples, "successful_samples": len(timings),
                                     "failures": failures, "row_count": row_count,
                                     "p50_ms": nearest_rank(timings, 0.50) if timings else None,
                                     "p95_ms": nearest_rank(timings, 0.95) if timings else None,
                                     "max_ms": max(timings) if timings else None,
                                     "correctness_checked": expected is not None}
    return {"version": 1, "generation": release.generation, "identity": release.identity, "database": release.database,
            "proof_digest": release.proof_digest, "query_cache": "disabled", "timing_scope": "client_request_and_decode",
            "percentile_method": "nearest_rank", "python": platform.python_version(),
            "clickhouse_version": client.query("SELECT version()").first_row[0],
            "resources": resource_metadata(), "workloads": results}
