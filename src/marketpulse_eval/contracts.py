"""Strict evidence checks and explicitly scoped statistics."""
from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from typing import Any


def digest(rows: list[dict[str, Any]]) -> str:
    canonical = [{k: v for k, v in row.items() if k != 'sent_ms'} for row in rows]
    return hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def percentiles(values: list[float]) -> dict[str, float | int]:
    if not values or any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in values):
        raise ValueError('timing samples must be nonempty, finite and nonnegative')
    ordered = sorted(values)
    return {**{f'p{p}_ms': ordered[math.ceil(p / 100 * len(values)) - 1] for p in (50, 95, 99)},
            'samples': len(values)}


def verify_outputs(inputs: list[dict], outputs: list[dict]) -> dict[str, Any]:
    from .workload import oracle
    if len(outputs) != len(inputs) or len({r['seq'] for r in outputs}) != len(inputs):
        raise ValueError('missing or duplicate committed output')
    ordered = sorted(outputs, key=lambda row: row['seq'])
    expected = oracle(inputs)
    if ordered != expected:
        raise ValueError('committed output differs from independent oracle')
    return {'count': len(inputs), 'input_hash': digest(inputs), 'output_hash': digest(ordered),
            'classifications': dict(Counter(r['classification'] for r in ordered)), 'passed': True}


def verify_recovery(proof: dict[str, Any], engine: str) -> None:
    if (proof.get('before_outputs', 0) <= 0 or proof.get('after_outputs', 0) <= proof['before_outputs']
            or proof.get('input_after', 0) <= proof.get('input_before', 0)
            or not proof.get('producer_active') or proof.get('seconds', -1) < 0):
        raise ValueError('insufficient recovery progress evidence')
    if engine == 'flink' and not (proof.get('completed_checkpoint') and proof.get('restored_checkpoint')):
        raise ValueError('missing Flink checkpoint recovery evidence')
    if engine == 'streams' and not proof.get('same_application_id'):
        raise ValueError('missing Streams application identity')
