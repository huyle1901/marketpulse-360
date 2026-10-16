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
    if engine not in ('flink', 'streams'):
        raise ValueError('unsupported recovery engine')
    duration = proof.get('seconds')
    if type(duration) not in (int, float) or not math.isfinite(duration) or duration < 0:
        raise ValueError('invalid recovery duration')
    for field in ('before_outputs','after_outputs','input_before','input_after'):
        if type(proof.get(field)) is not int or proof[field]<0:
            raise ValueError('recovery progress must use nonnegative integer counters')
    if (proof.get('before_outputs', 0) <= 0 or proof.get('after_outputs', 0) <= proof['before_outputs']
            or proof.get('input_after', 0) <= proof.get('input_before', 0)
            or proof.get('producer_active') is not True or proof['before_outputs']>proof['input_before']
            or proof['after_outputs']>proof['input_after']):
        raise ValueError('insufficient recovery progress evidence')
    if engine == 'flink':
        for field in ('completed_checkpoint','restored_checkpoint'):
            if type(proof.get(field)) is not int or proof[field]<1:
                raise ValueError('missing Flink checkpoint recovery identity')
        if proof['restored_checkpoint']<proof['completed_checkpoint']:
            raise ValueError('restored checkpoint predates observed completed checkpoint')
    if engine == 'streams' and proof.get('same_application_id') is not True:
        raise ValueError('missing Streams application identity')
