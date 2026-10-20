"""Periodic independent answers for Gold-shaped integer scale fixtures."""
from __future__ import annotations
from collections import defaultdict
from functools import lru_cache
from math import lcm

ANALYTICS_SCALES = {'analytics-smoke': 10_000, 'analytics-1m': 1_000_000,
                    'analytics-10m': 10_000_000, 'analytics-20m': 20_000_000}
WORKLOADS = ('platform', 'campaign_day', 'filtered')


def fact(row_id: int) -> tuple[int, int, int, int]:
    return row_id % 64, row_id % 30, row_id % 3, (row_id % 97 + 1) * 100


@lru_cache(maxsize=24)
def analytical_oracle(count: int, workload: str) -> list[tuple]:
    if type(count) is not int or count < 1 or workload not in WORKLOADS:
        raise ValueError('invalid analytical workload')
    period = lcm(64, 30, 3, 97)
    cycles, remainder = divmod(count, period)
    groups = defaultdict(lambda: [0, 0])
    for i in range(min(count, period)):
        campaign, day, platform, amount = fact(i)
        multiplier = cycles + (i < remainder)
        if not multiplier or workload == 'filtered' and not (campaign == 7 and 5 <= day < 20):
            continue
        key = (platform,) if workload == 'platform' else (campaign, day)
        groups[key][0] += multiplier
        groups[key][1] += amount * multiplier
    return [(*key, *value) for key, value in sorted(groups.items())]
