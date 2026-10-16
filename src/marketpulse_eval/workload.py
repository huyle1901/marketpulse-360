"""Deterministic finance workload and independent sequential oracle."""
from __future__ import annotations
from typing import Any

STREAM_SCALES = {'eval-small': 10_000, 'eval-medium': 100_000, 'eval-large': 1_000_000}


def events(count: int, seed: int = 360) -> list[dict[str, Any]]:
    if type(count) is not int or not 1 <= count <= 1_000_000 or type(seed) is not int:
        raise ValueError('count must be 1..1000000 and seed must be an integer')
    result = []
    for i in range(count):
        row = {'seq': i, 'event_id': f'{seed}-{i}', 'campaign': i % 64,
               'event_ms': 1_767_225_600_000 + i * 1000, 'kind': 'refund' if i % 11 == 0 else 'order',
               'amount_minor': ((i + seed) % 97 + 1) * 100, 'sent_ms': 0}
        if i > 1024:
            if i % 997 == 0:
                row['event_id'] = f'{seed}-{i - 64}'
            elif i % 503 == 0:
                row['amount_minor'] = -1
            elif i % 509 == 0:
                row['event_ms'] -= 500_000
            elif i % 521 == 0:
                row['event_ms'] -= 2_000_000
        result.append(row)
    return result


def oracle(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[int, str]] = set()
    totals: dict[int, list[int]] = {}
    maxima: dict[int, int] = {}
    output = []
    for row in rows:
        campaign = row.get('campaign')
        valid = (type(campaign) is int and 0 <= campaign < 64
                 and isinstance(row.get('event_id'), str) and bool(row['event_id'])
                 and type(row.get('event_ms')) is int and row['event_ms'] >= 0
                 and row.get('kind') in ('order', 'refund')
                 and type(row.get('amount_minor')) is int and 0 <= row['amount_minor'] <= 1_000_000_000)
        gross, refund = totals.get(campaign, [0, 0])
        classification = 'invalid'
        if valid:
            key = (campaign, row['event_id'])
            if key in seen:
                classification = 'duplicate'
            else:
                seen.add(key)
                watermark = maxima.get(campaign, 0) - 300_000
                classification = ('too_late' if row['event_ms'] < watermark - 600_000 else
                                  'late' if row['event_ms'] < watermark else 'accepted')
                maxima[campaign] = max(maxima.get(campaign, 0), row['event_ms'])
                if classification != 'too_late':
                    gross += row['amount_minor'] if row['kind'] == 'order' else 0
                    refund += row['amount_minor'] if row['kind'] == 'refund' else 0
                    totals[campaign] = [gross, refund]
        output.append({'seq': row['seq'], 'campaign': campaign, 'classification': classification,
                       'gross_minor': gross, 'refund_minor': refund, 'net_minor': gross - refund,
                       'sent_ms': row['sent_ms']})
    return output
