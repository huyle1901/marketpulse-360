import math
import unittest
from collections import Counter, defaultdict

from marketpulse_eval.contracts import digest, percentiles, verify_outputs
from marketpulse_eval.workload import events, oracle
from marketpulse_eval.analytics import analytical_oracle, fact
from marketpulse_eval.contracts import verify_recovery


class EvaluationTests(unittest.TestCase):
    def test_determinism_and_all_anomalies(self):
        rows = events(10_000)
        self.assertEqual(rows, events(10_000))
        self.assertNotEqual(digest(rows), digest(events(10_000, 361)))
        counts = Counter(r['classification'] for r in oracle(rows))
        self.assertEqual(set(counts), {'accepted', 'duplicate', 'invalid', 'late', 'too_late'})
        self.assertEqual(sum(counts.values()), 10_000)
        self.assertEqual(verify_outputs(rows, oracle(rows))['count'], 10_000)

    def test_boundary_late_dedup_and_invalid_boolean(self):
        row = {'seq': 0, 'event_id': 'a', 'campaign': 0, 'event_ms': 2_000_000,
               'kind': 'order', 'amount_minor': 100, 'sent_ms': 0}
        rows = [row, row | {'seq': 1, 'amount_minor': 999},
                row | {'seq': 2, 'event_id': 'b', 'event_ms': 1_700_000},
                row | {'seq': 3, 'event_id': 'c', 'event_ms': 1_100_000, 'kind': 'refund', 'amount_minor': 30},
                row | {'seq': 4, 'event_id': 'd', 'event_ms': 1_099_999},
                row | {'seq': 5, 'event_id': 'e', 'amount_minor': True}]
        out = oracle(rows)
        self.assertEqual([r['classification'] for r in out],
                         ['accepted', 'duplicate', 'accepted', 'late', 'too_late', 'invalid'])
        self.assertEqual((out[-1]['gross_minor'], out[-1]['refund_minor'], out[-1]['net_minor']), (200, 30, 170))
        self.assertEqual(oracle([row | {'amount_minor': None}])[0]['classification'], 'invalid')

    def test_exact_outputs_not_just_final_money(self):
        rows = events(100)
        out = oracle(rows)
        for wrong in (out[:-1], out + [out[0]], [out[0]] * len(out),
                      [out[0] | {'net_minor': 7}] + out[1:]):
            with self.assertRaises(ValueError):
                verify_outputs(rows, wrong)

    def test_statistics_reject_invalid_samples(self):
        self.assertEqual(percentiles(list(range(1, 101))), {'p50_ms': 50, 'p95_ms': 95, 'p99_ms': 99, 'samples': 100})
        for values in ([], [-1], [math.nan], [math.inf], [True]):
            with self.assertRaises(ValueError):
                percentiles(values)

    def test_periodic_analytical_oracle_against_rows(self):
        for count in (1, 100, 100_003):
            for workload in ('platform', 'campaign_day', 'filtered'):
                groups = defaultdict(lambda: [0, 0])
                for i in range(count):
                    campaign, day, platform, amount = fact(i)
                    if workload == 'filtered' and not (campaign == 7 and 5 <= day < 20):
                        continue
                    key = (platform,) if workload == 'platform' else (campaign, day)
                    groups[key][0] += 1
                    groups[key][1] += amount
                expected = [(*k, *v) for k, v in sorted(groups.items())]
                self.assertEqual(analytical_oracle(count, workload), expected)

    def test_workload_rejects_unsupported_counts(self):
        for count in (0, -1, True, 1_000_001):
            with self.assertRaises(ValueError):
                events(count)

    def test_recovery_requires_progress_and_checkpoint(self):
        proof = dict(before_outputs=10, after_outputs=20, input_before=40,
                     input_after=60, producer_active=True, seconds=3)
        for engine in ('flink', 'streams'):
            with self.assertRaises(ValueError):
                verify_recovery(proof, engine)
        verify_recovery(proof | {'completed_checkpoint': 1, 'restored_checkpoint': 1}, 'flink')
        verify_recovery(proof | {'same_application_id': True}, 'streams')
        with self.assertRaises(ValueError):
            verify_recovery(proof | {'input_after': 40, 'same_application_id': True}, 'streams')
