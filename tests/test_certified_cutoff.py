"""A certified release for logical day D must not contain business events after D."""
import unittest
from datetime import date

from marketpulse_app.journey_publish import release_events_query
from marketpulse_batch.engine import gold_cutoff
from marketpulse_lakehouse.gold import build_gold
from marketpulse_lakehouse.contracts import TABLE_CONTRACTS


class CertifiedCutoffTests(unittest.TestCase):
    def test_engine_takes_the_cutoff_from_the_preparation_logical_day(self):
        self.assertEqual(gold_cutoff({'logical_day': '2026-08-31'}), date(2026, 8, 31))
        self.assertIsNone(gold_cutoff({}))
        with self.assertRaises(ValueError):
            gold_cutoff({'logical_day': '31/08/2026'})

    def test_gold_rejects_a_non_date_cutoff_before_spark(self):
        snapshots = {name: 1 for name in TABLE_CONTRACTS if name.startswith('silver.')}
        with self.assertRaisesRegex(ValueError, 'through_date'):
            build_gold(None, silver_snapshots=snapshots, run_id='r', through_date='2026-08-31')

    def test_journey_projection_reads_only_events_through_the_release_day(self):
        sql = release_events_query('mp_batch_x_1', 42, date(2026, 8, 31))
        self.assertIn('VERSION AS OF 42', sql)
        self.assertIn("event_date <= DATE '2026-08-31'", sql)
        self.assertIn('lakehouse.`mp_batch_x_1`.customer_events', sql)
        with self.assertRaises(ValueError):
            release_events_query('bad name; drop', 42, date(2026, 8, 31))


if __name__ == '__main__':
    unittest.main()
