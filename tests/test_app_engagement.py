import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from marketpulse_app.lifecycle import LifecycleService

STAMP = datetime(2026, 2, 2, 12, tzinfo=timezone.utc)
EMAIL = [dict(month=date(2026, 1, 1), sent=10, delivered=8, opened=4, clicked=1, unsubscribed=0, recipients=7),
         dict(month=date(2026, 2, 1), sent=5, delivered=0, opened=0, clicked=0, unsubscribed=0, recipients=5)]
MIX = [dict(month=date(2026, 1, 1), new_customers=3, returning_customers=0, new_orders=3, returning_orders=0,
            new_gross_minor=9007199254740993, returning_gross_minor=0),
       dict(month=date(2026, 2, 1), new_customers=1, returning_customers=2, new_orders=1, returning_orders=4,
            new_gross_minor=5, returning_gross_minor=7)]


class EngagementTests(unittest.TestCase):
    def service(self, missing=False):
        calls = []

        class Session:
            release = SimpleNamespace(database='mp_release_a_1', identity='a' * 64, proof_digest='b' * 64, logical_day=date(2026, 2, 2))
            client = SimpleNamespace(query=lambda *a, **kw: SimpleNamespace(result_rows=[('VND',)]))
            def pin(self): calls.append(('pin', {})); return self
            def provenance(self): return {'identity': self.release.identity, 'proof_digest': self.release.proof_digest, 'generation': '1'}
            def _query(self, sql, parameters=None):
                calls.append((sql, parameters or {}))
                if 'max(last_activity_at)' in sql: return [{'last_activity_at': STAMP, 'population': 1}]
                if 'AS mismatches' in sql: return [{'mismatches': 0, 'gold_events': 2}]
                if "event_name='email_sent'" in sql: return EMAIL
                if 'first_order' in sql: return MIX
                return []
        publication = SimpleNamespace(database='mp_journey_a_1', row_count=2, silver_snapshot_id=123, checksum='c' * 64, version='mp07-journeys-v1')
        journal = patch('marketpulse_app.lifecycle.JourneyJournal'); mock = journal.start(); self.addCleanup(journal.stop)
        mock.return_value.resolve.return_value = None if missing else publication
        return LifecycleService(Session()), calls

    def test_email_engagement_counts_months_from_the_pinned_projection_with_safe_rates(self):
        service, calls = self.service()
        result = service.email_engagement()
        self.assertEqual(result['status'], 'available')
        january, february = result['months']
        self.assertEqual((january['month'], january['sent'], january['delivered'], january['recipients']), ('2026-01-01', '10', '8', '7'))
        self.assertEqual((january['open_rate'], january['click_rate'], january['unsubscribe_rate']), ('0.500000', '0.125000', '0.000000'))
        self.assertIsNone(february['open_rate'])
        self.assertEqual(result['totals']['sent'], '15'); self.assertEqual(result['totals']['open_rate'], '0.500000')
        email_sql = [sql for sql, _ in calls if "event_name='email_sent'" in sql]
        self.assertEqual(len(email_sql), 1); self.assertIn('mp_journey_a_1.events', email_sql[0]); self.assertIn("source='email'", email_sql[0])
        self.assertEqual(len([c for c, _ in calls if c == 'pin']), 1)
        self.assertEqual(result['provenance']['projection_checksum'], 'c' * 64)

    def test_customer_mix_splits_first_order_month_from_returning_orders(self):
        service, calls = self.service()
        result = service.customer_mix()
        self.assertEqual(result['currency'], 'VND')
        self.assertEqual(result['months'][0]['new_gross_minor'], '9007199254740993')
        self.assertEqual(result['months'][1]['returning_customers'], '2')
        self.assertEqual(result['months'][1]['returning_share'], '0.666666')
        sql = next(sql for sql, _ in calls if 'first_order' in sql)
        self.assertIn("event_name='order_created'", sql); self.assertIn('mp_journey_a_1.events', sql)

    def test_missing_projection_is_unavailable_not_zero(self):
        service, _ = self.service(missing=True)
        for result in (service.email_engagement(), service.customer_mix()):
            self.assertEqual(result['status'], 'unavailable'); self.assertEqual(result['months'], [])
            self.assertNotIn('totals', result)


if __name__ == '__main__':
    unittest.main()


class EngagementRouteTests(unittest.TestCase):
    def test_routes_are_viewer_aggregates_served_no_store(self):
        from app_auth_support import TrustedTestAccess
        from fastapi.testclient import TestClient
        from marketpulse_app.api import create_app
        from marketpulse_app.security import policy
        for path in ('/api/v1/lifecycle/email-engagement', '/api/v1/lifecycle/customer-mix'):
            self.assertEqual(policy('GET', path)[1], 'viewer')
        client = TestClient(create_app(SimpleNamespace(), SimpleNamespace(), access=TrustedTestAccess()))
        with patch('marketpulse_app.lifecycle_api.LifecycleService.email_engagement', return_value={'status': 'empty', 'months': []}), \
                patch('marketpulse_app.lifecycle_api.LifecycleService.customer_mix', return_value={'status': 'empty', 'months': []}):
            for path in ('email-engagement', 'customer-mix'):
                response = client.get('/api/v1/lifecycle/' + path)
                self.assertEqual(response.status_code, 200); self.assertEqual(response.headers['cache-control'], 'no-store')
