import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from marketpulse_app.insights import InsightsService
from marketpulse_app.lifecycle import LifecycleError

STAMP = datetime(2026, 8, 31, 12, tzinfo=timezone.utc)


class InsightsTests(unittest.TestCase):
    def service(self, *, missing=False, currency='VND', orders=10, loyalty=None, gold_net=100):
        calls = []
        tiers = loyalty if loyalty is not None else [
            {'tier': 'gold', 'customers': 2, 'orders': 3, 'net_revenue_minor': 60},
            {'tier': 'bronze', 'customers': 1, 'orders': 1, 'net_revenue_minor': 30},
            {'tier': 'none', 'customers': 4, 'orders': 2, 'net_revenue_minor': 10}]

        class Session:
            release = SimpleNamespace(database='mp_release_a_1', identity='a' * 64, proof_digest='b' * 64, logical_day=date(2026, 8, 31))
            client = SimpleNamespace(query=lambda *a, **kw: SimpleNamespace(result_rows=[(currency,)]))
            def pin(self): calls.append(('pin', {})); return self
            def provenance(self): return {'identity': self.release.identity, 'proof_digest': self.release.proof_digest, 'generation': '1'}
            def _query(self, sql, parameters=None):
                calls.append((sql, parameters or {}))
                if 'max(last_activity_at)' in sql: return [{'last_activity_at': STAMP, 'population': 1}]
                if 'AS mismatches' in sql: return [{'mismatches': 0, 'gold_events': 2}]
                if 'AS session_start' in sql: return [{'weekday': 3, 'hour': 10, 'sessions': 12}, {'weekday': 5, 'hour': 20, 'sessions': 7}]
                if 'AS order_hour' in sql: return [{'weekday': 3, 'hour': 10, 'orders': 2}, {'weekday': 7, 'hour': 23, 'orders': 1}]
                if 'quantilesExact' in sql: return [{'orders': orders, 'gross_minor': 9_000_000 if orders else 0, 'q': [1_250_000, 3_400_000, 3_900_000] if orders else []}]
                if 'AS band' in sql: return [{'band': 0, 'orders': 4, 'gross_minor': 1_000_000}, {'band': 7, 'orders': 6, 'gross_minor': 8_000_000}]
                if 'loyalty_tier' in sql: return tiers
                if 'AS gold_net' in sql: return [{'gold_net': gold_net}]
                if 'AS session_source' in sql: return [{'source': 'web', 'sessions': 100, 'orders': 5}, {'source': 'mobile', 'sessions': 50, 'orders': 4}, {'source': 'unknown', 'sessions': 0, 'orders': 1}]
                return []
        publication = SimpleNamespace(database='mp_journey_a_1', row_count=2, silver_snapshot_id=123, checksum='c' * 64, version='mp07-journeys-v1')
        journal = patch('marketpulse_app.lifecycle.JourneyJournal'); mock = journal.start(); self.addCleanup(journal.stop)
        mock.return_value.resolve.return_value = None if missing else publication
        return InsightsService(Session()), calls

    def test_heatmap_returns_168_cells_and_busiest_slot(self):
        service, calls = self.service()
        result = service.activity_heatmap()
        self.assertEqual(result['timezone'], 'UTC'); self.assertEqual(len(result['cells']), 168)
        cell = {(int(c['weekday']), int(c['hour'])): c for c in result['cells']}
        self.assertEqual((cell[(3, 10)]['sessions'], cell[(3, 10)]['orders']), ('12', '2'))
        self.assertEqual((cell[(3, 4)]['sessions'], cell[(3, 4)]['orders']), ('0', '0'))
        self.assertEqual(result['busiest'], {'weekday': '3', 'hour': '10', 'sessions': '12'})
        sessions_sql = next(sql for sql, _ in calls if 'AS session_start' in sql)
        self.assertIn('NOT is_bot_session', sessions_sql); self.assertIn("source IN ('web','mobile')", sessions_sql)
        self.assertIn('mp_journey_a_1.events', sessions_sql)
        self.assertIn("event_name='order_created'", next(sql for sql, _ in calls if 'AS order_hour' in sql))

    def test_order_values_bands_use_nice_width_from_p95(self):
        service, calls = self.service()
        result = service.order_values()
        self.assertEqual(result['currency'], 'VND'); self.assertEqual(result['band_width_minor'], '500000')
        self.assertEqual((result['median_minor'], result['p90_minor']), ('1250000', '3400000'))
        self.assertEqual(len(result['bands']), 8); self.assertIsNone(result['bands'][-1]['to_minor'])
        self.assertEqual((result['bands'][0]['from_minor'], result['bands'][0]['to_minor']), ('0', '500000'))
        self.assertEqual(result['bands'][-1]['from_minor'], '3500000'); self.assertEqual(result['bands'][3]['orders'], '0')
        self.assertEqual(sum(int(b['orders']) for b in result['bands']), int(result['orders']))
        self.assertEqual(next(p for sql, p in calls if 'AS band' in sql)['width'], 500000)

    def test_order_values_without_orders_is_empty_not_error(self):
        service, _ = self.service(orders=0)
        result = service.order_values()
        self.assertEqual(result['status'], 'empty'); self.assertEqual(result['bands'], [])
        self.assertIsNone(result['median_minor']); self.assertIsNone(result['band_width_minor'])

    def test_loyalty_keeps_unmatched_profiles_in_none_tier_and_reconciles(self):
        service, _ = self.service()
        result = service.loyalty()
        self.assertEqual([t['tier'] for t in result['tiers']], ['gold', 'silver', 'bronze', 'none'])
        silver = result['tiers'][1]; self.assertEqual((silver['customers'], silver['net_per_customer_minor']), ('0', None))
        self.assertEqual(result['tiers'][0]['net_per_customer_minor'], '30')
        self.assertEqual(result['total_net_revenue_minor'], '100')
        service, _ = self.service(gold_net=101)
        with self.assertRaises(LifecycleError) as error: service.loyalty()
        self.assertEqual(error.exception.code, 'reconciliation_failed')

    def test_channels_count_unknown_session_source_separately(self):
        service, calls = self.service()
        sources = {s['source']: s for s in service.channels()['sources']}
        self.assertEqual(sources['web']['conversion_rate'], '0.050000'); self.assertEqual(sources['unknown']['orders'], '1')
        self.assertIsNone(sources['unknown']['conversion_rate'])
        self.assertIn("'unknown'", next(sql for sql, _ in calls if 'AS session_source' in sql))
        # ClickHouse fills unmatched LEFT JOIN columns with '' unless NULLs are requested, which would lose unknown orders.
        self.assertIn('join_use_nulls=1', next(sql for sql, _ in calls if 'AS session_source' in sql))

    def test_mixed_currency_and_missing_projection(self):
        service, _ = self.service(currency=None)
        with patch('marketpulse_app.lifecycle.single_currency', side_effect=LifecycleError('unsupported_currency')):
            with self.assertRaises(LifecycleError): service.order_values()
        service, _ = self.service(missing=True)
        for read in (service.activity_heatmap, service.order_values, service.channels):
            result = read(); self.assertEqual(result['status'], 'unavailable'); self.assertNotIn('cells', {k for k, v in result.items() if v})


if __name__ == '__main__':
    unittest.main()


class InsightsRouteTests(unittest.TestCase):
    def test_insights_routes_are_viewer_aggregates(self):
        from app_auth_support import TrustedTestAccess
        from fastapi.testclient import TestClient
        from marketpulse_app.api import create_app
        from marketpulse_app.security import policy
        names = {'activity': 'activity_heatmap', 'order-values': 'order_values', 'loyalty': 'loyalty', 'channels': 'channels'}
        client = TestClient(create_app(SimpleNamespace(), SimpleNamespace(), access=TrustedTestAccess()))
        for path, method in names.items():
            self.assertEqual(policy('GET', '/api/v1/insights/' + path)[1], 'viewer')
            with patch('marketpulse_app.insights_api.InsightsService.' + method, return_value={'status': 'available'}):
                response = client.get('/api/v1/insights/' + path)
            self.assertEqual(response.status_code, 200); self.assertEqual(response.headers['cache-control'], 'no-store')
        with patch('marketpulse_app.insights_api.InsightsService.order_values', side_effect=LifecycleError('unsupported_currency')):
            response = client.get('/api/v1/insights/order-values')
        self.assertEqual(response.status_code, 409); self.assertEqual(response.json()['code'], 'unsupported_currency')


class LoyaltyPrecisionTests(InsightsTests):
    def test_net_per_customer_uses_exact_integer_division(self):
        tiers = [{'tier': 'gold', 'customers': 1, 'orders': 1, 'net_revenue_minor': 9007199254740993},
                 {'tier': 'none', 'customers': 3, 'orders': 1, 'net_revenue_minor': -7}]
        service, _ = self.service(loyalty=tiers, gold_net=9007199254740986)
        result = {t['tier']: t for t in service.loyalty()['tiers']}
        self.assertEqual(result['gold']['net_per_customer_minor'], '9007199254740993')
        self.assertEqual(result['none']['net_per_customer_minor'], '-2')
