import json
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from marketpulse_app.live_view import LiveViewService, STAGES
from marketpulse_batch.publication import NoPublishedRelease

ANCHOR = datetime(2026, 9, 3, 20, 15, 30, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 31, 9, 0, 0, tzinfo=timezone.utc)


class Session:
    def __init__(self, *, present=1, eligible=12, money=None, labels=True, release=True, products=None):
        self.calls = []
        self.present, self.eligible, self.labels, self.has_release = present, eligible, labels, release
        self.money = money if money is not None else [{'currency': 'VND', 'orders': 3, 'gross': 900, 'refunds': 100}]
        self.products = products if products is not None else [
            {'product_id': 'p1', 'units': 4, 'carts': 2, 'views': 9}, {'product_id': 'p9', 'units': 0, 'carts': 1, 'views': 3}]
        self.release = SimpleNamespace(database='mp_release_a_1')

    def pin(self):
        if not self.has_release:
            raise NoPublishedRelease('none')
        return self

    def _query(self, sql, parameters=None):
        self.calls.append((sql, parameters or {}))
        if "name='events'" in sql: return [{'present': self.present}]
        if "name='commerce_daily'" in sql: return [{'present': 1 if self.labels else 0}]
        if 'AS anchor' in sql: return [{'anchor': ANCHOR if self.eligible else None, 'eligible': self.eligible}]
        if 'AS last_received' in sql: return [{'rows': 40, 'last_received': NOW - timedelta(seconds=12)}]
        if 'event_source_identity' in sql: return [{'identity': 'cluster/topic'}]
        if 'AS from_offset' in sql: return [{'partition': 0, 'from_offset': 0, 'to_offset': 40}]
        if 'GROUP BY source' in sql: return [{'source': 'web', 'visitors': 2}, {'source': 'mobile', 'visitors': 1}]
        if 'GROUP BY device_type' in sql: return [{'device_type': 'mobile', 'visitors': 2}, {'device_type': 'desktop', 'visitors': 1}]
        if 'AS active_visitors' in sql: return [{'active_visitors': 3}]
        if 'AS stage' in sql: return [{'stage': 1, 'sessions': 1}, {'stage': 3, 'sessions': 2}, {'stage': 5, 'sessions': 1}]
        if 'AS purchased' in sql: return [{'visitors': 20, 'sessions': 25, 'purchased': 3}]
        if 'GROUP BY currency' in sql: return self.money
        if 'AS minute' in sql: return [{'minute': ANCHOR.replace(second=0), 'visitors': 2, 'orders': 1},
                                       {'minute': ANCHOR.replace(second=0) - timedelta(minutes=5), 'visitors': 1, 'orders': 0}]
        if 'ARRAY JOIN' in sql: return self.products
        if 'product_name' in sql: return [{'product_id': 'p1', 'name': 'Linen jacket', 'category': 'fashion'}]
        if 'LIMIT 12' in sql: return [
            {'event_time': ANCHOR, 'event_name': 'order_created', 'source': 'commerce', 'device_type': 'unknown', 'product_id': None,
             'items': [('p1', 3)], 'currency': 'VND', 'gross_amount_minor': 300, 'refund_amount_minor': 0},
            {'event_time': ANCHOR - timedelta(minutes=1), 'event_name': 'add_to_cart', 'source': 'mobile', 'device_type': 'mobile',
             'product_id': 'p9', 'items': [], 'currency': None, 'gross_amount_minor': 0, 'refund_amount_minor': 0}]
        return []


def view(**kwargs):
    session = Session(**kwargs)
    return LiveViewService(session, clock=lambda: NOW).view(), session


class LiveViewTests(unittest.TestCase):
    def test_missing_projection_or_no_eligible_events_is_empty(self):
        for kwargs in ({'present': 0}, {'eligible': 0}):
            with self.subTest(**kwargs):
                result, _ = view(**kwargs)
                self.assertEqual(result['status'], 'empty')
                self.assertIn('message', result)

    def test_every_event_query_reads_final_eligible_rows_and_never_wall_clock(self):
        result, session = view()
        self.assertEqual(result['status'], 'available')
        self.assertEqual(result['anchor'], ANCHOR.isoformat())
        event_sql = [sql for sql, _ in session.calls if 'marketpulse_live.events' in sql and 'system.tables' not in sql]
        for sql in event_sql:
            self.assertIn('FINAL', sql)
        eligible = [sql for sql in event_sql if 'AS last_received' not in sql and 'AS from_offset' not in sql]
        for sql in eligible:
            self.assertIn('eligible_for_aggregates AND consent_analytics', sql)
            self.assertIn('is_bot', sql)
            self.assertNotIn('now()', sql)
        window = next(params for sql, params in session.calls if 'AS active_visitors' in sql)
        self.assertEqual((window['anchor'], window['start']), (ANCHOR, ANCHOR - timedelta(minutes=30)))

    def test_active_visitors_and_behaviour_buckets(self):
        result, _ = view()
        self.assertEqual(result['window_minutes'], '30')
        self.assertEqual(result['active']['visitors'], '3')
        self.assertEqual(result['active']['by_source'], [{'source': 'mobile', 'visitors': '1'}, {'source': 'web', 'visitors': '2'}])
        self.assertEqual([row['device'] for row in result['active']['by_device']], ['desktop', 'mobile', 'tablet', 'unknown'])
        self.assertEqual([row['stage'] for row in result['behaviour']], list(STAGES))
        self.assertEqual([row['sessions'] for row in result['behaviour']], ['1', '0', '2', '0', '1'])

    def test_today_money_and_conversion(self):
        result, session = view()
        today = result['today']
        self.assertEqual((today['visitors'], today['sessions'], today['purchased_sessions'], today['conversion']), ('20', '25', '3', '0.120000'))
        self.assertEqual(today['money'], [{'currency': 'VND', 'orders': '3', 'gross_minor': '900', 'refunds_minor': '100', 'net_minor': '800'}])
        day = next(params for sql, params in session.calls if 'AS purchased' in sql)
        self.assertEqual(day['day_start'], datetime(2026, 9, 3, tzinfo=timezone.utc))
        empty, _ = view(money=[])
        self.assertEqual(empty['today']['money'], [])

    def test_series_has_thirty_zero_filled_minutes_ending_at_anchor(self):
        result, _ = view()
        series = result['series']
        self.assertEqual(len(series), 30)
        self.assertEqual(series[-1], {'minute': ANCHOR.replace(second=0).isoformat(), 'visitors': '2', 'orders': '1'})
        self.assertEqual(series[-6]['visitors'], '1')
        self.assertEqual(series[0], {'minute': (ANCHOR.replace(second=0) - timedelta(minutes=29)).isoformat(), 'visitors': '0', 'orders': '0'})

    def test_products_and_activity_use_labels_without_identifiers(self):
        result, _ = view()
        self.assertEqual(result['top_products'][0], {'label': 'Linen jacket', 'category': 'fashion', 'units': '4', 'carts': '2', 'views': '9'})
        self.assertEqual(result['top_products'][1]['label'], 'Unlisted product')
        first, second = result['activity']
        self.assertEqual((first['kind'], first['label'], first['amount_minor'], first['currency']), ('order_placed', 'Linen jacket', '300', 'VND'))
        self.assertEqual((second['kind'], second['device'], second['label'], second['amount_minor']), ('added_to_cart', 'mobile', 'Unlisted product', None))
        text = json.dumps(result)
        for forbidden in ('identity_key', 'session_id', 'event_id', 'p1', 'p9', 'order_id'):
            self.assertNotIn(forbidden, text)

    def test_labels_are_optional_without_a_release(self):
        for kwargs in ({'release': False}, {'labels': False}):
            with self.subTest(**kwargs):
                result, _ = view(**kwargs)
                self.assertEqual({row['label'] for row in result['top_products']}, {'Unlisted product'})

    def test_freshness_and_provenance(self):
        result, _ = view()
        self.assertEqual(result['freshness'], {'anchor': ANCHOR.isoformat(), 'last_received_at': (NOW - timedelta(seconds=12)).isoformat(),
                                               'seconds_since_receipt': '12', 'receiving': True})
        self.assertEqual(result['provenance'], {'source': 'marketing.clean.customer-events.v1', 'source_identity': 'cluster/topic', 'rows': '40',
                                                'offset_bounds': [{'partition': '0', 'from_offset': '0', 'to_offset': '40'}],
                                                'note': 'Provisional · not certified'})

    def test_zero_sessions_has_no_conversion(self):
        class Quiet(Session):
            def _query(self, sql, parameters=None):
                if 'AS purchased' in sql:
                    self.calls.append((sql, parameters or {}))
                    return [{'visitors': 0, 'sessions': 0, 'purchased': 0}]
                return super()._query(sql, parameters)
        result = LiveViewService(Quiet(), clock=lambda: NOW).view()
        self.assertIsNone(result['today']['conversion'])


if __name__ == '__main__':
    unittest.main()
