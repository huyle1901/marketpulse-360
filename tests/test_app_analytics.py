import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace

from marketpulse_app.analytics import Analytics
from marketpulse_app.contracts import CampaignSetting, DashboardFilter
from marketpulse_batch.ledger import PublicationReservation


class RecordingClient:
    def __init__(self):
        self.calls = []

    def query(self, sql, **kwargs):
        self.calls.append((sql, kwargs))
        if '.campaigns ' in sql and 'JOIN' not in sql:
            return SimpleNamespace(column_names=['campaign_id','currency','name','platform','objective'],
                result_rows=[('a','VND','Campaign A','google','traffic')])
        return SimpleNamespace(column_names=['event_date','campaign_id','currency','spend_minor',
            'impressions','clicks','orders','net_revenue_minor','gross_revenue_minor','refund_amount_minor',
            'campaign_name','platform','objective'], result_rows=[(date(2026,1,1),'a','VND',3,10,2,1,
            9007199254740993,9007199254740993,0,'Campaign A','google','traffic')])


class AnalyticsTests(unittest.TestCase):
    def session(self):
        client = RecordingClient()
        release = PublicationReservation(1,'a'*64,'mp_release_'+'a'*64+'_1',date(2026,1,2),'b'*64)
        return Analytics(client, None, release=release), client

    def test_parameterized_filters_pin_release_and_exact_totals(self):
        session, client = self.session()
        request = DashboardFilter(date(2026,1,1),date(2026,1,3),campaign_id="a' OR 1=1 --")
        session.daily(request)
        sql, kwargs = client.calls[-1]
        self.assertNotIn(request.campaign_id,sql)
        self.assertEqual(kwargs['parameters']['campaign_id'],request.campaign_id)
        self.assertIn('mp_release_'+'a'*64+'_1',sql)
        result = session.dashboard(DashboardFilter(date(2026,1,1),date(2026,1,3)), [])
        self.assertEqual(result['certified']['totals'][0]['net_revenue_minor'],'9007199254740993')
        self.assertEqual(result['certified']['provenance']['data_through'],'2026-01-02T00:00:00+00:00')
        self.assertTrue(result['certified']['freshness']['historical'])
        self.assertEqual(result['streaming']['status'],'unavailable')

    def test_configured_market_ids_bound_as_array(self):
        session, client = self.session()
        setting = CampaignSetting('a','VN','VND',date(2026,1,1),date(2026,1,3),100)
        session.daily(DashboardFilter(date(2026,1,1),date(2026,1,3),market='VN'), [setting])
        self.assertEqual(client.calls[-1][1]['parameters']['market_campaigns'], ['a'])
