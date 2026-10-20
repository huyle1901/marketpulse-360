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


class DailyClient(RecordingClient):
    def __init__(self, rows):
        super().__init__()
        self.rows = rows

    def query(self, sql, **kwargs):
        if '.campaigns ' in sql and 'JOIN' not in sql:
            return super().query(sql, **kwargs)
        self.calls.append((sql, kwargs))
        parameters = kwargs.get('parameters', {})
        rows = self.rows
        if 'from_date' in parameters:
            rows = [r for r in rows if parameters['from_date'] <= r['event_date'] < parameters['to_date']]
        for key in ('campaign_id', 'currency', 'platform', 'objective'):
            if key in parameters:
                rows = [r for r in rows if r[key] == parameters[key]]
        if 'market_campaigns' in parameters:
            rows = [r for r in rows if r['campaign_id'] in parameters['market_campaigns']]
        columns = list(self.rows[0]) if self.rows else ['event_date']
        return SimpleNamespace(column_names=columns, result_rows=[tuple(r[c] for c in columns) for r in rows])


def observed(day, campaign='a', currency='VND', spend=3, net=7):
    return dict(event_date=date.fromisoformat(day), campaign_id=campaign, currency=currency,
                spend_minor=spend, net_revenue_minor=net, gross_revenue_minor=max(0, net),
                refund_amount_minor=max(0, -net), orders=1, clicks=2, impressions=10,
                campaign_name='Campaign '+campaign, platform='google', objective='traffic')


class DashboardSeriesTests(unittest.TestCase):
    def dashboard(self, rows, filters):
        client = DailyClient(rows)
        release = PublicationReservation(1, 'a'*64, 'mp_release_'+'a'*64+'_1', date(2026,1,4), 'b'*64)
        return Analytics(client, None, release=release).dashboard(filters, []), client

    def test_daily_series_preserves_huge_negative_values_and_separates_currencies(self):
        rows = [observed('2026-01-03', net=9007199254740993), observed('2026-01-03','b',net=-3),
                observed('2026-01-04',net=-9007199254740993), observed('2026-01-04','c','USD',net=125)]
        result, client = self.dashboard(rows, DashboardFilter(date(2026,1,3),date(2026,1,5)))
        series = result['certified']['daily_series']
        self.assertEqual([(r['event_date'],r['currency'],r['net_revenue_minor'],r['spend_minor']) for r in series],
                         [('2026-01-03','VND','9007199254740990','6'),
                          ('2026-01-04','USD','125','3'),('2026-01-04','VND','-9007199254740993','3')])
        self.assertTrue(all('mp_release_'+'a'*64+'_1' in sql for sql, _ in client.calls))

    def test_equal_length_previous_period_uses_all_filters_and_complete_source_coverage(self):
        rows = [observed('2026-01-01',net=5), observed('2026-01-02','b',net=9),
                observed('2026-01-03',net=7), observed('2026-01-04',net=11)]
        result, client = self.dashboard(rows, DashboardFilter(date(2026,1,3),date(2026,1,5),campaign_id='a'))
        comparison = result['certified']['comparison']
        self.assertEqual((comparison['from_date'],comparison['to_date'],comparison['status']), ('2026-01-01','2026-01-03','complete'))
        self.assertEqual((comparison['observed_days'],comparison['expected_days']),('2','2'))
        self.assertEqual(comparison['totals'][0]['net_revenue_minor'],'5')
        self.assertEqual(result['certified']['totals'][0]['net_revenue_minor'],'18')
        prior = [kw['parameters'] for _,kw in client.calls if kw.get('parameters',{}).get('from_date')==date(2026,1,1)]
        self.assertEqual(prior[0]['campaign_id'],'a')

    def test_sparse_and_absent_previous_periods_are_not_complete_zero_baselines(self):
        filters = DashboardFilter(date(2026,1,3),date(2026,1,5))
        partial, _ = self.dashboard([observed('2026-01-01'),observed('2026-01-03')], filters)
        self.assertEqual(partial['certified']['comparison']['status'],'partial')
        self.assertEqual(partial['certified']['comparison']['observed_days'],'1')
        missing, _ = self.dashboard([observed('2026-01-03')], filters)
        self.assertEqual(missing['certified']['comparison']['status'],'unavailable')
        self.assertEqual(missing['certified']['comparison']['totals'],[])
        empty, _ = self.dashboard([], filters)
        self.assertEqual(empty['certified']['daily_series'],[])
        self.assertEqual(empty['certified']['comparison']['status'],'unavailable')

    def test_earliest_supported_interval_keeps_current_data_without_invalid_prior_query(self):
        result, client = self.dashboard([observed('1900-01-01')], DashboardFilter(date(1900,1,1),date(1900,1,2)))
        self.assertEqual(result['certified']['totals'][0]['spend_minor'],'3')
        self.assertEqual(result['certified']['comparison']['status'],'unavailable')
        self.assertFalse(any(kw.get('parameters',{}).get('from_date',date(1900,1,1))<date(1900,1,1) for _,kw in client.calls))

    def test_sparse_current_period_suppresses_comparison_with_complete_prior_period(self):
        rows = [observed('2026-01-01',net=100),observed('2026-01-02',net=100),observed('2026-01-03',net=100)]
        result, _ = self.dashboard(rows,DashboardFilter(date(2026,1,3),date(2026,1,5)))
        comparison=result['certified']['comparison']
        self.assertEqual(comparison['status'],'partial')
        self.assertEqual(comparison['previous_status'],'complete')
        self.assertEqual(comparison['current_status'],'partial')
        self.assertEqual((comparison['current_observed_days'],comparison['current_expected_days']),('1','2'))
