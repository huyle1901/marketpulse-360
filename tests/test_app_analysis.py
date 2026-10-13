import copy
import unittest
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from marketpulse_app.analysis import AnalysisError, AnalysisService, summarize_attribution, summarize_funnel
from marketpulse_app.analytics import Analytics
from marketpulse_app.contracts import DashboardFilter
from marketpulse_batch.ledger import PublicationReservation

DIMENSIONS=[{'campaign_id':'a','platform':'google','name':'A','currency':'VND','objective':'traffic'},
            {'campaign_id':'b','platform':'meta','name':'B','currency':'VND','objective':'conversions'}]


def attribution_rows():
    return [dict(model=model,event_date=date(2026,1,2),campaign_id='a',is_attributed=True,
                 allocated_order_count=Decimal('1.000000'),allocated_gross_minor=9007199254740993,
                 allocated_refund_minor=3,allocated_net_minor=9007199254740990)
            for model in ('first_touch','last_touch','linear')]


class AnalysisMathTests(unittest.TestCase):
    def test_stage_reach_incidence_not_sequential_or_unique_audience(self):
        rows=[dict(event_date=date(2026,1,1),campaign_id='a',sessions=1,page_view_sessions=0,
                   view_item_sessions=0,add_to_cart_sessions=1,checkout_sessions=0,purchase_sessions=1),
              dict(event_date=date(2026,1,2),campaign_id='b',sessions=1,page_view_sessions=1,
                   view_item_sessions=0,add_to_cart_sessions=0,checkout_sessions=0,purchase_sessions=0)]
        result=summarize_funnel(rows,DIMENSIONS)
        self.assertEqual(result['totals']['sessions'],'2')
        self.assertEqual(result['totals']['add_to_cart_sessions'],'1')
        self.assertEqual(result['unit'],'campaign-day session incidences')
        self.assertEqual(result['reach_ratios']['purchase_sessions'],'0.500000')
        self.assertIsNone(summarize_funnel([],DIMENSIONS)['reach_ratios']['purchase_sessions'])

    def test_exact_three_model_conservation_and_corruption(self):
        baseline={'gross_revenue_minor':9007199254740993,'refund_amount_minor':3,'net_revenue_minor':9007199254740990}
        result=summarize_attribution(attribution_rows(),baseline,DIMENSIONS)
        self.assertTrue(result['reconciliation']['balanced'])
        self.assertEqual(result['models'][0]['net_revenue_minor'],'9007199254740990')
        self.assertEqual(result['models'][0]['allocated_order_count'],'1.000000')
        bad=attribution_rows();bad[0]['allocated_net_minor']+=1
        with self.assertRaises(AnalysisError):summarize_attribution(bad,baseline,DIMENSIONS)
        with self.assertRaises(AnalysisError):summarize_attribution(attribution_rows()[:-1],baseline,DIMENSIONS)


class AnalysisQueryTests(unittest.TestCase):
    def session(self,mixed=False):
        calls=[]
        class Client:
            def query(self,sql,**kwargs):
                calls.append((sql,kwargs))
                if 'UNION DISTINCT' in sql:
                    return SimpleNamespace(result_rows=[('VND',),('USD',)] if mixed else [('VND',)])
                if '.campaigns ' in sql and 'JOIN' not in sql:
                    rows=DIMENSIONS
                elif 'sum(toInt128' in sql:
                    rows=[dict(gross_revenue_minor=9007199254740993,refund_amount_minor=3,net_revenue_minor=9007199254740990)]
                elif '.attribution_daily' in sql:rows=attribution_rows()
                else:rows=[]
                names=list(rows[0]) if rows else []
                return SimpleNamespace(column_names=names,result_rows=[tuple(row[name] for name in names) for row in rows])
        release=PublicationReservation(1,'a'*64,'mp_release_'+'a'*64+'_1',date(2026,1,2),'b'*64)
        return AnalysisService(Analytics(Client(),None,release=release)),calls

    def test_hostile_filters_are_parameters_and_global_reconciliation_ignores_them(self):
        service,calls=self.session()
        request=DashboardFilter(date(2026,1,1),date(2026,1,3),campaign_id="a' OR 1=1 --")
        result=service.attribution(request,[])
        self.assertTrue(result['global_reconciliation']['balanced'])
        filtered=[(sql,kw) for sql,kw in calls if kw.get('parameters',{}).get('campaign_id')]
        self.assertTrue(filtered)
        for sql,kw in filtered:
            self.assertNotIn(request.campaign_id,sql)
            self.assertEqual(kw['parameters']['campaign_id'],request.campaign_id)
        self.assertTrue(all('mp_release_'+'a'*64+'_1' in sql for sql,kw in calls))

    def test_mixed_currency_release_is_rejected(self):
        service,_=self.session(mixed=True)
        with self.assertRaises(AnalysisError) as error:
            service.attribution(DashboardFilter(date(2026,1,1),date(2026,1,3)),[])
        self.assertEqual(error.exception.code,'unsupported_currency')

class AnalysisProvenanceTests(unittest.TestCase):
    def test_actual_maximum_day_is_exposed_even_for_historical_data(self):
        from marketpulse_app.analysis import provenance
        session=SimpleNamespace(provenance=lambda:{'identity':'a'*64})
        result=provenance(session,'funnel_daily',[{'event_date':date(2026,1,2)}])
        self.assertEqual(result['max_event_date'],'2026-01-02')
        self.assertEqual(result['data_through'],'2026-01-03T00:00:00+00:00')
