import unittest
from datetime import date,datetime,timezone

from marketpulse_app.contracts import DashboardFilter
from marketpulse_app.journeys import explain_order, validate_sampling


def row(event_id,name,day,campaign='a',click=None,order='order-a',gross=None,refund=None):
    return dict(event_id=event_id,event_name=name,event_time=datetime(2026,1,day,tzinfo=timezone.utc),event_date=date(2026,1,day),
                source='commerce' if order else 'web',profile_ref='p',session_id='s',click_id=click,order_id=order,
                campaign_id=campaign,currency='VND' if order else None,gross_amount_minor=gross,refund_amount_minor=refund,is_bot_session=False)


class JourneyExplanationTests(unittest.TestCase):
    def test_refund_period_reuses_earlier_purchase_and_original_paid_touch(self):
        finance=[row('sale','order_created',2,gross=100),row('refund','order_refunded',5,gross=100,refund=30)]
        touch=row('touch','page_view',1,campaign='b',click='click-b',order=None)
        result=explain_order(finance,[touch],'a'*64,DashboardFilter(date(2026,1,5),date(2026,1,6)))
        self.assertEqual(result['purchase_time'],'2026-01-02T00:00:00+00:00')
        self.assertTrue(result['touches'][0]['outside_report_interval'])
        self.assertTrue(all(item['allocated_gross_minor']=='0' and item['allocated_net_minor']=='-30' for item in result['allocations']))
        self.assertEqual(result['allocations'][0]['campaign_id'],'b')
        self.assertNotIn('profile_ref',str(result))
        self.assertNotIn('order-a',str(result))
        self.assertEqual(result['financial_events'][0]['gross_revenue_minor'],'0')

    def test_context_limit_never_calculates_partial_credit(self):
        finance=[row('sale','order_created',2,gross=100)]
        result=explain_order(finance,[row('t','page_view',1,click='c',order=None)]*501,'a'*64,DashboardFilter(date(2026,1,1),date(2026,1,3)))
        self.assertEqual(result['status'],'context_too_large')
        self.assertEqual(result['allocations'],[])
        for model,limit in (('unknown',5),('linear',0),('linear',11),('linear',True)):
            with self.assertRaises(ValueError):validate_sampling(model,limit)

class JourneySelectionTests(unittest.TestCase):
    def test_credited_campaign_filters_do_not_follow_source_order_campaign(self):
        from marketpulse_app.journeys import credited_campaign_matches
        dimensions={'a':{'platform':'google','objective':'traffic'},'b':{'platform':'meta','objective':'conversions'}}
        filters=DashboardFilter(date(2026,1,1),date(2026,1,3),platform='meta',campaign_id='b')
        self.assertTrue(credited_campaign_matches('b',filters,dimensions,[]))
        self.assertFalse(credited_campaign_matches('a',filters,dimensions,[]))

    def test_missing_and_conflicting_projection_states(self):
        from unittest.mock import patch
        from marketpulse_app.journeys import JourneyService
        from marketpulse_app.analysis import AnalysisError
        from types import SimpleNamespace
        session=SimpleNamespace(pin=lambda:session,client=object(),provenance=lambda:{'identity':'a'*64},release=object())
        filters=DashboardFilter(date(2026,1,1),date(2026,1,3))
        with patch('marketpulse_app.journeys.single_currency',return_value='VND'),patch('marketpulse_app.journeys.JourneyJournal') as journal:
            journal.return_value.resolve.return_value=None
            result=JourneyService(session).sample(filters,[])
            self.assertEqual(result['status'],'unavailable')
            self.assertEqual(result['rows'],[])
            journal.return_value.resolve.side_effect=ValueError('different proof')
            with self.assertRaises(AnalysisError) as error:JourneyService(session).sample(filters,[])
            self.assertEqual(error.exception.code,'projection_mismatch')
