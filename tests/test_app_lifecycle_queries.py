import unittest
from datetime import datetime,date,timezone
from types import SimpleNamespace
from unittest.mock import patch
from marketpulse_app.lifecycle import LifecycleService,LifecycleError
from marketpulse_app.lifecycle_contracts import SegmentRule

STAMP=datetime(2026,1,2,12,tzinfo=timezone.utc)
PROFILE=dict(profile_ref='d'*64,profile_type='customer',profile_key='customer:private',customer_id='private',anonymous_id=None,
 first_activity_at=STAMP,last_activity_at=STAMP,cohort_month=date(2025,12,1),event_count=2,session_count=1,order_count=1,refund_count=1,
 gross_revenue_minor=9007199254740993,refund_amount_minor=1,net_revenue_minor=9007199254740992,consent_analytics=True,
 crm_matched=True,crm_consent_analytics=True,crm_consent_marketing=True,last_purchase_at=datetime(2026,1,1,tzinfo=timezone.utc),
 add_to_cart_count=0,email_click_count=0)

class LifecycleQueryTests(unittest.TestCase):
    def service(self,profile=None,missing=False,conflict=False):
        calls=[];record=PROFILE if profile is None else profile
        class Session:
            release=SimpleNamespace(database='mp_release_a_1',identity='a'*64,proof_digest='b'*64,logical_day=date(2026,1,2))
            client=SimpleNamespace(query=lambda *a,**kw:SimpleNamespace(result_rows=[('VND',)]))
            def pin(self):calls.append(('pin',{}));return self
            def provenance(self):return {'identity':self.release.identity,'proof_digest':self.release.proof_digest,'generation':'1'}
            def _query(self,sql,parameters=None):
                calls.append((sql,parameters or {}))
                if 'max(last_activity_at)' in sql:return [{'last_activity_at':STAMP,'population':1}]
                if 'AS mismatches' in sql:return [{'mismatches':0,'gold_events':2}]
                if 'count() AS total' in sql:return [{'total':1}]
                if 'AS activation_eligible_total' in sql:return [{'profiles_total':1,'activation_eligible_total':1}]
                if '.cohort_retention' in sql:return []
                if 'FROM profiles' in sql:return [] if record=={} else [record]
                return []
        publication=SimpleNamespace(database='mp_journey_a_1',row_count=2,silver_snapshot_id=123,checksum='c'*64,version='mp07-journeys-v1')
        journal=patch('marketpulse_app.lifecycle.JourneyJournal');mock=journal.start();self.addCleanup(journal.stop)
        mock.return_value.resolve.return_value=None if missing else publication
        if conflict:mock.return_value.resolve.side_effect=ValueError('injected proof conflict')
        return LifecycleService(Session()),calls

    def test_profile_source_pin_exact_metrics_hashes_and_activation_gate(self):
        service,calls=self.service()
        result=service.profiles(purpose='activation',cohort_month='2025-12-01',activity_month='2026-01-01')
        self.assertEqual(len([c for c,p in calls if c=='pin']),1)
        row=result['rows'][0];self.assertEqual(row['recency_days'],'2');self.assertEqual(row['net_revenue_minor'],'9007199254740992')
        self.assertTrue(row['activation_eligible']);self.assertEqual(row['confidence'],'deterministic_customer_reference')
        self.assertEqual(result['provenance']['as_of'],'2026-01-03T00:00:00+00:00')
        self.assertNotIn('private',str(result));self.assertNotIn('profile_key',str(result))
        filtered=[(sql,p) for sql,p in calls if 'AS total' in sql or 'SELECT * FROM profiles' in sql]
        self.assertTrue(all('activation_eligible' in sql and 'toStartOfMonth(event_date)' in sql for sql,p in filtered))
        self.assertTrue(all(p['activity_month']==date(2026,1,1) for sql,p in filtered))

    def test_missing_conflicting_and_denied_timeline_states(self):
        service,_=self.service(missing=True)
        self.assertEqual(service.profiles()['status'],'unavailable')
        with self.assertRaises(LifecycleError) as error:service.preview(SegmentRule('activation'))
        self.assertEqual(error.exception.code,'lifecycle_detail_unavailable')
        service,_=self.service(conflict=True)
        with self.assertRaises(LifecycleError):service.profiles()
        service,_=self.service(PROFILE|{'crm_consent_analytics':False})
        timeline=service.timeline('d'*64)
        self.assertEqual(timeline['status'],'consent_restricted');self.assertEqual(timeline['events'],[])
        service,_=self.service({})
        with self.assertRaises(LifecycleError) as error:service.timeline('d'*64)
        self.assertEqual(error.exception.code,'profile_not_found')

    def test_rule_numeric_values_bound_and_same_release_reproducible(self):
        service,calls=self.service()
        rule=SegmentRule.from_payload({'schema_version':'mp08-segment-v1','purpose':'activation','cohort_month':None,'activity_month':None,'profile_type':None,
            'conditions':[{'field':'net_revenue_minor','op':'gte','value':'-9007199254740993'}]})
        first=service.preview(rule);second=service.preview(rule)
        self.assertEqual(first['evaluation_key'],second['evaluation_key'])
        self.assertEqual(first['rule_digest'],rule.digest)
        for sql,params in calls:
            self.assertNotIn('-9007199254740993',sql)
        self.assertTrue(any(p.get('condition_0')==-9007199254740993 for s,p in calls))
        with self.assertRaises(ValueError):service.timeline("d' OR 1=1")
        for kwargs in ({'limit':0},{'offset':100001},{'purpose':'all'},{'activity_month':'2026-01-02'}):
            with self.assertRaises(ValueError):service.profiles(**kwargs)

import os
@unittest.skipUnless(os.getenv('MP08_CLICKHOUSE_TEST'),'opt-in existing governed ClickHouse release')
class LifecycleReadIntegrationTests(unittest.TestCase):
    def test_real_customer_rows_have_unambiguous_safe_columns_and_exact_counts(self):
        import clickhouse_connect
        from marketpulse_app.analytics import Analytics
        client=clickhouse_connect.get_client(host='127.0.0.1',port=48123,username=os.getenv('CLICKHOUSE_USER','marketpulse'),password=os.environ['CLICKHOUSE_PASSWORD'],autogenerate_session_id=False)
        try:
            service=LifecycleService(Analytics(client,None))
            report=service.profiles();self.assertTrue(report['rows'])
            self.assertIs(type(report['rows'][0]['consent_analytics']),bool)
            self.assertNotIn('profile_key',str(report))
            activation=service.profiles(purpose='activation')
            self.assertTrue(all(row['activation_eligible'] and row['crm_consent_analytics'] is True and row['crm_consent_marketing'] is True for row in activation['rows']))
        finally:client.close()
