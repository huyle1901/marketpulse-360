from app_auth_support import TrustedTestAccess
"""Owned ClickHouse fixtures exercise consent/RFM/cohort edge cases through HTTP."""
import os,unittest,uuid
from datetime import datetime,date,timezone
from types import SimpleNamespace
from unittest.mock import patch
from fastapi.testclient import TestClient
from marketpulse_app.api import create_app
from marketpulse_app.analytics import Analytics
from marketpulse_app.lifecycle import LifecycleService
from marketpulse_app.lifecycle_contracts import SegmentRule,Condition
from marketpulse_app.journey_projection import copy_journeys,project_event,reference
from marketpulse_batch.ledger import PublicationReservation
from marketpulse_batch.canonical import clickhouse_type
from marketpulse_batch.export import business_schema
from marketpulse_batch.publication import resolve_active
from marketpulse_lakehouse.gold import build_customer_360,build_cohort_retention


@unittest.skipUnless(os.getenv('MP08_CLICKHOUSE_TEST'),'opt-in owned lifecycle ClickHouse fault fixtures')
class LifecycleFaultIntegrationTests(unittest.TestCase):
    def setUp(self):
        import clickhouse_connect
        self.c=clickhouse_connect.get_client(host='127.0.0.1',port=48123,username=os.getenv('CLICKHOUSE_USER','marketpulse'),password=os.environ['CLICKHOUSE_PASSWORD'],autogenerate_session_id=False)
        active=resolve_active(self.c);self.identity='e'*64
        self.db='mp_release_'+self.identity+'_'+str(8000000000+uuid.uuid4().int%1000000000)
        self.c.command(f'CREATE DATABASE {self.db} ENGINE=Atomic');self.owned=[self.db]
        for name in ('gold.customer_360','gold.cohort_retention','silver.customers','silver.campaigns','gold.campaign_performance_daily'):
            columns=','.join(f'{field} {clickhouse_type(kind,nullable)}' for field,kind,nullable in business_schema(name))
            self.c.command(f'CREATE TABLE {self.db}.{name.split(".")[1]} ({columns}) ENGINE=Memory')
        self.c.command(f'INSERT INTO {self.db}.campaigns SELECT * FROM {active.database}.campaigns')
        self.release=PublicationReservation(1,self.identity,self.db,date(2026,2,2),'f'*64)
        customers=[]
        for customer,analytics,marketing in (('yes',True,True),('marketing_denied',True,False),('analytics_denied',False,True),('cart',True,True)):
            customers.append(dict(customer_id=customer,created_at=datetime(2025,12,1,tzinfo=timezone.utc),snapshot_date=date(2026,2,2),loyalty_tier='standard',lifetime_value_minor=0,last_order_at=None,
                consent_analytics=analytics,consent_marketing=marketing,source_file='owned-test',source_row_number=len(customers)+1))
        events=[]
        def event(profile,name,when,gross=None,refund=None):
            stamp=datetime.fromisoformat(when).replace(tzinfo=timezone.utc)
            events.append(dict(event_id='event-'+str(len(events)),profile_key=profile,session_id='session-'+profile,click_id=None,order_id='order-'+profile if name in {'order_created','order_refunded'} else None,
                campaign_id=None,event_name=name,event_time=stamp,event_date=stamp.date(),source='commerce' if name in {'order_created','order_refunded'} else 'web',currency='VND' if name in {'order_created','order_refunded'} else None,
                gross_amount_minor=gross,refund_amount_minor=refund,consent_analytics=True,is_bot_session=name=='order_refunded'))
        event('customer:yes','order_created','2026-01-01',9007199254740993)
        event('customer:yes','order_refunded','2026-01-20',9007199254740993,3)
        event('customer:yes','page_view','2026-02-02')
        event('customer:marketing_denied','page_view','2026-01-02')
        event('customer:marketing_denied','add_to_cart','2026-01-02')
        event('customer:analytics_denied','order_created','2026-01-03',100)
        event('customer:analytics_denied','order_refunded','2026-01-21',100,101)
        event('customer:unknown','page_view','2026-01-04')
        event('anonymous:anon','page_view','2026-01-05')
        event('session:only','page_view','2026-01-06')
        event('customer:cart','page_view','2026-01-02')
        event('customer:cart','add_to_cart','2026-01-02')
        profiles=build_customer_360(events,customers)
        def insert(table,rows):
            columns=[r[0] for r in self.c.query(f'DESCRIBE TABLE {self.db}.{table}').result_rows]
            self.c.insert(self.db+'.'+table,[[r[k] for k in columns] for r in rows],column_names=columns)
        insert('customers',customers);insert('customer_360',profiles);insert('cohort_retention',build_cohort_retention(events,profiles))
        self.publication=copy_journeys(self.c,self.release,123,(project_event(r,self.identity) for r in events));self.owned.append(self.publication.database)
        self.journal=patch('marketpulse_app.lifecycle.JourneyJournal.resolve',return_value=self.publication);self.journal.start()
        self.service=LifecycleService(Analytics(self.c,None,release=self.release))
        self.http=TestClient(create_app(Analytics(self.c,None,release=self.release),SimpleNamespace(),access=TrustedTestAccess()))

    def tearDown(self):
        self.http.close();self.journal.stop()
        for database in reversed(self.owned):
            if not database.startswith(('mp_release_'+self.identity+'_','mp_journey_'+self.identity+'_')):raise ValueError('unowned test database')
            self.c.command('DROP DATABASE '+database)
        self.c.close()

    def test_empty_activation_rule_cannot_admit_denied_unknown_or_device_identities(self):
        response=self.http.post('/api/v1/lifecycle/segments/preview',json={'rule':SegmentRule('activation').payload()})
        self.assertEqual(response.status_code,200);body=response.json()
        self.assertEqual(body['matched_total'],'2');self.assertEqual(body['excluded_from_activation_total'],'5')
        self.assertEqual({r['profile_ref'] for r in body['rows']},{reference(self.identity,'customer:yes'),reference(self.identity,'customer:cart')})
        self.assertTrue(all(r['activation_eligible'] for r in body['rows']))
        raw=self.http.get('/api/v1/lifecycle/profiles?limit=100').json()
        self.assertEqual(raw['total'],'7')
        for kind,category in (('anonymous','anonymous_device_only'),('session','session_only')):
            self.assertEqual(next(r['confidence'] for r in raw['rows'] if r['profile_type']==kind),category)
        denied=self.http.get('/api/v1/lifecycle/profiles/'+reference(self.identity,'customer:analytics_denied')+'/timeline').json()
        self.assertEqual(denied['status'],'consent_restricted');self.assertEqual(denied['events'],[])
        self.assertEqual(self.http.get('/api/v1/lifecycle/profiles/'+'a'*64+'/timeline').status_code,404)

    def test_original_purchase_rfm_exact_net_and_active_month_cohort_membership(self):
        result=self.service.profiles(cohort_month='2025-12-01',activity_month='2026-02-01')
        self.assertEqual(result['total'],'1');row=result['rows'][0]
        self.assertEqual(row['profile_ref'],reference(self.identity,'customer:yes'))
        self.assertEqual(row['recency_days'],'33');self.assertEqual(row['net_revenue_minor'],'9007199254740990')
        self.assertTrue(row['opportunities']['churn_risk'])
        refund=next(r for r in self.service.timeline(row['profile_ref'])['events'] if r['event_name']=='order_refunded')
        self.assertEqual(refund['gross_revenue_minor'],'0');self.assertEqual(refund['refund_amount_minor'],'3')
        self.assertEqual(self.service.preview(SegmentRule('activation',(Condition('recency_days','gte','30'),)))['matched_total'],'1')
        self.assertEqual(self.service.preview(SegmentRule('analytics',(Condition('net_revenue_minor','lte','-1'),)))['matched_total'],'1')
        cart=self.service.profiles(opportunity='cart_opportunity',purpose='activation')['rows'][0]
        self.assertIsNone(cart['recency_days']);self.assertEqual(cart['order_count'],'0')

    def test_mixed_currency_is_rejected_without_touching_live_release(self):
        self.c.command(f"INSERT INTO {self.db}.campaigns SELECT * REPLACE('USD' AS currency,'owned-mixed' AS campaign_id) FROM {self.db}.campaigns LIMIT 1")
        response=self.http.get('/api/v1/lifecycle/cohorts')
        self.assertEqual(response.status_code,409);self.assertEqual(response.json()['code'],'unsupported_currency')
