import os
import unittest
from dataclasses import replace
from datetime import date, datetime, timezone

from marketpulse_app.journey_projection import JourneyJournal, copy_journeys, project_event
from marketpulse_batch.ledger import PublicationReservation


def event():
    return dict(event_id='event-a',profile_key='customer:synthetic-a',session_id='session-a',click_id='click-a',order_id=None,
                campaign_id='a',event_name='page_view',event_time=datetime(2026,1,1,tzinfo=timezone.utc),event_date=date(2026,1,1),
                source='web',currency=None,gross_amount_minor=None,refund_amount_minor=None,
                consent_analytics=True,is_bot_session=False,payload_json='fake contact must not leak',customer_id='fake-customer')


class JourneyProjectionBoundaryTests(unittest.TestCase):
    def test_projection_hides_profile_payload_and_preserves_nullable_exact_values(self):
        row=project_event(event(),'a'*64)
        self.assertNotIn('profile_key',row)
        self.assertNotIn('customer_id',row)
        self.assertNotIn('payload_json',row)
        self.assertEqual(len(row['profile_ref']),64)
        self.assertEqual(project_event(event(),'a'*64),row)
        self.assertNotEqual(project_event(event(),'b'*64)['profile_ref'],row['profile_ref'])
        paid=event()|dict(source='commerce',event_name='order_created',order_id='o',currency='VND',gross_amount_minor=9007199254740993)
        self.assertEqual(project_event(paid,'a'*64)['gross_amount_minor'],9007199254740993)
        self.assertTrue(project_event(paid|{'is_bot_session':True},'a'*64)['is_bot_session'])
        self.assertIsNone(row['currency'])
        for update in ({'is_bot_session':True},{'consent_analytics':False},{'event_time':datetime(2026,1,1)}):
            with self.assertRaises(ValueError):project_event(event()|update,'a'*64)


@unittest.skipUnless(os.getenv('MP07_CLICKHOUSE_TEST'),'opt-in real journey projection ClickHouse')
class JourneyProjectionIntegrationTests(unittest.TestCase):
    def setUp(self):
        import clickhouse_connect
        self.client=clickhouse_connect.get_client(host='localhost',port=48123,username=os.getenv('CLICKHOUSE_USER','marketpulse'),
            password=os.environ['CLICKHOUSE_PASSWORD'],autogenerate_session_id=False)
        self.release=PublicationReservation(1,'c'*64,'mp_release_'+'c'*64+'_1',date(2026,1,2),'d'*64)
        self.client.command('DROP DATABASE IF EXISTS mp07_journal_test')
        self.journal=JourneyJournal(self.client,database='mp07_journal_test')
        self.owned=[]

    def tearDown(self):
        for database in self.owned:self.client.command(f'DROP DATABASE {database}')
        self.client.command('DROP DATABASE IF EXISTS mp07_journal_test')
        self.client.close()

    def test_exact_copy_journal_conflict_duplicates_and_global_click_canonicalization(self):
        first=event()|{'event_id':'event-earlier','event_time':datetime(2025,11,1,tzinfo=timezone.utc),'event_date':date(2025,11,1)}
        copied=copy_journeys(self.client,self.release,123,[project_event(first,self.release.identity),project_event(event(),self.release.identity)])
        self.owned.append(copied.database)
        self.journal.publish(copied);self.journal.publish(copied)
        self.assertEqual(self.journal.resolve(self.release),copied)
        rows=self.client.query(f'SELECT event_date FROM {copied.database}.paid_touches').result_rows
        self.assertEqual(rows,[(date(2025,11,1),)])
        self.journal.publish(replace(copied,silver_snapshot_id=124))
        with self.assertRaises(ValueError):self.journal.resolve(self.release)

    def test_duplicate_source_keys_never_publish(self):
        before=self.journal.resolve(self.release)
        with self.assertRaises(ValueError):
            copy_journeys(self.client,self.release,123,[project_event(event(),self.release.identity)]*2)
        self.assertEqual(self.journal.resolve(self.release),before)
        # Test-owned failed candidate cleanup lives only in this test.
        databases=self.client.query('SELECT name FROM system.databases WHERE startsWith(name,{prefix:String})',parameters={'prefix':'mp_journey_'+'c'*64+'_'}).result_rows
        self.owned.extend(row[0] for row in databases)
