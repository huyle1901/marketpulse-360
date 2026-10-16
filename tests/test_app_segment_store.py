import os,unittest,uuid
from concurrent.futures import ThreadPoolExecutor
from marketpulse_app.segment_store import SegmentStore
from marketpulse_app.store import AppStore,VersionConflict
from marketpulse_app.lifecycle_contracts import SegmentRule,Condition

@unittest.skipUnless(os.getenv('MP08_POSTGRES_TEST'),'dedicated mp08_test PostgreSQL only')
class SegmentStoreIntegrationTests(unittest.TestCase):
    def setUp(self):
        import psycopg
        self.dsn=os.environ['MP08_TEST_DSN']
        with psycopg.connect(self.dsn) as c:
            if c.execute('SELECT current_database()').fetchone()[0]!='mp08_test':raise ValueError('dedicated test DB required')
            c.execute('DROP SCHEMA IF EXISTS marketpulse_app CASCADE')
        self.store=SegmentStore(AppStore(self.dsn));self.store.initialize();self.id=str(uuid.uuid4())

    def test_empty_idempotent_canonical_revisions_and_concurrent_edit(self):
        self.store.initialize();self.assertEqual(self.store.list(),[])
        rule=SegmentRule('activation',(Condition('net_revenue_minor','gte','9007199254740993'),))
        first=self.store.save(self.id,' Value ',rule,0)
        self.assertEqual(first['version'],'1');self.assertEqual(first['name'],'Value')
        def update(name):
            try:return self.store.save(self.id,name,SegmentRule('analytics'),1)['version']
            except VersionConflict:return 'conflict'
        with ThreadPoolExecutor(2) as pool:results=list(pool.map(update,['A','B']))
        self.assertCountEqual(results,['2','conflict'])
        restored=SegmentStore(AppStore(self.dsn));self.assertEqual(restored.revision(self.id,1),first)
        self.assertEqual(first['rule_digest'],rule.digest)
        self.assertEqual(restored.list()[0]['version'],'2')
        self.assertIsNone(restored.revision(str(uuid.uuid4()),1))
        import psycopg
        with self.assertRaises(psycopg.Error),AppStore(self.dsn).connect() as c:
            c.execute('UPDATE marketpulse_app.segment_revisions SET name=%s WHERE segment_id=%s',('mutated',self.id))
