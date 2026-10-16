import os
import unittest
from types import SimpleNamespace
from fastapi.testclient import TestClient
from marketpulse_app.api import create_app
from marketpulse_app.security import password_hash, password_matches, username, policy, Principal
from marketpulse_app.access import AccessControl

class FakeAuth:
    def __init__(self,role='viewer',broken=False): self.role=role; self.broken=broken; self.events=[]
    def resolve(self,token): return Principal('test-user','tester',self.role) if token=='valid' else None
    def audit(self,*args):
        if self.broken: raise OSError('private')
        self.events.append(args)

class SecurityTests(unittest.TestCase):
    def test_password_hash_salted_and_bounded(self):
        a=password_hash('correct horse battery'); b=password_hash('correct horse battery')
        self.assertNotEqual(a,b); self.assertTrue(password_matches('correct horse battery',a))
        self.assertFalse(password_matches('wrong password',a)); self.assertFalse(password_matches('wrong','invalid'))
        for value in ('short','x'*129):
            with self.assertRaises(ValueError): password_hash(value)
    def test_username_and_default_deny_policy(self):
        self.assertEqual(username('Admin.User'),'admin.user')
        for value in ('a',' bob','a/b','ééé'):
            with self.assertRaises(ValueError): username(value)
        self.assertIsNone(policy('GET','/api/v1/future'))
        self.assertEqual(policy('GET','/api/v1/analysis/journeys')[1],'operator')
        self.assertEqual(policy('POST','/api/v1/budget-simulation')[1],'viewer')
    def client(self,auth=None):
        analytics=SimpleNamespace(pin=lambda: (_ for _ in ()).throw(AssertionError('must not read')))
        access=AccessControl(auth,'http://testserver') if auth else None
        return TestClient(create_app(analytics,SimpleNamespace(),access=access))
    def test_failclosed_and_denial_before_analytics(self):
        self.assertEqual(self.client().get('/api/v1/dashboard').status_code,401)
        c=self.client(FakeAuth()); c.cookies.set('mp_session','valid')
        self.assertEqual(c.get('/api/v1/analysis/journeys').status_code,403)
        self.assertEqual(c.get('/api/v1/future').status_code,403)
        self.assertEqual(c.get('/api/v1/health/live').status_code,200)
    def test_origin_and_audit_failure(self):
        c=self.client(FakeAuth());c.cookies.set('mp_session','valid')
        for origin in (None,'null','http://evil.test'):
            self.assertEqual(c.post('/api/v1/budget-simulation',headers={'Origin':origin} if origin else {},json={}).status_code,403)
        self.assertEqual(c.post('/api/v1/budget-simulation',headers={'Origin':'http://testserver'},json={}).status_code,422)
        c=self.client(FakeAuth(broken=True));c.cookies.set('mp_session','valid')
        self.assertEqual(c.get('/api/v1/dashboard').status_code,503)
    def test_audit_excludes_input_and_raw_path(self):
        auth=FakeAuth();c=self.client(auth);c.cookies.set('mp_session','valid')
        c.put('/api/v1/campaign-settings/private-customer?secret=value',headers={'Origin':'http://testserver'},json={})
        self.assertNotIn('private-customer',str(auth.events));self.assertNotIn('secret',str(auth.events))

@unittest.skipUnless(os.environ.get('MP09_TEST_DSN'),'dedicated PG opt-in')
class AuthPGTests(unittest.TestCase):
    def setUp(self):
        from marketpulse_app.auth_store import AuthStore
        from marketpulse_app.store import AppStore
        self.store=AppStore(os.environ['MP09_TEST_DSN']);self.auth=AuthStore(self.store);self.auth.initialize()
        with self.store.connect() as c:
            c.execute('TRUNCATE marketpulse_app.auth_sessions, marketpulse_app.auth_users, marketpulse_app.login_failures CASCADE')
        self.admin=self.auth.create_user('admin-test','secure password 123','admin')
    def test_session_role_revocation_last_admin_and_immutable_audit(self):
        token,p=self.auth.login('admin-test','secure password 123');self.assertEqual(self.auth.resolve(token).role,'admin')
        with self.assertRaises(ValueError):self.auth.update_user(p.user_id,'viewer',True,1)
        other=self.auth.create_user('admin-other','secure password 456','admin')
        self.auth.update_user(p.user_id,'operator',True,1);self.assertIsNone(self.auth.resolve(token))
        self.auth.audit('request','test-user','admin','/fixed','allow',None)
        with self.assertRaises(Exception):
            with self.store.connect() as c:c.execute('DELETE FROM marketpulse_app.access_audit')
    def test_throttle_persists_and_cleanup_never_removes_live(self):
        for _ in range(5):self.assertIsNone(self.auth.login('admin-test','wrong password'))
        self.assertIsNone(self.auth.login('admin-test','secure password 123'))
        token,_=self.auth.login('admin-test-unknown','wrong password') or ('',None)
        self.assertEqual(self.auth.cleanup_sessions(),0)

    def test_invalid_username_cannot_alias_a_valid_account(self):
        # A malformed input must never fall back to the real canonical user "invalid".
        self.auth.create_user('invalid','secure password 999','viewer')
        self.assertIsNone(self.auth.login('invalid/name','secure password 999'))

    def test_concurrent_demotions_preserve_one_admin(self):
        from concurrent.futures import ThreadPoolExecutor
        second=self.auth.create_user('second-admin','secure password 456','admin')
        def demote(identity):
            try:self.auth.update_user(identity,'viewer',True,1);return True
            except ValueError:return False
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(demote,[self.admin.user_id,second.user_id]))
        self.assertEqual(sorted(results),[False,True])
        self.assertEqual(sum(u['role']=='admin' and u['enabled'] for u in self.auth.users()),1)
    def test_expiry_disable_and_bounded_retention(self):
        token,p=self.auth.login('admin-test','secure password 123')
        with self.store.connect() as c:
            c.execute("INSERT INTO marketpulse_app.auth_sessions(token_hash,user_id,expires_at) SELECT md5(i::text),%s,now()-interval '1 minute' FROM generate_series(1,1005) i",(p.user_id,))
        self.assertEqual(self.auth.cleanup_sessions(),1000);self.assertEqual(self.auth.expired_sessions(),5)
        self.assertIsNotNone(self.auth.resolve(token))
        with self.store.connect() as c:c.execute("UPDATE marketpulse_app.auth_sessions SET expires_at=now()-interval '1 second' WHERE token_hash=%s",(self.auth.digest(token),))
        self.assertIsNone(self.auth.resolve(token))

class MetricsAccessTests(unittest.TestCase):
    def test_metrics_requires_separate_bearer(self):
        c=TestClient(create_app(SimpleNamespace(),SimpleNamespace(),access=AccessControl(FakeAuth(),'http://testserver',metrics_token='private-service-token')))
        self.assertEqual(c.get('/metrics').status_code,401)
        self.assertEqual(c.get('/metrics',headers={'Authorization':'Bearer private-service-token'}).status_code,200)
    def test_outcome_audit_failure_keeps_success_and_exposes_counter(self):
        auth=FakeAuth('admin');calls=[]
        def audit(*args):
            calls.append(args)
            if len(calls)==2:raise OSError('outcome unavailable')
        auth.audit=audit
        c=TestClient(create_app(SimpleNamespace(),SimpleNamespace(),access=AccessControl(auth,'http://testserver',metrics_token='service-token')));c.cookies.set('mp_session','valid')
        self.assertEqual(c.get('/api/v1/auth/session').status_code,200)
        response=c.get('/metrics',headers={'Authorization':'Bearer service-token'})
        self.assertIn('mp_audit_outcome_failures_total 1',response.text)
