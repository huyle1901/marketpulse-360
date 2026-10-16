from app_auth_support import TrustedTestAccess
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from fastapi.testclient import TestClient
from marketpulse_app.api import create_app
from marketpulse_app.lifecycle_contracts import SegmentRule

class LifecycleApiTests(unittest.TestCase):
    def client(self):return TestClient(create_app(SimpleNamespace(),SimpleNamespace(),access=TrustedTestAccess()))

    def test_validation_routes_and_read_only_preview(self):
        client=self.client()
        for suffix in ('profiles?purpose=all','profiles?limit=0','profiles?cohort_month=2026-01-02','profiles/invalid/timeline'):
            response=client.get('/api/v1/lifecycle/'+suffix)
            self.assertEqual(response.status_code,422)
            self.assertEqual(response.headers['cache-control'],'no-store')
        with patch('marketpulse_app.lifecycle_api.LifecycleService.preview',return_value={'matched_total':'0'}),patch('marketpulse_app.lifecycle_api.SegmentStore.save') as save:
            response=client.post('/api/v1/lifecycle/segments/preview',json={'rule':SegmentRule('activation').payload()})
            self.assertEqual(response.status_code,200);save.assert_not_called()
        for body in ({'rule':{'sql':'bad'}},{'name':'','rule':SegmentRule('activation').payload(),'expected_version':0},{'name':'A','rule':SegmentRule('activation').payload(),'expected_version':True}):
            endpoint='preview' if 'name' not in body else ''
            response=client.post('/api/v1/lifecycle/segments'+('/'+endpoint if endpoint else ''),json=body)
            self.assertEqual(response.status_code,422)

    def test_stable_missing_conflict_and_dependency_boundaries(self):
        from marketpulse_app.lifecycle import LifecycleError
        client=self.client()
        for error,status,code in ((LifecycleError('profile_not_found'),404,'profile_not_found'),(LifecycleError('projection_mismatch'),409,'projection_mismatch'),(RuntimeError('private'),503,'dependency_unavailable')):
            with patch('marketpulse_app.lifecycle_api.LifecycleService.cohorts',side_effect=error):response=client.get('/api/v1/lifecycle/cohorts')
            self.assertEqual(response.status_code,status);self.assertEqual(response.json()['code'],code);self.assertNotIn('private',response.text)
