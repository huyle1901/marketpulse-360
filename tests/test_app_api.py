import unittest
from datetime import date
from types import SimpleNamespace

from fastapi.testclient import TestClient
from marketpulse_app.api import create_app
from marketpulse_app.store import VersionConflict
from marketpulse_batch.publication import NoPublishedRelease


class ApiBoundaryTests(unittest.TestCase):
    def client(self, save=None, unavailable=False):
        store = SimpleNamespace(settings=lambda: [], rules=lambda: [], alerts=lambda: [],
            save_setting=save or (lambda setting,version: setting))
        class Session:
            def pin(self):
                if unavailable:
                    raise NoPublishedRelease('no release')
                return self
            def campaigns(self):
                return [{'campaign_id':'a','currency':'VND'}]
        return TestClient(create_app(Session(),store))

    def body(self):
        return dict(market='VN',currency='VND',period_start='2026-01-01',period_end='2026-01-03',
                    budget_minor='9007199254740993',expected_version=0)

    def test_validated_exact_save_and_no_store(self):
        result=self.client().put('/api/v1/campaign-settings/a',json=self.body())
        self.assertEqual(result.status_code,200)
        self.assertEqual(result.json()['budget_minor'],'9007199254740993')
        self.assertEqual(result.headers['cache-control'],'no-store')
        for amount in (True,1.5,'-1','9223372036854775808'):
            result=self.client().put('/api/v1/campaign-settings/a',json=self.body()|{'budget_minor':amount})
            self.assertEqual(result.status_code,422)

    def test_stale_unpublished_and_currency_mismatch(self):
        def stale(*args):
            raise VersionConflict('stale')
        self.assertEqual(self.client(stale).put('/api/v1/campaign-settings/a',json=self.body()).status_code,409)
        self.assertEqual(self.client(unavailable=True).put('/api/v1/campaign-settings/a',json=self.body()).json()['code'],'no_published_release')
        self.assertEqual(self.client().put('/api/v1/campaign-settings/a',json=self.body()|{'currency':'USD'}).status_code,422)
