import unittest
from datetime import date
from types import SimpleNamespace

from fastapi.testclient import TestClient
from marketpulse_app.api import create_app


class AnalysisHttpTests(unittest.TestCase):
    def client(self):
        class Analytics:
            def pin(self):return self
            def all_daily(self):return []
        return TestClient(create_app(Analytics(),SimpleNamespace(settings=lambda:[])))

    def test_analysis_routes_validate_before_dependency_queries(self):
        for path in ('funnel','attribution','journeys'):
            result=self.client().get('/api/v1/analysis/'+path+'?from_date=2026-01-03&to_date=2026-01-01')
            self.assertEqual(result.status_code,422)
            self.assertEqual(result.headers['cache-control'],'no-store')
        result=self.client().get('/api/v1/analysis/journeys?model=wrong&from_date=2026-01-01&to_date=2026-01-03')
        self.assertEqual(result.status_code,422)
