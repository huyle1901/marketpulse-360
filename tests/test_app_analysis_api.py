from app_auth_support import TrustedTestAccess
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
        return TestClient(create_app(Analytics(),SimpleNamespace(settings=lambda:[]),access=TrustedTestAccess()))

    def test_analysis_routes_validate_before_dependency_queries(self):
        for path in ('funnel','attribution','journeys'):
            result=self.client().get('/api/v1/analysis/'+path+'?from_date=2026-01-03&to_date=2026-01-01')
            self.assertEqual(result.status_code,422)
            self.assertEqual(result.headers['cache-control'],'no-store')
        result=self.client().get('/api/v1/analysis/journeys?model=wrong&from_date=2026-01-01&to_date=2026-01-03')
        self.assertEqual(result.status_code,422)

    def test_stable_analysis_and_dependency_errors_do_not_expose_exceptions(self):
        from unittest.mock import patch
        from marketpulse_app.analysis import AnalysisError
        url='/api/v1/analysis/funnel?from_date=2026-01-01&to_date=2026-01-03'
        for error,status,code in ((AnalysisError('reconciliation_failed'),409,'reconciliation_failed'),(RuntimeError('private dependency details'),503,'dependency_unavailable')):
            with patch('marketpulse_app.analysis_api.AnalysisService.funnel',side_effect=error):
                result=self.client().get(url)
            self.assertEqual(result.status_code,status)
            self.assertEqual(result.json()['code'],code)
            self.assertNotIn('private',result.text)
            self.assertEqual(result.headers['cache-control'],'no-store')
