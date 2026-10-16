import unittest
from pathlib import Path
from types import SimpleNamespace
from marketpulse_app.quality import QualityService, reconcile_finance

class QualityTests(unittest.TestCase):
    def test_source_finance_and_all_models(self):
        source={'gross_revenue_minor':100,'refund_amount_minor':20,'net_revenue_minor':80}
        models=[dict(model=m,allocated_gross_minor=100,allocated_refund_minor=20,allocated_net_minor=80) for m in ('first_touch','last_touch','linear')]
        reconcile_finance(source,source,models)
        with self.assertRaises(ValueError):reconcile_finance(source,source|{'gross_revenue_minor':101,'net_revenue_minor':81},models)
        with self.assertRaises(ValueError):reconcile_finance(source,source,models[:-1])
    def test_missing_evidence_is_unavailable(self):
        service=QualityService(SimpleNamespace(pin=lambda: (_ for _ in ()).throw(OSError('sensitive error'))),None,Path('/none'))
        report=service.check();self.assertEqual(report['status'],'unavailable');self.assertNotIn('sensitive',str(report))
    def test_probe_stops_before_create_when_baseline_unavailable(self):
        service=QualityService(SimpleNamespace(pin=lambda: (_ for _ in ()).throw(OSError())),None,Path('/none'))
        self.assertEqual(service.probe()['status'],'unavailable')
    def test_finance_uses_existing_governed_commerce_event_names(self):
        seen=[]
        class Client:
            def query(self,sql):
                seen.append(sql)
                if '.events' in sql:return SimpleNamespace(first_row=(100,20) if "event_name='order_created'" in sql and "event_name='order_refunded'" in sql else (0,0))
                if 'GROUP BY' in sql:return SimpleNamespace(result_rows=[(m,100,20,80) for m in ('first_touch','last_touch','linear')])
                return SimpleNamespace(first_row=(100,20,80))
        self.assertEqual(QualityService._finance(Client(),'release',SimpleNamespace(database='projection'))['net_revenue_minor'],'80')
