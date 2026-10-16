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

    def test_collector_runtime_initializes_security_before_evidence(self):
        from unittest.mock import Mock,patch
        from marketpulse_app.operations import runtime_operations
        auth=Mock();order=[];auth.initialize.side_effect=lambda:order.append('auth')
        with patch.dict('os.environ',{'DAGSTER_POSTGRES_PASSWORD':'synthetic-test'}),patch('marketpulse_app.ops_store.OpsStore') as evidence:
            evidence.return_value.initialize.side_effect=lambda:order.append('evidence')
            runtime_operations(None,None,auth)
        self.assertEqual(order,['auth','evidence'])
    def test_lineage_binds_existing_customer_events_snapshot(self):
        from unittest.mock import Mock,patch
        release=SimpleNamespace(identity='a',database='b',proof_digest='c')
        receipt=SimpleNamespace(output_snapshots={'silver.customer_events':42})
        cert=SimpleNamespace(identity='a',database='b',proof_digest='c',manifest_digest='d')
        manifest=SimpleNamespace(identity='a',digest='d')
        ledger=Mock();ledger.get_receipt.return_value=(receipt,'file:///x');ledger.get_certified_bundle.return_value=SimpleNamespace(certification_uri='file:///cert',export_manifest_uri='file:///manifest',**vars(release))
        service=QualityService(SimpleNamespace(pin=lambda:SimpleNamespace(release=release,client=Mock())),ledger,Path('/'))
        service._path=Mock(return_value=SimpleNamespace(read_bytes=lambda:b'{}'))
        with patch('marketpulse_app.quality.read_certification',return_value=cert),patch('marketpulse_app.quality.ExportManifest.from_payload',return_value=manifest),patch('marketpulse_app.quality.verify_certification_current'),patch('marketpulse_app.quality.JourneyJournal') as journal,patch('marketpulse_app.quality.verify_projection'):
            journal.return_value.resolve.return_value=SimpleNamespace(silver_snapshot_id=42)
            self.assertEqual(service._context()[1],receipt)
