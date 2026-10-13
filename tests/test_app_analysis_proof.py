import importlib.util
import unittest
from pathlib import Path

path=Path(__file__).resolve().parents[1]/'scripts/analysis-smoke-test.py'
spec=importlib.util.spec_from_file_location('analysis_proof',path)
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class AnalysisProofTests(unittest.TestCase):
    def test_keyed_equality_rejects_same_count_duplicate_and_changed_money(self):
        rows=[{'model':'first_touch','net':'9007199254740993'},{'model':'linear','net':'1'}]
        module.assert_keyed(rows,rows,('model',),('net',))
        for bad in ([rows[0],rows[0]],[rows[0]], [rows[0]|{'net':'9007199254740992'},rows[1]]):
            with self.assertRaises(AssertionError):module.assert_keyed(rows,bad,('model',),('net',))

    def test_independent_global_reconciliation_cannot_use_filtered_credit(self):
        source={'gross_revenue_minor':'100','refund_amount_minor':'30','net_revenue_minor':'70'}
        totals=[{'model':m,**source} for m in ('first_touch','last_touch','linear')]
        module.assert_global(totals,source)
        with self.assertRaises(AssertionError):module.assert_global(totals[:-1],source)
        with self.assertRaises(AssertionError):module.assert_global(totals,source|{'net_revenue_minor':'40'})
