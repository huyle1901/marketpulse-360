import unittest
import importlib.util
from pathlib import Path

path=Path(__file__).resolve().parents[1]/'scripts/app-smoke-test.py'
spec=importlib.util.spec_from_file_location('app_proof',path)
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class AppProofTests(unittest.TestCase):
    def test_independent_projection_assertion_catches_extra_or_changed_rows(self):
        expected={'a':{'partition':'0','offset':'2','payload':{'net_revenue_minor':'10'}}}
        observed=[{'metric_key':'a','partition':'0','offset':'2','net_revenue_minor':'10'}]
        module.assert_projection(expected,observed)
        with self.assertRaises(AssertionError):
            module.assert_projection(expected,observed+[observed[0]])
        with self.assertRaises(AssertionError):
            module.assert_projection(expected,[observed[0]|{'net_revenue_minor':'11'}])
        two_keys=expected|{'b':expected['a']}
        with self.assertRaises(AssertionError):
            module.assert_projection(two_keys,[observed[0],observed[0]])

    def test_percentile_is_nearest_rank(self):
        self.assertEqual(module.percentile(list(range(1,101)),0.95),95)
