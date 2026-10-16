import unittest
from marketpulse_eval.resources import parse_cgroup
from marketpulse_eval.runner import comparable, require_recovery, ensure_job_state
from marketpulse_eval.contracts import verify_recovery

class RuntimeEvidenceTests(unittest.TestCase):
    def test_cgroup_counters_working_set(self):
        result = parse_cgroup('usage_usec 2500000\nuser_usec 1\nMEMORY\n1000\nSTAT\ninactive_file 200\n')
        self.assertEqual(result, {'cpu_seconds': 2.5, 'working_set_bytes': 800})
        with self.assertRaises(ValueError):
            parse_cgroup('unavailable')

    def test_comparison_requires_same_inputs_and_exact_outputs(self):
        base = {'input_hash':'a','output_hash':'b','count':10,'passed':True}
        comparable(base, base)
        for bad in (base | {'input_hash':'c'},base | {'output_hash':'d'},base | {'count':9},base | {'passed':False}):
            with self.assertRaises(ValueError):comparable(base,bad)

    def test_requested_recovery_cannot_be_missing(self):
        with self.assertRaises(ValueError):require_recovery(None,'flink',True)
        require_recovery(None,'flink',False)

    def test_recovery_rejects_nonfinite_duration_and_unknown_engine(self):
        proof = dict(before_outputs=10,after_outputs=20,input_before=40,input_after=60,
                     producer_active=True,seconds=float('nan'),same_application_id=True)
        with self.assertRaises(ValueError):verify_recovery(proof,'streams')
        with self.assertRaises(ValueError):verify_recovery(proof | {'seconds':1},'unknown')

    def test_failed_engine_does_not_wait_for_missing_output_timeout(self):
        ensure_job_state('RUNNING',False)
        ensure_job_state('RESTARTING',True)
        for state,expected in [('FAILED',False),('CANCELED',True),('RESTARTING',False)]:
            with self.assertRaises(ValueError):ensure_job_state(state,expected)
