import tempfile
import unittest
from pathlib import Path
from marketpulse_eval.package import acceptance_status, validate_runs, verify_index
from marketpulse_eval.effectiveness import TASKS, check_decision

class PackageTests(unittest.TestCase):
    def valid_runs(self):
        from marketpulse_eval.workload import STREAM_SCALES
        from marketpulse_eval.analytics import ANALYTICS_SCALES
        resource={'worker':{'samples':2,'cpu_seconds_observed':1,'peak_sampled_working_set_bytes':100,'coverage_seconds':1}}
        timing={'p50_ms':1,'p95_ms':2,'p99_ms':3,'samples':100}
        runs=[]
        for engine in ('flink','streams'):
            for scale in ('eval-small','eval-medium'):
                runs.append({'version':1,'kind':'stream','engine':engine,'scale':scale,'recovery':None,
                             'correctness':{'passed':True,'count':STREAM_SCALES[scale],'input_hash':'a'*64,'output_hash':'b'*64},
                             'resources':resource,'latency':timing | {'samples':STREAM_SCALES[scale]}})
            recovery=dict(before_outputs=100,after_outputs=200,input_before=300,input_after=400,
                          producer_active=True,seconds=3,completed_checkpoint=1,restored_checkpoint=1,same_application_id=True)
            runs.append(runs[-2] | {'recovery':recovery})
        for engine in ('clickhouse','postgres'):
            for scale in ('analytics-1m','analytics-10m','analytics-20m'):
                runs.append({'version':1,'kind':'analytics','engine':engine,'scale':scale,'row_count':ANALYTICS_SCALES[scale],
                             'correctness':{'passed':True},'resources':resource,
                             'workloads':{k:timing | {'warmups':20,'failures':0} for k in ('platform','campaign_day','filtered')}})
        runs.append({'version':1,'kind':'app','role':'operator','human_study':'pending','operational_configuration_preserved':True,
                     'dashboard_http':timing | {'warmups':20},
                     'decisions':[{'id':r['id'],'passed':True,'mode':'automated_api_decision_readiness','seconds':1} for r in TASKS]})
        return runs

    def test_external_acceptance_cannot_be_invented(self):
        result = acceptance_status(None)
        self.assertEqual(result['human_study'],'pending')
        self.assertEqual(result['narrated_video'],'pending')
        self.assertEqual(result['group_coverage'],'unconfirmed')
        self.assertEqual(acceptance_status(5)['group_coverage'],'covered')
        self.assertEqual(acceptance_status(6)['group_coverage'],'insufficient')
        with self.assertRaises(ValueError):acceptance_status(True)

    def test_missing_benchmarks_fail_package(self):
        with self.assertRaises(ValueError):validate_runs([])

    def test_all_runs_pass_but_missing_one_scale_fails(self):
        runs=self.valid_runs();self.assertEqual(validate_runs(runs)['technical_evidence'],'passed')
        with self.assertRaises(ValueError):validate_runs(runs[:-2]+runs[-1:])

    def test_wrong_scale_invalid_latency_or_mutated_app_cannot_pass(self):
        for change in ({'correctness':{'passed':True,'count':10,'input_hash':'a'*64,'output_hash':'b'*64}},
                       {'latency':{'samples':10000,'p50_ms':float('nan'),'p95_ms':1,'p99_ms':1}}):
            runs=self.valid_runs();runs[0]=runs[0] | change
            with self.assertRaises(ValueError):validate_runs(runs)
        for change in ({'operational_configuration_preserved':False},{'human_study':'passed'}):
            runs=self.valid_runs();runs[-1]=runs[-1] | change
            with self.assertRaises(ValueError):validate_runs(runs)

    def test_ten_requirements_and_wrong_decision_fail(self):
        self.assertEqual(len(TASKS),10)
        self.assertEqual(len({r['id'] for r in TASKS}),10)
        with self.assertRaises(ValueError):check_decision('live_kpi',{})
        with self.assertRaises(ValueError):check_decision('unknown',{})

    def test_index_verifies_content_and_refuses_escape(self):
        with tempfile.TemporaryDirectory() as root:
            p=Path(root); (p/'a.txt').write_text('altered')
            with self.assertRaises(ValueError):verify_index(p,{'a.txt':'wronghash'})
            with self.assertRaises(ValueError):verify_index(p,{'../elsewhere':'wronghash'})
