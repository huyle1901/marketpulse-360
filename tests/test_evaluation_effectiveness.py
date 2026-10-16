"""Model a publication switch/rollback between independent authenticated reads."""
import copy
import io
import json
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

from marketpulse_eval.effectiveness import evaluate_app

BINDING={'identity':'a'*64,'proof_digest':'b'*64}
DASHBOARD={'certified':{'totals':[{'net_revenue_minor':'22193000'}],'provenance':BINDING},
           'streaming':{'status':'available','rows':[{}]*14}}


class DecisionTransport:
    def __init__(self,change=None):
        self.change=change;self.counts={}

    def open(self,request,timeout=30):
        path=urlsplit(request.full_url).path.removeprefix('/api/v1/')
        self.counts[path]=self.counts.get(path,0)+1
        if path=='auth/session':value={'role':'operator'}
        elif path in ('campaign-settings','alert-rules'):value=[]
        elif path=='lifecycle/segments':value=[{'segment_id':'saved'}]
        elif path.startswith('lifecycle/segments/saved/versions/'):value={'version':'1'}
        elif path=='dashboard':value=copy.deepcopy(DASHBOARD)
        elif path=='alerts':value=[{'evidence':{'kind':'budget_overrun'}}]
        elif path.startswith('budget-pacing/'):
            value={'spend_minor':'91461','forecast_minor':'91461','status':'complete','provenance':BINDING}
        elif path=='budget-simulation':value={'projected_net_minor':'35282','provenance':BINDING}
        elif path=='analysis/funnel':
            value={'totals':{'sessions':'1','page_view_sessions':'1'},'campaigns':[{}],'unit':'stage reach','provenance':BINDING}
        elif path=='analysis/attribution':
            value={'global_reconciliation':{'balanced':True},'models':[{'net_revenue_minor':'22193000'}]*3,'provenance':BINDING}
        elif path=='lifecycle/cohorts':
            value={'totals':{'profiles_total':'60'},'rows':[{}],'population':'governed','provenance':BINDING}
        elif path=='lifecycle/profiles':
            value={'rows':[{'profile_ref':'p','opportunities':{'high_value':True},'activation_eligible':True}],
                   'total':'1','purpose':'analytics','provenance':BINDING}
        elif path.endswith('/timeline'):
            value={'profile':{'confidence':'deterministic_customer_reference'},'events':[{}],'provenance':BINDING}
        elif path=='lifecycle/segments/preview':
            value={'evaluation_key':'same-key','matched_total':'1','rows':[{'activation_eligible':True}],'provenance':BINDING}
        else:raise AssertionError('unexpected test request '+path)
        value=copy.deepcopy(value)
        if self.change:self.change(path,self.counts[path],value)
        return io.BytesIO(json.dumps(value).encode())


class DecisionReleaseTests(unittest.TestCase):
    def run_proof(self,change=None):
        transport=DecisionTransport(change)
        with patch('marketpulse_eval.effectiveness.ProofHTTP',return_value=transport):
            return evaluate_app('http://proof.test','http://proof.test','operator',Path('unused'))

    def test_stable_release_proves_all_ten_read_only_decisions(self):
        self.assertEqual(len(self.run_proof()['decisions']),10)

    def test_dashboard_change_then_rollback_is_rejected(self):
        for field in ('identity','proof_digest'):
            def change(path,n,value):
                if path=='dashboard' and n==2:value['certified']['provenance'][field]='c'*64
            with self.subTest(field=field):
                with self.assertRaises(ValueError):self.run_proof(change)

    def test_each_segment_preview_is_checked_before_wrapping(self):
        for changed_read in (1,2):
            def change(path,n,value):
                if path=='lifecycle/segments/preview' and n==changed_read:value['provenance']['identity']='c'*64
            with self.subTest(read=changed_read):
                with self.assertRaises(ValueError):self.run_proof(change)

    def test_missing_preview_binding_is_rejected(self):
        def change(path,n,value):
            if path=='lifecycle/segments/preview':value.pop('provenance')
        with self.assertRaises(ValueError):self.run_proof(change)

    def test_timed_dashboard_proof_change_is_rejected(self):
        def change(path,n,value):
            if path=='dashboard' and n==3:value['certified']['provenance']['proof_digest']='c'*64
        with self.assertRaises(ValueError):self.run_proof(change)
