import importlib.util,unittest
from pathlib import Path
path=Path(__file__).resolve().parents[1]/'scripts/lifecycle-smoke-test.py'
spec=importlib.util.spec_from_file_location('lifecycle_proof',path)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class LifecycleProofTests(unittest.TestCase):
    def test_keyed_profiles_reject_duplicate_substitution_and_changed_consent(self):
        rows=[{'profile_ref':'a','order_count':'1','activation_eligible':True},{'profile_ref':'b','order_count':'0','activation_eligible':False}]
        module.assert_profiles(rows,rows,fields=('order_count','activation_eligible'))
        for bad in ([rows[0],rows[0]],[rows[0]],[rows[0]|{'activation_eligible':False},rows[1]]):
            with self.assertRaises(AssertionError):module.assert_profiles(rows,bad,fields=('order_count','activation_eligible'))

    def test_activation_assertion_requires_identified_and_both_true_flags(self):
        yes={'profile_type':'customer','consent_analytics':True,'crm_matched':True,'crm_consent_analytics':True,'crm_consent_marketing':True,'activation_eligible':True}
        module.assert_activation([yes])
        for change in ({'profile_type':'anonymous'},{'crm_matched':False},{'crm_consent_analytics':None},{'crm_consent_marketing':False}):
            with self.assertRaises(AssertionError):module.assert_activation([yes|change])

    def test_independent_rfm_uses_original_purchase_and_not_refund_gross_context(self):
        gold={'profile_key':'customer:c','customer_id':'c','profile_type':'customer','first_activity_at':'2026-01-01T00:00:00Z','last_activity_at':'2026-02-02T00:00:00Z','cohort_month':'2025-12-01','event_count':2,'session_count':1,'order_count':1,'refund_count':1,'gross_revenue_minor':9007199254740993,'refund_amount_minor':3,'net_revenue_minor':9007199254740990,'consent_analytics':True}
        events=[{'event_name':'order_created','event_time':'2026-01-01T00:00:00Z'},{'event_name':'order_refunded','event_time':'2026-02-02T00:00:00Z','gross_amount_minor':9007199254740993}]
        expected=module.independent_profile(gold,events,{'consent_analytics':True,'consent_marketing':True},'a'*64,'2026-02-03T00:00:00+00:00')
        self.assertEqual(expected['recency_days'],'33');self.assertEqual(expected['net_revenue_minor'],'9007199254740990')
        self.assertTrue(expected['opportunities']['churn_risk'])

    def test_privacy_assertion_checks_keys_not_the_allowed_email_source_value(self):
        module.assert_public_payload({'source':'email','profile':{'profile_ref':'a'*64}})
        with self.assertRaises(AssertionError):module.assert_public_payload({'nested':{'email':'synthetic@example.invalid'}})
