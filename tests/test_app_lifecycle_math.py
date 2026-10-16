import unittest
from datetime import datetime,timezone
from marketpulse_app.lifecycle_math import lifecycle_metrics,identity_evidence,activation_eligible,matches_condition
from marketpulse_app.lifecycle_contracts import Condition

class LifecycleMetricTests(unittest.TestCase):
    def test_original_purchase_recency_and_exact_observed_net(self):
        result=lifecycle_metrics({'order_count':1,'net_revenue_minor':9007199254740993},
            {'last_purchase_at':datetime(2026,1,1,tzinfo=timezone.utc),'add_to_cart_count':2,'email_click_count':1},datetime(2026,2,2,tzinfo=timezone.utc))
        self.assertEqual(result['recency_days'],32)
        self.assertTrue(result['opportunities']['high_value'])
        self.assertTrue(result['opportunities']['churn_risk'])
        self.assertFalse(result['opportunities']['cart_opportunity'])
        self.assertEqual(result['email_click_count'],1)

    def test_never_purchased_and_refund_heavy_customer_are_not_fabricated(self):
        as_of=datetime(2026,1,3,tzinfo=timezone.utc)
        result=lifecycle_metrics({'order_count':0,'net_revenue_minor':0},{'last_purchase_at':None,'add_to_cart_count':1,'email_click_count':0},as_of)
        self.assertIsNone(result['recency_days']);self.assertTrue(result['opportunities']['cart_opportunity'])
        self.assertFalse(matches_condition(result,Condition('recency_days','gte','0')))
        refund=lifecycle_metrics({'order_count':1,'net_revenue_minor':-100},{'last_purchase_at':datetime(2026,1,1,tzinfo=timezone.utc),'add_to_cart_count':0,'email_click_count':0},as_of)
        self.assertEqual(refund['recency_days'],2);self.assertFalse(refund['opportunities']['high_value'])

    def test_confidence_and_activation_gate_fail_closed(self):
        allowed={'profile_type':'customer','consent_analytics':True,'crm_matched':True,'crm_consent_analytics':True,'crm_consent_marketing':True}
        self.assertTrue(activation_eligible(allowed))
        for key in ('crm_matched','consent_analytics','crm_consent_analytics','crm_consent_marketing'):
            for value in (None,False,1):self.assertFalse(activation_eligible(allowed|{key:value}))
        for kind,confidence in (('customer','deterministic_customer_reference'),('anonymous','anonymous_device_only'),('session','session_only')):
            self.assertEqual(identity_evidence(kind)['confidence'],confidence)
            if kind!='customer':self.assertFalse(activation_eligible(allowed|{'profile_type':kind}))
