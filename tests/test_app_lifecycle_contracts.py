import unittest
from marketpulse_app.lifecycle_contracts import SegmentRule,Condition,month


def rule(**changes):
    return {'schema_version':'mp08-segment-v1','purpose':'activation','cohort_month':None,'activity_month':None,'profile_type':None,'conditions':[],**changes}

class SegmentContractTests(unittest.TestCase):
    def test_canonical_exact_values_and_order_independent_digest(self):
        predicates=[{'field':'net_revenue_minor','op':'gte','value':'-9007199254740993'},{'field':'recency_days','op':'lte','value':'30'}]
        first=SegmentRule.from_payload(rule(conditions=predicates));other=SegmentRule.from_payload(rule(conditions=predicates[::-1]))
        self.assertEqual(first.digest,other.digest)
        self.assertEqual(first.payload(),other.payload())
        self.assertEqual(first.payload()['conditions'][0]['value'],'-9007199254740993')
        self.assertEqual(len(first.digest),64)
        self.assertEqual(month('2026-01-01').isoformat(),'2026-01-01')

    def test_reject_unknown_and_noncanonical_contracts(self):
        invalid=[rule(extra=True),rule(purpose='all'),rule(schema_version='other'),rule(cohort_month='2026-01-02'),rule(activity_month='bad'),rule(profile_type='person')]
        for value in (True,1,'01','-0','+1','1e3',str(2**63)):
            invalid.append(rule(conditions=[{'field':'order_count','op':'gte','value':value}]))
        invalid.extend([rule(conditions=[{'field':'DROP TABLE','op':'gte','value':'1'}]),rule(conditions=[{'field':'order_count','op':'or','value':'1'}]),rule(conditions=[{'field':'order_count','op':'gte','value':'-1'}])])
        predicate={'field':'event_count','op':'gte','value':'1'}
        invalid.extend([rule(conditions=[predicate,predicate]),rule(conditions=[predicate]*9),rule(conditions=[predicate|{'sql':'bad'}])])
        for payload in invalid:
            with self.subTest(payload=payload),self.assertRaises(ValueError):SegmentRule.from_payload(payload)
