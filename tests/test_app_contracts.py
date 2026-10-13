import unittest
from datetime import date
from decimal import Decimal

from marketpulse_app.contracts import AlertRule, CampaignSetting, DashboardFilter


class AppContractTests(unittest.TestCase):
    def test_intervals_enums_and_market_are_validated(self):
        for end in (date(2026, 1, 1), date(2027, 1, 3)):
            with self.assertRaises(ValueError):
                DashboardFilter(date(2026, 1, 1), end)
        for kwargs in ({"platform": "google_ads"}, {"objective": "wrong"}, {"market": "vn"}):
            with self.assertRaises(ValueError):
                DashboardFilter(date(2026, 1, 1), date(2026, 1, 3), **kwargs)

    def test_setting_money_strict_and_exact(self):
        args = ("campaign-a", "VN", "VND", date(2026, 1, 1), date(2026, 1, 11))
        for amount in (True, 1.5, 0, -1, 2**63):
            with self.assertRaises(ValueError):
                CampaignSetting(*args, amount)
        self.assertEqual(CampaignSetting(*args, 9007199254740993).budget_minor, 9007199254740993)

    def test_rule_kind_and_threshold(self):
        for kind, threshold in (("unknown", "1"), ("spend_spike", "1"), ("refund_share", "1.1"), ("refund_share", "NaN")):
            with self.assertRaises(ValueError):
                AlertRule("a", "campaign-a", kind, Decimal(threshold))
