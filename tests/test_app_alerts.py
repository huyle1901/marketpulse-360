import unittest
from datetime import date
from decimal import Decimal

from marketpulse_app.alerts import build_evaluations
from marketpulse_app.contracts import AlertRule, CampaignSetting


class AlertEvidenceTests(unittest.TestCase):
    def test_global_day_missing_baseline_and_version_identity(self):
        campaigns = [{"campaign_id": "a", "currency": "VND"}, {"campaign_id": "b", "currency": "VND"}]
        daily = [dict(campaign_id="a", currency="VND", event_date=date(2026, 1, 1), spend_minor=100,
                      gross_revenue_minor=10, refund_amount_minor=1),
                 dict(campaign_id="b", currency="VND", event_date=date(2026, 1, 2), spend_minor=100,
                      gross_revenue_minor=10, refund_amount_minor=1)]
        rules = [AlertRule("a", "a", "refund_share", Decimal(0)), AlertRule("b", "b", "refund_share", Decimal(0))]
        results = build_evaluations({"identity": "a"*64}, campaigns, daily, [], rules, date(2026, 1, 3))
        self.assertEqual([row[1]["outcome"] for row in results], ["insufficient_data", "triggered"])
        self.assertEqual(results[1][1]["from_date"], "2026-01-02")
        repeated = build_evaluations({"identity": "a"*64}, campaigns, daily, [], rules, date(2026, 1, 4))
        self.assertEqual([row[0] for row in results], [row[0] for row in repeated])
        edited = [AlertRule("b", "b", "refund_share", Decimal("0.2"), version=2)]
        changed = build_evaluations({"identity": "a"*64}, campaigns, daily, [], edited, date(2026, 1, 3))
        self.assertNotEqual(changed[0][0], results[1][0])

    def test_budget_uses_entire_period_and_missing_date_is_insufficient(self):
        setting = CampaignSetting("a", "VN", "VND", date(2026, 1, 1), date(2026, 1, 3), 100, 1)
        rule = AlertRule("r", "a", "budget_overrun", Decimal(1), version=1)
        row = dict(campaign_id="a",currency="VND",event_date=date(2026, 1, 1),spend_minor=50)
        results = build_evaluations({"identity": "a"*64}, [{"campaign_id":"a","currency":"VND"}], [row], [setting], [rule], date(2026, 1, 3))
        self.assertEqual(results[0][1]["outcome"], "insufficient_data")
        daily = [row, row | {"event_date": date(2026, 1, 2), "spend_minor": 250}]
        result = build_evaluations({"identity": "a"*64}, [{"campaign_id":"a","currency":"VND"}], daily, [setting], [rule], date(2026, 1, 3))[0][1]
        self.assertEqual(result["outcome"], "triggered")
        self.assertEqual(result["operands"]["spend_minor"], "300")

    def test_active_period_overrun_and_day_transition_have_distinct_identity(self):
        setting = CampaignSetting("a", None, "VND", date(2026,1,1), date(2026,1,3), 100, 1)
        rule = AlertRule("r", "a", "budget_overrun", Decimal(1), version=1)
        campaigns=[{"campaign_id":"a","currency":"VND"}]
        daily=[dict(campaign_id="a",currency="VND",event_date=date(2026,1,1),spend_minor=200),
               dict(campaign_id="a",currency="VND",event_date=date(2026,1,2),spend_minor=100)]
        first=build_evaluations({"identity":"a"*64},campaigns,daily,[setting],[rule],date(2026,1,2))[0]
        second=build_evaluations({"identity":"a"*64},campaigns,daily,[setting],[rule],date(2026,1,3))[0]
        self.assertEqual(first[1]["outcome"],"triggered")
        self.assertEqual(first[1]["to_date"],"2026-01-02")
        self.assertNotEqual(first[0],second[0])
