import unittest
from datetime import date
from decimal import Decimal

from marketpulse_app.contracts import AlertRule, CampaignSetting
from marketpulse_app.math import evaluate_rule, pacing, simulate


class AppMathTests(unittest.TestCase):
    def setting(self):
        return CampaignSetting("a", None, "VND", date(2026, 1, 1), date(2026, 1, 11), 1000)

    def test_contiguous_pacing_math(self):
        result = pacing(self.setting(), {date(2026, 1, 1): 100, date(2026, 1, 2): 200}, date(2026, 1, 3))
        self.assertEqual(result["expected_spend_minor"], "200.000000")
        self.assertEqual(result["remaining_minor"], "700")
        self.assertEqual(result["forecast_minor"], "1500")
        self.assertEqual(result["pace_ratio"], "1.500000")

    def test_gap_future_tail_and_ended_period(self):
        gap = pacing(self.setting(), {date(2026, 1, 1): 100, date(2026, 1, 3): 200}, date(2026, 1, 4))
        self.assertEqual(gap["status"], "insufficient_data")
        self.assertEqual(gap["spend_minor"], "300")
        self.assertIsNone(gap["forecast_minor"])
        self.assertIsNone(pacing(self.setting(), {}, date(2025, 12, 31))["forecast_minor"])
        tail = pacing(self.setting(), {date(2026, 1, 1): 100}, date(2026, 1, 4))
        self.assertEqual(tail["status"], "incomplete")
        self.assertEqual(tail["forecast_minor"], "1000")
        full = {date(2026, 1, day): 100 for day in range(1, 11)}
        self.assertEqual(pacing(self.setting(), full, date(2026, 2, 1))["forecast_minor"], "1000")

    def test_scenarios_exact_negative_and_unknown_baselines(self):
        result = simulate({"a": (3, 6)}, {"a": 9007199254740993}, 9007199254740993)
        self.assertEqual(result["projected_net_minor"], "18014398509481986")
        self.assertEqual(simulate({"a": (10, -5)}, {"a": 100}, 100)["projected_net_minor"], "-50")
        self.assertIsNone(simulate({"a": (0, 10)}, {"a": 100}, 100)["projected_net_minor"])
        result = simulate({"a": (0, 10), "b": (10, 20)}, {"a": 0, "b": 100}, 100)
        self.assertEqual(result["projected_net_minor"], "200")
        for allocations, total in (({"a": 1}, 2), ({"a": -1}, 1), ({"a": True}, 1)):
            with self.assertRaises(ValueError):
                simulate({"a": (10, 20)}, allocations, total)

    def test_alert_thresholds_and_insufficient_operands(self):
        rule = AlertRule("r", "a", "refund_share", Decimal("0.05"))
        self.assertEqual(evaluate_rule(rule, {"gross_revenue_minor": 10, "refund_amount_minor": 1})["outcome"], "triggered")
        self.assertEqual(evaluate_rule(rule, {"gross_revenue_minor": 0, "refund_amount_minor": 1})["outcome"], "insufficient_data")
        spike = AlertRule("s", "a", "spend_spike", Decimal(2))
        self.assertEqual(evaluate_rule(spike, {"spend_minor": 300, "previous_spend_minor": 100})["outcome"], "triggered")
        self.assertEqual(evaluate_rule(spike, {"spend_minor": 300})["outcome"], "insufficient_data")
        budget = AlertRule("b", "a", "budget_overrun", Decimal(1))
        self.assertEqual(evaluate_rule(budget, {"spend_minor": 100, "budget_minor": 100})["outcome"], "clear")
