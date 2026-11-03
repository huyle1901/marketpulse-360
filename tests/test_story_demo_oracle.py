import importlib.util
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "story_demo_oracle", ROOT / "scripts" / "story_demo_oracle.py"
)
oracle = importlib.util.module_from_spec(SPEC)
sys.modules["story_demo_oracle"] = oracle
SPEC.loader.exec_module(oracle)


def _totals(**overrides):
    values = {
        "impressions": {("google", "2026-01"): 84},
        "clicks": {("google", "2026-01"): 4},
        "spend_minor": {("google", "2026-01"): 50_559},
        "orders": {("2026-01",): 3},
        "gross_minor": {("2026-01",): 5_337_000},
        "refunds_minor": {("2026-01",): 0},
        "city_gross_minor": {("Ha Noi",): 5_337_000},
    }
    values.update(overrides)
    return oracle.Totals(**values)


class CompareTests(unittest.TestCase):
    def test_equal_totals_give_only_equal_checks(self) -> None:
        checks = oracle.compare(_totals(), _totals())

        self.assertTrue(checks)
        self.assertTrue(all(check.equal for check in checks))
        self.assertTrue(all(check.reason is None for check in checks))

    def test_one_minor_unit_of_spend_is_a_failed_check(self) -> None:
        certified = _totals(spend_minor={("google", "2026-01"): 50_560})

        failed = [check for check in oracle.compare(_totals(), certified) if not check.equal]

        self.assertEqual(1, len(failed))
        self.assertEqual("spend_minor google 2026-01", failed[0].name)
        self.assertEqual((50_559, 50_560), (failed[0].expected, failed[0].actual))

    def test_governed_checks_compare_the_governed_truth_and_explain_the_gap(self) -> None:
        raw = _totals(orders={("2026-01",): 4})
        governed = _totals(orders={("2026-01",): 3})
        certified = _totals(orders={("2026-01",): 3})

        checks = oracle.compare(
            raw,
            certified,
            governed=governed,
            reasons={"orders": "bot-session orders are excluded"},
        )
        orders = next(check for check in checks if check.name == "orders 2026-01")

        self.assertTrue(orders.equal)
        self.assertEqual((3, 3), (orders.expected, orders.actual))
        self.assertEqual(4, orders.raw)
        self.assertEqual("bot-session orders are excluded", orders.reason)

    def test_governed_gap_without_a_reason_is_reported(self) -> None:
        checks = oracle.compare(
            _totals(orders={("2026-01",): 4}),
            _totals(orders={("2026-01",): 3}),
            governed=_totals(orders={("2026-01",): 3}),
        )
        orders = next(check for check in checks if check.name == "orders 2026-01")

        self.assertEqual("unexplained raw−governed gap", orders.reason)

    def test_ungoverned_measures_never_use_the_governed_truth(self) -> None:
        checks = oracle.compare(
            _totals(),
            _totals(),
            governed=_totals(spend_minor={("google", "2026-01"): 1}),
        )

        self.assertTrue(all(check.equal for check in checks))

    def test_a_key_missing_on_one_side_compares_against_zero(self) -> None:
        certified = _totals(spend_minor={("google", "2026-01"): 50_559, ("shopee", "2026-01"): 7})

        failed = [check for check in oracle.compare(_totals(), certified) if not check.equal]

        self.assertEqual(["spend_minor shopee 2026-01"], [check.name for check in failed])
        self.assertEqual((0, 7), (failed[0].expected, failed[0].actual))

    def test_measures_the_certified_side_does_not_serve_are_not_compared(self) -> None:
        truth = _totals(sessions={("organic",): 5})

        checks = oracle.compare(truth, _totals())

        self.assertFalse([check for check in checks if check.name.startswith("sessions")])
        self.assertEqual(("sessions",), oracle.uncompared(truth, _totals()))

    def test_money_must_be_integer_minor_units(self) -> None:
        with self.assertRaises(TypeError):
            oracle.Totals(spend_minor={("google", "2026-01"): 1.5})

    def test_markdown_table_lists_every_check(self) -> None:
        table = oracle.markdown(oracle.compare(_totals(), _totals()))

        self.assertIn("| Check | Expected | Certified | Equal | Reason |", table)
        self.assertIn("| spend_minor google 2026-01 | 50,559 | 50,559 | yes |  |", table)


class GroundTruthTests(unittest.TestCase):
    def test_fixture_totals_are_parsed_from_the_generator_files(self) -> None:
        totals = oracle.ground_truth_totals(ROOT / "data" / "mock" / "fixtures" / "seed-360")

        self.assertEqual({"google", "meta", "tiktok"}, {key[0] for key in totals.spend_minor})
        self.assertEqual({"2026-01"}, {key[1] for key in totals.spend_minor})
        self.assertGreater(sum(totals.impressions.values()), 0)
        self.assertGreater(sum(totals.orders.values()), 0)
        # Duplicated anomaly rows count once: orders are distinct order ids.
        self.assertEqual(sum(totals.orders.values()), totals.distinct_orders)
        self.assertEqual(sum(totals.gross_minor.values()), sum(totals.city_gross_minor.values()))
        # The flat fixture predates traffic_source and email_type.
        self.assertEqual({("paid",)}, set(totals.sessions))
        self.assertEqual({"untyped"}, {key[0] for key in totals.email_sends})

    def test_through_limits_every_measure_to_the_certified_days(self) -> None:
        root = ROOT / "data" / "mock" / "fixtures" / "seed-360"
        full = oracle.ground_truth_totals(root)
        first_day = oracle.ground_truth_totals(root, through=date(2026, 1, 1))

        self.assertLess(sum(first_day.impressions.values()), sum(full.impressions.values()))
        self.assertLess(sum(first_day.orders.values()), sum(full.orders.values()))


    def test_governed_totals_drop_only_consent_denied_orders(self) -> None:
        root = ROOT / "data" / "mock" / "fixtures" / "seed-360"
        raw = oracle.ground_truth_totals(root)
        governed = oracle.governed_totals(root)
        denied = oracle.consent_denied_orders(root)

        self.assertGreater(denied, 0)
        self.assertEqual(sum(raw.orders.values()) - denied, sum(governed.orders.values()))
        self.assertLess(sum(governed.gross_minor.values()), sum(raw.gross_minor.values()))
        self.assertEqual(sum(governed.gross_minor.values()), sum(governed.city_gross_minor.values()))
        # Ungoverned measures are the generator's ground truth either way.
        self.assertEqual(raw.spend_minor, governed.spend_minor)

if __name__ == "__main__":
    unittest.main()
