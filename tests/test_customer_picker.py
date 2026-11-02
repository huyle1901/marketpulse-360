import unittest
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

from marketpulse_mock.config import GenerationConfig, ScaleProfile
from marketpulse_mock.customer_picker import (
    DAYS_PER_MONTH,
    WEIGHT_SCALE,
    CustomerPicker,
    tenure_weight,
)
from marketpulse_mock.masters import generate_master_data


class TenureWeightTests(unittest.TestCase):
    def test_weight_follows_one_over_one_plus_months_over_six(self) -> None:
        self.assertEqual(WEIGHT_SCALE, tenure_weight(0, 6.0))
        half = tenure_weight(round(6 * DAYS_PER_MONTH), 6.0)
        third = tenure_weight(round(12 * DAYS_PER_MONTH), 6.0)
        self.assertAlmostEqual(WEIGHT_SCALE / 2, half, delta=WEIGHT_SCALE * 0.002)
        self.assertAlmostEqual(WEIGHT_SCALE / 3, third, delta=WEIGHT_SCALE * 0.002)
        self.assertGreater(tenure_weight(1, 6.0), half)
        self.assertGreater(half, third)

    def test_weight_never_reaches_zero(self) -> None:
        self.assertGreaterEqual(tenure_weight(10**9, 6.0), 1)


class PickerFrequencyTests(unittest.TestCase):
    def test_picks_follow_tenure_weights(self) -> None:
        profile = ScaleProfile(
            "picker", 2, 2, 3, 1, 30_000, 1_000, "vietnam-retail"
        )
        config = GenerationConfig(
            profile, 360, Path("unused"), date(2025, 1, 1), 1_000
        )
        fresh, veteran = generate_master_data(config).customers
        day = date(2025, 1, 2)
        fresh = replace(
            fresh, created_at=datetime(2025, 1, 1, tzinfo=timezone.utc)
        )
        veteran = replace(
            veteran, created_at=datetime(2024, 1, 2, tzinfo=timezone.utc)
        )
        picker = CustomerPicker.for_day(
            config, (fresh, veteran), day, session_ordinals=range(30_000)
        )
        self.assertEqual((), picker.joiners)
        picks = [picker.pick(1, ordinal) for ordinal in range(30_000)]
        known = [p for p in picks if p is not None]
        self.assertAlmostEqual(0.7, len(known) / len(picks), delta=0.02)
        weights = {
            fresh.customer_id: tenure_weight(1, 6.0),
            veteran.customer_id: tenure_weight(366, 6.0),
        }
        expected = weights[veteran.customer_id] / sum(weights.values())
        share = sum(p.customer_id == veteran.customer_id for p in known) / len(known)
        self.assertAlmostEqual(expected, share, delta=0.02)
        self.assertAlmostEqual(0.25, share, delta=0.02)
        self.assertAlmostEqual(0.25, expected, delta=0.01)


if __name__ == "__main__":
    unittest.main()
