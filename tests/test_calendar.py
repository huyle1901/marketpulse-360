from __future__ import annotations

import unittest
from datetime import date, timedelta, timezone

from marketpulse_mock.calendar import (
    HOURLY_WEIGHTS,
    VIETNAM,
    day_multiplier,
    is_double_day,
    is_promo_day,
    utc_hour_weights,
)


class DayMultiplierTests(unittest.TestCase):
    def test_constants(self) -> None:
        self.assertEqual(VIETNAM.utcoffset(None), timedelta(hours=7))
        self.assertIsInstance(VIETNAM, timezone)
        self.assertEqual(len(HOURLY_WEIGHTS), 24)
        self.assertEqual(HOURLY_WEIGHTS[0], 1.0)
        self.assertEqual(HOURLY_WEIGHTS[21], 4.4)

    def test_plain_weekday_is_one(self) -> None:
        self.assertEqual(day_multiplier(date(2025, 11, 12)), 1.0)

    def test_double_day(self) -> None:
        self.assertAlmostEqual(day_multiplier(date(2025, 11, 11)), 1.8)
        self.assertTrue(is_double_day(date(2025, 11, 11)))
        self.assertFalse(is_double_day(date(2025, 11, 12)))

    def test_weekend(self) -> None:
        self.assertAlmostEqual(day_multiplier(date(2025, 11, 15)), 1.15)
        self.assertAlmostEqual(day_multiplier(date(2025, 11, 16)), 1.15)

    def test_black_friday(self) -> None:
        self.assertAlmostEqual(day_multiplier(date(2025, 11, 28)), 1.6)

    def test_double_day_on_weekend_multiplies(self) -> None:
        self.assertAlmostEqual(day_multiplier(date(2026, 12, 12)), 1.8 * 1.15)

    def test_pre_tet(self) -> None:
        self.assertAlmostEqual(day_multiplier(date(2026, 2, 10)), 1.4)
        self.assertAlmostEqual(day_multiplier(date(2026, 2, 7)), 1.4 * 1.15)
        self.assertAlmostEqual(day_multiplier(date(2026, 2, 15)), 1.4 * 1.15)
        self.assertAlmostEqual(day_multiplier(date(2025, 1, 19)), 1.4 * 1.15)
        self.assertAlmostEqual(day_multiplier(date(2025, 1, 27)), 1.4)

    def test_day_before_pre_tet_window_is_plain(self) -> None:
        self.assertEqual(day_multiplier(date(2026, 2, 6)), 1.0)
        self.assertAlmostEqual(day_multiplier(date(2025, 1, 18)), 1.15)

    def test_tet_window_overrides_pre_tet(self) -> None:
        self.assertAlmostEqual(day_multiplier(date(2026, 2, 16)), 0.5)
        self.assertAlmostEqual(day_multiplier(date(2026, 2, 17)), 0.5)
        self.assertAlmostEqual(day_multiplier(date(2026, 2, 20)), 0.5)
        self.assertAlmostEqual(day_multiplier(date(2025, 1, 28)), 0.5)
        self.assertAlmostEqual(day_multiplier(date(2025, 1, 29)), 0.5)

    def test_tet_window_weekend_still_multiplies(self) -> None:
        self.assertAlmostEqual(day_multiplier(date(2025, 2, 1)), 0.5 * 1.15)

    def test_after_tet_window_is_plain(self) -> None:
        self.assertAlmostEqual(day_multiplier(date(2026, 2, 21)), 1.15)
        self.assertEqual(day_multiplier(date(2025, 2, 3)), 1.0)

    def test_2027_tet(self) -> None:
        self.assertAlmostEqual(day_multiplier(date(2027, 2, 6)), 0.5 * 1.15)
        self.assertAlmostEqual(day_multiplier(date(2027, 1, 27)), 1.4)


class PromoDayTests(unittest.TestCase):
    def test_double_days_are_promo(self) -> None:
        self.assertTrue(is_promo_day(date(2025, 11, 11)))
        self.assertTrue(is_promo_day(date(2026, 12, 12)))

    def test_three_days_before_tet_are_promo(self) -> None:
        for day in (date(2026, 2, 14), date(2026, 2, 15), date(2026, 2, 16)):
            self.assertTrue(is_promo_day(day), day)
        for day in (date(2025, 1, 26), date(2025, 1, 27), date(2025, 1, 28)):
            self.assertTrue(is_promo_day(day), day)

    def test_other_days_are_not_promo(self) -> None:
        for day in (date(2026, 2, 13), date(2026, 2, 17), date(2025, 1, 25), date(2025, 11, 28)):
            self.assertFalse(is_promo_day(day), day)


class UtcHourWeightsTests(unittest.TestCase):
    def test_length_and_plain_day(self) -> None:
        weights = utc_hour_weights(date(2025, 11, 12))
        self.assertEqual(len(weights), 24)
        # UTC 00 is VN 07 on 11/12 (multiplier 1.0 on a Wednesday).
        self.assertAlmostEqual(weights[0], HOURLY_WEIGHTS[7])
        # UTC 17 is VN 00 on 11/13.
        self.assertAlmostEqual(weights[17], HOURLY_WEIGHTS[0])

    def test_utc_evening_uses_next_vn_date_multiplier(self) -> None:
        weights = utc_hour_weights(date(2025, 11, 10))
        for h in range(17, 24):
            self.assertAlmostEqual(weights[h], HOURLY_WEIGHTS[h - 17] * 1.8)
        for h in range(0, 17):
            self.assertAlmostEqual(weights[h], HOURLY_WEIGHTS[h + 7] * day_multiplier(date(2025, 11, 10)))


if __name__ == "__main__":
    unittest.main()
