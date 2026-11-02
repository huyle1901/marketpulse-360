import tempfile
import unittest
from collections import Counter
from datetime import timedelta
from pathlib import Path

from marketpulse_mock.config import SCENARIOS, resolve_config
from marketpulse_mock.masters import generate_master_data


class StoryCustomerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cls.config = resolve_config("story-demo", 360, Path(tmp))
        cls.customers = generate_master_data(cls.config).customers

    def test_city_shares_follow_the_weights(self) -> None:
        counts = Counter(c.city for c in self.customers)
        total = len(self.customers)
        self.assertEqual(total, 3000)
        for _country, city, weight in SCENARIOS["vietnam-retail"].cities:
            share = counts[city] / total
            self.assertLessEqual(abs(share * 100 - weight), 3, city)

    def test_forty_percent_joined_before_the_window(self) -> None:
        start = self.config.start_date
        last_day = start + timedelta(days=self.config.profile.days - 1)
        dates = [c.created_at.date() for c in self.customers]
        share = sum(d < start for d in dates) / len(dates)
        self.assertTrue(0.37 <= share <= 0.43, share)
        self.assertGreaterEqual(min(dates), start - timedelta(days=730))
        self.assertLessEqual(max(dates), last_day)

    def test_joins_ramp_up(self) -> None:
        start = self.config.start_date
        days = self.config.profile.days
        first = last = 0
        for c in self.customers:
            offset = (c.created_at.date() - start).days
            if 0 <= offset < 60:
                first += 1
            elif days - 60 <= offset < days:
                last += 1
        in_window = sum(
            0 <= (c.created_at.date() - start).days < days
            for c in self.customers
        )
        self.assertTrue(0.55 <= in_window / len(self.customers) <= 0.65)
        self.assertGreater(first, 0)
        self.assertGreaterEqual(last, 1.6 * first)

    def test_a_shorter_window_is_the_start_of_the_full_story(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            window = resolve_config("story-demo", 360, Path(tmp), days=30)
        customers = generate_master_data(window).customers
        self.assertEqual(
            [c.created_at for c in self.customers],
            [c.created_at for c in customers],
        )
        start = window.start_date
        joins = sum(0 <= (c.created_at.date() - start).days < 30 for c in customers)
        # 1,800 joiners over 638 days, starting at ~1.9 a day.
        self.assertTrue(30 <= joins <= 90, joins)


if __name__ == "__main__":
    unittest.main()
