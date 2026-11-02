import tempfile
import unittest
from collections import Counter
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

from marketpulse_mock.config import (
    PROFILES,
    SCENARIOS,
    GenerationConfig,
    Scenario,
    resolve_config,
)
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


class StoryCampaignTests(unittest.TestCase):
    def test_story_has_four_campaigns_per_platform(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_config("story-demo", 360, Path(tmp))
        masters = generate_master_data(config)
        self.assertEqual(len(masters.campaigns), 16)
        counts = Counter(c.platform for c in masters.campaigns)
        self.assertEqual(
            counts, {"google": 4, "meta": 4, "tiktok": 4, "shopee": 4}
        )
        shopee = [c for c in masters.campaigns if c.platform == "shopee"]
        self.assertEqual(
            [c.platform_campaign_id for c in shopee],
            [f"S-360-{n:05d}" for n in (4, 8, 12, 16)],
        )
        by_campaign = Counter(a.campaign_id for a in masters.ads)
        self.assertEqual(set(by_campaign.values()), {2})
        shopee_ids = {c.campaign_id for c in shopee}
        shopee_ads = [a for a in masters.ads if a.campaign_id in shopee_ids]
        self.assertEqual(len(shopee_ads), 8)
        for ad in shopee_ads:
            self.assertTrue(ad.platform_ad_id.startswith("SA-360-"))

    def test_flat_keeps_three_platforms(self) -> None:
        self.assertEqual(
            SCENARIOS["flat"].platforms, ("google", "meta", "tiktok")
        )
        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_config("fixture", 360, Path(tmp))
        campaigns = generate_master_data(config).campaigns
        self.assertEqual(
            [c.platform for c in campaigns],
            ["google", "meta", "tiktok"] * 2,
        )
        self.assertEqual(campaigns[0].platform_campaign_id, "G-360-00001")

    def test_story_scenario_platform_weights_and_personas(self) -> None:
        story = SCENARIOS["vietnam-retail"]
        self.assertEqual(
            story.platforms, ("google", "meta", "tiktok", "shopee")
        )
        self.assertEqual(
            dict(story.platform_spend_share),
            {"google": 30, "meta": 28, "tiktok": 24, "shopee": 18},
        )
        self.assertEqual(
            dict(story.objective_spend_weight),
            {"conversions": 1.3, "traffic": 1.0, "awareness": 0.7},
        )
        self.assertEqual(story.personas["shopee"], (1.0, 1.1, 1.6))
        self.assertEqual(story.personas["tiktok"], (0.6, 1.6, 0.5))
        self.assertIsNone(SCENARIOS["flat"].platform_spend_share)

    def test_campaigns_must_cover_platforms_evenly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "divisible by 4"):
                GenerationConfig(
                    profile=replace(PROFILES["story-demo"], campaigns=12 + 2),
                    seed=1,
                    output_dir=Path(tmp),
                    start_date=date(2025, 1, 1),
                    rows_per_file=10,
                )

    def test_scenario_rejects_bad_platform_settings(self) -> None:
        with self.assertRaisesRegex(ValueError, "platform_spend_share"):
            Scenario("x", platform_spend_share={"google": 0})
        with self.assertRaisesRegex(ValueError, "platform_spend_share"):
            Scenario("x", platform_spend_share={"bing": 5})
        with self.assertRaisesRegex(ValueError, "personas"):
            Scenario("x", personas={"google": (1.0, 1.0)})
        with self.assertRaisesRegex(ValueError, "personas"):
            Scenario("x", personas={"google": (1.0, 0.0, 1.0)})
        with self.assertRaisesRegex(ValueError, "objective_spend_weight"):
            Scenario("x", objective_spend_weight={"traffic": -1.0})


if __name__ == "__main__":
    unittest.main()
