from __future__ import annotations

import dataclasses
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from marketpulse_mock.config import (
    DEFAULT_ANOMALY_RATES,
    PROFILES,
    ScaleProfile,
    resolve_config,
)


class ScaleProfileTests(unittest.TestCase):
    def test_named_profiles_match_the_version_one_contract(self) -> None:
        expected = {
            "fixture": (2, 100, 6, 50, 500, 1_000),
            "smoke": (3, 2_000, 12, 200, 10_000, 50_000),
            "demo": (14, 50_000, 30, 1_000, 150_000, 100_000),
            "benchmark": (30, 250_000, 60, 5_000, 1_000_000, 250_000),
            "story-demo": (638, 1_000, 12, 100, 2_000, 50_000),
        }

        actual = {
            name: (
                profile.days,
                profile.customers,
                profile.campaigns,
                profile.products,
                profile.impressions_per_day,
                profile.rows_per_file,
            )
            for name, profile in PROFILES.items()
        }

        self.assertEqual(expected, actual)

    def test_profiles_are_immutable(self) -> None:
        with self.assertRaises(dataclasses.FrozenInstanceError):
            PROFILES["fixture"].days = 10  # type: ignore[misc]

    def test_profile_rejects_non_positive_capacities(self) -> None:
        with self.assertRaisesRegex(ValueError, "customers must be positive"):
            ScaleProfile(
                name="invalid",
                days=1,
                customers=0,
                campaigns=3,
                products=1,
                impressions_per_day=1,
                rows_per_file=1,
            )

    def test_campaign_count_must_cover_platforms_evenly(self) -> None:
        with self.assertRaisesRegex(ValueError, "campaigns must be divisible by 3"):
            ScaleProfile(
                name="invalid",
                days=1,
                customers=1,
                campaigns=4,
                products=1,
                impressions_per_day=1,
                rows_per_file=1,
            )


class GenerationConfigTests(unittest.TestCase):
    def test_story_demo_is_one_world_across_history_and_replay(self) -> None:
        config = resolve_config("story-demo", seed=360, output_dir=Path("data/generated/story"))
        self.assertEqual(date(2025, 1, 1), config.start_date)
        self.assertEqual(date(2026, 9, 1), config.start_date + timedelta(days=608))
        self.assertEqual(date(2026, 9, 30), config.start_date + timedelta(days=config.profile.days - 1))
        with self.assertRaisesRegex(ValueError, "fixed UTC world"):
            resolve_config("story-demo", seed=360, output_dir=Path("data/generated/story"), start_date=date(2026, 9, 1))

    def test_resolve_config_uses_fixed_utc_calendar_start(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            config = resolve_config(
                "fixture", seed=360, output_dir=Path(temporary_directory)
            )

        self.assertEqual(date(2026, 1, 1), config.start_date)
        self.assertEqual(360, config.seed)
        self.assertEqual("fixture", config.profile.name)
        self.assertEqual(1_000, config.rows_per_file)

    def test_resolve_config_accepts_start_and_rotation_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            config = resolve_config(
                "smoke",
                seed=42,
                output_dir=Path(temporary_directory),
                rows_per_file=123,
                start_date=date(2026, 9, 1),
            )

        self.assertEqual(date(2026, 9, 1), config.start_date)
        self.assertEqual(123, config.rows_per_file)

    def test_resolve_config_rejects_unknown_profile(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown profile"):
            resolve_config("huge", seed=1, output_dir=Path("output"))

    def test_resolve_config_rejects_non_positive_rotation_limit(self) -> None:
        with self.assertRaisesRegex(ValueError, "rows_per_file must be positive"):
            resolve_config(
                "fixture",
                seed=1,
                output_dir=Path("output"),
                rows_per_file=0,
            )

    def test_default_anomaly_rates_match_the_specification(self) -> None:
        self.assertEqual(0.005, DEFAULT_ANOMALY_RATES.duplicate_event)
        self.assertEqual(0.01, DEFAULT_ANOMALY_RATES.late_event)
        self.assertEqual(0.002, DEFAULT_ANOMALY_RATES.invalid_case)
        self.assertEqual(0.05, DEFAULT_ANOMALY_RATES.no_analytics_consent)
        self.assertEqual(0.002, DEFAULT_ANOMALY_RATES.bot_session)
        self.assertEqual(0.05, DEFAULT_ANOMALY_RATES.refund)

    def test_generation_config_is_immutable(self) -> None:
        config = resolve_config("fixture", seed=360, output_dir=Path("output"))

        with self.assertRaises(dataclasses.FrozenInstanceError):
            config.seed = 10  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
