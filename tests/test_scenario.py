import json
import tempfile
import unittest
from pathlib import Path

from marketpulse_mock.config import SCENARIOS, ScaleProfile, Scenario, resolve_config
from marketpulse_mock.generator import generate_dataset

FIXTURE_MANIFEST = Path("data/mock/fixtures/seed-360/manifest.json")


class ScenarioTests(unittest.TestCase):
    def test_flat_profiles_reproduce_the_committed_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "fixture"
            generate_dataset(resolve_config("fixture", 360, out))
            produced = json.loads((out / "manifest.json").read_text())
        committed = json.loads(FIXTURE_MANIFEST.read_text())
        def summarize(manifest: dict) -> dict[str, tuple[str, int]]:
            return {
                f["relative_path"]: (f["sha256"], f["row_count"])
                for f in manifest["files"]
            }

        self.assertEqual(summarize(produced), summarize(committed))
        self.assertEqual(produced["totals"], committed["totals"])

    def test_story_demo_uses_vietnam_retail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_config("story-demo", 360, Path(tmp))
        self.assertEqual(config.scenario.name, "vietnam-retail")
        self.assertEqual(config.profile.customers, 3000)
        self.assertEqual(config.profile.impressions_per_day, 4000)

    def test_days_override_shortens_the_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_config("story-demo", 360, Path(tmp), days=30)
            self.assertEqual(config.profile.days, 30)
            self.assertEqual(config.profile.story_days, 638)
            self.assertEqual(config.profile.full_days, 638)
            for good in (1, 638):
                window = resolve_config("story-demo", 360, Path(tmp), days=good)
                self.assertEqual(window.profile.days, good)
            for bad in (0, 639):
                with self.assertRaises(ValueError):
                    resolve_config("story-demo", 360, Path(tmp), days=bad)

    def test_scenario_defaults_are_neutral(self) -> None:
        self.assertEqual(Scenario("plain").join_ramp, 1.0)
        self.assertEqual(sum(w for _, _, w in SCENARIOS["vietnam-retail"].cities), 100)

    def test_scenario_rejects_out_of_range_values(self) -> None:
        bad = (
            {"customers_before_start_share": 1.5},
            {"customers_before_start_share": -0.1},
            {"join_ramp": -1.0},
            {"tenure_months": 0.0},
            {"returning_device_share": 1.2},
            {"returning_device_share": -0.2},
            {"returning_window_days": -1},
            {"cities": (("VN", "Hue", 0),)},
            {"cities": ()},
        )
        for overrides in bad:
            with self.subTest(overrides), self.assertRaises(ValueError):
                Scenario("bad", **overrides)

    def test_email_lifecycle_requires_calendar_rhythm(self) -> None:
        with self.assertRaisesRegex(ValueError, "email_lifecycle requires calendar_rhythm"):
            Scenario("bad", email_lifecycle=True)
        self.assertTrue(Scenario("ok", calendar_rhythm=True, email_lifecycle=True).email_lifecycle)

    def test_profile_rejects_unknown_scenario(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown scenario"):
            ScaleProfile("odd", 1, 1, 3, 1, 1, 1, "atlantis")


if __name__ == "__main__":
    unittest.main()
