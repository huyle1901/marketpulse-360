import json
import tempfile
import unittest
from pathlib import Path

from marketpulse_mock.config import resolve_config
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


if __name__ == "__main__":
    unittest.main()
