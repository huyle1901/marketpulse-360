import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KEYS = (
    "COMPOSE_PROJECT_NAME", "WEB_HOST_PORT", "CLICKHOUSE_HOST_PORT", "MINIO_API_HOST_PORT",
    "APP_PUBLIC_ORIGIN", "BATCH_SOURCE_ROOT", "BATCH_REQUIRED_PLATFORMS",
)


def _env_after(*scripts: str, shell: str = "bash") -> dict[str, str]:
    sources = " && ".join(f"source scripts/{script}" for script in scripts)
    printed = "; ".join(f'echo "{key}=${{{key}-<unset>}}"' for key in KEYS)
    result = subprocess.run(
        [shell, "-c", f"{sources} && {printed}"],
        cwd=ROOT, capture_output=True, text=True, check=True,
        env={"PATH": "/usr/bin:/bin", "HOME": str(Path.home())},
    )
    return dict(line.split("=", 1) for line in result.stdout.splitlines())


class StoryDemoEnvTests(unittest.TestCase):
    def test_v2_after_v1_is_pure_v2(self) -> None:
        self.assertEqual(_env_after("story-demo-v2-env.sh"), _env_after("story-demo-v1-env.sh", "story-demo-v2-env.sh"))

    def test_v1_after_v2_is_pure_v1(self) -> None:
        v1 = _env_after("story-demo-v1-env.sh")
        self.assertEqual(v1, _env_after("story-demo-v2-env.sh", "story-demo-v1-env.sh"))
        self.assertEqual("<unset>", v1["BATCH_REQUIRED_PLATFORMS"])

    def test_default_points_at_v2_in_bash_and_zsh(self) -> None:
        v2 = _env_after("story-demo-v2-env.sh")
        self.assertEqual("marketpulse-story-demo-v2", v2["COMPOSE_PROJECT_NAME"])
        self.assertEqual("43210", v2["WEB_HOST_PORT"])
        self.assertEqual("http://localhost:43210", v2["APP_PUBLIC_ORIGIN"])
        self.assertEqual("/opt/marketpulse/data/generated/story-demo-v2", v2["BATCH_SOURCE_ROOT"])
        self.assertEqual(v2, _env_after("story-demo-env.sh"))
        self.assertEqual(v2, _env_after("story-demo-env.sh", shell="zsh"))

    def test_env_scripts_are_sourced_not_executed(self) -> None:
        for name in ("story-demo-env.sh", "story-demo-v1-env.sh", "story-demo-v2-env.sh"):
            self.assertEqual(0o644, (ROOT / "scripts" / name).stat().st_mode & 0o777, name)


if __name__ == "__main__":
    unittest.main()
