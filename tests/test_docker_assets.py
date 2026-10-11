from __future__ import annotations

import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DockerAssetTests(unittest.TestCase):
    def test_compose_uses_pinned_streaming_services_and_kraft_listeners(self) -> None:
        compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")

        for image in (
            "apache/kafka:4.3.1",
            "apicurio/apicurio-registry:3.3.3",
            "ghcr.io/kafbat/kafka-ui:v1.5.0",
        ):
            self.assertIn(image, compose)
        self.assertNotIn(":latest", compose)
        self.assertIn("KAFKA_PROCESS_ROLES: broker,controller", compose)
        self.assertIn("PLAINTEXT_HOST://localhost:9092", compose)
        self.assertIn("PLAINTEXT://kafka:29092", compose)
        self.assertIn("KAFKA_LOG_DIRS: /var/lib/kafka/data", compose)
        self.assertGreaterEqual(compose.count("healthcheck:"), 3)
        self.assertGreaterEqual(compose.count("- streaming"), 4)

    def test_topic_bootstrap_contains_the_exact_contract_topics(self) -> None:
        script = (ROOT / "docker/kafka/create-topics.sh").read_text()

        expected = {
            "marketing.raw.web-events.v1",
            "marketing.raw.order-events.v1",
            "marketing.raw.email-events.v1",
            "marketing.dlq.validation.v1",
        }
        self.assertEqual(expected, {line.strip() for line in script.splitlines() if line.startswith("marketing.")})
        self.assertIn("--if-not-exists", script)
        self.assertIn("--partitions 6", script)
        self.assertIn("--replication-factor 1", script)
        self.assertIn("--config retention.ms=604800000", script)
        self.assertIn("PartitionCount: 6", script)
        self.assertIn("retention.ms=604800000", script)

    def test_replay_image_is_pinned_and_runs_as_non_root(self) -> None:
        dockerfile = (ROOT / "docker/replay/Dockerfile").read_text()

        self.assertIn("FROM python:3.12.12-slim-bookworm", dockerfile)
        self.assertIn("USER marketpulse", dockerfile)
        self.assertIn('ENTRYPOINT ["marketpulse-mock"]', dockerfile)

    def test_smoke_script_asserts_fixture_delivery_counts(self) -> None:
        script = (ROOT / "scripts/kafka-smoke-test.sh").read_text()

        for expected in ("199", "14", "6", "4"):
            self.assertIn(expected, script)
        self.assertIn("COMPOSE_PROJECT_NAME", script)
        self.assertIn(
            'compose=(docker compose -f "${project_root}/compose.yaml" -p "${COMPOSE_PROJECT_NAME}" --profile streaming)',
            script,
        )
        self.assertIn("register-contracts", script)
        self.assertIn("--include-dlq", script)
        self.assertIn("down --volumes --remove-orphans", script)
        self.assertIn("marketpulse_mock.smoke_verify", script)

    @unittest.skipUnless(shutil.which("docker"), "Docker CLI is not installed")
    def test_compose_configuration_is_valid(self) -> None:
        result = subprocess.run(
            ["docker", "compose", "-f", "compose.yaml", "config", "--quiet"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr)


if __name__ == "__main__":
    unittest.main()
