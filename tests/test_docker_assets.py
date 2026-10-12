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
            "marketing.clean.customer-events.v1",
            "marketing.clean.sessions.v1",
            "marketing.gold.funnel-metrics.v1",
            "marketing.gold.revenue-metrics.v1",
            "marketing.gold.campaign-metrics.v1",
            "marketing.dlq.flink-validation.v1",
        }
        self.assertEqual(expected, {line.strip() for line in script.splitlines() if line.startswith("marketing.")})
        self.assertIn("--if-not-exists", script)
        self.assertIn('--partitions "${partitions}"', script)
        self.assertIn("--replication-factor 1", script)
        self.assertIn('--config retention.ms="${retention_ms}"', script)
        self.assertIn('--config cleanup.policy="${cleanup_policy}"', script)
        self.assertIn("PartitionCount: ${partitions}", script)
        self.assertIn("retention.ms=${retention_ms}", script)
        self.assertIn("cleanup.policy=", script)
        self.assertIn("s/^[[:space:]]*cleanup.policy=", script)

    def test_replay_image_is_pinned_and_runs_as_non_root(self) -> None:
        dockerfile = (ROOT / "docker/replay/Dockerfile").read_text()

        self.assertIn("FROM python:3.12.12-slim-bookworm", dockerfile)
        self.assertIn("USER marketpulse", dockerfile)
        self.assertIn('ENTRYPOINT ["marketpulse-mock"]', dockerfile)

    def test_flink_image_is_pinned_multistage_and_runs_as_non_root(self) -> None:
        dockerfile = (ROOT / "docker/flink/Dockerfile").read_text()

        self.assertIn("FROM maven:3.9.11-eclipse-temurin-17 AS builder", dockerfile)
        self.assertIn("FROM flink:2.2.1-java17", dockerfile)
        self.assertIn("mvn -q -f flink-job/pom.xml clean package -DskipTests", dockerfile)
        self.assertIn("/opt/flink/usrlib/marketpulse-flink-job.jar", dockerfile)
        self.assertIn("USER flink", dockerfile)

    def test_compose_defines_healthy_flink_cluster_and_exact_job_arguments(self) -> None:
        compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")

        for service in ("flink-jobmanager:", "flink-taskmanager:", "flink-job-submit:"):
            self.assertIn(service, compose)
        self.assertIn('"${FLINK_HOST_PORT:-8082}:8081"', compose)
        self.assertIn("taskmanager.numberOfTaskSlots: 2", compose)
        self.assertIn("jobmanager.memory.process.size: 768m", compose)
        self.assertIn("taskmanager.memory.process.size: 1024m", compose)
        self.assertIn("flink_checkpoints:/opt/flink/checkpoints", compose)
        self.assertGreaterEqual(compose.count("- flink"), 3)
        self.assertGreaterEqual(compose.count("condition: service_healthy"), 7)
        for argument in (
            "--bootstrap-servers=kafka:29092",
            "--group-id=marketpulse-flink-v1",
            "--checkpoint-uri=file:///opt/flink/checkpoints",
            "--checkpoint-interval-ms=${FLINK_CHECKPOINT_INTERVAL_MS:-5000}",
            "--watermark-ms=300000",
            "--idleness-ms=${FLINK_IDLENESS_MS:-60000}",
            "--correction-horizon-ms=600000",
            "--session-gap-ms=1800000",
            "--dedup-ttl-ms=604800000",
            "--parallelism=2",
            "--transaction-prefix=marketpulse-flink-v1-",
            "--job-name=marketpulse-stream-v1",
        ):
            self.assertIn(argument, compose)
        self.assertIn("flink_checkpoints:", compose)

    def test_flink_smoke_script_proves_recovery_and_read_committed_outputs(self) -> None:
        script = (ROOT / "scripts/flink-smoke-test.sh").read_text()

        self.assertIn('COMPOSE_PROJECT_NAME="marketpulse-mp03-smoke-$$"', script)
        self.assertNotIn('${COMPOSE_PROJECT_NAME:-', script)
        self.assertIn("--events-per-second 20", script)
        self.assertIn("restart flink-taskmanager", script)
        self.assertNotIn('docker pause "${replay_container}"', script)
        self.assertIn("raw_records_before_restart", script)
        self.assertIn("raw_records_after_restart", script)
        self.assertIn("/subtasks/metrics", script)
        self.assertIn("deduplicate-events.accepted", script)
        self.assertIn("deduplicate-events.duplicates", script)
        self.assertIn("classify-late-events.source_marked_late", script)
        self.assertIn("classify-late-events.watermark_late", script)
        self.assertIn("parse-and-validate.invalid", script)
        self.assertIn("metrics.json", script)
        self.assertIn("checkpoint_before_restart", script)
        self.assertIn("checkpoint_after_restart", script)
        self.assertIn("--consume-bootstrap kafka:29092", script)
        self.assertIn("isolation.level", (
            ROOT / "src/marketpulse_mock/flink_smoke_verify.py"
        ).read_text())
        self.assertIn("KEEP_STACK", script)
        self.assertIn("down --volumes --remove-orphans", script)

    def test_flink_submitter_is_idempotent_for_running_named_job(self) -> None:
        script = (ROOT / "docker/flink/submit-job.sh").read_text()
        dockerfile = (ROOT / "docker/flink/Dockerfile").read_text()

        self.assertIn("/jobs/overview", script)
        self.assertIn("marketpulse-stream-v1", script)
        self.assertIn("jq -r", script)
        self.assertIn('state != "FINISHED"', script)
        self.assertIn("already active", script)
        self.assertIn("flink run -d", script)
        self.assertIn("apt-get install -y --no-install-recommends jq", dockerfile)

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
