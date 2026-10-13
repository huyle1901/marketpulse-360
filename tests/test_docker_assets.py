from __future__ import annotations

import json
import os
import shutil
import subprocess
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMPOSE_TEST_ENV = {
    **os.environ,
    "MINIO_ROOT_PASSWORD": uuid.uuid4().hex,
    "LAKEHOUSE_POSTGRES_PASSWORD": uuid.uuid4().hex,
}


class DockerAssetTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("docker"), "Docker CLI is not installed")
    def test_lakehouse_compose_requires_passwords_and_propagates_explicit_values(self) -> None:
        command = ["docker", "compose", "--env-file", "/dev/null", "-f", "compose.yaml",
                   "--profile", "lakehouse", "--profile", "lakehouse-tools", "config", "--format", "json"]
        for missing in ("MINIO_ROOT_PASSWORD", "LAKEHOUSE_POSTGRES_PASSWORD"):
            env = {key: value for key, value in COMPOSE_TEST_ENV.items() if key != missing}
            result = subprocess.run(command, cwd=ROOT, env=env, text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0, "Compose silently used a built-in password")
            self.assertIn(missing, result.stderr)
        result = subprocess.run(command, cwd=ROOT, env=COMPOSE_TEST_ENV, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        services = json.loads(result.stdout)["services"]
        for service in ("minio", "minio-init", "spark-lakehouse"):
            self.assertEqual(services[service]["environment"]["MINIO_ROOT_PASSWORD"],
                             COMPOSE_TEST_ENV["MINIO_ROOT_PASSWORD"])
        self.assertEqual(services["spark-lakehouse"]["environment"]["AWS_SECRET_ACCESS_KEY"],
                         COMPOSE_TEST_ENV["MINIO_ROOT_PASSWORD"])
        self.assertEqual(services["lakehouse-postgres"]["environment"]["POSTGRES_PASSWORD"],
                         COMPOSE_TEST_ENV["LAKEHOUSE_POSTGRES_PASSWORD"])
        self.assertEqual(services["nessie"]["environment"]["QUARKUS_DATASOURCE_POSTGRESQL_PASSWORD"],
                         COMPOSE_TEST_ENV["LAKEHOUSE_POSTGRES_PASSWORD"])

    def test_docker_context_excludes_local_and_generated_material(self) -> None:
        dockerignore = (ROOT / ".dockerignore").read_text()

        self.assertTrue(dockerignore.startswith("**\n"))
        for allowed in ("!pyproject.toml", "!src/**", "!contracts/**", "!flink-job/src/**", "!docker/**"):
            self.assertIn(allowed, dockerignore)
        for excluded in ("data/generated", ".lakehouse", ".env", ".git", ".worktrees"):
            self.assertNotIn(f"!{excluded}", dockerignore)

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

    def test_lakehouse_image_pins_spark_and_offline_runtime_jars(self) -> None:
        dockerfile = (ROOT / "docker/lakehouse/Dockerfile").read_text()
        entrypoint = (ROOT / "docker/lakehouse/entrypoint.sh").read_text()

        self.assertIn("FROM python:3.12.12-slim-bookworm", dockerfile)
        self.assertIn(
            "FROM apache/spark:4.0.4-scala2.13-java17-python3-ubuntu",
            dockerfile,
        )
        for artifact in (
            "iceberg-spark-runtime-4.0_2.13-1.12.0.jar",
            "iceberg-aws-bundle-1.12.0.jar",
            "spark-sql-kafka-0-10_2.13-4.0.4.jar",
        ):
            self.assertIn(artifact, dockerfile)
        self.assertIn("USER spark", dockerfile)
        self.assertIn("/opt/spark/bin/spark-submit", entrypoint)
        self.assertIn("--master", entrypoint)
        self.assertIn("local[2]", entrypoint)
        self.assertIn("marketpulse_lakehouse/cli.py", entrypoint)
        self.assertIn("'.[lakehouse]'", dockerfile)

    def test_minio_images_are_built_from_pinned_verified_release_assets(self) -> None:
        server = (ROOT / "docker/minio/Dockerfile.server").read_text()
        client = (ROOT / "docker/minio/Dockerfile.client").read_text()

        self.assertIn("RELEASE.2025-04-22T22-12-26Z", server)
        self.assertIn("53e2a2cb16c5366ea6fbbc479c19ddb4c6a0948273e752f740fb1fbf27bb817c", server)
        self.assertIn("6c2f3142c94240206123177f4ba1e360daa5d1e0a4962e90757ef4f92c3ab57c", server)
        self.assertIn("sha256sum -c -", server)
        self.assertIn("RELEASE.2025-05-21T01-59-54Z", client)
        self.assertIn("fb11c542a9d781fb228de1126c267a7933e98bee831654462fb352d5c9e94d24", client)
        self.assertIn("0ec5f3f22cec2b7f86d02dff44e2ef9632374b742d74f234bbc3f9d366541ed9", client)
        self.assertIn("sha256sum -c -", client)

    @unittest.skipUnless(shutil.which("docker"), "Docker CLI is not installed")
    def test_compose_defines_persistent_lakehouse_and_on_demand_tool(self) -> None:
        compose_env = dict(COMPOSE_TEST_ENV)
        compose_env["COMPOSE_DISABLE_ENV_FILE"] = "true"
        for name in ("MINIO_API_HOST_PORT", "MINIO_CONSOLE_HOST_PORT", "NESSIE_HOST_PORT"):
            compose_env.pop(name, None)
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                "compose.yaml",
                "--profile",
                "lakehouse",
                "--profile",
                "lakehouse-tools",
                "config",
                "--format",
                "json",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            env=compose_env,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        rendered = json.loads(result.stdout)
        services = rendered["services"]

        self.assertEqual(
            services["lakehouse-postgres"]["image"],
            "postgres:17.6-alpine",
        )
        self.assertEqual(services["minio"]["image"], "marketpulse-360/minio:RELEASE.2025-04-22T22-12-26Z")
        self.assertIn("docker/minio/Dockerfile.server", str(services["minio"]["build"]))
        self.assertEqual(services["minio-init"]["image"], "marketpulse-360/mc:RELEASE.2025-05-21T01-59-54Z")
        self.assertIn("docker/minio/Dockerfile.client", str(services["minio-init"]["build"]))
        self.assertEqual(services["nessie"]["image"], "ghcr.io/projectnessie/nessie:0.109.0")
        self.assertEqual(services["spark-lakehouse"]["profiles"], ["lakehouse-tools"])
        for service_name in ("lakehouse-postgres", "minio", "minio-init", "nessie"):
            self.assertEqual(services[service_name]["profiles"], ["lakehouse"])

        postgres = services["lakehouse-postgres"]
        self.assertNotIn("ports", postgres)
        self.assertIn("healthcheck", postgres)
        self.assertIn("lakehouse_catalog", str(postgres["volumes"]))

        minio = services["minio"]
        self.assertEqual(
            {(port["target"], port["published"]) for port in minio["ports"]},
            {(9000, "9000"), (9001, "9001")},
        )
        self.assertIn("lakehouse_data", str(minio["volumes"]))
        self.assertIn("healthcheck", minio)

        nessie = services["nessie"]
        self.assertEqual(
            {(port["target"], port["published"]) for port in nessie["ports"]},
            {(19120, "19120")},
        )
        self.assertEqual(nessie["environment"]["NESSIE_VERSION_STORE_TYPE"], "JDBC2")
        self.assertEqual(
            nessie["environment"]["NESSIE_VERSION_STORE_PERSIST_JDBC_DATASOURCE"],
            "postgresql",
        )
        self.assertIn("jdbc:postgresql://lakehouse-postgres:5432/nessie", str(nessie["environment"]))
        self.assertIn("healthcheck", nessie)
        self.assertIn("localhost:9000/q/health/ready", str(nessie["healthcheck"]))

        spark = services["spark-lakehouse"]
        self.assertIn("docker/lakehouse/Dockerfile", str(spark["build"]))
        self.assertEqual(spark["environment"]["SPARK_MASTER"], "local[2]")
        self.assertEqual(spark["environment"]["SPARK_SQL_SESSION_TIMEZONE"], "UTC")
        self.assertEqual(spark["environment"]["SPARK_SQL_SHUFFLE_PARTITIONS"], "2")
        self.assertEqual(spark["environment"]["NESSIE_REF"], "main")
        self.assertEqual(spark["environment"]["MINIO_REGION"], "us-east-1")
        self.assertIn("lakehouse_evidence", str(spark["volumes"]))
        self.assertIn(
            "${SPARK_CONTAINER_MEMORY_LIMIT:-2560M}",
            (ROOT / "compose.yaml").read_text(),
        )

        self.assertIn("lakehouse_data", rendered["volumes"])
        self.assertIn("lakehouse_catalog", rendered["volumes"])
        self.assertIn("lakehouse_evidence", rendered["volumes"])

    def test_minio_initializer_is_idempotent_and_creates_lakehouse_bucket(self) -> None:
        script = (ROOT / "docker/minio/create-bucket.sh").read_text()
        self.assertIn("mc alias set", script)
        self.assertIn("mc mb --ignore-existing", script)
        self.assertIn("marketpulse-lakehouse", script)

    def test_lakehouse_smoke_script_proves_catalog_persistence(self) -> None:
        script = (ROOT / "scripts/lakehouse-smoke-test.sh").read_text()

        self.assertIn('COMPOSE_PROJECT_NAME="marketpulse-mp04-smoke-$$"', script)
        self.assertIn("--profile lakehouse", script)
        self.assertIn("--profile lakehouse-tools", script)
        self.assertIn("build spark-lakehouse", script)
        self.assertIn("up -d --wait", script)
        self.assertIn("restart lakehouse-postgres minio nessie", script)
        self.assertIn("run_lakehouse verify verify", script)
        self.assertIn("later_snapshot_id", script)
        self.assertIn("down --volumes --remove-orphans", script)
        self.assertIn("KEEP_STACK", script)

    def test_lakehouse_smoke_and_runbook_cover_complete_mp04_proof(self) -> None:
        script = (ROOT / "scripts/lakehouse-smoke-test.sh").read_text()
        runbook = (ROOT / "docs/runbooks/lakehouse-local.md").read_text()

        for profile in ("streaming", "flink", "lakehouse", "lakehouse-tools"):
            self.assertIn(f"--profile {profile}", script)
        for command in (
            "capture-offsets",
            "ingest-kafka",
            "ingest-files",
            "build-silver",
            "build-gold",
            "write-evidence",
            "run_lakehouse verify verify",
            "backfill-proof",
        ):
            self.assertIn(command, script)
        self.assertGreaterEqual(script.count("ingest-kafka"), 2)
        self.assertGreaterEqual(script.count("ingest-files"), 2)
        self.assertGreaterEqual(script.count("build-gold"), 2)
        self.assertIn("stop flink-job-submit flink-taskmanager flink-jobmanager", script)
        self.assertIn("later_snapshot_id", script)
        self.assertIn("restart lakehouse-postgres minio nessie", script)
        self.assertIn("rebuild_", script)
        self.assertIn("lakehouse-evidence.json", script)
        self.assertIn("KEEP_STACK", script)
        self.assertIn("down --volumes --remove-orphans", script)

        for text in (
            "Kafka UI",
            "Flink dashboard",
            "MinIO console",
            "Nessie",
            "Time travel",
            "backfill-proof",
            "KEEP_STACK=1",
            "down --volumes --remove-orphans",
        ):
            self.assertIn(text, runbook)

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
            env=COMPOSE_TEST_ENV,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr)


if __name__ == "__main__":
    unittest.main()
