from __future__ import annotations

import unittest
import json
import os
import subprocess

from marketpulse_batch.settings import load_settings


class BatchRuntimeTests(unittest.TestCase):
    def environment(self) -> dict[str, str]:
        return {"CLICKHOUSE_PASSWORD": "fake-clickhouse-test-value",
                "DAGSTER_POSTGRES_PASSWORD": "fake-ledger-test-value",
                "MINIO_ROOT_PASSWORD": "fake-minio-test-value"}

    def test_missing_secret_fails_without_echo(self) -> None:
        for name in ("CLICKHOUSE_PASSWORD", "DAGSTER_POSTGRES_PASSWORD", "MINIO_ROOT_PASSWORD"):
            values = self.environment()
            values[name] = "   "
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, name):
                load_settings(values)

    def test_repr_redacts(self) -> None:
        settings = load_settings(self.environment())
        for secret in self.environment().values():
            self.assertNotIn(secret, repr(settings))
        self.assertIn("clickhouse", repr(settings))

    def test_configuration_defaults_and_ports(self) -> None:
        settings = load_settings(self.environment())
        self.assertEqual(settings.clickhouse_port, 8123)
        self.assertEqual(settings.ledger_port, 5432)
        self.assertEqual(settings.source_root.as_posix(), "/opt/marketpulse/data/mock/fixtures/seed-360")

    def test_invalid_port_or_injected_identifier_rejected(self) -> None:
        for key, value in (("CLICKHOUSE_PORT", "0"), ("CLICKHOUSE_PORT", "65536"),
                           ("DAGSTER_POSTGRES_USER", "ops; DROP DATABASE x")):
            values = self.environment() | {key: value}
            with self.subTest(key=key), self.assertRaises(ValueError):
                load_settings(values)

    def compose_services(self) -> dict[str, object]:
        # Capture rendered configuration: never print expanded credentials.
        env = os.environ | self.environment() | {"LAKEHOUSE_POSTGRES_PASSWORD": "fake-compose-test-value"}
        result = subprocess.run(["docker", "compose", "--profile", "batch", "--profile", "serving",
                                 "config", "--format", "json"], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, "Compose validation failed")
        return json.loads(result.stdout)["services"]

    def test_no_docker_socket(self) -> None:
        services = self.compose_services()
        self.assertTrue({"batch-code", "dagster-webserver", "dagster-daemon", "batch-postgres"} <= set(services))
        for name in ("batch-code", "dagster-webserver", "dagster-daemon"):
            self.assertFalse(any("docker.sock" in mount.get("source", "") for mount in services[name].get("volumes", [])))

    def test_serving_native_port_not_published(self) -> None:
        services = self.compose_services()
        self.assertIn("clickhouse", services)
        ports = services["clickhouse"].get("ports", [])
        self.assertEqual({p["target"] for p in ports}, {8123})
        self.assertTrue(all(p["host_ip"] == "127.0.0.1" for p in ports))
