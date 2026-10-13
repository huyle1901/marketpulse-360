from __future__ import annotations

import os
import stat
import tempfile
import unittest
from pathlib import Path

from marketpulse_batch.local_env import initialize_batch_secrets


class LocalEnvTests(unittest.TestCase):
    def test_preserves_existing_and_generates_only_missing(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / ".env"
            original = b"# keep this\nMINIO_ROOT_PASSWORD=fake-existing-test-value\n"
            path.write_bytes(original)
            names = initialize_batch_secrets(path)
            self.assertEqual(set(names), {"CLICKHOUSE_PASSWORD", "DAGSTER_POSTGRES_PASSWORD"})
            self.assertTrue(path.read_bytes().startswith(original))
            values = dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)
            self.assertEqual(len(values["CLICKHOUSE_PASSWORD"]), 48)
            self.assertNotEqual(values["CLICKHOUSE_PASSWORD"], values["DAGSTER_POSTGRES_PASSWORD"])
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            content = path.read_bytes()
            self.assertEqual(initialize_batch_secrets(path), ())
            self.assertEqual(path.read_bytes(), content)

    def test_empty_template_fields_filled_without_duplicate_keys(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / ".env"
            path.write_text("CLICKHOUSE_PASSWORD=\nDAGSTER_POSTGRES_PASSWORD=\n")
            initialize_batch_secrets(path)
            self.assertEqual(path.read_text().count("CLICKHOUSE_PASSWORD="), 1)
            self.assertEqual(path.read_text().count("DAGSTER_POSTGRES_PASSWORD="), 1)

    def test_duplicate_keys_and_symlink_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / ".env"
            path.write_text("CLICKHOUSE_PASSWORD=\nCLICKHOUSE_PASSWORD=\n")
            with self.assertRaises(ValueError):
                initialize_batch_secrets(path)
            link = Path(root) / "link"
            link.symlink_to(path)
            with self.assertRaises(ValueError):
                initialize_batch_secrets(link)
