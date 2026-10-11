from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from marketpulse_mock.cli import main


class CommandLineTests(unittest.TestCase):
    def test_profiles_lists_all_named_scales(self) -> None:
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            exit_code = main(["profiles"])

        self.assertEqual(0, exit_code)
        for profile in ("fixture", "smoke", "demo", "benchmark"):
            self.assertIn(profile, stdout.getvalue())

    def test_generate_then_validate_returns_success(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "fixture"
            generate_stdout = io.StringIO()
            with redirect_stdout(generate_stdout):
                generate_exit = main(
                    [
                        "generate",
                        "--profile",
                        "fixture",
                        "--seed",
                        "360",
                        "--output",
                        str(output),
                    ]
                )
            validate_stdout = io.StringIO()
            with redirect_stdout(validate_stdout):
                validate_exit = main(["validate", "--input", str(output)])

        self.assertEqual(0, generate_exit)
        self.assertIn("generated", generate_stdout.getvalue().lower())
        self.assertEqual(0, validate_exit)
        self.assertIn("valid", validate_stdout.getvalue().lower())

    def test_unknown_profile_returns_configuration_error(self) -> None:
        stderr = io.StringIO()

        with redirect_stderr(stderr):
            exit_code = main(
                ["generate", "--profile", "huge", "--output", "unused"]
            )

        self.assertEqual(2, exit_code)
        self.assertIn("unknown profile", stderr.getvalue())

    def test_validate_missing_directory_returns_validation_error(self) -> None:
        stderr = io.StringIO()

        with redirect_stderr(stderr):
            exit_code = main(["validate", "--input", "/path/that/does/not/exist"])

        self.assertEqual(2, exit_code)
        self.assertIn("manifest", stderr.getvalue().lower())


if __name__ == "__main__":
    unittest.main()
