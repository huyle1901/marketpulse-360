from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

from marketpulse_mock.cli import build_parser, install_signal_handlers, main
from marketpulse_mock.kafka_client import TopicBootstrapSummary
from marketpulse_mock.registry import RegistrationSummary
from marketpulse_mock.replay import ReplaySummary
from marketpulse_mock.replay_pacing import ReplayController


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

        with redirect_stderr(stderr), redirect_stdout(io.StringIO()):
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

    def test_kafka_replay_pacing_flags_are_mutually_exclusive(self) -> None:
        parser = build_parser()

        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(
                [
                    "kafka", "replay", "--input", "fixture",
                    "--speed", "10", "--no-sleep",
                ]
            )

    @patch("marketpulse_mock.cli.create_topics")
    def test_kafka_topics_returns_nonzero_on_failure(self, create: Mock) -> None:
        create.return_value = TopicBootstrapSummary(0, 0, 1, ("broker down",))
        stderr = io.StringIO()

        with redirect_stderr(stderr), redirect_stdout(io.StringIO()):
            exit_code = main(["kafka", "topics", "--bootstrap-servers", "broker:9092"])

        self.assertEqual(2, exit_code)
        self.assertIn("broker down", stderr.getvalue())

    @patch("marketpulse_mock.cli.register_contracts")
    def test_registry_failure_returns_nonzero(self, register: Mock) -> None:
        register.return_value = RegistrationSummary(0, 0, 0, 1, ("registry down",))

        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            exit_code = main(["kafka", "register-contracts"])

        self.assertEqual(2, exit_code)

    @patch("marketpulse_mock.cli.replay")
    @patch("marketpulse_mock.cli.iter_replay_records")
    @patch("marketpulse_mock.cli.ConfluentKafkaPublisher")
    def test_replay_prints_machine_readable_summary(
        self, publisher: Mock, reader: Mock, run_replay: Mock
    ) -> None:
        summary = ReplaySummary(
            attempted=3,
            delivered=3,
            failed=0,
            dlq=1,
            duration_seconds=1.5,
            events_per_second=2.0,
            first_event_time=None,
            last_event_time=None,
            topic_counts={"topic": 3},
            errors=(),
        )
        run_replay.return_value = summary
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            exit_code = main(
                ["kafka", "replay", "--input", "fixture", "--no-sleep", "--include-dlq"]
            )

        self.assertEqual(0, exit_code)
        self.assertEqual(summary.to_dict(), json.loads(stdout.getvalue()))
        self.assertTrue(reader.call_args.kwargs["include_dlq"])

    @patch("marketpulse_mock.cli.signal.signal")
    def test_signal_handlers_toggle_pause_and_stop(self, signal_call: Mock) -> None:
        controller = ReplayController()
        install_signal_handlers(controller)
        handlers = {call.args[0]: call.args[1] for call in signal_call.call_args_list}

        import signal

        handlers[signal.SIGUSR1](signal.SIGUSR1, None)
        self.assertTrue(controller.paused)
        handlers[signal.SIGINT](signal.SIGINT, None)
        self.assertTrue(controller.stopped)


if __name__ == "__main__":
    unittest.main()
