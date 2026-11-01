from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from marketpulse_mock.replay_cursor import replay_window
from marketpulse_mock.replay_pacing import PacingConfig, ReplayController, ReplayPacer
from marketpulse_mock.replay_reader import iter_replay_records


FIXTURE = Path("data/mock/fixtures/seed-360")


class FakePublisher:
    def __init__(self, controller: ReplayController | None = None) -> None:
        self.controller = controller
        self.records = []
        self.callbacks = []

    def publish(self, record, callback, *, cancelled=lambda: False) -> None:
        self.records.append(record)
        self.callbacks.append(callback)

    def flush(self, timeout: float) -> int:
        for callback in self.callbacks:
            callback(None)
        self.callbacks.clear()
        if self.controller:
            self.controller.stop()
        return 0


class ReplayCursorTests(unittest.TestCase):
    def test_acknowledged_batch_resumes_without_repeating_events(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary) / "cursor.json"
            from_day, to_day = date(2026, 1, 1), date(2026, 1, 3)
            first_control = ReplayController()
            first = FakePublisher(first_control)
            first_result = replay_window(
                FIXTURE, state, from_day, to_day, first,
                ReplayPacer(PacingConfig(no_sleep=True), first_control), first_control,
                target_id="test-kafka", batch_size=10, initialize=True,
            )
            self.assertEqual(10, first_result.committed)
            second_control = ReplayController()
            second = FakePublisher()
            result = replay_window(
                FIXTURE, state, from_day, to_day, second,
                ReplayPacer(PacingConfig(no_sleep=True), second_control), second_control,
                target_id="test-kafka", batch_size=10,
            )
            expected = [r.event_id for r in iter_replay_records(FIXTURE)]
            self.assertEqual(expected, [r.event_id for r in first.records + second.records])
            self.assertTrue(result.complete)
            self.assertEqual(len(expected), result.committed)

    def test_ambiguous_acknowledgement_and_changed_world_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary) / "cursor.json"
            control = ReplayController()
            replay_window(FIXTURE, state, date(2026, 1, 1), date(2026, 1, 3), FakePublisher(control),
                          ReplayPacer(PacingConfig(no_sleep=True), control), control,
                          target_id="test-kafka", batch_size=10, initialize=True)
            payload = json.loads(state.read_text())
            payload["pending"] = {"start": payload["next_index"], "end": payload["next_index"] + 10}
            state.write_text(json.dumps(payload))
            next_control = ReplayController()
            with self.assertRaisesRegex(ValueError, "ambiguous"):
                replay_window(FIXTURE, state, date(2026, 1, 1), date(2026, 1, 3), FakePublisher(),
                              ReplayPacer(PacingConfig(no_sleep=True), next_control), next_control,
                              target_id="test-kafka")
            payload["pending"] = None
            state.write_text(json.dumps(payload))
            with self.assertRaisesRegex(ValueError, "identity"):
                replay_window(FIXTURE, state, date(2026, 1, 1), date(2026, 1, 3), FakePublisher(),
                              ReplayPacer(PacingConfig(no_sleep=True), next_control), next_control,
                              target_id="other-kafka")


class RecordingPacer:
    """Logs each wait; optionally stops the controller once a given ordinal is due."""

    def __init__(self, log: list, controller: ReplayController, stop_at: int | None = None) -> None:
        self.log, self.controller, self.stop_at = log, controller, stop_at

    def wait(self, event_time, ordinal: int) -> None:
        self.log.append(("wait", ordinal))
        if ordinal == self.stop_at:
            self.controller.stop()


class LoggingPublisher(FakePublisher):
    def __init__(self, log: list) -> None:
        super().__init__()
        self.log = log

    def publish(self, record, callback, *, cancelled=lambda: False) -> None:
        self.log.append(("publish", len(self.records)))
        super().publish(record, callback, cancelled=cancelled)


class RealTimeReplayTests(unittest.TestCase):
    def test_each_record_is_published_as_soon_as_it_is_due(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            log: list = []
            control = ReplayController()
            replay_window(FIXTURE, Path(temporary) / "cursor.json", date(2026, 1, 1), date(2026, 1, 3),
                          LoggingPublisher(log), RecordingPacer(log, control), control,
                          target_id="test-kafka", batch_size=10, initialize=True)
            self.assertEqual([("wait", 0), ("publish", 0), ("wait", 1), ("publish", 1), ("wait", 2)], log[:5])

    def test_stopping_mid_batch_commits_what_was_sent_and_resumes_after_it(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary) / "cursor.json"
            log: list = []
            control = ReplayController()
            first = LoggingPublisher(log)
            result = replay_window(FIXTURE, state, date(2026, 1, 1), date(2026, 1, 3), first,
                                   RecordingPacer(log, control, stop_at=3), control,
                                   target_id="test-kafka", batch_size=10, initialize=True)
            self.assertEqual(3, result.committed)
            saved = json.loads(state.read_text())
            self.assertEqual((3, None), (saved["next_index"], saved["pending"]))
            again = ReplayController()
            second = FakePublisher()
            replay_window(FIXTURE, state, date(2026, 1, 1), date(2026, 1, 3), second,
                          ReplayPacer(PacingConfig(no_sleep=True), again), again,
                          target_id="test-kafka", batch_size=10)
            expected = [r.event_id for r in iter_replay_records(FIXTURE)]
            self.assertEqual(expected, [r.event_id for r in first.records + second.records])
