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
