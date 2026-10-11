from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from marketpulse_mock.replay_pacing import (
    PacingConfig,
    ReplayController,
    ReplayPacer,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds

    def advance(self, seconds: float) -> None:
        self.now += seconds


class ReplayPacingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.controller = ReplayController(monotonic=self.clock.monotonic)
        self.start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def test_real_time_speed_replays_event_time_gap(self) -> None:
        pacer = ReplayPacer(
            PacingConfig(speed=1.0),
            self.controller,
            monotonic=self.clock.monotonic,
            sleep=self.clock.sleep,
        )

        pacer.wait(self.start, 0)
        pacer.wait(self.start + timedelta(seconds=2.5), 1)

        self.assertEqual([2.5], self.clock.sleeps)

    def test_accelerated_speed_divides_event_time_gap(self) -> None:
        pacer = ReplayPacer(
            PacingConfig(speed=100.0),
            self.controller,
            monotonic=self.clock.monotonic,
            sleep=self.clock.sleep,
        )

        pacer.wait(self.start, 0)
        pacer.wait(self.start + timedelta(seconds=250), 1)

        self.assertEqual([2.5], self.clock.sleeps)

    def test_fixed_rate_uses_record_ordinal(self) -> None:
        pacer = ReplayPacer(
            PacingConfig(events_per_second=4.0),
            self.controller,
            monotonic=self.clock.monotonic,
            sleep=self.clock.sleep,
        )

        for ordinal in range(3):
            pacer.wait(self.start, ordinal)

        self.assertEqual([0.25, 0.25], self.clock.sleeps)

    def test_no_sleep_never_invokes_clock_sleep(self) -> None:
        pacer = ReplayPacer(
            PacingConfig(no_sleep=True),
            self.controller,
            monotonic=self.clock.monotonic,
            sleep=self.clock.sleep,
        )

        pacer.wait(self.start, 0)
        pacer.wait(self.start + timedelta(days=1), 1)

        self.assertEqual([], self.clock.sleeps)

    def test_exactly_one_positive_mode_is_required(self) -> None:
        invalid = (
            {},
            {"speed": 0.0},
            {"events_per_second": -1.0},
            {"speed": 1.0, "no_sleep": True},
            {"events_per_second": 1.0, "speed": 1.0},
        )
        for values in invalid:
            with self.subTest(values=values):
                with self.assertRaises(ValueError):
                    PacingConfig(**values)

    def test_paused_time_is_excluded_without_resume_burst(self) -> None:
        pacer = ReplayPacer(
            PacingConfig(speed=1.0),
            self.controller,
            monotonic=self.clock.monotonic,
            sleep=self.clock.sleep,
        )
        pacer.wait(self.start, 0)
        self.controller.pause()
        self.clock.advance(5.0)
        self.controller.resume()

        pacer.wait(self.start + timedelta(seconds=10), 1)

        self.assertEqual([10.0], self.clock.sleeps)
        self.assertEqual(5.0, self.controller.paused_seconds)

    def test_toggle_changes_pause_state(self) -> None:
        self.assertFalse(self.controller.paused)
        self.controller.toggle()
        self.assertTrue(self.controller.paused)
        self.clock.advance(3.0)
        self.controller.toggle()
        self.assertFalse(self.controller.paused)
        self.assertEqual(3.0, self.controller.paused_seconds)


if __name__ == "__main__":
    unittest.main()
