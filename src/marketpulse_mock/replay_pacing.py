"""Deterministic replay pacing and signal-safe pause state."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable


@dataclass(frozen=True, slots=True)
class PacingConfig:
    speed: float | None = None
    events_per_second: float | None = None
    no_sleep: bool = False

    def __post_init__(self) -> None:
        selected = sum(
            (
                self.speed is not None,
                self.events_per_second is not None,
                self.no_sleep,
            )
        )
        if selected != 1:
            raise ValueError("select exactly one replay pacing mode")
        if self.speed is not None and self.speed <= 0:
            raise ValueError("speed must be positive")
        if self.events_per_second is not None and self.events_per_second <= 0:
            raise ValueError("events_per_second must be positive")


class ReplayController:
    """Coordinate pause/resume while accounting for paused monotonic time."""

    def __init__(self, monotonic: Callable[[], float] = time.monotonic) -> None:
        self._monotonic = monotonic
        self._condition = threading.Condition()
        self._paused_at: float | None = None
        self._paused_seconds = 0.0
        self._stopped = False

    @property
    def paused(self) -> bool:
        with self._condition:
            return self._paused_at is not None

    @property
    def stopped(self) -> bool:
        with self._condition:
            return self._stopped

    @property
    def paused_seconds(self) -> float:
        with self._condition:
            active = (
                self._monotonic() - self._paused_at
                if self._paused_at is not None
                else 0.0
            )
            return self._paused_seconds + active

    def pause(self) -> None:
        with self._condition:
            if self._paused_at is None and not self._stopped:
                self._paused_at = self._monotonic()

    def resume(self) -> None:
        with self._condition:
            if self._paused_at is not None:
                self._paused_seconds += self._monotonic() - self._paused_at
                self._paused_at = None
                self._condition.notify_all()

    def toggle(self) -> None:
        if self.paused:
            self.resume()
        else:
            self.pause()

    def stop(self) -> None:
        with self._condition:
            self._stopped = True
            self._condition.notify_all()

    def wait_until_running(self) -> float:
        with self._condition:
            while self._paused_at is not None and not self._stopped:
                self._condition.wait()
            return self._paused_seconds

    def wait_for_signal(self, timeout: float) -> None:
        """Wait for a timeout, pause/resume, or stop notification."""

        with self._condition:
            if not self._stopped:
                self._condition.wait(timeout=timeout)


class ReplayPacer:
    def __init__(
        self,
        config: PacingConfig,
        controller: ReplayController,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self.controller = controller
        self._monotonic = monotonic
        self._sleep = sleep
        self._interruptible = sleep is time.sleep
        self._first_event_time: datetime | None = None
        self._started_at: float | None = None
        self._pause_baseline = 0.0

    def wait(self, event_time: datetime, ordinal: int) -> None:
        paused_seconds = self.controller.wait_until_running()
        if self._started_at is None:
            self._started_at = self._monotonic()
            self._first_event_time = event_time
            self._pause_baseline = paused_seconds
            return
        if self.config.no_sleep:
            return
        assert self._first_event_time is not None
        if self.config.speed is not None:
            schedule_offset = (
                event_time - self._first_event_time
            ).total_seconds() / self.config.speed
        else:
            assert self.config.events_per_second is not None
            schedule_offset = ordinal / self.config.events_per_second

        while not self.controller.stopped:
            paused_seconds = self.controller.wait_until_running()
            target = (
                self._started_at
                + schedule_offset
                + paused_seconds
                - self._pause_baseline
            )
            remaining = target - self._monotonic()
            if remaining <= 0:
                return
            if self._interruptible:
                self.controller.wait_for_signal(remaining)
            else:
                self._sleep(remaining)
