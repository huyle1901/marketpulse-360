"""Transport-neutral replay orchestration with acknowledgement accounting."""

from __future__ import annotations

import threading
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Iterable

from .kafka_client import KafkaPublisher
from .replay_pacing import ReplayController, ReplayPacer
from .replay_reader import ReplayRecord


@dataclass(frozen=True, slots=True)
class ReplaySummary:
    attempted: int
    delivered: int
    failed: int
    dlq: int
    duration_seconds: float
    events_per_second: float
    first_event_time: datetime | None
    last_event_time: datetime | None
    topic_counts: dict[str, int]
    errors: tuple[str, ...]

    @property
    def success(self) -> bool:
        return self.failed == 0

    def to_dict(self) -> dict[str, object]:
        return {
            "attempted": self.attempted,
            "delivered": self.delivered,
            "failed": self.failed,
            "dlq": self.dlq,
            "duration_seconds": self.duration_seconds,
            "events_per_second": self.events_per_second,
            "first_event_time": (
                self.first_event_time.isoformat() if self.first_event_time else None
            ),
            "last_event_time": (
                self.last_event_time.isoformat() if self.last_event_time else None
            ),
            "topic_counts": dict(self.topic_counts),
            "errors": list(self.errors),
        }


def replay(
    records: Iterable[ReplayRecord],
    publisher: KafkaPublisher,
    pacer: ReplayPacer,
    controller: ReplayController,
    *,
    flush_timeout: float = 30.0,
    monotonic: Callable[[], float] = time.monotonic,
) -> ReplaySummary:
    started = monotonic()
    attempted = delivered = callback_failures = dlq = 0
    topic_counts: Counter[str] = Counter()
    errors: list[str] = []
    first_event_time: datetime | None = None
    last_event_time: datetime | None = None
    lock = threading.Lock()

    def delivery_callback(record: ReplayRecord) -> Callable[[Exception | None], None]:
        def acknowledge(error: Exception | None) -> None:
            nonlocal delivered, callback_failures, dlq
            with lock:
                if error is None:
                    delivered += 1
                    topic_counts[record.topic] += 1
                    if record.dataset == "quality_cases":
                        dlq += 1
                else:
                    callback_failures += 1
                    errors.append(str(error))

        return acknowledge

    for ordinal, record in enumerate(records):
        if controller.stopped:
            break
        pacer.wait(record.event_time, ordinal)
        if controller.stopped:
            break
        attempted += 1
        if first_event_time is None:
            first_event_time = record.event_time
        last_event_time = record.event_time
        try:
            publisher.publish(record, delivery_callback(record))
        except Exception as error:
            callback_failures += 1
            errors.append(str(error))

    remaining = publisher.flush(flush_timeout)
    if remaining:
        errors.append(f"flush timed out with {remaining} undelivered message(s)")
    with lock:
        failed = callback_failures + remaining
    duration = max(0.0, monotonic() - started)
    return ReplaySummary(
        attempted=attempted,
        delivered=delivered,
        failed=failed,
        dlq=dlq,
        duration_seconds=duration,
        events_per_second=(delivered / duration if duration else 0.0),
        first_event_time=first_event_time,
        last_event_time=last_event_time,
        topic_counts=dict(topic_counts),
        errors=tuple(errors),
    )
