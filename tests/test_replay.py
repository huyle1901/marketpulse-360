from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from marketpulse_mock.replay import replay
from marketpulse_mock.replay_pacing import PacingConfig, ReplayController, ReplayPacer
from marketpulse_mock.replay_reader import ReplayRecord


def records() -> list[ReplayRecord]:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [
        ReplayRecord(
            dataset=dataset,
            topic=topic,
            key=f"key-{index}",
            value=f'{{"event_id":"event-{index}"}}'.encode(),
            event_id=f"event-{index}",
            event_time=start + timedelta(seconds=index),
            headers=(("event-id", f"event-{index}".encode()),),
            source_file=f"part-{index}.jsonl.gz",
        )
        for index, (dataset, topic) in enumerate(
            (
                ("web_events", "marketing.raw.web-events.v1"),
                ("orders", "marketing.raw.order-events.v1"),
                ("quality_cases", "marketing.dlq.validation.v1"),
            )
        )
    ]


class Clock:
    def __init__(self) -> None:
        self.now = 10.0

    def monotonic(self) -> float:
        value = self.now
        self.now += 2.0
        return value


class FakePublisher:
    def __init__(self, failure_index: int | None = None, flush_remaining: int = 0):
        self.failure_index = failure_index
        self.flush_remaining = flush_remaining
        self.records: list[ReplayRecord] = []

    def publish(self, record: ReplayRecord, callback: object) -> None:
        self.records.append(record)
        error = RuntimeError("delivery failed") if len(self.records) - 1 == self.failure_index else None
        callback(error)  # type: ignore[operator]

    def flush(self, timeout: float) -> int:
        self.timeout = timeout
        return self.flush_remaining


class ReplayTests(unittest.TestCase):
    def make_pacer(self, controller: ReplayController) -> ReplayPacer:
        return ReplayPacer(PacingConfig(no_sleep=True), controller)

    def test_summary_counts_acknowledged_records_topics_dlq_and_throughput(self) -> None:
        clock = Clock()
        controller = ReplayController(monotonic=clock.monotonic)
        publisher = FakePublisher()

        summary = replay(
            records(),
            publisher,
            self.make_pacer(controller),
            controller,
            monotonic=clock.monotonic,
        )

        self.assertEqual(3, summary.attempted)
        self.assertEqual(3, summary.delivered)
        self.assertEqual(0, summary.failed)
        self.assertEqual(1, summary.dlq)
        self.assertEqual(
            {
                "marketing.raw.web-events.v1": 1,
                "marketing.raw.order-events.v1": 1,
                "marketing.dlq.validation.v1": 1,
            },
            summary.topic_counts,
        )
        self.assertEqual(2.0, summary.duration_seconds)
        self.assertEqual(1.5, summary.events_per_second)
        self.assertTrue(summary.success)

    def test_callback_failure_and_flush_timeout_make_summary_fail(self) -> None:
        clock = Clock()
        controller = ReplayController(monotonic=clock.monotonic)
        summary = replay(
            records(),
            FakePublisher(failure_index=1, flush_remaining=1),
            self.make_pacer(controller),
            controller,
            monotonic=clock.monotonic,
        )

        self.assertEqual(3, summary.attempted)
        self.assertEqual(2, summary.delivered)
        self.assertEqual(2, summary.failed)
        self.assertFalse(summary.success)
        self.assertIn("delivery failed", summary.errors[0])
        self.assertIn("flush timed out", summary.errors[1])

    def test_replay_stops_cleanly_before_next_record(self) -> None:
        controller = ReplayController()
        publisher = FakePublisher()
        source = iter(records())
        first = next(source)
        controller.stop()

        summary = replay(
            [first, *source], publisher, self.make_pacer(controller), controller
        )

        self.assertEqual(0, summary.attempted)
        self.assertEqual([], publisher.records)

    def test_repeated_runs_publish_identical_event_ids_and_payloads(self) -> None:
        outputs: list[list[tuple[str, bytes]]] = []
        for _ in range(2):
            controller = ReplayController()
            publisher = FakePublisher()
            replay(records(), publisher, self.make_pacer(controller), controller)
            outputs.append([(row.event_id, row.value) for row in publisher.records])

        self.assertEqual(outputs[0], outputs[1])


if __name__ == "__main__":
    unittest.main()
