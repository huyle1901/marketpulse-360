import unittest
from datetime import datetime, timezone
from pathlib import Path

from marketpulse_app.event_worker import consumer_config, drain
from test_app_live_events import event

NOW = datetime(2026, 10, 31, 9, tzinfo=timezone.utc)


class Message:
    def __init__(self, value, offset, error=None):
        self._value, self._offset, self._error = value, offset, error

    def error(self): return self._error
    def value(self): return self._value
    def partition(self): return 0
    def offset(self): return self._offset


class Consumer:
    def __init__(self, messages):
        self.messages, self.commits, self.calls = messages, 0, []

    def consume(self, num_messages, timeout):
        self.calls.append((num_messages, timeout))
        return self.messages

    def commit(self, asynchronous=True):
        assert asynchronous is False
        self.commits += 1


class Store:
    def __init__(self, fail=False):
        self.batches, self.fail = [], fail

    def write_batch(self, rows):
        if self.fail:
            raise RuntimeError("clickhouse down")
        self.batches.append(rows)


class EventWorkerTests(unittest.TestCase):
    def test_consumer_reads_committed_from_earliest_without_auto_commit(self):
        config = consumer_config("kafka:29092")
        self.assertEqual(config["group.id"], "marketpulse-live-events-v1")
        self.assertEqual(config["isolation.level"], "read_committed")
        self.assertFalse(config["enable.auto.commit"])
        self.assertEqual(config["auto.offset.reset"], "earliest")

    def test_drain_writes_one_batch_then_commits(self):
        consumer, store = Consumer([Message(event(), 1), Message(event(event_id="e2"), 2)]), Store()
        self.assertEqual(drain(consumer, store, lambda: NOW, max_records=50, max_wait=0.5), 2)
        self.assertEqual(consumer.calls, [(50, 0.5)])
        self.assertEqual([row["offset"] for row in store.batches[0]], [1, 2])
        self.assertEqual(consumer.commits, 1)

    def test_empty_poll_neither_writes_nor_commits(self):
        consumer, store = Consumer([]), Store()
        self.assertEqual(drain(consumer, store, lambda: NOW), 0)
        self.assertEqual((store.batches, consumer.commits), ([], 0))

    def test_failures_commit_nothing(self):
        consumer = Consumer([Message(event(), 1)])
        with self.assertRaises(RuntimeError):
            drain(consumer, Store(fail=True), lambda: NOW)
        self.assertEqual(consumer.commits, 0)
        broken, store = Consumer([Message(event(), 1), Message(None, 2, error="boom")]), Store()
        with self.assertRaises(ValueError):
            drain(broken, store, lambda: NOW)
        self.assertEqual((store.batches, broken.commits), ([], 0))

    def test_compose_runs_the_worker_in_the_app_profile(self):
        compose = Path(__file__).resolve().parents[1].joinpath("compose.yaml").read_text()
        self.assertIn("\n  live-events-worker:\n", compose)
        service = compose.split("\n  live-events-worker:\n", 1)[1].split("\n\n", 1)[0]
        self.assertIn('command: ["python", "-m", "marketpulse_app.event_worker"]', service)
        self.assertIn("profiles: [app]", service)
        self.assertIn("marketpulse-live-events-heartbeat", service)


if __name__ == "__main__":
    unittest.main()
