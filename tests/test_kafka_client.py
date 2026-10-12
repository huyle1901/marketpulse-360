from __future__ import annotations

import unittest
from datetime import datetime, timezone

from marketpulse_mock.kafka_client import ConfluentKafkaPublisher, create_topics
from marketpulse_mock.kafka_contracts import ALL_TOPIC_CONTRACTS
from marketpulse_mock.replay_reader import ReplayRecord


def sample_record() -> ReplayRecord:
    return ReplayRecord(
        dataset="web_events",
        topic="marketing.raw.web-events.v1",
        key="customer-1",
        value=b'{"event_id":"event-1"}',
        event_id="event-1",
        event_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        headers=(("event-id", b"event-1"),),
        source_file="web_events/date=2026-01-01/part-00000.jsonl.gz",
    )


class FakeProducer:
    def __init__(self, config: dict[str, object], *, queue_failures: int = 0) -> None:
        self.config = config
        self.queue_failures = queue_failures
        self.produced: list[dict[str, object]] = []
        self.poll_calls: list[float] = []
        self.flush_result = 0

    def produce(self, **kwargs: object) -> None:
        if self.queue_failures:
            self.queue_failures -= 1
            raise BufferError("queue full")
        self.produced.append(kwargs)

    def poll(self, timeout: float) -> None:
        self.poll_calls.append(timeout)

    def flush(self, timeout: float) -> int:
        self.flush_timeout = timeout
        return self.flush_result


class KafkaClientTests(unittest.TestCase):
    def test_producer_uses_idempotent_delivery_configuration(self) -> None:
        created: list[FakeProducer] = []

        def factory(config: dict[str, object]) -> FakeProducer:
            created.append(FakeProducer(config))
            return created[-1]

        ConfluentKafkaPublisher("kafka:29092", producer_factory=factory)

        config = created[0].config
        self.assertEqual(True, config["enable.idempotence"])
        self.assertEqual("all", config["acks"])
        self.assertEqual("marketpulse-replay", config["client.id"])
        self.assertEqual("kafka:29092", config["bootstrap.servers"])

    def test_publish_preserves_exact_record_and_reports_callback_error(self) -> None:
        producer = FakeProducer({})
        publisher = ConfluentKafkaPublisher(
            "unused", producer_factory=lambda _: producer
        )
        errors: list[Exception | None] = []

        publisher.publish(sample_record(), errors.append)
        produced = producer.produced[0]
        self.assertEqual("marketing.raw.web-events.v1", produced["topic"])
        self.assertEqual(b"customer-1", produced["key"])
        self.assertEqual(b'{"event_id":"event-1"}', produced["value"])
        self.assertEqual([("event-id", b"event-1")], produced["headers"])

        callback = produced["on_delivery"]
        callback("broker rejected", None)  # type: ignore[operator]
        self.assertEqual("broker rejected", str(errors[0]))

    def test_queue_full_retries_same_record_after_polling(self) -> None:
        producer = FakeProducer({}, queue_failures=1)
        publisher = ConfluentKafkaPublisher(
            "unused", producer_factory=lambda _: producer
        )

        publisher.publish(sample_record(), lambda _: None)

        self.assertEqual(1, len(producer.produced))
        self.assertEqual([0.1, 0.0], producer.poll_calls)

    def test_permanent_queue_full_honors_cancellation(self) -> None:
        producer = FakeProducer({}, queue_failures=100)
        publisher = ConfluentKafkaPublisher(
            "unused", producer_factory=lambda _: producer
        )

        with self.assertRaisesRegex(RuntimeError, "cancelled"):
            publisher.publish(
                sample_record(),
                lambda _: None,
                cancelled=lambda: bool(producer.poll_calls),
            )

        self.assertEqual([0.1], producer.poll_calls)

    def test_flush_timeout_is_returned_to_replay_service(self) -> None:
        producer = FakeProducer({})
        producer.flush_result = 2
        publisher = ConfluentKafkaPublisher(
            "unused", producer_factory=lambda _: producer
        )

        self.assertEqual(2, publisher.flush(7.5))
        self.assertEqual(7.5, producer.flush_timeout)

    def test_create_topics_uses_all_contracts_and_is_idempotent(self) -> None:
        calls: list[tuple[dict[str, object], list[object]]] = []

        class Admin:
            def __init__(self, config: dict[str, object]) -> None:
                self.config = config

            def create_topics(self, topics: list[object]) -> dict[str, object]:
                calls.append((self.config, topics))
                return {topic.name: AlreadyExistsFuture() for topic in topics}

        class Topic:
            def __init__(self, name: str, num_partitions: int, replication_factor: int, config: dict[str, str]):
                self.name = name
                self.num_partitions = num_partitions
                self.replication_factor = replication_factor
                self.config = config

        class AlreadyExistsFuture:
            def result(self) -> None:
                raise RuntimeError("TOPIC_ALREADY_EXISTS")

        result = create_topics(
            "kafka:29092", admin_factory=Admin, topic_factory=Topic
        )

        self.assertEqual(10, len(calls[0][1]))
        self.assertTrue(all(topic.replication_factor == 1 for topic in calls[0][1]))
        expected = {contract.topic: contract for contract in ALL_TOPIC_CONTRACTS.values()}
        for topic in calls[0][1]:
            contract = expected[topic.name]
            self.assertEqual(contract.partitions, topic.num_partitions)
            self.assertEqual(
                {
                    "retention.ms": str(contract.retention_ms),
                    "cleanup.policy": contract.cleanup_policy,
                },
                topic.config,
            )
        self.assertEqual(10, result.existing)
        self.assertEqual(0, result.failed)

    def test_create_topics_rejects_partition_and_retention_drift(self) -> None:
        class Value:
            def __init__(self, value: str) -> None:
                self.value = value

        class Future:
            def __init__(self, value: object) -> None:
                self.value = value

            def result(self) -> object:
                return self.value

        class Metadata:
            topics = {
                contract.topic: type("TopicMetadata", (), {"partitions": {0: object()}})()
                for contract in ALL_TOPIC_CONTRACTS.values()
            }

        class Resource:
            def __init__(self, name: str) -> None:
                self.name = name

        class Topic:
            def __init__(self, name: str, **kwargs: object) -> None:
                self.name = name

        class Admin:
            def __init__(self, config: dict[str, object]) -> None:
                pass

            def create_topics(self, topics: list[object]) -> dict[str, Future]:
                return {topic.name: Future(None) for topic in topics}

            def list_topics(self, timeout: float) -> Metadata:
                return Metadata()

            def describe_configs(self, resources: list[Resource]) -> dict[Resource, Future]:
                return {
                    resource: Future(
                        {
                            "retention.ms": Value("123"),
                            "cleanup.policy": Value("unexpected"),
                        }
                    )
                    for resource in resources
                }

        summary = create_topics(
            "kafka:29092",
            admin_factory=Admin,
            topic_factory=Topic,
            config_resource_factory=Resource,
        )

        self.assertEqual(30, summary.failed)
        self.assertTrue(any("expected 3 partitions" in error for error in summary.errors))
        self.assertTrue(any("expected 6 partitions" in error for error in summary.errors))
        self.assertTrue(any("expected retention.ms" in error for error in summary.errors))
        self.assertTrue(any("expected cleanup.policy" in error for error in summary.errors))


if __name__ == "__main__":
    unittest.main()
