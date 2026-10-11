"""Thin, lazy-loaded confluent-kafka adapter for replay delivery."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol

from .kafka_contracts import TOPIC_CONTRACTS
from .replay_reader import ReplayRecord


DeliveryCallback = Callable[[Exception | None], None]


class KafkaPublisher(Protocol):
    def publish(self, record: ReplayRecord, callback: DeliveryCallback) -> None: ...

    def flush(self, timeout: float) -> int: ...


@dataclass(frozen=True, slots=True)
class TopicBootstrapSummary:
    created: int
    existing: int
    failed: int
    errors: tuple[str, ...] = ()

    @property
    def success(self) -> bool:
        return self.failed == 0


class ConfluentKafkaPublisher:
    """Publish exact replay bytes with Kafka idempotence and backpressure."""

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        producer_factory: Callable[[dict[str, object]], object] | None = None,
    ) -> None:
        if producer_factory is None:
            try:
                from confluent_kafka import Producer
            except ImportError as error:
                raise RuntimeError(
                    "Kafka support is not installed; run pip install '.[kafka]'"
                ) from error
            producer_factory = Producer
        config: dict[str, object] = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "marketpulse-replay",
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 120_000,
        }
        self._producer = producer_factory(config)

    def publish(self, record: ReplayRecord, callback: DeliveryCallback) -> None:
        def on_delivery(error: object, _message: object) -> None:
            callback(RuntimeError(str(error)) if error is not None else None)

        arguments = {
            "topic": record.topic,
            "key": record.key.encode("utf-8"),
            "value": record.value,
            "headers": list(record.headers),
            "on_delivery": on_delivery,
        }
        while True:
            try:
                self._producer.produce(**arguments)
                break
            except BufferError:
                self._producer.poll(0.1)
        self._producer.poll(0.0)

    def flush(self, timeout: float) -> int:
        return int(self._producer.flush(timeout))


def create_topics(
    bootstrap_servers: str,
    *,
    admin_factory: Callable[[dict[str, object]], object] | None = None,
    topic_factory: Callable[..., object] | None = None,
) -> TopicBootstrapSummary:
    """Create all versioned topics, treating existing topics as success."""

    if admin_factory is None or topic_factory is None:
        try:
            from confluent_kafka.admin import AdminClient, NewTopic
        except ImportError as error:
            raise RuntimeError(
                "Kafka support is not installed; run pip install '.[kafka]'"
            ) from error
        admin_factory = admin_factory or AdminClient
        topic_factory = topic_factory or NewTopic

    admin = admin_factory({"bootstrap.servers": bootstrap_servers})
    topics = [
        topic_factory(
            contract.topic,
            num_partitions=contract.partitions,
            replication_factor=1,
        )
        for contract in TOPIC_CONTRACTS.values()
    ]
    futures = admin.create_topics(topics)
    created = existing = failed = 0
    errors: list[str] = []
    for topic, future in futures.items():
        try:
            future.result()
            created += 1
        except Exception as error:  # confluent exceptions are optional at import time
            message = str(error)
            if "TOPIC_ALREADY_EXISTS" in message.upper().replace(" ", "_"):
                existing += 1
            else:
                failed += 1
                errors.append(f"{topic}: {message}")
    return TopicBootstrapSummary(created, existing, failed, tuple(errors))
