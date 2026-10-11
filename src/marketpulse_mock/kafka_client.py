"""Thin, lazy-loaded confluent-kafka adapter for replay delivery."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol

from .kafka_contracts import TOPIC_CONTRACTS
from .replay_reader import ReplayRecord


DeliveryCallback = Callable[[Exception | None], None]


class KafkaPublisher(Protocol):
    def publish(
        self,
        record: ReplayRecord,
        callback: DeliveryCallback,
        *,
        cancelled: Callable[[], bool] = lambda: False,
    ) -> None: ...

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

    def publish(
        self,
        record: ReplayRecord,
        callback: DeliveryCallback,
        *,
        cancelled: Callable[[], bool] = lambda: False,
    ) -> None:
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
                if cancelled():
                    raise RuntimeError("Kafka publication cancelled during backpressure")
        self._producer.poll(0.0)

    def flush(self, timeout: float) -> int:
        return int(self._producer.flush(timeout))


def create_topics(
    bootstrap_servers: str,
    *,
    admin_factory: Callable[[dict[str, object]], object] | None = None,
    topic_factory: Callable[..., object] | None = None,
    config_resource_factory: Callable[[str], object] | None = None,
) -> TopicBootstrapSummary:
    """Create all versioned topics, treating existing topics as success."""

    if admin_factory is None or topic_factory is None:
        try:
            from confluent_kafka.admin import (
                AdminClient,
                ConfigResource,
                NewTopic,
                ResourceType,
            )
        except ImportError as error:
            raise RuntimeError(
                "Kafka support is not installed; run pip install '.[kafka]'"
            ) from error
        admin_factory = admin_factory or AdminClient
        topic_factory = topic_factory or NewTopic
        config_resource_factory = config_resource_factory or (
            lambda name: ConfigResource(ResourceType.TOPIC, name)
        )

    admin = admin_factory({"bootstrap.servers": bootstrap_servers})
    topics = [
        topic_factory(
            contract.topic,
            num_partitions=contract.partitions,
            replication_factor=1,
            config={"retention.ms": "604800000"},
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

    if hasattr(admin, "list_topics"):
        metadata = admin.list_topics(timeout=10.0)
        for contract in TOPIC_CONTRACTS.values():
            topic_metadata = metadata.topics.get(contract.topic)
            actual_partitions = (
                len(topic_metadata.partitions) if topic_metadata is not None else 0
            )
            if actual_partitions != contract.partitions:
                failed += 1
                errors.append(
                    f"{contract.topic}: expected {contract.partitions} partitions, "
                    f"got {actual_partitions}"
                )
    if config_resource_factory is not None and hasattr(admin, "describe_configs"):
        resources = [
            config_resource_factory(contract.topic)
            for contract in TOPIC_CONTRACTS.values()
        ]
        for resource, future in admin.describe_configs(resources).items():
            try:
                configuration = future.result()
                retention = configuration.get("retention.ms")
                actual_retention = getattr(retention, "value", retention)
                if actual_retention != "604800000":
                    failed += 1
                    errors.append(
                        f"{resource.name}: expected retention.ms=604800000, "
                        f"got {actual_retention}"
                    )
            except Exception as error:
                failed += 1
                errors.append(f"{resource.name}: cannot verify config: {error}")
    return TopicBootstrapSummary(created, existing, failed, tuple(errors))
