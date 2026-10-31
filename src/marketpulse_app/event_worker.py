"""Projects Flink's clean customer events into the live store for the Live View."""
from __future__ import annotations

import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .live_events import TOPIC, LiveEventStore, parse_event
from .settings import load_settings

HEARTBEAT = Path("/tmp/marketpulse-live-events-heartbeat")


def consumer_config(bootstrap_servers: str) -> dict[str, object]:
    return {"bootstrap.servers": bootstrap_servers, "group.id": "marketpulse-live-events-v1",
            "isolation.level": "read_committed", "enable.auto.commit": False,
            "enable.auto.offset.store": False, "auto.offset.reset": "earliest"}


def drain(consumer: Any, store: Any, now: Callable[[], datetime], max_records: int = 500, max_wait: float = 1.0) -> int:
    """Write one polled batch, then commit; any failure leaves offsets uncommitted for replay."""
    messages = consumer.consume(max_records, max_wait)
    if not messages:
        return 0
    received_at = now()
    rows = []
    for message in messages:
        if message.error():
            raise ValueError("Kafka consumption failed")
        rows.append(parse_event(message.value(), message.partition(), message.offset(), received_at))
    store.write_batch(rows)
    from confluent_kafka import TopicPartition
    # Auto offset store is off, so commit the next position of every partition in the batch explicitly.
    following: dict[tuple[str, int], int] = {}
    for message in messages:
        key = (message.topic(), message.partition())
        following[key] = max(following.get(key, 0), message.offset() + 1)
    consumer.commit(offsets=[TopicPartition(topic, partition, offset) for (topic, partition), offset in sorted(following.items())],
                    asynchronous=False)
    return len(rows)


def main() -> int:
    import clickhouse_connect
    from confluent_kafka import Consumer, TopicCollection
    from confluent_kafka.admin import AdminClient
    settings = load_settings(os.environ)
    client = clickhouse_connect.get_client(host=settings.clickhouse_host, port=settings.clickhouse_port,
        username=settings.clickhouse_user, password=settings.clickhouse_password, autogenerate_session_id=False,
        connect_timeout=5, send_receive_timeout=30)
    store = LiveEventStore(client)
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    cluster = admin.describe_cluster().result(10)
    topic = admin.describe_topics(TopicCollection([TOPIC]))[TOPIC].result(10)
    store.initialize(f"{cluster.cluster_id}/{topic.topic_id}")
    consumer = Consumer(consumer_config(settings.kafka_bootstrap_servers))
    consumer.subscribe([TOPIC])
    stopped = False
    def stop(*args):
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        while not stopped:
            drain(consumer, store, lambda: datetime.now(timezone.utc))
            HEARTBEAT.touch()
    finally:
        consumer.close()
        client.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"live events worker stopped: {type(error).__name__}; no unwritten offsets committed", file=sys.stderr)
        raise SystemExit(1)
