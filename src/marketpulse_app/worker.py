"""Serving adapter and periodic certified-rule evaluator, separate from FastAPI."""
from __future__ import annotations

import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from .alerts import evaluate_alerts
from .settings import clients, load_settings
from .streaming import TOPIC, deliver
from marketpulse_batch.publication import NoPublishedRelease


def consumer_config(bootstrap_servers: str) -> dict[str, object]:
    return {"bootstrap.servers": bootstrap_servers, "group.id": "marketpulse-campaign-serving-v1",
            "isolation.level": "read_committed", "enable.auto.commit": False,
            "enable.auto.offset.store": False, "auto.offset.reset": "earliest"}


def main() -> int:
    from confluent_kafka import Consumer, TopicCollection
    from confluent_kafka.admin import AdminClient
    settings = load_settings(os.environ)
    analytics, store = clients(settings)
    store.initialize()
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    cluster = admin.describe_cluster().result(10)
    topic = admin.describe_topics(TopicCollection([TOPIC]))[TOPIC].result(10)
    analytics.live_store.initialize(f"{cluster.cluster_id}/{topic.topic_id}")
    consumer = Consumer(consumer_config(settings.kafka_bootstrap_servers))
    consumer.subscribe([TOPIC])
    stopped = False
    def stop(*args):
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    next_evaluation = 0.0
    try:
        while not stopped:
            message = consumer.poll(1.0)
            if message is not None:
                deliver(message, analytics.live_store, consumer, datetime.now(timezone.utc))
            if time.monotonic() >= next_evaluation:
                try:
                    evaluate_alerts(analytics, store, datetime.now(timezone.utc).date())
                except NoPublishedRelease:
                    pass
                except Exception:
                    print("alert evaluation unavailable; retrying", file=sys.stderr, flush=True)
                next_evaluation = time.monotonic()+5
            Path("/tmp/marketpulse-worker-heartbeat").touch()
    finally:
        consumer.close()
        analytics.client.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"serving worker stopped: {type(error).__name__}; no unacknowledged offsets committed", file=sys.stderr)
        raise SystemExit(1)
