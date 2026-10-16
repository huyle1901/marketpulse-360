"""Strict boundary for existing Flink cumulative campaign snapshots."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from .contracts import code, identifier, integer

TOPIC = "marketing.gold.campaign-metrics.v1"
COUNTERS = ("sessions", "page_view_sessions", "view_item_sessions", "add_to_cart_sessions",
            "checkout_sessions", "purchase_sessions", "order_count", "refund_count",
            "gross_revenue_minor", "refund_amount_minor", "net_revenue_minor")
FIELDS = {"schema_version", "metric_key", "campaign_id", "event_date", "currency", "max_event_time", "processed_at", *COUNTERS}


def utc_timestamp(value: str) -> datetime:
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if timestamp.tzinfo is None or timestamp.utcoffset().total_seconds() != 0:
            raise ValueError
        return timestamp.astimezone(timezone.utc)
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("timestamp must be UTC") from error


@dataclass(frozen=True, slots=True)
class CampaignSnapshot:
    payload: dict[str, object]
    partition: int
    offset: int
    received_at: datetime
    canonical: str
    digest: str


def parse_snapshot(value: bytes, key: bytes, partition: int, offset: int, received_at: datetime) -> CampaignSnapshot:
    if not isinstance(value, bytes) or len(value) > 1048576:
        raise ValueError("invalid snapshot bytes/size")
    try:
        row = json.loads(value)
        if not isinstance(row, dict) or set(row) != FIELDS or type(row["schema_version"]) is not int or row["schema_version"] != 1:
            raise ValueError("snapshot schema mismatch")
        identifier(row["campaign_id"])
        day = date.fromisoformat(row["event_date"])
        if row["event_date"] != day.isoformat() or not date(1900, 1, 1) <= day <= date(2299, 12, 31):
            raise ValueError("invalid event date")
        expected_key = f"{row['campaign_id']}|{day}"
        if key.decode("utf-8") != expected_key or row["metric_key"] != expected_key:
            raise ValueError("snapshot key mismatch")
        for name in COUNTERS:
            integer(row[name], -(2**63) if name == "net_revenue_minor" else 0)
        if any(row[name] > row["sessions"] for name in COUNTERS[1:6]):
            raise ValueError("stage exceeds sessions")
        if row["gross_revenue_minor"] - row["refund_amount_minor"] != row["net_revenue_minor"]:
            raise ValueError("snapshot money mismatch")
        code(row["currency"], 3)
        if row["currency"] is None and (row["gross_revenue_minor"] or row["refund_amount_minor"]):
            raise ValueError("money requires currency")
        if utc_timestamp(row["max_event_time"]).date() != day:
            raise ValueError("event time/date mismatch")
        utc_timestamp(row["processed_at"])
        integer(partition)
        integer(offset)
        if not isinstance(received_at, datetime) or received_at.tzinfo is None:
            raise ValueError("received_at must be aware")
        canonical = json.dumps(row, sort_keys=True, separators=(",", ":"))
        return CampaignSnapshot(row, partition, offset, received_at.astimezone(timezone.utc), canonical,
                                hashlib.sha256(canonical.encode()).hexdigest())
    except (TypeError, KeyError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("invalid campaign snapshot") from error


def deliver(message: Any, store: Any, consumer: Any, now: datetime) -> None:
    if message.error():
        raise ValueError("Kafka consumption failed")
    snapshot = parse_snapshot(message.value(), message.key(), message.partition(), message.offset(), now)
    store.write(snapshot)
    consumer.commit(message=message, asynchronous=False)
