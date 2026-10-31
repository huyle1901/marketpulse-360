"""Strict boundary for Flink's clean customer events projected into the live store."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .contracts import code, identifier, integer
from .streaming import utc_timestamp

TOPIC = "marketing.clean.customer-events.v1"
EVENTS = {"web": {"page_view", "view_item", "add_to_cart", "begin_checkout", "purchase"},
          "mobile": {"page_view", "view_item", "add_to_cart", "begin_checkout", "purchase"},
          "commerce": {"order_created", "order_refunded"},
          "email": {"email_sent", "email_delivered", "email_opened", "email_clicked", "email_unsubscribed"}}
DEVICES = {"desktop", "mobile", "tablet"}
COLUMNS = ("partition", "offset", "received_at", "event_id", "event_name", "source", "event_time", "identity_key",
           "session_id", "campaign_key", "consent_analytics", "eligible_for_aggregates", "late_status", "is_bot",
           "device_type", "product_id", "currency", "gross_amount_minor", "refund_amount_minor", "items")
DDL = ("partition UInt32,offset UInt64,received_at DateTime64(6,'UTC'),event_id String,event_name LowCardinality(String),"
       "source LowCardinality(String),event_time DateTime64(6,'UTC'),identity_key String,session_id Nullable(String),"
       "campaign_key Nullable(String),consent_analytics Bool,eligible_for_aggregates Bool,late_status LowCardinality(String),"
       "is_bot Bool,device_type LowCardinality(String),product_id Nullable(String),currency Nullable(String),"
       "gross_amount_minor Int64,refund_amount_minor Int64,items Array(Tuple(String,UInt32))")


def _optional(value: object) -> str | None:
    if value is None:
        return None
    identifier(value)
    return value


def _boolean(value: object) -> bool:
    if type(value) is not bool:
        raise ValueError("expected boolean")
    return value


def parse_event(value: bytes, partition: int, offset: int, received_at: datetime) -> dict[str, object]:
    if not isinstance(value, bytes) or len(value) > 1048576:
        raise ValueError("invalid event bytes/size")
    try:
        row = json.loads(value)
        if not isinstance(row, dict) or row.get("schema_version") != 1 or type(row["schema_version"]) is not int:
            raise ValueError("clean event schema mismatch")
        source, name, payload = row["source"], row["event_name"], row["payload"]
        if source not in EVENTS or name not in EVENTS[source] or not isinstance(payload, dict):
            raise ValueError("unsupported event for source")
        identifier(row["event_id"])
        identifier(row["identity_key"])
        integer(partition)
        integer(offset)
        if not isinstance(received_at, datetime) or received_at.tzinfo is None:
            raise ValueError("received_at must be aware")
        currency = payload.get("currency") if source == "commerce" else None
        gross = payload.get("gross_amount_minor", 0) if source == "commerce" else 0
        refund = payload.get("refund_amount_minor", 0) if source == "commerce" else 0
        integer(gross)
        integer(refund)
        code(currency, 3)
        if source == "commerce" and currency is None:
            raise ValueError("commerce money requires currency")
        items = []
        for item in (payload.get("items") or []) if source == "commerce" else []:
            identifier(item["product_id"])
            integer(item["quantity"], 1)
            items.append((item["product_id"], item["quantity"]))
        device = payload.get("device_type")
        return {"partition": partition, "offset": offset, "received_at": received_at.astimezone(timezone.utc),
                "event_id": row["event_id"], "event_name": name, "source": source,
                "event_time": utc_timestamp(row["event_time"]), "identity_key": row["identity_key"],
                "session_id": _optional(row.get("session_id")), "campaign_key": _optional(row.get("campaign_key")),
                "consent_analytics": _boolean(row["consent_analytics"]),
                "eligible_for_aggregates": _boolean(row["eligible_for_aggregates"]), "late_status": str(row["late_status"]),
                "is_bot": payload.get("is_bot") is True, "device_type": device if device in DEVICES else "unknown",
                "product_id": _optional(payload.get("product_id")), "currency": currency,
                "gross_amount_minor": gross, "refund_amount_minor": refund, "items": items}
    except (TypeError, KeyError, AttributeError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("invalid clean event") from error


class LiveEventStore:
    def __init__(self, client: Any, database: str = "marketpulse_live"):
        self.client, self.database = client, database

    def initialize(self, source_identity: str) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.client.command(f"CREATE TABLE IF NOT EXISTS {self.database}.event_source_identity (identity String) "
                            "ENGINE=MergeTree ORDER BY identity")
        rows = self.client.query(f"SELECT DISTINCT identity FROM {self.database}.event_source_identity").result_rows
        if rows and rows != [(source_identity,)]:
            raise ValueError("Kafka cluster/topic changed; explicit recovery required")
        if not rows:
            self.client.insert(f"{self.database}.event_source_identity", [(source_identity,)], column_names=["identity"])
        # Kafka position is the replay-safe key: re-delivered records collapse under FINAL.
        self.client.command(f"CREATE TABLE IF NOT EXISTS {self.database}.events ({DDL}) "
                            "ENGINE=ReplacingMergeTree ORDER BY (partition,offset)")

    def write_batch(self, rows: list[dict[str, object]]) -> None:
        if not rows:
            return
        self.client.insert(f"{self.database}.events", [[row[name] for name in COLUMNS] for row in rows],
                           column_names=list(COLUMNS), settings={"async_insert": 0})

    def source_identity(self) -> str:
        rows = self.client.query(f"SELECT DISTINCT identity FROM {self.database}.event_source_identity").result_rows
        if len(rows) != 1:
            raise ValueError("ambiguous live event source identity")
        return rows[0][0]
