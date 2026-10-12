"""Deterministic fixture and latest-by-key verification for the MP-03 Flink smoke test."""

from __future__ import annotations

import argparse
import base64
import binascii
from collections import Counter, defaultdict
from datetime import date, datetime
import gzip
import hashlib
import json
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .kafka_contracts import FLINK_TOPIC_CONTRACTS, load_contract_schema


OUTPUT_TOPICS = (
    "marketing.clean.customer-events.v1",
    "marketing.clean.sessions.v1",
    "marketing.gold.funnel-metrics.v1",
    "marketing.gold.revenue-metrics.v1",
    "marketing.gold.campaign-metrics.v1",
    "marketing.dlq.flink-validation.v1",
)
LATE_PROBE_ID = "00000000-0000-5000-8000-000000000099"
ERROR_NAMESPACE = uuid.UUID("9a3d4d70-cd8b-4b65-9b50-f86265f41915")


@dataclass(frozen=True, slots=True)
class FixtureOracle:
    valid_rows: int
    unique_events: dict[str, dict[str, Any]]
    duplicate_copies: int
    source_marked_late: int
    session_ids: frozenset[str]
    order_count: int
    refund_count: int
    gross_revenue_minor: int
    refund_amount_minor: int
    net_revenue_minor: int
    session_metrics: dict[str, dict[str, Any]]
    campaign_metrics: dict[str, dict[str, Any]]


def canonical_instant(value: str) -> str:
    parsed = parse_instant(value)
    if parsed.microsecond == 0:
        timespec = "seconds"
    elif parsed.microsecond % 1000 == 0:
        timespec = "milliseconds"
    else:
        timespec = "microseconds"
    return parsed.isoformat(timespec=timespec).replace("+00:00", "Z")


def parse_instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _matches_json_type(value: Any, expected: str) -> bool:
    return {
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool),
        "boolean": isinstance(value, bool),
        "null": value is None,
    }.get(expected, False)


def _schema_errors(value: Any, schema: dict[str, Any], path: str = "$") -> list[str]:
    """Validate the JSON-Schema subset used by the six committed Flink contracts."""

    errors: list[str] = []
    declared_type = schema.get("type")
    types = declared_type if isinstance(declared_type, list) else [declared_type]
    types = [item for item in types if isinstance(item, str)]
    if types and not any(_matches_json_type(value, item) for item in types):
        return [f"{path} expected type {types} actual={type(value).__name__}"]
    if "const" in schema and value != schema["const"]:
        errors.append(f"{path} expected const {schema['const']!r} actual={value!r}")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path} expected enum {schema['enum']!r} actual={value!r}")
    if isinstance(value, str):
        if len(value) < int(schema.get("minLength", 0)):
            errors.append(f"{path} shorter than minLength")
        try:
            if schema.get("format") == "uuid":
                uuid.UUID(value)
            elif schema.get("format") == "date-time":
                parse_instant(value)
            elif schema.get("format") == "date":
                date.fromisoformat(value)
            if schema.get("contentEncoding") == "base64":
                base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error) as error:
            errors.append(f"{path} invalid {schema.get('format') or 'base64'}: {error}")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            errors.append(f"{path} below minimum {schema['minimum']}")
    if isinstance(value, dict):
        required = schema.get("required", [])
        for field in required:
            if field not in value:
                errors.append(f"{path}.{field} is required")
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            for field in value.keys() - properties.keys():
                errors.append(f"{path}.{field} is not allowed")
        for field, child in properties.items():
            if field in value:
                errors.extend(_schema_errors(value[field], child, f"{path}.{field}"))
    return errors


def resolve_contract_dir() -> Path:
    candidates = (
        Path.cwd() / "contracts" / "kafka",
        Path(__file__).resolve().parents[2] / "contracts" / "kafka",
        Path("/app/contracts/kafka"),
    )
    contract_dir = next((path for path in candidates if path.is_dir()), None)
    if contract_dir is None:
        raise ValueError(f"cannot locate Kafka contracts; searched={list(candidates)}")
    return contract_dir


def validate_output_contracts(output_dir: Path) -> None:
    contract_dir = resolve_contract_dir()
    for contract in FLINK_TOPIC_CONTRACTS.values():
        schema = load_contract_schema(contract_dir, contract)
        for index, row in enumerate(read_dump(output_dir / f"{contract.topic}.jsonl")):
            errors = _schema_errors(row.get("value"), schema)
            if errors:
                raise ValueError(
                    f"output schema mismatch topic={contract.topic} row={index}: "
                    + "; ".join(errors[:5])
                )


def expected_clean_value(row: dict[str, Any]) -> dict[str, Any]:
    payload = row["payload"]
    identity_key = next(
        (
            value
            for value in (
                row.get("customer_id"),
                row.get("anonymous_id"),
                row.get("session_id"),
                payload.get("order_id"),
                payload.get("message_id"),
            )
            if value
        ),
        None,
    )
    campaign_key = row.get("campaign_id") or payload.get("lifecycle_campaign_id")
    return {
        "schema_version": 1,
        "event_id": str(row["event_id"]),
        "event_name": row["event_name"],
        "event_time": canonical_instant(str(row["event_time"])),
        "ingested_at": canonical_instant(str(row["ingested_at"])),
        "source": row["source"],
        "channel": row["source"],
        "customer_id": row.get("customer_id"),
        "anonymous_id": row.get("anonymous_id"),
        "identity_key": identity_key,
        "session_id": row["session_id"],
        "campaign_id": row.get("campaign_id"),
        "campaign_key": campaign_key,
        "consent_analytics": row["consent_analytics"],
        "late_status": "source_marked_late" if row["is_late"] else "on_time",
        "eligible_for_aggregates": True,
        "payload": payload,
    }


def derive_fixture_oracle(fixture: Path) -> FixtureOracle:
    rows: list[dict[str, Any]] = []
    for dataset in ("web_events", "orders", "email_events"):
        for path in sorted((fixture / dataset).glob("date=*/*.jsonl.gz")):
            with gzip.open(path, "rt", encoding="utf-8") as stream:
                rows.extend(json.loads(line) for line in stream if line.strip())

    unique: dict[str, dict[str, Any]] = {}
    for row in rows:
        event_id = str(row["event_id"])
        previous = unique.get(event_id)
        if previous is None or parse_instant(str(row["event_time"])) < parse_instant(
            str(previous["event_time"])
        ):
            unique[event_id] = row
    web = [row for row in unique.values() if row["source"] in {"web", "mobile"}]
    orders = [row for row in unique.values() if row["source"] == "commerce"]
    created = [row for row in orders if row["event_name"] == "order_created"]
    refunded = [row for row in orders if row["event_name"] == "order_refunded"]
    gross = sum(int(row["payload"]["gross_amount_minor"]) for row in created)
    refunds = sum(int(row["payload"]["refund_amount_minor"]) for row in refunded)
    session_metrics: dict[str, dict[str, Any]] = {}
    campaign_sessions: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    campaign_revenue: dict[str, Counter[str]] = defaultdict(Counter)
    campaign_metadata: dict[str, dict[str, Any]] = {}
    stage_names = {
        "page_view": "page_view_sessions",
        "view_item": "view_item_sessions",
        "add_to_cart": "add_to_cart_sessions",
        "begin_checkout": "checkout_sessions",
        "purchase": "purchase_sessions",
    }
    for row in unique.values():
        event_date = str(row["event_time"])[:10]
        campaign_id = row.get("campaign_id") or row.get("payload", {}).get(
            "lifecycle_campaign_id"
        )
        metric_key = f"{campaign_id}|{event_date}" if campaign_id else None
        if metric_key:
            campaign_sessions[metric_key]
            metadata = campaign_metadata.setdefault(
                metric_key,
                {
                    "schema_version": 1,
                    "campaign_id": str(campaign_id),
                    "event_date": event_date,
                    "currency": None,
                    "max_event_time": canonical_instant(str(row["event_time"])),
                },
            )
            metadata["max_event_time"] = canonical_instant(
                max(
                    parse_instant(metadata["max_event_time"]),
                    parse_instant(str(row["event_time"])),
                ).isoformat()
            )
        if row["source"] in {"web", "mobile"}:
            session_id = str(row["session_id"])
            current = session_metrics.setdefault(
                session_id,
                {
                    "event_count": 0,
                    "page_view_count": 0,
                    "view_item_count": 0,
                    "add_to_cart_count": 0,
                    "checkout_count": 0,
                    "purchase_count": 0,
                    "gross_revenue_minor": 0,
                    "late_event_count": 0,
                },
            )
            current["event_count"] += 1
            counter_name = {
                "page_view": "page_view_count",
                "view_item": "view_item_count",
                "add_to_cart": "add_to_cart_count",
                "begin_checkout": "checkout_count",
                "purchase": "purchase_count",
            }.get(str(row["event_name"]))
            if counter_name:
                current[counter_name] += 1
            if row["event_name"] == "purchase":
                current["gross_revenue_minor"] += int(
                    row["payload"].get("amount_minor", 0)
                )
            if bool(row["is_late"]):
                current["late_event_count"] += 1
            if metric_key:
                campaign_sessions[metric_key]["sessions"].add(session_id)
                stage = stage_names.get(str(row["event_name"]))
                if stage:
                    campaign_sessions[metric_key][stage].add(session_id)
        if metric_key and row["source"] == "commerce":
            if row["event_name"] == "order_created":
                campaign_revenue[metric_key]["order_count"] += 1
                campaign_revenue[metric_key]["gross_revenue_minor"] += int(
                    row["payload"]["gross_amount_minor"]
                )
                campaign_metadata[metric_key]["currency"] = row["payload"]["currency"]
            elif row["event_name"] == "order_refunded":
                campaign_revenue[metric_key]["refund_count"] += 1
                campaign_revenue[metric_key]["refund_amount_minor"] += int(
                    row["payload"]["refund_amount_minor"]
                )

    for session_id, current in session_metrics.items():
        session_rows = sorted(
            (row for row in web if str(row["session_id"]) == session_id),
            key=lambda row: (parse_instant(str(row["event_time"])), str(row["event_id"])),
        )
        current.update(
            {
                "schema_version": 1,
                "customer_id": next(
                    (row["customer_id"] for row in session_rows if row.get("customer_id")),
                    None,
                ),
                "anonymous_id": next(
                    (row["anonymous_id"] for row in session_rows if row.get("anonymous_id")),
                    None,
                ),
                "campaign_id": next(
                    (row["campaign_id"] for row in session_rows if row.get("campaign_id")),
                    None,
                ),
                "session_start": canonical_instant(str(session_rows[0]["event_time"])),
                "session_end": canonical_instant(str(session_rows[-1]["event_time"])),
                "currency": next(
                    (
                        row["payload"].get("currency")
                        for row in session_rows
                        if row["event_name"] == "purchase"
                        and row["payload"].get("currency")
                    ),
                    None,
                ),
                "status": "closed",
            }
        )

    campaign_metrics: dict[str, dict[str, Any]] = {}
    for metric_key in set(campaign_sessions) | set(campaign_revenue):
        stages = campaign_sessions[metric_key]
        revenue = campaign_revenue[metric_key]
        campaign_metrics[metric_key] = {"metric_key": metric_key} | campaign_metadata[
            metric_key
        ] | {
            name: len(stages[name])
            for name in (
                "sessions",
                "page_view_sessions",
                "view_item_sessions",
                "add_to_cart_sessions",
                "checkout_sessions",
                "purchase_sessions",
            )
        } | {
            "order_count": revenue["order_count"],
            "refund_count": revenue["refund_count"],
            "gross_revenue_minor": revenue["gross_revenue_minor"],
            "refund_amount_minor": revenue["refund_amount_minor"],
            "net_revenue_minor": (
                revenue["gross_revenue_minor"] - revenue["refund_amount_minor"]
            ),
        }
    return FixtureOracle(
        valid_rows=len(rows),
        unique_events=unique,
        duplicate_copies=len(rows) - len(unique),
        source_marked_late=sum(bool(row["is_late"]) for row in unique.values()),
        session_ids=frozenset(str(row["session_id"]) for row in web),
        order_count=len(created),
        refund_count=len(refunded),
        gross_revenue_minor=gross,
        refund_amount_minor=refunds,
        net_revenue_minor=gross - refunds,
        session_metrics=session_metrics,
        campaign_metrics=campaign_metrics,
    )


def latest_by_key(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, tuple[int, int, dict[str, Any]]] = {}
    for row in rows:
        key = str(row["key"])
        position = (int(row.get("partition", 0)), int(row.get("offset", 0)))
        previous = latest.get(key)
        if previous is None or (position[0] == previous[0] and position[1] >= previous[1]):
            latest[key] = (position[0], position[1], dict(row["value"]))
    return {key: value for key, (_, _, value) in latest.items()}


def stable_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: stable_value(item)
            for key, item in value.items()
            if key not in {"processed_at", "detected_at"}
        }
    if isinstance(value, list):
        return [stable_value(item) for item in value]
    return value


def read_dump(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for ordinal, line in enumerate(path.read_text(encoding="utf-8").splitlines()):
        if not line.strip():
            continue
        decoded = json.loads(line)
        if isinstance(decoded.get("value"), dict) and "key" in decoded:
            rows.append(decoded)
            continue
        key = decoded.get("event_id") or decoded.get("session_id") or decoded.get("metric_key") or decoded.get("error_id")
        rows.append({"partition": 0, "offset": ordinal, "key": key, "value": decoded})
    return rows


def write_probe_records(fixture: Path, output_dir: Path) -> dict[str, Path]:
    """Create deterministic watermark, late-event, and malformed smoke inputs."""

    source_files = sorted((fixture / "web_events").glob("date=*/*.jsonl.gz"))
    if not source_files:
        raise ValueError(f"fixture has no web event parts: {fixture}")
    with gzip.open(source_files[0], "rt", encoding="utf-8") as stream:
        base = json.loads(next(line for line in stream if line.strip()))
    frontier = dict(base)
    frontier["event_time"] = "2030-01-01T00:00:00+00:00"
    frontier["ingested_at"] = "2030-01-01T00:00:01+00:00"
    frontier["is_late"] = False
    late = dict(base)
    late["event_id"] = LATE_PROBE_ID
    late["session_id"] = "smoke-watermark-late-session"
    late["event_time"] = "2025-01-01T00:00:00+00:00"
    late["ingested_at"] = "2030-01-01T00:00:02+00:00"
    late["is_late"] = False
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "frontier": output_dir / "frontier.json",
        "late": output_dir / "late.json",
        "malformed": output_dir / "malformed.txt",
    }
    paths["frontier"].write_text(json.dumps(frontier) + "\n", encoding="utf-8")
    paths["late"].write_text(json.dumps(late) + "\n", encoding="utf-8")
    paths["malformed"].write_text("not-json\n", encoding="utf-8")
    return paths


def consume_outputs(
    bootstrap_servers: str,
    output_dir: Path,
    *,
    timeout_seconds: float = 30.0,
) -> dict[str, int]:
    """Dump committed output records from the beginning of every output partition."""

    try:
        from confluent_kafka import Consumer, KafkaError, OFFSET_BEGINNING, TopicPartition
    except ImportError as error:  # pragma: no cover - exercised in the replay image
        raise RuntimeError("install marketpulse-mock[kafka] to consume smoke outputs") from error

    output_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for topic in OUTPUT_TOPICS:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": f"marketpulse-flink-smoke-{uuid.uuid4()}",
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
                "enable.partition.eof": True,
            }
        )
        try:
            metadata = consumer.list_topics(topic, timeout=10)
            topic_metadata = metadata.topics.get(topic)
            if topic_metadata is None or topic_metadata.error is not None:
                raise RuntimeError(f"cannot inspect Kafka topic {topic}")
            partitions = sorted(topic_metadata.partitions)
            consumer.assign(
                [TopicPartition(topic, partition, OFFSET_BEGINNING) for partition in partitions]
            )
            eof: set[int] = set()
            rows: list[dict[str, Any]] = []
            deadline = time.monotonic() + timeout_seconds
            while len(eof) < len(partitions):
                message = consumer.poll(1.0)
                if message is None:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"timed out consuming {topic}")
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        eof.add(message.partition())
                        continue
                    raise RuntimeError(f"Kafka consume error for {topic}: {message.error()}")
                deadline = time.monotonic() + timeout_seconds
                try:
                    value = json.loads(message.value().decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise RuntimeError(
                        f"non-JSON output at {topic}:{message.partition()}:{message.offset()}"
                    ) from error
                key_bytes = message.key()
                rows.append(
                    {
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "key": key_bytes.decode("utf-8") if key_bytes else None,
                        "value": value,
                    }
                )
            destination = output_dir / f"{topic}.jsonl"
            destination.write_text(
                "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
                encoding="utf-8",
            )
            counts[topic] = len(rows)
        finally:
            consumer.close()
    return counts


def publish_frontier_to_partitions(
    bootstrap_servers: str,
    frontier_file: Path,
    *,
    partitions: int = 6,
) -> int:
    """Advance every raw-web partition watermark with a deduplicated fixture event."""

    try:
        from confluent_kafka import Producer
    except ImportError as error:  # pragma: no cover - exercised in the replay image
        raise RuntimeError("install marketpulse-mock[kafka] to publish smoke probes") from error
    value = frontier_file.read_bytes().rstrip(b"\n")
    failures: list[str] = []

    def delivered(error: object, _message: object) -> None:
        if error is not None:
            failures.append(str(error))

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    for partition in range(partitions):
        producer.produce(
            "marketing.raw.web-events.v1",
            value=value,
            partition=partition,
            on_delivery=delivered,
        )
    remaining = producer.flush(30.0)
    if failures or remaining:
        raise RuntimeError(
            f"frontier publication failed errors={failures} remaining={remaining}"
        )
    return partitions


def verify(
    fixture: Path,
    output_dir: Path,
    input_dump: Path | None = None,
) -> dict[str, Any]:
    validate_output_contracts(output_dir)
    oracle = derive_fixture_oracle(fixture)
    clean_rows = read_dump(output_dir / "marketing.clean.customer-events.v1.jsonl")
    clean = latest_by_key({**row, "key": row["value"]["event_id"]} for row in clean_rows)
    expected_ids = set(oracle.unique_events)
    actual_fixture_ids = expected_ids & set(clean)
    errors: list[str] = []
    if actual_fixture_ids != expected_ids:
        errors.append(f"clean IDs missing={sorted(expected_ids - actual_fixture_ids)[:5]}")
    clean_counts = Counter(str(row["value"].get("event_id")) for row in clean_rows)
    repeated = sorted(event_id for event_id in expected_ids if clean_counts[event_id] != 1)
    if repeated:
        errors.append(f"clean fixture IDs must occur once, mismatched={repeated[:5]}")
    for event_id, expected in oracle.unique_events.items():
        actual = clean.get(event_id)
        if actual is None:
            continue
        for field, expected_value in expected_clean_value(expected).items():
            if actual.get(field) != expected_value:
                errors.append(
                    f"clean mismatch {event_id}.{field} "
                    f"expected={expected_value} actual={actual.get(field)}"
                )
    extra_keys = set(clean) - expected_ids
    probe_rows = [
        row for row in clean_rows if row["value"].get("event_id") == LATE_PROBE_ID
    ]
    probe = clean.get(LATE_PROBE_ID)
    if extra_keys != {LATE_PROBE_ID} or len(probe_rows) != 1 or probe is None:
        errors.append(
            f"watermark-late probe expected exactly once actual={len(probe_rows)} "
            f"extra_keys={sorted(extra_keys)}"
        )
    elif not (
        probe.get("late_status") == "watermark_late"
        and probe.get("eligible_for_aggregates") is False
        and probe.get("event_time") == "2025-01-01T00:00:00Z"
        and probe.get("session_id") == "smoke-watermark-late-session"
    ):
        errors.append("watermark-late probe payload mismatch")

    sessions = latest_by_key(read_dump(output_dir / "marketing.clean.sessions.v1.jsonl"))
    if set(sessions) != set(oracle.session_ids):
        errors.append(f"session keys expected={len(oracle.session_ids)} actual={len(sessions)}")
    for session_id, expected in oracle.session_metrics.items():
        actual = sessions.get(session_id)
        if actual is None:
            continue
        for field, expected_value in {"session_id": session_id, **expected}.items():
            if actual.get(field) != expected_value:
                errors.append(
                    f"session mismatch {session_id}.{field} "
                    f"expected={expected_value} actual={actual.get(field)}"
                )

    revenue = latest_by_key(read_dump(output_dir / "marketing.gold.revenue-metrics.v1.jsonl"))
    totals = {
        "order_count": sum(int(value["order_count"]) for value in revenue.values()),
        "refund_count": sum(int(value["refund_count"]) for value in revenue.values()),
        "gross_revenue_minor": sum(int(value["gross_revenue_minor"]) for value in revenue.values()),
        "refund_amount_minor": sum(int(value["refund_amount_minor"]) for value in revenue.values()),
        "net_revenue_minor": sum(int(value["net_revenue_minor"]) for value in revenue.values()),
    }
    expected_totals = {
        "order_count": oracle.order_count,
        "refund_count": oracle.refund_count,
        "gross_revenue_minor": oracle.gross_revenue_minor,
        "refund_amount_minor": oracle.refund_amount_minor,
        "net_revenue_minor": oracle.net_revenue_minor,
    }
    if totals != expected_totals:
        errors.append(f"revenue expected={expected_totals} actual={totals}")

    campaign = latest_by_key(read_dump(output_dir / "marketing.gold.campaign-metrics.v1.jsonl"))
    funnel = latest_by_key(read_dump(output_dir / "marketing.gold.funnel-metrics.v1.jsonl"))
    expected_metric_keys = set(oracle.campaign_metrics)
    if set(revenue) != expected_metric_keys:
        errors.append(f"revenue keys expected={len(expected_metric_keys)} actual={len(revenue)}")
    if set(campaign) != expected_metric_keys:
        errors.append(
            f"campaign keys expected={len(expected_metric_keys)} actual={len(campaign)}"
        )
    if set(funnel) != expected_metric_keys:
        errors.append(f"funnel keys expected={len(expected_metric_keys)} actual={len(funnel)}")
    funnel_fields = (
        "schema_version",
        "metric_key",
        "campaign_id",
        "event_date",
        "sessions",
        "page_view_sessions",
        "view_item_sessions",
        "add_to_cart_sessions",
        "checkout_sessions",
        "purchase_sessions",
        "max_event_time",
    )
    revenue_fields = (
        "schema_version",
        "metric_key",
        "campaign_id",
        "event_date",
        "order_count",
        "refund_count",
        "gross_revenue_minor",
        "refund_amount_minor",
        "net_revenue_minor",
        "currency",
        "max_event_time",
    )
    for key, expected in oracle.campaign_metrics.items():
        for output_name, output, fields in (
            ("campaign", campaign, funnel_fields + revenue_fields[4:]),
            ("funnel", funnel, funnel_fields),
            ("revenue", revenue, revenue_fields),
        ):
            if key not in output:
                continue
            for field in fields:
                if output[key].get(field) != expected[field]:
                    errors.append(
                        f"{output_name} mismatch {key}.{field} "
                        f"expected={expected[field]} actual={output[key].get(field)}"
                    )
    for key, revenue_value in revenue.items():
        if key not in campaign:
            errors.append(f"campaign snapshot missing {key}")
            continue
        for field in expected_totals:
            if campaign[key][field] != revenue_value[field]:
                errors.append(f"campaign/revenue mismatch {key}.{field}")

    dlq_rows = read_dump(output_dir / "marketing.dlq.flink-validation.v1.jsonl")
    dlq = [row["value"] for row in dlq_rows]
    if len(dlq_rows) != 1:
        errors.append(f"processing DLQ expected exactly one record actual={len(dlq_rows)}")
    else:
        row = dlq_rows[0]
        value = row["value"]
        failed = b"not-json"
        expected_error_id = str(
            uuid.uuid5(
                ERROR_NAMESPACE,
                f"marketing.raw.web-events.v1:{value.get('source_partition')}:"
                f"{value.get('source_offset')}:{hashlib.sha256(failed).hexdigest()}",
            )
        )
        expected_dlq = {
            "schema_version": 1,
            "error_id": expected_error_id,
            "processor": "marketpulse-flink",
            "source_topic": "marketing.raw.web-events.v1",
            "source_key": None,
            "reason_code": "MALFORMED_JSON",
            "failed_value_base64": base64.b64encode(failed).decode("ascii"),
            "failed_value_utf8": "not-json",
        }
        mismatched = {
            field: (expected, value.get(field))
            for field, expected in expected_dlq.items()
            if value.get(field) != expected
        }
        if row.get("key") != expected_error_id:
            mismatched["record_key"] = (expected_error_id, row.get("key"))
        if not isinstance(value.get("source_partition"), int) or value.get(
            "source_partition", -1
        ) < 0:
            mismatched["source_partition"] = (">=0", value.get("source_partition"))
        if not isinstance(value.get("source_offset"), int) or value.get(
            "source_offset", -1
        ) < 0:
            mismatched["source_offset"] = (">=0", value.get("source_offset"))
        if mismatched:
            errors.append(f"DLQ mismatch {mismatched}")
    if errors:
        raise ValueError("; ".join(errors))
    report = {
        "status": "PASS",
        "valid_rows": oracle.valid_rows,
        "unique_event_ids": len(expected_ids),
        "duplicate_copies": oracle.duplicate_copies,
        "source_marked_late": oracle.source_marked_late,
        "session_keys": len(sessions),
        **totals,
        "extra_watermark_late": len(probe_rows),
        "processing_dlq": len(dlq),
    }
    if input_dump is not None:
        evidence = json.loads(input_dump.read_text(encoding="utf-8"))
        report["checkpoint_before_restart"] = evidence["checkpoint_before_restart"]
        report["checkpoint_after_restart"] = evidence["checkpoint_after_restart"]
        report["recovery_seconds"] = evidence["recovery_seconds"]
        if int(evidence["checkpoint_after_restart"]) <= int(
            evidence["checkpoint_before_restart"]
        ):
            raise ValueError("checkpoint did not advance after TaskManager restart")
        before = int(evidence.get("raw_records_before_restart", 0))
        after = int(evidence.get("raw_records_after_restart", 0))
        if before <= 0 or after != oracle.valid_rows or after <= before:
            raise ValueError(f"raw input did not progress across recovery: {before} -> {after}")
        report["raw_records_before_restart"] = before
        report["raw_records_after_restart"] = after
        metrics = evidence.get("metrics", {})
        required_metrics = {
            "accepted",
            "duplicates",
            "source_marked_late",
            "watermark_late",
            "aggregate_ineligible",
            "invalid",
        }
        if set(metrics) != required_metrics:
            raise ValueError(f"Flink counter evidence is incomplete: {metrics}")
        if (
            int(metrics["accepted"]) <= 0
            or int(metrics["duplicates"]) < oracle.valid_rows
            or int(metrics["watermark_late"]) != 1
            or int(metrics["aggregate_ineligible"]) != 1
            or int(metrics["invalid"]) != 1
        ):
            raise ValueError(f"Flink counter evidence is inconsistent: {metrics}")
        report["flink_metrics"] = metrics
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--input-dump", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--consume-bootstrap")
    parser.add_argument("--publish-frontier-bootstrap")
    parser.add_argument("--consume-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--write-probes", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.write_probes:
            paths = write_probe_records(args.fixture, args.output_dir)
            print(json.dumps({key: str(path) for key, path in paths.items()}, sort_keys=True))
            return 0
        if args.publish_frontier_bootstrap:
            published = publish_frontier_to_partitions(
                args.publish_frontier_bootstrap,
                args.output_dir / "frontier.json",
            )
            print(json.dumps({"frontier_partitions": published}, sort_keys=True))
            return 0
        consumed = None
        if args.consume_bootstrap:
            consumed = consume_outputs(
                args.consume_bootstrap,
                args.output_dir,
                timeout_seconds=args.consume_timeout_seconds,
            )
        report = verify(args.fixture, args.output_dir, args.input_dump)
        if consumed is not None:
            report["consumed_records"] = consumed
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(json.dumps({"status": "FAIL", "error": str(error)}, sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
