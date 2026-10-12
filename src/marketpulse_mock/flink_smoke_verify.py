"""Deterministic fixture and latest-by-key verification for the MP-03 Flink smoke test."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import gzip
import json
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


OUTPUT_TOPICS = (
    "marketing.clean.customer-events.v1",
    "marketing.clean.sessions.v1",
    "marketing.gold.funnel-metrics.v1",
    "marketing.gold.revenue-metrics.v1",
    "marketing.gold.campaign-metrics.v1",
    "marketing.dlq.flink-validation.v1",
)


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
    campaign_metrics: dict[str, dict[str, int]]


def derive_fixture_oracle(fixture: Path) -> FixtureOracle:
    rows: list[dict[str, Any]] = []
    for dataset in ("web_events", "orders", "email_events"):
        for path in sorted((fixture / dataset).glob("date=*/*.jsonl.gz")):
            with gzip.open(path, "rt", encoding="utf-8") as stream:
                rows.extend(json.loads(line) for line in stream if line.strip())

    unique: dict[str, dict[str, Any]] = {}
    for row in rows:
        unique.setdefault(str(row["event_id"]), row)
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
            elif row["event_name"] == "order_refunded":
                campaign_revenue[metric_key]["refund_count"] += 1
                campaign_revenue[metric_key]["refund_amount_minor"] += int(
                    row["payload"]["refund_amount_minor"]
                )

    campaign_metrics: dict[str, dict[str, int]] = {}
    for metric_key in set(campaign_sessions) | set(campaign_revenue):
        stages = campaign_sessions[metric_key]
        revenue = campaign_revenue[metric_key]
        campaign_metrics[metric_key] = {
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
    late["event_id"] = "00000000-0000-5000-8000-000000000099"
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


def verify(
    fixture: Path,
    output_dir: Path,
    input_dump: Path | None = None,
) -> dict[str, Any]:
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
        expected_late = "source_marked_late" if expected["is_late"] else "on_time"
        if actual.get("late_status") != expected_late:
            errors.append(
                f"clean late_status {event_id} expected={expected_late} "
                f"actual={actual.get('late_status')}"
            )
    extras = [value for key, value in clean.items() if key not in expected_ids]
    if extras and not all(
        value.get("late_status") == "watermark_late"
        and value.get("eligible_for_aggregates") is False
        for value in extras
    ):
        errors.append("unexpected non-fixture clean records")

    sessions = latest_by_key(read_dump(output_dir / "marketing.clean.sessions.v1.jsonl"))
    if set(sessions) != set(oracle.session_ids):
        errors.append(f"session keys expected={len(oracle.session_ids)} actual={len(sessions)}")
    for session_id, expected in oracle.session_metrics.items():
        actual = sessions.get(session_id)
        if actual is None:
            continue
        for field, expected_value in expected.items():
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
    if set(campaign) != expected_metric_keys:
        errors.append(
            f"campaign keys expected={len(expected_metric_keys)} actual={len(campaign)}"
        )
    if set(funnel) != expected_metric_keys:
        errors.append(f"funnel keys expected={len(expected_metric_keys)} actual={len(funnel)}")
    for key, expected in oracle.campaign_metrics.items():
        for output_name, output in (("campaign", campaign), ("funnel", funnel)):
            if key not in output:
                continue
            for field in (
                "sessions",
                "page_view_sessions",
                "view_item_sessions",
                "add_to_cart_sessions",
                "checkout_sessions",
                "purchase_sessions",
            ):
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

    dlq = [row["value"] for row in read_dump(output_dir / "marketing.dlq.flink-validation.v1.jsonl")]
    malformed = [value for value in dlq if value.get("reason_code") == "MALFORMED_JSON"]
    if len(malformed) != 1:
        errors.append(f"processing DLQ expected one MALFORMED_JSON actual={len(malformed)}")
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
        "extra_watermark_late": len(extras),
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
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--input-dump", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--consume-bootstrap")
    parser.add_argument("--consume-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--write-probes", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.write_probes:
            paths = write_probe_records(args.fixture, args.output_dir)
            print(json.dumps({key: str(path) for key, path in paths.items()}, sort_keys=True))
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
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(json.dumps({"status": "FAIL", "error": str(error)}, sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
