"""Deterministic Silver governance and typed dimension transformations."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Iterable, Mapping, Sequence

from .bronze import _spark_schema, _table_statement, current_snapshot_id
from .contracts import TABLE_CONTRACTS
from .identity import resolve_profile_key


CLEAN_TOPIC = "marketing.clean.customer-events.v1"
_CLEAN_FIELDS = frozenset(
    {
        "schema_version",
        "event_id",
        "event_name",
        "event_time",
        "ingested_at",
        "processed_at",
        "source",
        "channel",
        "customer_id",
        "anonymous_id",
        "identity_key",
        "session_id",
        "campaign_id",
        "campaign_key",
        "consent_analytics",
        "late_status",
        "eligible_for_aggregates",
        "payload",
        "lineage",
    }
)
_LINEAGE_FIELDS = frozenset(
    {"topic", "partition", "offset", "record_timestamp", "record_key"}
)
_SOURCES = frozenset({"web", "mobile", "commerce", "email"})
_LATE_STATUSES = frozenset({"on_time", "source_marked_late", "watermark_late"})
_RAW_TOPICS = frozenset(
    {
        "marketing.raw.web-events.v1",
        "marketing.raw.order-events.v1",
        "marketing.raw.email-events.v1",
    }
)
_DLQ_TOPIC = "marketing.dlq.flink-validation.v1"
_DLQ_REASONS = frozenset(
    {
        "MALFORMED_JSON",
        "UNSUPPORTED_SCHEMA_VERSION",
        "TOPIC_SOURCE_MISMATCH",
        "MISSING_REQUIRED_FIELD",
        "INVALID_FIELD_TYPE",
        "INVALID_EVENT_ID",
        "INVALID_TIMESTAMP",
        "INVALID_EVENT_NAME",
        "MISSING_IDENTITY",
        "INVALID_PAYLOAD",
    }
)


def _instant(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a date-time string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} is not a valid date-time") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _required_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _nullable_string(value: object, field: str) -> str | None:
    if value is None:
        return None
    return _required_string(value, field)


def _strict_bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be a boolean")
    return value


def _canonical(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class CleanEvent:
    value: dict[str, Any]
    event_id: str
    event_time: datetime
    ingested_at: datetime
    source_topic: str
    bronze_partition: int
    bronze_offset: int
    source_payload_sha256: str
    lineage_topic: str
    lineage_partition: int
    lineage_offset: int
    lineage_record_timestamp: int | None
    logical_payload_sha256: str

    @property
    def order_key(self) -> tuple[object, ...]:
        return (
            self.lineage_record_timestamp is None,
            self.lineage_record_timestamp or 0,
            self.lineage_topic,
            self.lineage_partition,
            self.lineage_offset,
            self.source_topic,
            self.bronze_partition,
            self.bronze_offset,
        )


def parse_clean_bronze_record(record: Mapping[str, object]) -> CleanEvent:
    if record.get("source_topic") != CLEAN_TOPIC:
        raise ValueError("Bronze record is not from the clean customer-events topic")
    payload_text = record.get("payload_text")
    if not isinstance(payload_text, str):
        raise ValueError("clean payload_text must be non-null UTF-8 JSON")
    try:
        value = json.loads(payload_text)
    except json.JSONDecodeError as error:
        raise ValueError("clean payload is malformed JSON") from error
    if not isinstance(value, dict):
        raise ValueError("clean payload must be an object")
    missing = _CLEAN_FIELDS.difference(value)
    additional = set(value).difference(_CLEAN_FIELDS)
    if missing:
        raise ValueError(f"clean payload missing field: {sorted(missing)[0]}")
    if additional:
        raise ValueError(f"clean payload additional field: {sorted(additional)[0]}")
    if value["schema_version"] != 1 or isinstance(value["schema_version"], bool):
        raise ValueError("schema_version must be exactly 1")
    event_id = _required_string(value["event_id"], "event_id")
    try:
        uuid.UUID(event_id)
    except ValueError as error:
        raise ValueError("event_id must be a UUID") from error
    _required_string(value["event_name"], "event_name")
    event_time = _instant(value["event_time"], "event_time")
    ingested_at = _instant(value["ingested_at"], "ingested_at")
    _instant(value["processed_at"], "processed_at")
    if value["source"] not in _SOURCES:
        raise ValueError("source is not allowed")
    if value["channel"] not in _SOURCES:
        raise ValueError("channel is not allowed")
    _nullable_string(value["customer_id"], "customer_id")
    _nullable_string(value["anonymous_id"], "anonymous_id")
    _required_string(value["identity_key"], "identity_key")
    _required_string(value["session_id"], "session_id")
    campaign_id = _nullable_string(value["campaign_id"], "campaign_id")
    if campaign_id == "__unattributed__":
        raise ValueError("campaign_id uses reserved attribution sentinel")
    _nullable_string(value["campaign_key"], "campaign_key")
    _strict_bool(value["consent_analytics"], "consent_analytics")
    if value["late_status"] not in _LATE_STATUSES:
        raise ValueError("late_status is not allowed")
    _strict_bool(value["eligible_for_aggregates"], "eligible_for_aggregates")
    if not isinstance(value["payload"], dict):
        raise ValueError("payload must be an object")
    lineage = value["lineage"]
    if not isinstance(lineage, dict):
        raise ValueError("lineage must be an object")
    if set(lineage) != _LINEAGE_FIELDS:
        raise ValueError("lineage fields do not match the contract")
    lineage_topic = _required_string(lineage["topic"], "lineage.topic")
    lineage_partition = lineage["partition"]
    lineage_offset = lineage["offset"]
    record_timestamp = lineage["record_timestamp"]
    if (
        not isinstance(lineage_partition, int)
        or isinstance(lineage_partition, bool)
        or lineage_partition < 0
    ):
        raise ValueError("lineage.partition must be a non-negative integer")
    if (
        not isinstance(lineage_offset, int)
        or isinstance(lineage_offset, bool)
        or lineage_offset < 0
    ):
        raise ValueError("lineage.offset must be a non-negative integer")
    if record_timestamp is not None and (
        not isinstance(record_timestamp, int) or isinstance(record_timestamp, bool)
    ):
        raise ValueError("lineage.record_timestamp must be an integer or null")
    _nullable_string(lineage["record_key"], "lineage.record_key")
    bronze_partition = record.get("source_partition")
    bronze_offset = record.get("source_offset")
    if not isinstance(bronze_partition, int) or not isinstance(bronze_offset, int):
        raise ValueError("Bronze Kafka position is invalid")
    payload_sha256 = record.get("payload_sha256")
    if not isinstance(payload_sha256, str) or len(payload_sha256) != 64:
        raise ValueError("Bronze payload_sha256 is invalid")
    stable_value = {
        key: item for key, item in value.items() if key not in {"processed_at", "lineage"}
    }
    return CleanEvent(
        value=value,
        event_id=event_id,
        event_time=event_time,
        ingested_at=ingested_at,
        source_topic=CLEAN_TOPIC,
        bronze_partition=bronze_partition,
        bronze_offset=bronze_offset,
        source_payload_sha256=payload_sha256,
        lineage_topic=lineage_topic,
        lineage_partition=lineage_partition,
        lineage_offset=lineage_offset,
        lineage_record_timestamp=record_timestamp,
        logical_payload_sha256=_digest(stable_value),
    )


@dataclass(frozen=True, slots=True)
class CustomerEventPlan:
    rows: tuple[dict[str, object], ...]
    quality: tuple[dict[str, object], ...]


@dataclass(frozen=True, slots=True)
class KafkaReconciliation:
    raw_records: int
    clean_records: int
    dlq_records: int
    duplicate_raw_records: int
    dlq_reason_counts: dict[str, int]


@dataclass(frozen=True, slots=True)
class DlqOutcome:
    source_topic: str
    source_partition: int
    source_offset: int
    event_date: date
    error_id: str
    reason_code: str


def parse_dlq_bronze_record(record: Mapping[str, object]) -> DlqOutcome:
    if record.get("source_topic") != _DLQ_TOPIC:
        raise ValueError("Bronze record is not from the Flink DLQ topic")
    value = _json_object(record, "DLQ")
    required = {
        "schema_version",
        "error_id",
        "detected_at",
        "processor",
        "source_topic",
        "source_partition",
        "source_offset",
        "source_key",
        "reason_code",
        "failed_value_base64",
        "failed_value_utf8",
    }
    if set(value) != required:
        raise ValueError("DLQ fields do not match the contract")
    if value["schema_version"] != 1 or isinstance(value["schema_version"], bool):
        raise ValueError("DLQ schema_version must be exactly 1")
    error_id = _required_string(value["error_id"], "error_id")
    try:
        uuid.UUID(error_id)
    except ValueError as error:
        raise ValueError("DLQ error_id must be a UUID") from error
    detected_at = _instant(value["detected_at"], "detected_at")
    if value["processor"] != "marketpulse-flink":
        raise ValueError("DLQ processor is not supported")
    source_topic = _required_string(value["source_topic"], "source_topic")
    if source_topic not in _RAW_TOPICS:
        raise ValueError("DLQ source_topic is not a raw topic")
    source_partition = value["source_partition"]
    source_offset = value["source_offset"]
    if (
        not isinstance(source_partition, int)
        or isinstance(source_partition, bool)
        or not isinstance(source_offset, int)
        or isinstance(source_offset, bool)
        or source_partition < 0
        or source_offset < 0
    ):
        raise ValueError("DLQ source position must be non-negative")
    if value["source_key"] is not None and not isinstance(value["source_key"], str):
        raise ValueError("DLQ source_key must be a string or null")
    reason = value["reason_code"]
    if reason not in _DLQ_REASONS:
        raise ValueError(f"unsupported DLQ reason: {reason}")
    encoded = value["failed_value_base64"]
    if not isinstance(encoded, str):
        raise ValueError("failed_value_base64 must be a string")
    try:
        failed = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as error:
        raise ValueError("failed_value_base64 is invalid") from error
    utf8 = value["failed_value_utf8"]
    if utf8 is not None and not isinstance(utf8, str):
        raise ValueError("failed_value_utf8 must be a string or null")
    if utf8 is not None and failed.decode("utf-8", errors="replace") != utf8:
        raise ValueError("DLQ failed value encodings disagree")
    return DlqOutcome(
        source_topic=source_topic,
        source_partition=source_partition,
        source_offset=source_offset,
        event_date=detected_at.date(),
        error_id=error_id,
        reason_code=str(reason),
    )


def _position(record: Mapping[str, object]) -> tuple[str, int, int]:
    topic = record.get("source_topic")
    partition = record.get("source_partition")
    offset = record.get("source_offset")
    if (
        not isinstance(topic, str)
        or not isinstance(partition, int)
        or isinstance(partition, bool)
        or not isinstance(offset, int)
        or isinstance(offset, bool)
        or partition < 0
        or offset < 0
    ):
        raise ValueError("Bronze Kafka position is invalid")
    return topic, partition, offset


def _json_object(record: Mapping[str, object], label: str) -> dict[str, Any]:
    payload = record.get("payload_text")
    if not isinstance(payload, str):
        raise ValueError(f"{label} payload is not UTF-8 JSON")
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError as error:
        raise ValueError(f"{label} payload is malformed JSON") from error
    if not isinstance(decoded, dict):
        raise ValueError(f"{label} payload must be an object")
    return decoded


def reconcile_kafka_outcomes(
    records: Iterable[Mapping[str, object]],
) -> KafkaReconciliation:
    """Require each raw position to be accepted, rejected, or a proved duplicate."""

    materialized = list(records)
    raw: dict[tuple[str, int, int], str | None] = {}
    outcomes: dict[tuple[str, int, int], str] = {}
    accepted_event_ids: set[str] = set()
    clean_count = 0
    dlq_count = 0
    reason_counts: dict[str, int] = defaultdict(int)
    for record in materialized:
        topic, partition, offset = _position(record)
        if topic in _RAW_TOPICS:
            position = (topic, partition, offset)
            if position in raw:
                raise ValueError(f"duplicate raw position: {position}")
            event_id: str | None = None
            payload = record.get("payload_text")
            if isinstance(payload, str):
                try:
                    decoded = json.loads(payload)
                except json.JSONDecodeError:
                    decoded = None
                if isinstance(decoded, dict) and isinstance(decoded.get("event_id"), str):
                    event_id = str(decoded["event_id"])
            raw[position] = event_id
        elif topic == CLEAN_TOPIC:
            clean = parse_clean_bronze_record(record)
            source_position = (
                clean.lineage_topic,
                clean.lineage_partition,
                clean.lineage_offset,
            )
            if source_position in outcomes:
                raise ValueError(f"multiple outcomes for raw position: {source_position}")
            outcomes[source_position] = "clean"
            accepted_event_ids.add(clean.event_id)
            clean_count += 1
        elif topic == _DLQ_TOPIC:
            outcome = parse_dlq_bronze_record(record)
            source_position = (
                outcome.source_topic,
                outcome.source_partition,
                outcome.source_offset,
            )
            if source_position in outcomes:
                raise ValueError(f"multiple outcomes for raw position: {source_position}")
            outcomes[source_position] = "dlq"
            reason_counts[outcome.reason_code] += 1
            dlq_count += 1
    orphaned = set(outcomes).difference(raw)
    if orphaned:
        raise ValueError(f"outcome references absent raw position: {sorted(orphaned)[0]}")
    duplicates = 0
    for position, event_id in raw.items():
        if position in outcomes:
            continue
        if event_id is not None and event_id in accepted_event_ids:
            duplicates += 1
            continue
        raise ValueError(f"unreconciled raw position: {position}")
    return KafkaReconciliation(
        raw_records=len(raw),
        clean_records=clean_count,
        dlq_records=dlq_count,
        duplicate_raw_records=duplicates,
        dlq_reason_counts=dict(sorted(reason_counts.items())),
    )


def _quality_rows(
    observations: Iterable[tuple[date, str, str, str]], run_id: str
) -> tuple[dict[str, object], ...]:
    grouped: dict[tuple[date, str, str], list[str]] = defaultdict(list)
    for event_date, source_topic, reason, event_id in observations:
        grouped[(event_date, source_topic, reason)].append(event_id)
    return tuple(
        {
            "event_date": key[0],
            "source_topic": key[1],
            "reason_code": key[2],
            "record_count": len(event_ids),
            "distinct_event_count": len(set(event_ids)),
            "run_id": run_id,
        }
        for key, event_ids in sorted(grouped.items())
    )


def govern_customer_events(
    records: Iterable[Mapping[str, object]], *, run_id: str
) -> CustomerEventPlan:
    if not run_id:
        raise ValueError("run_id must not be empty")
    parsed = [parse_clean_bronze_record(record) for record in records]
    grouped: dict[str, list[CleanEvent]] = defaultdict(list)
    for event in parsed:
        grouped[event.event_id].append(event)
    winners: list[CleanEvent] = []
    observations: list[tuple[date, str, str, str]] = []
    for event_id, copies in sorted(grouped.items()):
        ordered = sorted(copies, key=lambda item: item.order_key)
        winner = ordered[0]
        winners.append(winner)
        for _copy in ordered[1:]:
            observations.append(
                (winner.event_time.date(), winner.source_topic, "DUPLICATE_EVENT", event_id)
            )
        distinct_payloads = {copy.logical_payload_sha256 for copy in copies}
        if len(distinct_payloads) > 1:
            observations.append(
                (
                    winner.event_time.date(),
                    winner.source_topic,
                    "EVENT_ID_PAYLOAD_CONFLICT",
                    event_id,
                )
            )

    bot_sessions = {
        str(event.value["session_id"])
        for event in winners
        if event.value["source"] in {"web", "mobile"}
        and event.value["payload"].get("is_bot") is True
    }
    stitched_candidates: dict[tuple[str, str], set[str]] = defaultdict(set)
    for event in winners:
        customer_id = event.value["customer_id"]
        anonymous_id = event.value["anonymous_id"]
        if customer_id and anonymous_id:
            stitched_candidates[
                (str(event.value["session_id"]), str(anonymous_id))
            ].add(str(customer_id))
    stitches = {
        key: next(iter(customer_ids))
        for key, customer_ids in stitched_candidates.items()
        if len(customer_ids) == 1
    }

    rows: list[dict[str, object]] = []
    for event in sorted(winners, key=lambda item: (item.event_time, item.event_id)):
        value = event.value
        session_id = str(value["session_id"])
        event_date = event.event_time.date()
        reason = None
        if value["consent_analytics"] is not True:
            reason = "CONSENT_DENIED"
        elif value["eligible_for_aggregates"] is not True:
            reason = "LATE_INELIGIBLE"
        elif value["source"] in {"web", "mobile"} and session_id in bot_sessions:
            reason = "BOT_EXCLUDED"
        if reason is not None:
            observations.append((event_date, event.source_topic, reason, event.event_id))
            continue
        anonymous_id = value["anonymous_id"]
        stitched = (
            stitches.get((session_id, str(anonymous_id))) if anonymous_id else None
        )
        payload = value["payload"]
        observations.append((event_date, event.source_topic, "ACCEPTED", event.event_id))
        rows.append(
            {
                "event_id": event.event_id,
                "event_name": str(value["event_name"]),
                "event_time": event.event_time,
                "ingested_at": event.ingested_at,
                "event_date": event_date,
                "source": str(value["source"]),
                "channel": str(value["channel"]),
                "customer_id": value["customer_id"],
                "anonymous_id": anonymous_id,
                "profile_key": resolve_profile_key(
                    value["customer_id"],
                    anonymous_id,
                    session_id,
                    stitched_customer_id=stitched,
                ),
                "session_id": session_id,
                "campaign_id": value["campaign_id"],
                "campaign_key": value["campaign_key"],
                "click_id": payload.get("click_id"),
                "consent_analytics": True,
                "late_status": str(value["late_status"]),
                "is_bot_session": session_id in bot_sessions,
                "order_id": payload.get("order_id"),
                "gross_amount_minor": payload.get("gross_amount_minor"),
                "refund_amount_minor": payload.get("refund_amount_minor"),
                "currency": payload.get("currency"),
                "payload_json": _canonical(payload),
                "source_payload_sha256": event.source_payload_sha256,
                "lineage_topic": event.lineage_topic,
                "lineage_partition": event.lineage_partition,
                "lineage_offset": event.lineage_offset,
            }
        )
    return CustomerEventPlan(tuple(rows), _quality_rows(observations, run_id))


def _source_payload(row: Mapping[str, object]) -> dict[str, Any]:
    payload_json = row.get("payload_json")
    if not isinstance(payload_json, str):
        raise ValueError("source payload_json must be a string")
    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError as error:
        raise ValueError("source payload_json is invalid") from error
    if not isinstance(payload, dict):
        raise ValueError("source payload_json must be an object")
    return payload


def _row_lineage(row: Mapping[str, object]) -> tuple[str, int]:
    source_file = row.get("source_file")
    row_number = row.get("row_number")
    if not isinstance(source_file, str) or not isinstance(row_number, int):
        raise ValueError("source row lineage is invalid")
    return source_file, row_number


def _boolean_text(value: object, field: str) -> bool:
    if value == "true" or value is True:
        return True
    if value == "false" or value is False:
        return False
    raise ValueError(f"{field} must be true or false")


def _integer_text(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field} must be an integer") from error
    return parsed


def build_customers(source_rows: Iterable[Mapping[str, object]]) -> tuple[dict[str, object], ...]:
    created_at: dict[str, datetime] = {}
    crm: dict[str, tuple[tuple[date, str, int], Mapping[str, object], dict[str, Any]]] = {}
    for row in source_rows:
        dataset = row.get("source_dataset")
        payload = _source_payload(row)
        source_file, row_number = _row_lineage(row)
        if dataset == "master/customers":
            customer_id = _required_string(payload.get("customer_id"), "customer_id")
            if customer_id in created_at:
                raise ValueError(f"duplicate master customer: {customer_id}")
            created_at[customer_id] = _instant(payload.get("created_at"), "created_at")
        elif dataset == "crm":
            customer_id = _required_string(payload.get("customer_id"), "customer_id")
            try:
                snapshot_date = date.fromisoformat(str(payload.get("snapshot_date")))
            except ValueError as error:
                raise ValueError("snapshot_date is invalid") from error
            order = (snapshot_date, source_file, row_number)
            previous = crm.get(customer_id)
            if previous is None or order > previous[0]:
                crm[customer_id] = (order, row, payload)
    output = []
    for customer_id, (order, row, payload) in sorted(crm.items()):
        if customer_id not in created_at:
            raise ValueError(f"CRM customer has no master record: {customer_id}")
        source_file, row_number = _row_lineage(row)
        last_order = payload.get("last_order_at")
        output.append(
            {
                "customer_id": customer_id,
                "created_at": created_at[customer_id],
                "snapshot_date": order[0],
                "loyalty_tier": _required_string(payload.get("loyalty_tier"), "loyalty_tier"),
                "lifetime_value_minor": _integer_text(
                    payload.get("lifetime_value_minor"), "lifetime_value_minor"
                ),
                "last_order_at": None if last_order in {None, ""} else _instant(last_order, "last_order_at"),
                "consent_analytics": _boolean_text(
                    payload.get("consent_analytics"), "consent_analytics"
                ),
                "consent_marketing": _boolean_text(
                    payload.get("consent_marketing"), "consent_marketing"
                ),
                "source_file": source_file,
                "source_row_number": row_number,
            }
        )
    return tuple(output)


def build_campaigns(source_rows: Iterable[Mapping[str, object]]) -> tuple[dict[str, object], ...]:
    campaigns: dict[str, tuple[Mapping[str, object], dict[str, Any]]] = {}
    platform_ids: dict[str, str] = {}
    ad_counts: dict[str, int] = defaultdict(int)
    for row in source_rows:
        dataset = row.get("source_dataset")
        payload = _source_payload(row)
        if dataset == "master/campaigns":
            campaign_id = _required_string(payload.get("campaign_id"), "campaign_id")
            if campaign_id == "__unattributed__":
                raise ValueError("campaign_id uses reserved attribution sentinel")
            platform_id = _required_string(
                payload.get("platform_campaign_id"), "platform_campaign_id"
            )
            if campaign_id in campaigns:
                raise ValueError(f"duplicate campaign_id: {campaign_id}")
            previous = platform_ids.get(platform_id)
            if previous is not None and previous != campaign_id:
                raise ValueError(f"duplicate platform_campaign_id: {platform_id}")
            platform_ids[platform_id] = campaign_id
            campaigns[campaign_id] = (row, payload)
        elif dataset == "master/ads":
            ad_counts[_required_string(payload.get("campaign_id"), "campaign_id")] += 1
    unknown = set(ad_counts).difference(campaigns)
    if unknown:
        raise ValueError(f"ad references unknown campaign: {sorted(unknown)[0]}")
    output = []
    for campaign_id, (row, payload) in sorted(campaigns.items()):
        source_file, row_number = _row_lineage(row)
        try:
            start_date = date.fromisoformat(str(payload.get("start_date")))
            end_date = date.fromisoformat(str(payload.get("end_date")))
        except ValueError as error:
            raise ValueError("campaign date is invalid") from error
        if end_date < start_date:
            raise ValueError(f"campaign ends before it starts: {campaign_id}")
        currency = _required_string(payload.get("currency"), "currency")
        if currency != "VND":
            raise ValueError(f"unsupported campaign currency: {currency}")
        output.append(
            {
                "campaign_id": campaign_id,
                "platform": _required_string(payload.get("platform"), "platform"),
                "platform_campaign_id": _required_string(
                    payload.get("platform_campaign_id"), "platform_campaign_id"
                ),
                "name": _required_string(payload.get("name"), "name"),
                "objective": _required_string(payload.get("objective"), "objective"),
                "status": _required_string(payload.get("status"), "status"),
                "start_date": start_date,
                "end_date": end_date,
                "daily_budget_minor": _integer_text(
                    payload.get("daily_budget_minor"), "daily_budget_minor"
                ),
                "currency": currency,
                "ad_count": ad_counts.get(campaign_id, 0),
                "source_file": source_file,
                "source_row_number": row_number,
            }
        )
    return tuple(output)


def _half_up(value: object, field: str, divisor: Decimal = Decimal(1)) -> int:
    try:
        amount = Decimal(str(value)) / divisor
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"{field} must be decimal") from error
    return int(amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _action_value(items: object, field: str) -> Decimal:
    if not isinstance(items, list):
        raise ValueError(f"{field} must be an array")
    total = Decimal(0)
    for item in items:
        if not isinstance(item, dict):
            raise ValueError(f"{field} item must be an object")
        if item.get("action_type") == "purchase":
            try:
                total += Decimal(str(item.get("value")))
            except InvalidOperation as error:
                raise ValueError(f"{field} purchase value must be decimal") from error
    return total


def build_ad_spend_daily(
    source_rows: Iterable[Mapping[str, object]],
    campaigns: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    mapping: dict[tuple[str, str], Mapping[str, object]] = {}
    for campaign in campaigns:
        currency = campaign.get("currency")
        if currency != "VND":
            raise ValueError(f"unsupported campaign currency: {currency}")
        key = (
            _required_string(campaign.get("platform"), "platform"),
            _required_string(campaign.get("platform_campaign_id"), "platform_campaign_id"),
        )
        if key in mapping:
            raise ValueError(f"duplicate platform_campaign_id: {key[1]}")
        mapping[key] = campaign
    dataset_platform = {
        "google_ads": "google",
        "meta_ads": "meta",
        "tiktok_ads": "tiktok",
    }
    output: dict[str, dict[str, object]] = {}
    for source in source_rows:
        dataset = source.get("source_dataset")
        if dataset not in dataset_platform:
            continue
        platform = dataset_platform[str(dataset)]
        payload = _source_payload(source)
        platform_campaign_id = _required_string(payload.get("campaign_id"), "campaign_id")
        campaign = mapping.get((platform, platform_campaign_id))
        if campaign is None:
            raise ValueError(
                f"ad spend has no campaign mapping: {platform}:{platform_campaign_id}"
            )
        if platform == "google":
            spend_date = date.fromisoformat(str(payload.get("segments_date")))
            conversions = _integer_text(payload.get("conversions"), "conversions")
            spend_minor = _half_up(
                payload.get("cost_micros"), "cost_micros", Decimal(1_000_000)
            )
            conversion_value = _half_up(
                payload.get("conversions_value"), "conversions_value"
            )
        elif platform == "meta":
            spend_date = date.fromisoformat(str(payload.get("date_start")))
            if str(payload.get("date_stop")) != spend_date.isoformat():
                raise ValueError("Meta report spans multiple dates")
            conversions = _half_up(_action_value(payload.get("actions"), "actions"), "actions")
            spend_minor = _half_up(payload.get("spend"), "spend")
            conversion_value = _half_up(
                _action_value(payload.get("action_values"), "action_values"),
                "action_values",
            )
        else:
            spend_date = date.fromisoformat(str(payload.get("stat_time_day")))
            conversions = _integer_text(payload.get("conversion"), "conversion")
            spend_minor = _half_up(payload.get("spend"), "spend")
            conversion_value = _half_up(
                payload.get("total_purchase_value"), "total_purchase_value"
            )
        campaign_id = _required_string(campaign.get("campaign_id"), "campaign_id")
        spend_key = _digest([spend_date.isoformat(), campaign_id, platform])
        if spend_key in output:
            raise ValueError(
                f"duplicate ad spend grain: {spend_date}:{campaign_id}:{platform}"
            )
        source_file, row_number = _row_lineage(source)
        output[spend_key] = {
            "spend_key": spend_key,
            "spend_date": spend_date,
            "campaign_id": campaign_id,
            "platform": platform,
            "platform_campaign_id": platform_campaign_id,
            "impressions": _integer_text(payload.get("impressions"), "impressions"),
            "clicks": _integer_text(payload.get("clicks"), "clicks"),
            "conversions": conversions,
            "spend_minor": spend_minor,
            "conversion_value_minor": conversion_value,
            "currency": "VND",
            "source_file": source_file,
            "source_row_number": row_number,
        }
    return tuple(output[key] for key in sorted(output))


def validate_namespace(namespace: str) -> str:
    if (
        not namespace
        or not namespace.replace("_", "a").isalnum()
        or namespace[0].isdigit()
    ):
        raise ValueError("target namespace must be a SQL identifier")
    return namespace


def _target_table(namespace: str, table: str) -> str:
    namespace = validate_namespace(namespace)
    return f"lakehouse.`{namespace}`.`{table}`"


def _ensure_silver_tables(spark: Any, namespace: str) -> None:
    namespace = validate_namespace(namespace)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.`{namespace}`")
    for name in (
        "silver.customer_events",
        "silver.customers",
        "silver.campaigns",
        "silver.ad_spend_daily",
        "silver.quality_summary",
    ):
        statement = _table_statement("lakehouse", TABLE_CONTRACTS[name])
        spark.sql(statement.replace("`silver`", f"`{namespace}`", 1))


def _replace_table(
    spark: Any,
    *,
    namespace: str,
    table: str,
    frame: Any,
) -> int:
    view = f"silver_build_{table}_{uuid.uuid4().hex}"
    frame.createOrReplaceTempView(view)
    columns = [column.name for column in TABLE_CONTRACTS[f"silver.{table}"].columns]
    projection = ", ".join(f"`{column}`" for column in columns)
    try:
        spark.sql(
            f"INSERT OVERWRITE {_target_table(namespace, table)} "
            f"SELECT {projection} FROM `{view}`"
        )
        return int(frame.count())
    finally:
        spark.catalog.dropTempView(view)


def _snapshot_id(spark: Any, namespace: str, table: str) -> int | None:
    if namespace == "silver":
        return current_snapshot_id(spark, f"silver.{table}")
    rows = spark.sql(
        f"SELECT snapshot_id FROM lakehouse.`{namespace}`.`{table}`.refs "
        "WHERE name = 'main'"
    ).collect()
    if not rows:
        return None
    if len(rows) != 1:
        raise ValueError(f"Iceberg main ref is ambiguous: {namespace}.{table}")
    return int(rows[0]["snapshot_id"])


def _winner_record(copies: Iterable[CleanEvent]) -> dict[str, object]:
    ordered = sorted(copies, key=lambda item: item.order_key)
    winner = ordered[0]
    value = winner.value
    payload = value["payload"]
    return {
        "event_id": winner.event_id,
        "event_name": value["event_name"],
        "event_time": winner.event_time,
        "ingested_at": winner.ingested_at,
        "event_date": winner.event_time.date(),
        "source": value["source"],
        "channel": value["channel"],
        "customer_id": value["customer_id"],
        "anonymous_id": value["anonymous_id"],
        "session_id": value["session_id"],
        "campaign_id": value["campaign_id"],
        "campaign_key": value["campaign_key"],
        "click_id": payload.get("click_id"),
        "consent_analytics": value["consent_analytics"],
        "late_status": value["late_status"],
        "eligible_for_aggregates": value["eligible_for_aggregates"],
        "is_bot_event": payload.get("is_bot") is True,
        "order_id": payload.get("order_id"),
        "gross_amount_minor": payload.get("gross_amount_minor"),
        "refund_amount_minor": payload.get("refund_amount_minor"),
        "currency": payload.get("currency"),
        "payload_json": _canonical(payload),
        "source_payload_sha256": winner.source_payload_sha256,
        "lineage_topic": winner.lineage_topic,
        "lineage_partition": winner.lineage_partition,
        "lineage_offset": winner.lineage_offset,
        "source_topic": winner.source_topic,
        "duplicate_count": len(ordered) - 1,
        "conflict_count": int(
            len({item.logical_payload_sha256 for item in ordered}) > 1
        ),
    }


def _clean_event_internal_schema() -> str:
    return (
        "event_id string, event_name string, event_time timestamp, ingested_at timestamp, "
        "event_date date, source string, channel string, customer_id string, "
        "anonymous_id string, session_id string, campaign_id string, campaign_key string, "
        "click_id string, consent_analytics boolean, late_status string, "
        "eligible_for_aggregates boolean, is_bot_event boolean, order_id string, "
        "gross_amount_minor bigint, refund_amount_minor bigint, currency string, "
        "payload_json string, source_payload_sha256 string, lineage_topic string, "
        "lineage_partition int, lineage_offset bigint, source_topic string, "
        "duplicate_count bigint, conflict_count bigint"
    )


def recover_raw_click_id(payload_text: str | None, event_id: str) -> str | None:
    """Recover the source envelope field omitted by MP-03 clean schema v1."""

    if payload_text is None:
        raise ValueError("raw event identity is missing for clean lineage")
    try:
        value = json.loads(payload_text)
    except json.JSONDecodeError as error:
        raise ValueError("raw event identity payload is malformed") from error
    if not isinstance(value, dict) or value.get("event_id") != event_id:
        raise ValueError("raw event identity does not match clean lineage")
    return _nullable_string(value.get("click_id"), "click_id")


def _build_customer_event_frames(spark: Any, kafka_frame: Any, run_id: str) -> tuple[Any, Any]:
    from pyspark.sql import functions as F
    from pyspark.sql import Window
    from pyspark.storagelevel import StorageLevel

    clean = kafka_frame.filter(F.col("source_topic") == CLEAN_TOPIC)

    def parse_row(row: Any) -> dict[str, object]:
        event = parse_clean_bronze_record(row.asDict(recursive=True))
        result = _winner_record((event,))
        result.update(
            {
                "lineage_timestamp_missing": event.lineage_record_timestamp is None,
                "lineage_record_timestamp": event.lineage_record_timestamp or 0,
                "bronze_partition": event.bronze_partition,
                "bronze_offset": event.bronze_offset,
                "logical_payload_sha256": event.logical_payload_sha256,
            }
        )
        return result

    parsed_schema = _clean_event_internal_schema() + (
        ", lineage_timestamp_missing boolean, lineage_record_timestamp long, "
        "bronze_partition int, bronze_offset long, logical_payload_sha256 string"
    )
    raw_lineage = kafka_frame.filter(F.col("source_topic").isin(*sorted(_RAW_TOPICS))).select(
        F.col("source_topic").alias("lineage_topic"),
        F.col("source_partition").alias("lineage_partition"),
        F.col("source_offset").alias("lineage_offset"),
        F.col("payload_text").alias("raw_payload_text"),
    )
    recover_click = F.udf(recover_raw_click_id, "string")
    parsed = (
        spark.createDataFrame(clean.rdd.map(parse_row), parsed_schema)
        .join(raw_lineage, ["lineage_topic", "lineage_partition", "lineage_offset"], "left")
        .withColumn("click_id", recover_click("raw_payload_text", "event_id"))
        .drop("raw_payload_text")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    parsed.count()
    order = Window.partitionBy("event_id").orderBy(
        "lineage_timestamp_missing",
        "lineage_record_timestamp",
        "lineage_topic",
        "lineage_partition",
        "lineage_offset",
        "source_topic",
        "bronze_partition",
        "bronze_offset",
    )
    copy_stats = parsed.groupBy("event_id").agg(
        (F.count(F.lit(1)) - F.lit(1)).cast("long").alias("duplicate_count"),
        # The raw envelope adds a business field omitted by clean v1. A struct
        # preserves null click IDs instead of countDistinct dropping them.
        (F.countDistinct(F.struct("logical_payload_sha256", "click_id")) > F.lit(1))
        .cast("long")
        .alias("conflict_count"),
    )
    winners = (
        parsed.drop("duplicate_count", "conflict_count")
        .withColumn("winner_rank", F.row_number().over(order))
        .filter(F.col("winner_rank") == 1)
        .drop("winner_rank")
        .join(copy_stats, "event_id", "inner")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    winners.count()
    bot_sessions = (
        winners.filter(
            F.col("source").isin("web", "mobile") & F.col("is_bot_event")
        )
        .select("session_id")
        .distinct()
        .withColumn("is_bot_session", F.lit(True))
    )
    stitch = (
        winners.filter(
            F.col("customer_id").isNotNull() & F.col("anonymous_id").isNotNull()
        )
        .groupBy("session_id", "anonymous_id")
        .agg(
            F.countDistinct("customer_id").alias("customer_candidates"),
            F.min("customer_id").alias("stitched_customer_id"),
        )
        .filter(F.col("customer_candidates") == 1)
        .drop("customer_candidates")
    )
    enriched = (
        winners.join(bot_sessions, "session_id", "left")
        .join(stitch, ["session_id", "anonymous_id"], "left")
        .fillna({"is_bot_session": False})
        .withColumn(
            "reason_code",
            F.when(~F.col("consent_analytics"), F.lit("CONSENT_DENIED"))
            .when(~F.col("eligible_for_aggregates"), F.lit("LATE_INELIGIBLE"))
            .when(
                F.col("source").isin("web", "mobile") & F.col("is_bot_session"),
                F.lit("BOT_EXCLUDED"),
            ),
        )
        .withColumn(
            "profile_key",
            F.when(
                F.col("customer_id").isNotNull(),
                F.concat(F.lit("customer:"), F.col("customer_id")),
            )
            .when(
                F.col("stitched_customer_id").isNotNull(),
                F.concat(F.lit("customer:"), F.col("stitched_customer_id")),
            )
            .when(
                F.col("anonymous_id").isNotNull(),
                F.concat(F.lit("anonymous:"), F.col("anonymous_id")),
            )
            .otherwise(F.concat(F.lit("session:"), F.col("session_id"))),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    enriched.count()
    accepted = enriched.filter(F.col("reason_code").isNull()).select(
        *[column.name for column in TABLE_CONTRACTS["silver.customer_events"].columns]
    ).persist(StorageLevel.MEMORY_AND_DISK)
    base_quality = enriched.select(
        "event_date",
        "source_topic",
        F.coalesce(F.col("reason_code"), F.lit("ACCEPTED")).alias("reason_code"),
        F.lit(1).cast("long").alias("record_count"),
        F.col("event_id").alias("observation_id"),
    )
    duplicates = enriched.filter(F.col("duplicate_count") > 0).select(
        "event_date",
        "source_topic",
        F.lit("DUPLICATE_EVENT").alias("reason_code"),
        F.col("duplicate_count").cast("long").alias("record_count"),
        F.col("event_id").alias("observation_id"),
    )
    conflicts = enriched.filter(F.col("conflict_count") > 0).select(
        "event_date",
        "source_topic",
        F.lit("EVENT_ID_PAYLOAD_CONFLICT").alias("reason_code"),
        F.lit(1).cast("long").alias("record_count"),
        F.col("event_id").alias("observation_id"),
    )
    raw = kafka_frame.filter(F.col("source_topic").isin(*sorted(_RAW_TOPICS))).select(
        "source_topic",
        "source_partition",
        "source_offset",
        F.to_date("kafka_timestamp").alias("event_date"),
        F.get_json_object("payload_text", "$.event_id").alias("event_id"),
    )
    duplicate_raw_position = raw.groupBy(
        "source_topic", "source_partition", "source_offset"
    ).count().filter(F.col("count") != 1).limit(1)
    if duplicate_raw_position.count():
        raise ValueError("duplicate raw Kafka position in Bronze snapshot")
    clean_outcomes = parsed.select(
        F.col("lineage_topic").alias("source_topic"),
        F.col("lineage_partition").alias("source_partition"),
        F.col("lineage_offset").alias("source_offset"),
        F.lit("clean").alias("outcome_kind"),
        F.col("event_id").alias("outcome_id"),
    )
    dlq_schema = (
        "raw_topic string, raw_partition int, raw_offset long, event_date date, "
        "error_id string, reason_code string"
    )

    def parse_dlq(row: Any) -> tuple[object, ...]:
        outcome = parse_dlq_bronze_record(row.asDict(recursive=True))
        return (
            outcome.source_topic,
            outcome.source_partition,
            outcome.source_offset,
            outcome.event_date,
            outcome.error_id,
            outcome.reason_code,
        )

    dlq = spark.createDataFrame(
        kafka_frame.filter(F.col("source_topic") == _DLQ_TOPIC).rdd.map(parse_dlq),
        dlq_schema,
    ).persist(StorageLevel.MEMORY_AND_DISK)
    dlq.count()
    dlq_outcomes = dlq.select(
        F.col("raw_topic").alias("source_topic"),
        F.col("raw_partition").alias("source_partition"),
        F.col("raw_offset").alias("source_offset"),
        F.lit("dlq").alias("outcome_kind"),
        F.col("error_id").alias("outcome_id"),
    )
    outcomes = clean_outcomes.unionByName(dlq_outcomes).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    outcomes.count()
    multiple = outcomes.groupBy(
        "source_topic", "source_partition", "source_offset"
    ).count().filter(F.col("count") != 1).limit(1)
    if multiple.count():
        raise ValueError("multiple outcomes for raw Kafka position")
    orphaned = outcomes.join(
        raw.select("source_topic", "source_partition", "source_offset"),
        ["source_topic", "source_partition", "source_offset"],
        "left_anti",
    ).limit(1)
    if orphaned.count():
        raise ValueError("outcome references absent raw Kafka position")
    missing = raw.join(
        outcomes,
        ["source_topic", "source_partition", "source_offset"],
        "left_anti",
    )
    accepted_ids = winners.select("event_id").distinct()
    duplicate_raw = missing.join(accepted_ids, "event_id", "inner")
    unreconciled = missing.join(accepted_ids, "event_id", "left_anti").limit(1)
    if unreconciled.count():
        raise ValueError("unreconciled raw Kafka position in Silver build")
    duplicate_quality = duplicate_raw.select(
        "event_date",
        "source_topic",
        F.lit("DUPLICATE_EVENT").alias("reason_code"),
        F.lit(1).cast("long").alias("record_count"),
        F.col("event_id").alias("observation_id"),
    )
    dlq_quality = dlq.select(
        "event_date",
        F.col("raw_topic").alias("source_topic"),
        "reason_code",
        F.lit(1).cast("long").alias("record_count"),
        F.concat(F.lit("dlq:"), F.col("error_id")).alias("observation_id"),
    )
    quality = (
        base_quality.unionByName(duplicates)
        .unionByName(conflicts)
        .unionByName(duplicate_quality)
        .unionByName(dlq_quality)
        .groupBy("event_date", "source_topic", "reason_code")
        .agg(
            F.sum("record_count").cast("long").alias("record_count"),
            F.countDistinct("observation_id").cast("long").alias("distinct_event_count"),
        )
        .withColumn("run_id", F.lit(run_id))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    accepted.count()
    quality.count()
    for cached in (parsed, winners, enriched, dlq, outcomes):
        cached.unpersist()
    return accepted, quality


def _build_dimension_frames(spark: Any, source: Any) -> dict[str, Any]:
    """Build source dimensions with distributed parsing, joins, and windows."""

    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.storagelevel import StorageLevel

    def as_dict(row: Any) -> dict[str, object]:
        return row.asDict(recursive=True)

    def parse_customer_master(row: Any) -> tuple[object, ...]:
        value = as_dict(row)
        payload = _source_payload(value)
        return (
            _required_string(payload.get("customer_id"), "customer_id"),
            _instant(payload.get("created_at"), "created_at"),
        )

    def parse_crm(row: Any) -> tuple[object, ...]:
        value = as_dict(row)
        payload = _source_payload(value)
        source_file, row_number = _row_lineage(value)
        try:
            snapshot_date = date.fromisoformat(str(payload.get("snapshot_date")))
        except ValueError as error:
            raise ValueError("snapshot_date is invalid") from error
        last_order = payload.get("last_order_at")
        return (
            _required_string(payload.get("customer_id"), "customer_id"),
            snapshot_date,
            _required_string(payload.get("loyalty_tier"), "loyalty_tier"),
            _integer_text(payload.get("lifetime_value_minor"), "lifetime_value_minor"),
            None
            if last_order in {None, ""}
            else _instant(last_order, "last_order_at"),
            _boolean_text(payload.get("consent_analytics"), "consent_analytics"),
            _boolean_text(payload.get("consent_marketing"), "consent_marketing"),
            source_file,
            row_number,
        )

    customer_master = spark.createDataFrame(
        source.filter(F.col("source_dataset") == "master/customers").rdd.map(
            parse_customer_master
        ),
        "customer_id string, created_at timestamp",
    )
    duplicate_customer = customer_master.groupBy("customer_id").count().filter(
        F.col("count") != 1
    ).limit(1)
    if duplicate_customer.count():
        raise ValueError("duplicate master customer")
    crm = spark.createDataFrame(
        source.filter(F.col("source_dataset") == "crm").rdd.map(parse_crm),
        (
            "customer_id string, snapshot_date date, loyalty_tier string, "
            "lifetime_value_minor long, last_order_at timestamp, "
            "consent_analytics boolean, consent_marketing boolean, "
            "source_file string, source_row_number long"
        ),
    )
    latest_crm = (
        crm.withColumn(
            "latest_rank",
            F.row_number().over(
                Window.partitionBy("customer_id").orderBy(
                    F.col("snapshot_date").desc(),
                    F.col("source_file").desc(),
                    F.col("source_row_number").desc(),
                )
            ),
        )
        .filter(F.col("latest_rank") == 1)
        .drop("latest_rank")
    )
    orphan_crm = latest_crm.join(customer_master, "customer_id", "left_anti").limit(1)
    if orphan_crm.count():
        raise ValueError("CRM customer has no master record")
    customers = latest_crm.join(customer_master, "customer_id", "inner").select(
        *[column.name for column in TABLE_CONTRACTS["silver.customers"].columns]
    )

    def parse_campaign(row: Any) -> tuple[object, ...]:
        value = as_dict(row)
        payload = _source_payload(value)
        source_file, row_number = _row_lineage(value)
        try:
            start_date = date.fromisoformat(str(payload.get("start_date")))
            end_date = date.fromisoformat(str(payload.get("end_date")))
        except ValueError as error:
            raise ValueError("campaign date is invalid") from error
        if end_date < start_date:
            raise ValueError("campaign ends before it starts")
        currency = _required_string(payload.get("currency"), "currency")
        if currency != "VND":
            raise ValueError(f"unsupported campaign currency: {currency}")
        campaign_id = _required_string(payload.get("campaign_id"), "campaign_id")
        if campaign_id == "__unattributed__":
            raise ValueError("campaign_id uses reserved attribution sentinel")
        return (
            campaign_id,
            _required_string(payload.get("platform"), "platform"),
            _required_string(payload.get("platform_campaign_id"), "platform_campaign_id"),
            _required_string(payload.get("name"), "name"),
            _required_string(payload.get("objective"), "objective"),
            _required_string(payload.get("status"), "status"),
            start_date,
            end_date,
            _integer_text(payload.get("daily_budget_minor"), "daily_budget_minor"),
            currency,
            source_file,
            row_number,
        )

    campaigns_base = spark.createDataFrame(
        source.filter(F.col("source_dataset") == "master/campaigns").rdd.map(
            parse_campaign
        ),
        (
            "campaign_id string, platform string, platform_campaign_id string, "
            "name string, objective string, status string, start_date date, "
            "end_date date, daily_budget_minor long, currency string, "
            "source_file string, source_row_number long"
        ),
    )
    for field in ("campaign_id", "platform_campaign_id"):
        duplicate = campaigns_base.groupBy(field).count().filter(F.col("count") != 1).limit(1)
        if duplicate.count():
            raise ValueError(f"duplicate {field}")

    def parse_ad(row: Any) -> tuple[str]:
        payload = _source_payload(as_dict(row))
        return (_required_string(payload.get("campaign_id"), "campaign_id"),)

    ads = spark.createDataFrame(
        source.filter(F.col("source_dataset") == "master/ads").rdd.map(parse_ad),
        "campaign_id string",
    )
    orphan_ad = ads.join(campaigns_base.select("campaign_id"), "campaign_id", "left_anti").limit(1)
    if orphan_ad.count():
        raise ValueError("ad references unknown campaign")
    ad_counts = ads.groupBy("campaign_id").agg(
        F.count(F.lit(1)).cast("long").alias("ad_count")
    )
    campaigns = (
        campaigns_base.join(ad_counts, "campaign_id", "left")
        .fillna({"ad_count": 0})
        .select(*[column.name for column in TABLE_CONTRACTS["silver.campaigns"].columns])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    campaigns.count()

    def parse_spend(row: Any) -> tuple[object, ...]:
        value = as_dict(row)
        dataset = str(value.get("source_dataset"))
        payload = _source_payload(value)
        source_file, row_number = _row_lineage(value)
        platform = {"google_ads": "google", "meta_ads": "meta", "tiktok_ads": "tiktok"}[dataset]
        platform_campaign_id = _required_string(payload.get("campaign_id"), "campaign_id")
        if platform == "google":
            spend_date = date.fromisoformat(str(payload.get("segments_date")))
            conversions = _integer_text(payload.get("conversions"), "conversions")
            spend_minor = _half_up(payload.get("cost_micros"), "cost_micros", Decimal(1_000_000))
            conversion_value = _half_up(payload.get("conversions_value"), "conversions_value")
        elif platform == "meta":
            spend_date = date.fromisoformat(str(payload.get("date_start")))
            if str(payload.get("date_stop")) != spend_date.isoformat():
                raise ValueError("Meta report spans multiple dates")
            conversions = _half_up(_action_value(payload.get("actions"), "actions"), "actions")
            spend_minor = _half_up(payload.get("spend"), "spend")
            conversion_value = _half_up(
                _action_value(payload.get("action_values"), "action_values"),
                "action_values",
            )
        else:
            spend_date = date.fromisoformat(str(payload.get("stat_time_day")))
            conversions = _integer_text(payload.get("conversion"), "conversion")
            spend_minor = _half_up(payload.get("spend"), "spend")
            conversion_value = _half_up(payload.get("total_purchase_value"), "total_purchase_value")
        return (
            spend_date,
            platform,
            platform_campaign_id,
            _integer_text(payload.get("impressions"), "impressions"),
            _integer_text(payload.get("clicks"), "clicks"),
            conversions,
            spend_minor,
            conversion_value,
            source_file,
            row_number,
        )

    spend_raw = spark.createDataFrame(
        source.filter(F.col("source_dataset").isin("google_ads", "meta_ads", "tiktok_ads"))
        .rdd.map(parse_spend),
        (
            "spend_date date, platform string, platform_campaign_id string, "
            "impressions long, clicks long, conversions long, spend_minor long, "
            "conversion_value_minor long, source_file string, source_row_number long"
        ),
    )
    campaign_mapping = campaigns.select(
        "campaign_id", "platform", "platform_campaign_id", "currency"
    )
    unmatched_spend = spend_raw.join(
        campaign_mapping,
        ["platform", "platform_campaign_id"],
        "left_anti",
    ).limit(1)
    if unmatched_spend.count():
        raise ValueError("ad spend has no campaign mapping")
    spend = spend_raw.join(
        campaign_mapping,
        ["platform", "platform_campaign_id"],
        "inner",
    )
    duplicate_spend = spend.groupBy("spend_date", "campaign_id", "platform").count().filter(
        F.col("count") != 1
    ).limit(1)
    if duplicate_spend.count():
        raise ValueError("duplicate ad spend grain")
    digest_udf = F.udf(
        lambda day, campaign_id, platform: _digest([str(day), campaign_id, platform]),
        T.StringType(),
    )
    ad_spend = spend.withColumn(
        "spend_key", digest_udf("spend_date", "campaign_id", "platform")
    ).select(*[column.name for column in TABLE_CONTRACTS["silver.ad_spend_daily"].columns])
    return {
        "customers": customers,
        "campaigns": campaigns,
        "ad_spend_daily": ad_spend,
    }


def build_silver(
    spark: Any,
    *,
    kafka_snapshot_id: int,
    source_rows_snapshot_id: int,
    run_id: str,
    target_namespace: str = "silver",
) -> dict[str, object]:
    """Build all Silver tables from explicit immutable Bronze snapshots."""

    if kafka_snapshot_id < 1 or source_rows_snapshot_id < 1:
        raise ValueError("Bronze snapshot IDs must be positive")
    if not run_id:
        raise ValueError("run_id must not be empty")
    target_namespace = validate_namespace(target_namespace)
    _ensure_silver_tables(spark, target_namespace)
    kafka = spark.sql(
        "SELECT * FROM lakehouse.bronze.kafka_records "
        f"VERSION AS OF {int(kafka_snapshot_id)}"
    )
    source = spark.sql(
        "SELECT * FROM lakehouse.bronze.source_file_rows "
        f"VERSION AS OF {int(source_rows_snapshot_id)}"
    )
    accepted, quality = _build_customer_event_frames(spark, kafka, run_id)
    dimensions = _build_dimension_frames(spark, source)
    frames = {
        "customer_events": accepted,
        **dimensions,
        "quality_summary": quality.select(
            *[column.name for column in TABLE_CONTRACTS["silver.quality_summary"].columns]
        ),
    }
    try:
        counts = {
            table: _replace_table(
                spark,
                namespace=target_namespace,
                table=table,
                frame=frame,
            )
            for table, frame in frames.items()
        }
    finally:
        for frame in frames.values():
            frame.unpersist(blocking=False)
    snapshots = {
        table: _snapshot_id(spark, target_namespace, table) for table in frames
    }
    return {
        "command": "build-silver",
        "run_id": run_id,
        "target_namespace": target_namespace,
        "bronze_snapshots": {
            "bronze.kafka_records": kafka_snapshot_id,
            "bronze.source_file_rows": source_rows_snapshot_id,
        },
        "counts": counts,
        "snapshots": snapshots,
    }
