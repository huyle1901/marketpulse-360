"""Versioned Kafka topics, keys, and JSON Schema artifact contracts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class TopicContract:
    dataset: str
    topic: str
    artifact_id: str
    schema_file: str
    partitions: int = 6
    retention_ms: int = 604_800_000
    cleanup_policy: str = "delete"


SOURCE_TOPIC_CONTRACTS = {
    contract.dataset: contract
    for contract in (
        TopicContract(
            "web_events",
            "marketing.raw.web-events.v1",
            "web-events-v1",
            "web-events-v1.json",
        ),
        TopicContract(
            "orders",
            "marketing.raw.order-events.v1",
            "order-events-v1",
            "order-events-v1.json",
        ),
        TopicContract(
            "email_events",
            "marketing.raw.email-events.v1",
            "email-events-v1",
            "email-events-v1.json",
        ),
        TopicContract(
            "quality_cases",
            "marketing.dlq.validation.v1",
            "validation-dlq-v1",
            "validation-dlq-v1.json",
        ),
    )
}

FLINK_TOPIC_CONTRACTS = {
    contract.dataset: contract
    for contract in (
        TopicContract(
            "clean_customer_events",
            "marketing.clean.customer-events.v1",
            "customer-events-v1",
            "customer-events-v1.json",
        ),
        TopicContract(
            "sessions",
            "marketing.clean.sessions.v1",
            "sessions-v1",
            "sessions-v1.json",
            cleanup_policy="compact,delete",
        ),
        TopicContract(
            "funnel_metrics",
            "marketing.gold.funnel-metrics.v1",
            "funnel-metrics-v1",
            "funnel-metrics-v1.json",
            partitions=3,
            retention_ms=2_592_000_000,
            cleanup_policy="compact,delete",
        ),
        TopicContract(
            "revenue_metrics",
            "marketing.gold.revenue-metrics.v1",
            "revenue-metrics-v1",
            "revenue-metrics-v1.json",
            partitions=3,
            retention_ms=2_592_000_000,
            cleanup_policy="compact,delete",
        ),
        TopicContract(
            "campaign_metrics",
            "marketing.gold.campaign-metrics.v1",
            "campaign-metrics-v1",
            "campaign-metrics-v1.json",
            partitions=3,
            retention_ms=2_592_000_000,
            cleanup_policy="compact,delete",
        ),
        TopicContract(
            "flink_validation_dlq",
            "marketing.dlq.flink-validation.v1",
            "flink-validation-dlq-v1",
            "flink-validation-dlq-v1.json",
        ),
    )
}

ALL_TOPIC_CONTRACTS = {
    **SOURCE_TOPIC_CONTRACTS,
    **FLINK_TOPIC_CONTRACTS,
}

# Backward-compatible source-only name used by the replay reader.
TOPIC_CONTRACTS = SOURCE_TOPIC_CONTRACTS


def record_key(dataset: str, value: dict[str, object]) -> str:
    """Return the stable Kafka partition key for a source-shaped record."""

    if dataset == "quality_cases":
        candidates = (value.get("case_id"),)
    elif dataset == "web_events":
        candidates = (value.get("customer_id"), value.get("session_id"))
    elif dataset == "orders":
        payload = value.get("payload")
        order_id = payload.get("order_id") if isinstance(payload, dict) else None
        candidates = (value.get("customer_id"), order_id)
    elif dataset == "email_events":
        payload = value.get("payload")
        message_id = payload.get("message_id") if isinstance(payload, dict) else None
        candidates = (value.get("customer_id"), message_id)
    else:
        raise ValueError(f"unsupported replay dataset: {dataset}")
    key = next((str(candidate) for candidate in candidates if candidate), None)
    if key is None:
        raise ValueError(f"{dataset} record has no usable partition key")
    return key


def load_contract_schema(
    contract_dir: Path,
    contract: TopicContract,
) -> dict[str, object]:
    path = Path(contract_dir) / contract.schema_file
    try:
        schema = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot load schema {path}: {error}") from error
    if not isinstance(schema, dict):
        raise ValueError(f"schema must be a JSON object: {path}")
    return schema
