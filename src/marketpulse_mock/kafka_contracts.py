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


TOPIC_CONTRACTS = {
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


def record_key(dataset: str, value: dict[str, object]) -> str:
    """Return the stable Kafka partition key for a source-shaped record."""

    if dataset == "quality_cases":
        candidates = (value.get("case_id"),)
    elif dataset == "web_events":
        candidates = (value.get("customer_id"), value.get("session_id"))
    elif dataset == "orders":
        payload = value.get("payload")
        order_id = payload.get("order_id") if isinstance(payload, dict) else None
        candidates = (value.get("customer_id"), order_id, value.get("session_id"))
    elif dataset == "email_events":
        payload = value.get("payload")
        message_id = payload.get("message_id") if isinstance(payload, dict) else None
        candidates = (value.get("customer_id"), message_id, value.get("session_id"))
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
