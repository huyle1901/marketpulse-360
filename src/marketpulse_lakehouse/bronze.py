"""Immutable Kafka Bronze normalization, manifests, and merge guards."""

from __future__ import annotations

import base64
import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Iterable, Mapping, Protocol, Sequence

from .contracts import TABLE_CONTRACTS, TableContract
from .identity import kafka_ingest_id


ALLOWED_KAFKA_TOPICS = frozenset(
    {
        "marketing.raw.web-events.v1",
        "marketing.raw.order-events.v1",
        "marketing.raw.email-events.v1",
        "marketing.clean.customer-events.v1",
        "marketing.dlq.flink-validation.v1",
    }
)
_TIMESTAMP_TYPES = {
    -1: "NO_TIMESTAMP_TYPE",
    0: "CREATE_TIME",
    1: "LOG_APPEND_TIME",
}
_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PARTITION_TRANSFORM = re.compile(
    r"^(?P<transform>days|months)\((?P<column>[A-Za-z_][A-Za-z0-9_]*)\)$"
)
_IMMUTABLE_KAFKA_FIELDS = (
    "ingest_id",
    "source_topic",
    "source_partition",
    "source_offset",
    "kafka_timestamp",
    "kafka_timestamp_type",
    "key_base64",
    "key_sha256",
    "headers_json",
    "payload_base64",
    "payload_text",
    "payload_sha256",
)


def _binary(value: bytes | bytearray | memoryview | None, field: str) -> bytes | None:
    if value is None:
        return None
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise ValueError(f"{field} must be bytes or null")
    return bytes(value)


def _binary_fields(value: bytes | None) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    return base64.b64encode(value).decode("ascii"), hashlib.sha256(value).hexdigest()


def _strict_text(value: bytes | None) -> str | None:
    if value is None:
        return None
    try:
        return value.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return None


def _header_pair(item: object) -> tuple[str, bytes | None]:
    if isinstance(item, (tuple, list)) and len(item) == 2:
        key, value = item
    elif isinstance(item, Mapping):
        key, value = item.get("key"), item.get("value")
    else:
        key, value = getattr(item, "key", None), getattr(item, "value", None)
    if not isinstance(key, str):
        raise ValueError("Kafka header key must be a string")
    return key, _binary(value, "Kafka header value")


def normalize_kafka_record(
    *,
    topic: str,
    partition: int,
    offset: int,
    timestamp: datetime,
    timestamp_type: int | str,
    key: bytes | bytearray | memoryview | None,
    value: bytes | bytearray | memoryview | None,
    headers: Sequence[object] | None,
    run_id: str,
    ingested_at: datetime,
    manifest_sha256: str | None = None,
) -> dict[str, object]:
    """Normalize one Kafka source row without parsing its key or value."""

    if topic not in ALLOWED_KAFKA_TOPICS:
        raise ValueError(f"topic is not allowed for Bronze archival: {topic}")
    if partition < 0 or offset < 0:
        raise ValueError("Kafka partition and offset must be non-negative")
    if not isinstance(timestamp, datetime):
        raise ValueError("Kafka timestamp must be a datetime")
    if not run_id:
        raise ValueError("run_id must not be empty")
    if manifest_sha256 is not None and not re.fullmatch(r"[0-9a-f]{64}", manifest_sha256):
        raise ValueError("manifest_sha256 must be a SHA-256 digest")
    if isinstance(timestamp_type, int):
        try:
            timestamp_type_name = _TIMESTAMP_TYPES[timestamp_type]
        except KeyError as error:
            raise ValueError(f"unknown Kafka timestamp type: {timestamp_type}") from error
    elif timestamp_type in _TIMESTAMP_TYPES.values():
        timestamp_type_name = timestamp_type
    else:
        raise ValueError(f"unknown Kafka timestamp type: {timestamp_type}")

    key_bytes = _binary(key, "Kafka key")
    value_bytes = _binary(value, "Kafka value")
    key_base64, key_sha256 = _binary_fields(key_bytes)
    payload_base64, payload_sha256 = _binary_fields(value_bytes)
    header_values = []
    for item in headers or ():
        header_key, header_value = _header_pair(item)
        header_values.append(
            {
                "key": header_key,
                "value_base64": (
                    None
                    if header_value is None
                    else base64.b64encode(header_value).decode("ascii")
                ),
            }
        )

    return {
        "ingest_id": kafka_ingest_id(topic, partition, offset),
        "source_topic": topic,
        "source_partition": partition,
        "source_offset": offset,
        "kafka_timestamp": timestamp,
        "kafka_timestamp_type": timestamp_type_name,
        "key_base64": key_base64,
        "key_sha256": key_sha256,
        "headers_json": json.dumps(
            header_values,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "payload_base64": payload_base64,
        "payload_text": _strict_text(value_bytes),
        "payload_sha256": payload_sha256,
        "ingestion_run_id": run_id,
        "ingestion_manifest_sha256": manifest_sha256,
        "ingested_at": ingested_at,
    }


def _immutable_values(record: Mapping[str, object]) -> tuple[object, ...]:
    try:
        return tuple(record[field] for field in _IMMUTABLE_KAFKA_FIELDS)
    except KeyError as error:
        raise ValueError(f"Kafka Bronze row is missing {error.args[0]}") from error


@dataclass(frozen=True, slots=True)
class KafkaMergePlan:
    insert_records: tuple[dict[str, object], ...]
    noop_count: int


def classify_kafka_records(
    incoming_records: Iterable[Mapping[str, object]],
    *,
    existing_records: Iterable[Mapping[str, object]],
) -> KafkaMergePlan:
    """Separate new positions from exact retries and reject any collision."""

    observed: dict[str, tuple[object, ...]] = {}
    for record in existing_records:
        ingest_id = record.get("ingest_id")
        if not isinstance(ingest_id, str) or not ingest_id:
            raise ValueError("Kafka Bronze row has invalid ingest_id")
        values = _immutable_values(record)
        if ingest_id in observed and observed[ingest_id] != values:
            raise ValueError(f"KAFKA_POSITION_COLLISION: {ingest_id}")
        observed[ingest_id] = values

    inserts: list[dict[str, object]] = []
    noop_count = 0
    for record in incoming_records:
        ingest_id = record.get("ingest_id")
        if not isinstance(ingest_id, str) or not ingest_id:
            raise ValueError("Kafka Bronze row has invalid ingest_id")
        values = _immutable_values(record)
        previous = observed.get(ingest_id)
        if previous is not None:
            if previous != values:
                raise ValueError(f"KAFKA_POSITION_COLLISION: {ingest_id}")
            noop_count += 1
            continue
        copied = dict(record)
        inserts.append(copied)
        observed[ingest_id] = values
    return KafkaMergePlan(tuple(inserts), noop_count)


@dataclass(frozen=True, order=True, slots=True)
class KafkaOffsetRange:
    topic: str
    partition: int
    start_offset: int
    end_offset: int

    def __post_init__(self) -> None:
        if self.topic not in ALLOWED_KAFKA_TOPICS:
            raise ValueError(f"topic is not allowed in offset manifest: {self.topic}")
        if self.partition < 0 or self.start_offset < 0 or self.end_offset < 0:
            raise ValueError("Kafka offsets and partition must be non-negative")
        if self.end_offset < self.start_offset:
            raise ValueError("end_offset must be greater than or equal to start_offset")


@dataclass(frozen=True, slots=True)
class KafkaOffsetManifest:
    cluster_id: str
    captured_at: datetime
    allowed_topics: tuple[str, ...]
    ranges: tuple[KafkaOffsetRange, ...]

    def __post_init__(self) -> None:
        if not self.cluster_id:
            raise ValueError("cluster_id must not be empty")
        if self.captured_at.tzinfo is None:
            raise ValueError("captured_at must be timezone-aware")
        topics = tuple(sorted(set(self.allowed_topics)))
        if not topics or any(topic not in ALLOWED_KAFKA_TOPICS for topic in topics):
            raise ValueError("offset manifest contains an unapproved topic")
        ranges = tuple(sorted(self.ranges))
        positions = [(item.topic, item.partition) for item in ranges]
        if len(positions) != len(set(positions)):
            raise ValueError("offset manifest contains a duplicate topic-partition")
        if any(item.topic not in topics for item in ranges):
            raise ValueError("offset range topic is absent from allowed_topics")
        if set(topics) != {item.topic for item in ranges}:
            raise ValueError("every allowed topic must have at least one offset range")
        object.__setattr__(self, "allowed_topics", topics)
        object.__setattr__(self, "ranges", ranges)

    def to_json(self) -> str:
        captured_at = self.captured_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
        return json.dumps(
            {
                "version": 1,
                "cluster_id": self.cluster_id,
                "captured_at": captured_at,
                "allowed_topics": list(self.allowed_topics),
                "ranges": [
                    {
                        "topic": item.topic,
                        "partition": item.partition,
                        "start_offset": item.start_offset,
                        "end_offset": item.end_offset,
                    }
                    for item in self.ranges
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.to_json().encode()).hexdigest()

    @classmethod
    def from_json(cls, content: str) -> "KafkaOffsetManifest":
        try:
            raw = json.loads(content)
            if not isinstance(raw, dict):
                raise TypeError("offset manifest must be an object")
            if raw.get("version") != 1:
                raise ValueError("unsupported offset manifest version")
            captured_at = datetime.fromisoformat(
                str(raw["captured_at"]).replace("Z", "+00:00")
            )
            return cls(
                cluster_id=str(raw["cluster_id"]),
                captured_at=captured_at,
                allowed_topics=tuple(raw["allowed_topics"]),
                ranges=tuple(
                    KafkaOffsetRange(
                        topic=str(item["topic"]),
                        partition=int(item["partition"]),
                        start_offset=int(item["start_offset"]),
                        end_offset=int(item["end_offset"]),
                    )
                    for item in raw["ranges"]
                ),
            )
        except (KeyError, TypeError, json.JSONDecodeError) as error:
            raise ValueError(f"invalid offset manifest: {error}") from error


def validate_offset_manifest(
    manifest: KafkaOffsetManifest,
    *,
    broker_bounds: Mapping[str, Mapping[int, tuple[int, int]]],
) -> None:
    """Reject partition drift, retention gaps, and future ending offsets."""

    if set(broker_bounds) != set(manifest.allowed_topics):
        raise ValueError("Kafka topic drift detected")
    ranges = {(item.topic, item.partition): item for item in manifest.ranges}
    broker_positions = {
        (topic, partition)
        for topic, partitions in broker_bounds.items()
        for partition in partitions
    }
    if set(ranges) != broker_positions:
        raise ValueError("Kafka partition drift detected")
    for position, item in ranges.items():
        earliest, end = broker_bounds[position[0]][position[1]]
        if earliest < 0 or end < earliest:
            raise ValueError(f"invalid broker bounds for {position}")
        if item.start_offset < earliest:
            raise ValueError(f"Kafka retention gap for {position}")
        if item.end_offset > end:
            raise ValueError(f"offset manifest is beyond broker end for {position}")


def _gap_sample(start: int, end: int, observed: set[int], limit: int = 20) -> tuple[int, ...]:
    sample: list[int] = []
    candidate = start
    for offset in sorted(observed):
        while candidate < offset and len(sample) < limit:
            sample.append(candidate)
            candidate += 1
        candidate = max(candidate, offset + 1)
        if len(sample) == limit:
            break
    while candidate < end and len(sample) < limit:
        sample.append(candidate)
        candidate += 1
    return tuple(sample)


def offset_observation_summary(
    manifest: KafkaOffsetManifest,
    records: Iterable[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    ranges = {(item.topic, item.partition): item for item in manifest.ranges}
    observed: dict[tuple[str, int], set[int]] = {position: set() for position in ranges}
    for record in records:
        try:
            position = (str(record["source_topic"]), int(record["source_partition"]))
            offset = int(record["source_offset"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("invalid observed Kafka position") from error
        item = ranges.get(position)
        if item is None or not (item.start_offset <= offset < item.end_offset):
            raise ValueError(f"observed Kafka position is outside manifest: {position}:{offset}")
        observed[position].add(offset)

    summaries: list[dict[str, object]] = []
    for item in manifest.ranges:
        offsets = observed[(item.topic, item.partition)]
        requested = item.end_offset - item.start_offset
        summaries.append(
            {
                "source_topic": item.topic,
                "source_partition": item.partition,
                "start_offset": item.start_offset,
                "end_offset": item.end_offset,
                "requested_positions": requested,
                "observed_records": len(offsets),
                "observed_min_offset": min(offsets) if offsets else None,
                "observed_max_offset": max(offsets) if offsets else None,
                "offset_gap_count": requested - len(offsets),
                "offset_gaps_sample": _gap_sample(
                    item.start_offset, item.end_offset, offsets
                ),
            }
        )
    return tuple(summaries)


def _quoted(identifier: str) -> str:
    if not _SQL_IDENTIFIER.fullmatch(identifier):
        raise ValueError(f"invalid SQL identifier: {identifier}")
    return f"`{identifier}`"


def _partition_expression(expression: str) -> str:
    transform = _PARTITION_TRANSFORM.fullmatch(expression)
    if transform:
        return f"{transform.group('transform')}({_quoted(transform.group('column'))})"
    return _quoted(expression)


def _table_statement(catalog_name: str, contract: TableContract) -> str:
    namespace, table = contract.name.split(".", maxsplit=1)
    columns = ",\n  ".join(
        f"{_quoted(column.name)} {column.iceberg_type.upper()}"
        + ("" if column.nullable else " NOT NULL")
        for column in contract.columns
    )
    statement = (
        f"CREATE TABLE IF NOT EXISTS {_quoted(catalog_name)}."
        f"{_quoted(namespace)}.{_quoted(table)} (\n  {columns}\n) USING iceberg"
    )
    if contract.partition_by:
        partitions = ", ".join(
            _partition_expression(expression) for expression in contract.partition_by
        )
        statement += f"\nPARTITIONED BY ({partitions})"
    return statement + "\nTBLPROPERTIES ('format-version'='2')"


def bootstrap_statements(catalog_name: str = "lakehouse") -> tuple[str, ...]:
    """Return deterministic DDL for every governed namespace and table."""

    try:
        _quoted(catalog_name)
    except ValueError as error:
        raise ValueError("catalog name must be a SQL identifier") from error
    namespaces = tuple(
        sorted({contract.name.split(".", maxsplit=1)[0] for contract in TABLE_CONTRACTS.values()})
    )
    namespace_statements = tuple(
        f"CREATE NAMESPACE IF NOT EXISTS {_quoted(catalog_name)}.{_quoted(namespace)}"
        for namespace in namespaces
    )
    table_statements = tuple(
        _table_statement(catalog_name, contract) for contract in TABLE_CONTRACTS.values()
    )
    return (*namespace_statements, *table_statements)


class SparkSql(Protocol):
    def sql(self, statement: str) -> object: ...


def bootstrap_lakehouse(
    spark: SparkSql,
    catalog_name: str = "lakehouse",
) -> tuple[str, ...]:
    statements = bootstrap_statements(catalog_name)
    for statement in statements:
        spark.sql(statement)
    return statements


def kafka_broker_bounds(
    bootstrap_servers: str,
    topics: Sequence[str] = tuple(sorted(ALLOWED_KAFKA_TOPICS)),
) -> tuple[str, dict[str, dict[int, tuple[int, int]]]]:
    """Read exact partition topology and earliest/latest offsets from Kafka."""

    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError as error:
        raise RuntimeError("Kafka support is not installed in the lakehouse image") from error
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"marketpulse-lakehouse-metadata-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(timeout=20.0)
        cluster_id = str(getattr(metadata, "cluster_id", "") or "")
        if not cluster_id:
            raise RuntimeError("Kafka did not report a cluster ID")
        bounds: dict[str, dict[int, tuple[int, int]]] = {}
        for topic in sorted(set(topics)):
            if topic not in ALLOWED_KAFKA_TOPICS:
                raise ValueError(f"topic is not allowed for Bronze archival: {topic}")
            topic_metadata = metadata.topics.get(topic)
            if topic_metadata is None or topic_metadata.error is not None:
                raise RuntimeError(f"Kafka topic is unavailable: {topic}")
            bounds[topic] = {}
            for partition in sorted(topic_metadata.partitions):
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=20.0, cached=False
                )
                bounds[topic][int(partition)] = (int(low), int(high))
        return cluster_id, bounds
    finally:
        consumer.close()


def capture_offset_manifest(
    spark: Any,
    *,
    bootstrap_servers: str,
    captured_at: datetime,
) -> KafkaOffsetManifest:
    cluster_id, bounds = kafka_broker_bounds(bootstrap_servers)
    existing_rows = spark.sql(
        """
        SELECT source_topic, source_partition, MAX(source_offset) AS max_offset
        FROM lakehouse.bronze.kafka_records
        GROUP BY source_topic, source_partition
        """
    ).collect()
    existing = {
        (str(row["source_topic"]), int(row["source_partition"])): int(row["max_offset"])
        for row in existing_rows
    }
    ranges = []
    for topic, partitions in sorted(bounds.items()):
        for partition, (earliest, end) in sorted(partitions.items()):
            start = existing.get((topic, partition), earliest - 1) + 1
            ranges.append(KafkaOffsetRange(topic, partition, start, end))
    manifest = KafkaOffsetManifest(
        cluster_id=cluster_id,
        captured_at=captured_at,
        allowed_topics=tuple(sorted(bounds)),
        ranges=tuple(ranges),
    )
    validate_offset_manifest(manifest, broker_bounds=bounds)
    return manifest


def spark_kafka_options(
    manifest: KafkaOffsetManifest,
    *,
    bootstrap_servers: str,
) -> dict[str, str]:
    starting: dict[str, dict[str, int]] = {}
    ending: dict[str, dict[str, int]] = {}
    for item in manifest.ranges:
        starting.setdefault(item.topic, {})[str(item.partition)] = item.start_offset
        ending.setdefault(item.topic, {})[str(item.partition)] = item.end_offset
    return {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": ",".join(manifest.allowed_topics),
        "startingOffsets": json.dumps(starting, sort_keys=True, separators=(",", ":")),
        "endingOffsets": json.dumps(ending, sort_keys=True, separators=(",", ":")),
        "includeHeaders": "true",
        "failOnDataLoss": "true",
        "kafka.isolation.level": "read_committed",
    }


def _spark_schema(contract: TableContract) -> str:
    type_names = {
        "string": "string",
        "int": "int",
        "long": "bigint",
        "timestamp": "timestamp",
        "date": "date",
        "boolean": "boolean",
    }
    fields = []
    for column in contract.columns:
        spark_type = type_names.get(column.iceberg_type, column.iceberg_type)
        fields.append(f"`{column.name}` {spark_type}")
    return ", ".join(fields)


def current_snapshot_id(spark: Any, table_name: str) -> int | None:
    rows = spark.sql(
        f"SELECT snapshot_id FROM lakehouse.{table_name}.refs WHERE name = 'main'"
    ).collect()
    if not rows:
        return None
    if len(rows) != 1:
        raise ValueError(f"Iceberg main ref is ambiguous: {table_name}")
    return int(rows[0]["snapshot_id"])


def merge_insert_only(
    spark: Any,
    *,
    table_name: str,
    records: Any,
) -> tuple[int, int]:
    """Fail on immutable-key collisions and insert only previously unseen keys."""

    contract = TABLE_CONTRACTS[table_name]
    suffix = uuid.uuid4().hex
    raw_view = f"incoming_raw_{table_name.replace('.', '_')}_{suffix}"
    deduplicated_view = f"incoming_distinct_{table_name.replace('.', '_')}_{suffix}"
    records.createOrReplaceTempView(raw_view)
    keys = " AND ".join(f"target.`{key}` = source.`{key}`" for key in contract.key_fields)
    immutable = tuple(column.name for column in contract.columns if not column.operational)
    same = " AND ".join(
        f"target.`{field}` <=> source.`{field}`" for field in immutable
    )
    target = f"lakehouse.`{table_name.split('.')[0]}`.`{table_name.split('.')[1]}`"
    incoming_keys = " AND ".join(
        f"left_row.`{key}` <=> right_row.`{key}`" for key in contract.key_fields
    )
    incoming_same = " AND ".join(
        f"left_row.`{field}` <=> right_row.`{field}`" for field in immutable
    )
    created_deduplicated_view = False
    try:
        incoming_collision = spark.sql(
            f"SELECT 1 AS value FROM `{raw_view}` left_row "
            f"JOIN `{raw_view}` right_row ON {incoming_keys} "
            f"WHERE NOT ({incoming_same}) LIMIT 1"
        ).collect()
        if incoming_collision:
            raise ValueError(f"IMMUTABLE_KEY_COLLISION: {table_name}")

        records.dropDuplicates(list(contract.key_fields)).createOrReplaceTempView(
            deduplicated_view
        )
        created_deduplicated_view = True
        collision = spark.sql(
            f"SELECT COUNT(*) AS value FROM `{deduplicated_view}` source "
            f"JOIN {target} target ON {keys} WHERE NOT ({same})"
        ).collect()[0]["value"]
        if int(collision):
            raise ValueError(f"IMMUTABLE_KEY_COLLISION: {table_name}")
        noop = int(
            spark.sql(
                f"SELECT COUNT(*) AS value FROM `{deduplicated_view}` source "
                f"JOIN {target} target ON {keys}"
            ).collect()[0]["value"]
        )
        inserted = int(
            spark.sql(
                f"SELECT COUNT(*) AS value FROM `{deduplicated_view}` source "
                f"LEFT ANTI JOIN {target} target ON {keys}"
            ).collect()[0]["value"]
        )
        if inserted:
            spark.sql(
                f"MERGE INTO {target} target USING `{deduplicated_view}` source ON {keys} "
                "WHEN NOT MATCHED THEN INSERT *"
            )
        return inserted, noop
    finally:
        if created_deduplicated_view:
            spark.catalog.dropTempView(deduplicated_view)
        spark.catalog.dropTempView(raw_view)


def ingest_kafka_manifest(
    spark: Any,
    manifest: KafkaOffsetManifest,
    *,
    bootstrap_servers: str,
    run_id: str,
    ingested_at: datetime,
) -> dict[str, object]:
    if not run_id:
        raise ValueError("run_id must not be empty")
    cluster_id, bounds = kafka_broker_bounds(bootstrap_servers)
    if cluster_id != manifest.cluster_id:
        raise ValueError("Kafka cluster ID does not match offset manifest")
    validate_offset_manifest(manifest, broker_bounds=bounds)
    reader = spark.read.format("kafka")
    for key, value in spark_kafka_options(
        manifest, bootstrap_servers=bootstrap_servers
    ).items():
        reader = reader.option(key, value)
    raw = reader.load()

    def normalize(row: Any) -> dict[str, object]:
        return normalize_kafka_record(
            topic=row["topic"],
            partition=row["partition"],
            offset=row["offset"],
            timestamp=row["timestamp"],
            timestamp_type=row["timestampType"],
            key=row["key"],
            value=row["value"],
            headers=row["headers"],
            run_id=run_id,
            ingested_at=ingested_at,
            manifest_sha256=manifest.sha256,
        )

    contract = TABLE_CONTRACTS["bronze.kafka_records"]
    normalized = spark.createDataFrame(raw.rdd.map(normalize), schema=_spark_schema(contract))
    normalized.persist()
    try:
        positions = (
            normalized.select("source_topic", "source_partition", "source_offset")
            .collect()
        )
        summaries = offset_observation_summary(manifest, positions)
        inserted, noops = merge_insert_only(
            spark,
            table_name="bronze.kafka_records",
            records=normalized,
        )
    finally:
        normalized.unpersist()
    return {
        "command": "ingest-kafka",
        "run_id": run_id,
        "manifest_sha256": manifest.sha256,
        "observed_records": sum(int(item["observed_records"]) for item in summaries),
        "inserted_records": inserted,
        "noop_records": noops,
        "offsets": list(summaries),
        "snapshot_id": current_snapshot_id(spark, "bronze.kafka_records"),
    }
