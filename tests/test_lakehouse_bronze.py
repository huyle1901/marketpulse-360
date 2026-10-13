from __future__ import annotations

import base64
import hashlib
import json
import unittest
from datetime import UTC, datetime
from unittest.mock import patch

from marketpulse_lakehouse.bronze import (
    KafkaOffsetManifest,
    KafkaOffsetRange,
    bootstrap_lakehouse,
    bootstrap_statements,
    classify_kafka_records,
    current_snapshot_id,
    normalize_kafka_record,
    offset_observation_summary,
    capture_offset_manifest,
    spark_kafka_options,
    validate_offset_manifest,
)
from marketpulse_lakehouse.identity import kafka_ingest_id


class BronzeKafkaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.timestamp = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
        self.ingested_at = datetime(2026, 1, 2, 4, 0, tzinfo=UTC)

    def _record(self, **overrides: object) -> dict[str, object]:
        values: dict[str, object] = {
            "topic": "marketing.raw.web-events.v1",
            "partition": 0,
            "offset": 1,
            "timestamp": self.timestamp,
            "timestamp_type": 0,
            "key": b"event-1",
            "value": b'{"event_id":"event-1"}',
            "headers": [("trace", b"one"), ("trace", None), ("binary", b"\xff")],
            "run_id": "run-1",
            "ingested_at": self.ingested_at,
        }
        values.update(overrides)
        return normalize_kafka_record(**values)  # type: ignore[arg-type]

    def test_current_snapshot_uses_main_ref_not_timestamp_or_numeric_order(self) -> None:
        class Result:
            def collect(self) -> list[dict[str, int]]:
                return [{"snapshot_id": 7}]

        class Spark:
            query = ""

            def sql(self, query: str) -> Result:
                self.query = query
                return Result()

        spark = Spark()
        self.assertEqual(current_snapshot_id(spark, "bronze.kafka_records"), 7)
        self.assertIn(".refs WHERE name = 'main'", spark.query)
        self.assertNotIn("committed_at", spark.query)

    def test_binary_record_preserves_null_key_ordered_duplicate_headers_and_invalid_utf8(self) -> None:
        row = self._record(key=None, value=b"\xff\x00")

        self.assertEqual(row["ingest_id"], kafka_ingest_id(str(row["source_topic"]), 0, 1))
        self.assertIsNone(row["key_base64"])
        self.assertIsNone(row["key_sha256"])
        self.assertEqual(row["payload_base64"], base64.b64encode(b"\xff\x00").decode())
        self.assertEqual(row["payload_sha256"], hashlib.sha256(b"\xff\x00").hexdigest())
        self.assertIsNone(row["payload_text"])
        self.assertEqual(
            json.loads(str(row["headers_json"])),
            [
                {"key": "trace", "value_base64": base64.b64encode(b"one").decode()},
                {"key": "trace", "value_base64": None},
                {"key": "binary", "value_base64": base64.b64encode(b"\xff").decode()},
            ],
        )

    def test_tombstone_and_strict_utf8_view_remain_distinct(self) -> None:
        tombstone = self._record(value=None, headers=[])
        text = self._record(offset=2, value="xin chào".encode(), headers=[])

        for field in ("payload_base64", "payload_text", "payload_sha256"):
            self.assertIsNone(tombstone[field])
        self.assertEqual(text["payload_text"], "xin chào")
        self.assertEqual(text["kafka_timestamp_type"], "CREATE_TIME")

    def test_timestamp_type_and_position_validation_fail_closed(self) -> None:
        self.assertEqual(
            self._record(offset=2, timestamp_type=1)["kafka_timestamp_type"],
            "LOG_APPEND_TIME",
        )
        self.assertEqual(
            self._record(offset=3, timestamp_type=-1)["kafka_timestamp_type"],
            "NO_TIMESTAMP_TYPE",
        )
        with self.assertRaisesRegex(ValueError, "timestamp type"):
            self._record(timestamp_type=9)
        with self.assertRaisesRegex(ValueError, "allowed"):
            self._record(topic="unknown.topic")
        with self.assertRaisesRegex(ValueError, "non-negative"):
            self._record(offset=-1)

    def test_insert_only_classification_detects_position_collision(self) -> None:
        first = self._record()
        repeat = self._record(run_id="retry", ingested_at=datetime(2026, 1, 3, tzinfo=UTC))
        duplicate_event_new_position = self._record(offset=2)

        plan = classify_kafka_records(
            [first, repeat, duplicate_event_new_position], existing_records=[]
        )
        self.assertEqual([row["source_offset"] for row in plan.insert_records], [1, 2])
        self.assertEqual(plan.noop_count, 1)

        collision = dict(first)
        collision["payload_base64"] = base64.b64encode(b"changed").decode()
        collision["payload_sha256"] = hashlib.sha256(b"changed").hexdigest()
        with self.assertRaisesRegex(ValueError, "KAFKA_POSITION_COLLISION"):
            classify_kafka_records([collision], existing_records=[first])

        against_existing = classify_kafka_records([repeat], existing_records=[first])
        self.assertEqual(against_existing.insert_records, ())
        self.assertEqual(against_existing.noop_count, 1)

    def test_offset_manifest_round_trip_and_hash_are_canonical(self) -> None:
        ranges = (
            KafkaOffsetRange("marketing.raw.web-events.v1", 0, 2, 5),
            KafkaOffsetRange("marketing.raw.order-events.v1", 0, 0, 1),
        )
        manifest = KafkaOffsetManifest(
            cluster_id="cluster-1",
            captured_at=self.timestamp,
            allowed_topics=tuple(sorted({item.topic for item in ranges})),
            ranges=ranges,
        )

        restored = KafkaOffsetManifest.from_json(manifest.to_json())
        self.assertEqual(restored, manifest)
        self.assertEqual(restored.sha256, hashlib.sha256(manifest.to_json().encode()).hexdigest())
        parsed = json.loads(manifest.to_json())
        self.assertEqual(parsed["version"], 1)
        by_topic = {item["topic"]: item for item in parsed["ranges"]}
        self.assertEqual(by_topic["marketing.raw.web-events.v1"]["end_offset"], 5)
        with self.assertRaisesRegex(ValueError, "invalid offset manifest"):
            KafkaOffsetManifest.from_json("[]")

    def test_capture_uses_broker_earliest_then_stored_max_plus_one(self) -> None:
        class Result:
            def collect(self) -> list[dict[str, object]]:
                return [
                    {
                        "source_topic": "marketing.raw.web-events.v1",
                        "source_partition": 0,
                        "max_offset": 3,
                    }
                ]

        class Spark:
            def sql(self, _query: str) -> Result:
                return Result()

        bounds = {
            "marketing.raw.web-events.v1": {0: (2, 8), 1: (5, 9)},
        }
        with patch(
            "marketpulse_lakehouse.bronze.kafka_broker_bounds",
            return_value=("cluster-1", bounds),
        ):
            manifest = capture_offset_manifest(
                Spark(),
                bootstrap_servers="kafka:29092",
                captured_at=self.timestamp,
            )

        self.assertEqual(
            manifest.ranges,
            (
                KafkaOffsetRange("marketing.raw.web-events.v1", 0, 4, 8),
                KafkaOffsetRange("marketing.raw.web-events.v1", 1, 5, 9),
            ),
        )
        options = spark_kafka_options(manifest, bootstrap_servers="kafka:29092")
        self.assertEqual(options["includeHeaders"], "true")
        self.assertEqual(options["failOnDataLoss"], "true")
        self.assertEqual(options["kafka.isolation.level"], "read_committed")
        self.assertEqual(
            json.loads(options["startingOffsets"]),
            {"marketing.raw.web-events.v1": {"0": 4, "1": 5}},
        )

    def test_manifest_validation_rejects_partition_and_retention_drift(self) -> None:
        manifest = KafkaOffsetManifest(
            cluster_id="cluster-1",
            captured_at=self.timestamp,
            allowed_topics=("marketing.raw.web-events.v1",),
            ranges=(KafkaOffsetRange("marketing.raw.web-events.v1", 0, 2, 5),),
        )

        validate_offset_manifest(
            manifest,
            broker_bounds={"marketing.raw.web-events.v1": {0: (1, 7)}},
        )
        with self.assertRaisesRegex(ValueError, "partition drift"):
            validate_offset_manifest(
                manifest,
                broker_bounds={"marketing.raw.web-events.v1": {0: (1, 7), 1: (0, 0)}},
            )
        with self.assertRaisesRegex(ValueError, "retention gap"):
            validate_offset_manifest(
                manifest,
                broker_bounds={"marketing.raw.web-events.v1": {0: (3, 7)}},
            )
        with self.assertRaisesRegex(ValueError, "beyond broker end"):
            validate_offset_manifest(
                manifest,
                broker_bounds={"marketing.raw.web-events.v1": {0: (1, 4)}},
            )

    def test_offset_gaps_are_reported_without_inventing_missing_records(self) -> None:
        manifest = KafkaOffsetManifest(
            cluster_id="cluster-1",
            captured_at=self.timestamp,
            allowed_topics=("marketing.raw.web-events.v1",),
            ranges=(KafkaOffsetRange("marketing.raw.web-events.v1", 0, 0, 3),),
        )
        rows = [self._record(offset=0), self._record(offset=2)]

        summary = offset_observation_summary(manifest, rows)

        self.assertEqual(
            summary,
            ({
                "source_topic": "marketing.raw.web-events.v1",
                "source_partition": 0,
                "start_offset": 0,
                "end_offset": 3,
                "requested_positions": 3,
                "observed_records": 2,
                "observed_min_offset": 0,
                "observed_max_offset": 2,
                "offset_gap_count": 1,
                "offset_gaps_sample": (1,),
            },),
        )

    def test_bootstrap_generates_all_namespaces_and_thirteen_partitioned_v2_tables(self) -> None:
        statements = bootstrap_statements("lakehouse")

        self.assertEqual(
            statements[:3],
            (
                "CREATE NAMESPACE IF NOT EXISTS `lakehouse`.`bronze`",
                "CREATE NAMESPACE IF NOT EXISTS `lakehouse`.`gold`",
                "CREATE NAMESPACE IF NOT EXISTS `lakehouse`.`silver`",
            ),
        )
        self.assertEqual(len(statements), 16)
        kafka_ddl = next(
            statement
            for statement in statements
            if "`bronze`.`kafka_records`" in statement
        )
        self.assertIn("`ingest_id` STRING NOT NULL", kafka_ddl)
        self.assertIn("`ingested_at` TIMESTAMP NOT NULL", kafka_ddl)
        self.assertIn("PARTITIONED BY (days(`kafka_timestamp`))", kafka_ddl)
        self.assertIn("'format-version'='2'", kafka_ddl)

        class RecordingSpark:
            def __init__(self) -> None:
                self.statements: list[str] = []

            def sql(self, statement: str) -> None:
                self.statements.append(statement)

        spark = RecordingSpark()
        executed = bootstrap_lakehouse(spark, "lakehouse")
        self.assertEqual(executed, statements)
        self.assertEqual(tuple(spark.statements), statements)

        with self.assertRaisesRegex(ValueError, "catalog"):
            bootstrap_statements("bad.name")


if __name__ == "__main__":
    unittest.main()
