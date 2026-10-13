"""Independent, read-only fixture oracle for the MP-04 Docker smoke proof."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

from marketpulse_lakehouse.identity import kafka_ingest_id
from marketpulse_lakehouse.session import create_spark_session
from marketpulse_lakehouse.source_files import build_source_archive_plan, create_s3_client
from marketpulse_lakehouse.verify import load_evidence, parse_input_provenance
from marketpulse_mock.replay_reader import iter_replay_records


def verify_click_conflict(spark) -> None:
    """Exercise raw-lineage conflict detection and total-order selection in Spark."""
    from marketpulse_lakehouse.silver import _build_customer_event_frames

    event_id = "00000000-0000-5000-8000-000000000001"
    value = {
        "schema_version": 1, "event_id": event_id, "event_name": "page_view",
        "event_time": "2026-01-02T03:04:05Z", "ingested_at": "2026-01-02T03:04:06Z",
        "processed_at": "2026-01-02T03:04:07Z", "source": "web", "channel": "web",
        "customer_id": None, "anonymous_id": "anon-1", "identity_key": "anon-1",
        "session_id": "session-1", "campaign_id": "campaign-1", "campaign_key": "campaign-1",
        "consent_analytics": True, "late_status": "on_time", "eligible_for_aggregates": True,
        "payload": {"is_bot": False},
    }
    rows = []
    # Reverse input order: the lower lineage offset, not collection order, must win.
    for offset, click_id in ((2, None), (1, "paid-click-1")):
        clean = {**value, "lineage": {
            "topic": "marketing.raw.web-events.v1", "partition": 0, "offset": offset,
            "record_timestamp": 1767323045000, "record_key": "anon-1",
        }}
        raw = {"event_id": event_id, "click_id": click_id}
        for topic, position, payload in (
            ("marketing.raw.web-events.v1", offset, raw),
            ("marketing.clean.customer-events.v1", offset + 10, clean),
        ):
            text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            rows.append((topic, 0, position, text, hashlib.sha256(text.encode()).hexdigest(),
                         datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)))
    frame = spark.createDataFrame(rows, (
        "source_topic string, source_partition int, source_offset long, payload_text string, "
        "payload_sha256 string, kafka_timestamp timestamp"
    ))
    accepted, quality = _build_customer_event_frames(spark, frame, "click-conflict-regression")
    try:
        winners = accepted.collect()
        counts = {row["reason_code"]: row["record_count"] for row in quality.collect()}
        assert len(winners) == 1 and winners[0]["click_id"] == "paid-click-1", winners
        assert winners[0]["lineage_offset"] == 1, winners
        assert counts == {"ACCEPTED": 1, "DUPLICATE_EVENT": 1,
                          "EVENT_ID_PAYLOAD_CONFLICT": 1}, counts
    finally:
        accepted.unpersist()
        quality.unpersist()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    manifest = load_evidence(args.evidence)
    silver_reconciliation = manifest["tables"]["silver.customer_events"]["reconciliations"]
    governed = manifest["revenue"]["governed"]
    if (
        silver_reconciliation["gross_amount_minor"] != governed["gross"]
        or silver_reconciliation["refund_amount_minor"] != governed["refund"]
    ):
        raise ValueError("Silver financial reconciliation counts refund gross context as a sale")
    samples = {}
    for record in iter_replay_records(args.input):
        samples.setdefault(record.topic, record)
        if len(samples) == 3:
            break
    if len(samples) != 3:
        raise ValueError("fixture must supply samples for all three raw topics")
    spark = create_spark_session("marketpulse-lakehouse-fixture-oracle")
    spark.sparkContext.setLogLevel("WARN")
    try:
        from pyspark.sql import functions as F

        verify_click_conflict(spark)
        kafka_snapshot = manifest["inputs"]["bronze_snapshots"]["bronze.kafka_records"]
        bronze = spark.sql(
            "SELECT * FROM "
            f"lakehouse.bronze.kafka_records VERSION AS OF {kafka_snapshot}"
        )
        offset_manifest, _, _ = parse_input_provenance(manifest["inputs"])
        from confluent_kafka import Consumer, TopicPartition

        consumer = Consumer({
            "bootstrap.servers": "kafka:29092",
            "group.id": f"mp04-byte-oracle-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        })

        def verify_broker_row(row) -> None:
            topic, partition, offset = (
                str(row["source_topic"]), int(row["source_partition"]), int(row["source_offset"])
            )
            consumer.assign([TopicPartition(topic, partition, offset)])
            message = consumer.poll(15)
            if message is None or message.error() or message.offset() != offset:
                raise ValueError(f"cannot independently read Kafka position {topic}:{partition}:{offset}")
            key, value = message.key(), message.value()
            headers = json.dumps([
                {"key": name, "value_base64": (
                    base64.b64encode(content).decode("ascii") if content is not None else None
                )} for name, content in (message.headers() or [])
            ], ensure_ascii=False, separators=(",", ":"))
            timestamp_type, timestamp_ms = message.timestamp()
            expected = {
                "ingest_id": kafka_ingest_id(topic, partition, offset),
                "key_base64": base64.b64encode(key).decode("ascii"),
                "key_sha256": hashlib.sha256(key).hexdigest(),
                "payload_base64": base64.b64encode(value).decode("ascii"),
                "payload_text": value.decode("utf-8"),
                "payload_sha256": hashlib.sha256(value).hexdigest(),
                "headers_json": headers,
                "kafka_timestamp_type": {0: "NO_TIMESTAMP_TYPE", 1: "CREATE_TIME", 2: "LOG_APPEND_TIME"}[timestamp_type],
                "ingestion_run_id": manifest["run_id"],
                "ingestion_manifest_sha256": offset_manifest.sha256,
            }
            for field, expected_value in expected.items():
                if row[field] != expected_value:
                    raise ValueError(f"Kafka Bronze audit mismatch at {topic}:{partition}:{offset}: {field}")
            observed_ms = round(row["kafka_timestamp"].replace(tzinfo=UTC).timestamp() * 1000)
            if observed_ms != timestamp_ms or row["ingested_at"] is None:
                raise ValueError("Kafka Bronze timestamp/ingested_at mismatch")

        for topic, sample in samples.items():
            expected_key = base64.b64encode(sample.key.encode("utf-8")).decode("ascii")
            expected_value = base64.b64encode(sample.value).decode("ascii")
            expected_headers = json.dumps([
                {"key": key, "value_base64": base64.b64encode(value).decode("ascii")}
                for key, value in sample.headers
            ], ensure_ascii=False, separators=(",", ":"), sort_keys=True)
            matches = bronze.filter(
                (F.col("source_topic") == topic)
                & (F.col("key_base64") == expected_key)
                & (F.col("payload_base64") == expected_value)
                & (F.col("headers_json") == expected_headers)
            ).orderBy("source_partition", "source_offset").limit(1).collect()
            if not matches:
                raise ValueError(f"Bronze bytes differ from independently replayed fixture: {topic}")
            verify_broker_row(matches[0])
        probe_key = base64.b64encode(b"mp04-header-probe").decode("ascii")
        probes = bronze.filter(F.col("key_base64") == probe_key).collect()
        if len(probes) != 1:
            raise ValueError("duplicate-header audit probe is missing or duplicated")
        verify_broker_row(probes[0])
        probe_headers = json.loads(probes[0]["headers_json"])
        if probe_headers != [
            {"key": "duplicate", "value_base64": "b25l"},
            {"key": "duplicate", "value_base64": None},
            {"key": "binary", "value_base64": "/w=="},
        ]:
            raise ValueError("ordered duplicate/null/binary Kafka headers were not preserved")
        consumer.close()

        archive_plan = build_source_archive_plan(args.input)
        files_snapshot = manifest["inputs"]["bronze_snapshots"]["bronze.source_files"]
        stored_files = spark.sql(
            f"SELECT * FROM lakehouse.bronze.source_files VERSION AS OF {files_snapshot}"
        ).collect()
        planned = {item.file_sha256: item for item in archive_plan.source_files}
        if {row["file_sha256"] for row in stored_files} != set(planned):
            raise ValueError("archived source-file identities differ from fixture")
        s3 = create_s3_client()
        for row in stored_files:
            item = planned[row["file_sha256"]]
            if row["source_object_uri"] != item.source_object_uri:
                raise ValueError("archived source-object URI differs from fixture")
        for item in archive_plan.archive_objects:
            uri = urlparse(item.source_object_uri)
            response = s3.get_object(Bucket=uri.netloc, Key=uri.path.lstrip("/"))
            try:
                body = response["Body"].read()
            finally:
                response["Body"].close()
            if (
                body != item.content
                or len(body) != item.compressed_size_bytes
                or hashlib.sha256(body).hexdigest() != item.file_sha256
            ):
                raise ValueError(f"archived source object differs after restart: {item.source_file}")
        profile_entry = manifest["tables"]["gold.customer_360"]
        attribution_entry = manifest["tables"]["gold.attribution_daily"]
        attribution = spark.sql(
            f"SELECT * FROM lakehouse.{attribution_entry['physical_table']} "
            f"VERSION AS OF {attribution_entry['snapshot_id']}"
        )
        attributed_models = {
            row["model"] for row in attribution.filter(F.col("is_attributed"))
            .select("model").distinct().collect()
        }
        if attributed_models != {"first_touch", "last_touch", "linear"}:
            raise ValueError("fixture paid-click attribution is missing for a marketing model")
        profiles = spark.sql(
            f"SELECT * FROM lakehouse.{profile_entry['physical_table']} "
            f"VERSION AS OF {profile_entry['snapshot_id']}"
        )
        anonymous = profiles.filter(F.col("profile_type") == "anonymous").agg(
            F.sum("order_count").alias("orders"),
            F.sum("gross_revenue_minor").alias("gross"),
        ).collect()[0]
        if anonymous["orders"] != 3 or anonymous["gross"] != 4_046_000:
            raise ValueError(f"anonymous fixture revenue differs: {anonymous}")
        print(json.dumps({
            "verified_raw_byte_samples": len(samples),
            "verified_header_probe": True,
            "verified_click_conflict_regression": True,
            "verified_paid_click_attribution_models": sorted(attributed_models),
            "verified_archive_objects_after_restart": len(archive_plan.archive_objects),
            "anonymous_orders": int(anonymous["orders"]),
            "anonymous_gross_minor": int(anonymous["gross"]),
            "verified": True,
        }, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
