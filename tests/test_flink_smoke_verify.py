from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from marketpulse_mock.flink_smoke_verify import (
    derive_fixture_oracle,
    expected_clean_value,
    latest_by_key,
    resolve_contract_dir,
    stable_value,
    verify,
    write_probe_records,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "data/mock/fixtures/seed-360"


class FlinkSmokeVerifyTests(unittest.TestCase):
    def _write_rows(
        self,
        root: Path,
        topic: str,
        values: list[tuple[str, dict[str, object]]],
    ) -> None:
        rows = [
            {"partition": 0, "offset": offset, "key": key, "value": value}
            for offset, (key, value) in enumerate(values)
        ]
        (root / f"{topic}.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows),
            encoding="utf-8",
        )

    def _write_valid_dump(self, root: Path) -> None:
        oracle = derive_fixture_oracle(FIXTURE)
        source_topic = {
            "web": "marketing.raw.web-events.v1",
            "mobile": "marketing.raw.web-events.v1",
            "commerce": "marketing.raw.order-events.v1",
            "email": "marketing.raw.email-events.v1",
        }
        clean = [
            (
                event_id,
                expected_clean_value(row)
                | {
                    "processed_at": "2026-10-11T00:00:00Z",
                    "lineage": {
                        "topic": source_topic[row["source"]],
                        "partition": 0,
                        "offset": offset,
                        "record_timestamp": None,
                        "record_key": None,
                    },
                },
            )
            for offset, (event_id, row) in enumerate(oracle.unique_events.items())
        ]
        late_source = dict(next(iter(oracle.unique_events.values())))
        late_source.update(
            {
                "event_id": "00000000-0000-5000-8000-000000000099",
                "event_time": "2025-01-01T00:00:00+00:00",
                "ingested_at": "2030-01-01T00:00:02+00:00",
                "session_id": "smoke-watermark-late-session",
                "is_late": False,
            }
        )
        clean.append(
            (
                "00000000-0000-5000-8000-000000000099",
                expected_clean_value(late_source)
                | {
                    "processed_at": "2026-10-11T00:00:00Z",
                    "late_status": "watermark_late",
                    "eligible_for_aggregates": False,
                    "lineage": {
                        "topic": "marketing.raw.web-events.v1",
                        "partition": 0,
                        "offset": 999,
                        "record_timestamp": None,
                        "record_key": None,
                    },
                },
            )
        )
        self._write_rows(root, "marketing.clean.customer-events.v1", clean)
        self._write_rows(
            root,
            "marketing.clean.sessions.v1",
            [
                (
                    key,
                    {
                        "session_id": key,
                        "status": "closed",
                        **value,
                        "processed_at": "2026-10-11T00:00:00Z",
                    },
                )
                for key, value in oracle.session_metrics.items()
            ],
        )
        topic_fields = {
            "marketing.gold.funnel-metrics.v1": (
                "schema_version", "metric_key", "campaign_id", "event_date",
                "sessions", "page_view_sessions", "view_item_sessions",
                "add_to_cart_sessions", "checkout_sessions", "purchase_sessions",
                "max_event_time",
            ),
            "marketing.gold.revenue-metrics.v1": (
                "schema_version", "metric_key", "campaign_id", "event_date",
                "order_count", "refund_count", "gross_revenue_minor",
                "refund_amount_minor", "net_revenue_minor", "currency",
                "max_event_time",
            ),
            "marketing.gold.campaign-metrics.v1": tuple(
                next(iter(oracle.campaign_metrics.values())).keys()
            ),
        }
        for topic, fields in topic_fields.items():
            self._write_rows(
                root,
                topic,
                [
                    (
                        key,
                        {field: value[field] for field in fields}
                        | {"processed_at": "2026-10-11T00:00:00Z"},
                    )
                    for key, value in oracle.campaign_metrics.items()
                ],
            )
        self._write_rows(
            root,
            "marketing.dlq.flink-validation.v1",
            [
                (
                    "cbd621da-a680-5d46-999c-f9c483fb72df",
                    {
                        "schema_version": 1,
                        "error_id": "cbd621da-a680-5d46-999c-f9c483fb72df",
                        "detected_at": "2026-10-11T00:00:00Z",
                        "processor": "marketpulse-flink",
                        "source_topic": "marketing.raw.web-events.v1",
                        "source_partition": 2,
                        "source_offset": 52,
                        "source_key": None,
                        "reason_code": "MALFORMED_JSON",
                        "failed_value_base64": "bm90LWpzb24=",
                        "failed_value_utf8": "not-json",
                    },
                )
            ],
        )

    def test_committed_fixture_has_literal_acceptance_oracles(self) -> None:
        oracle = derive_fixture_oracle(FIXTURE)
        self.assertEqual(219, oracle.valid_rows)
        self.assertEqual(217, len(oracle.unique_events))
        self.assertEqual(2, oracle.duplicate_copies)
        self.assertEqual(3, oracle.source_marked_late)
        self.assertEqual(73, len(oracle.session_ids))
        self.assertEqual(14, len(oracle.campaign_metrics))
        self.assertEqual(12, oracle.order_count)
        self.assertEqual(2, oracle.refund_count)
        self.assertEqual(29_718_000, oracle.gross_revenue_minor)
        self.assertEqual(4_967_000, oracle.refund_amount_minor)
        self.assertEqual(24_751_000, oracle.net_revenue_minor)

    def test_contract_directory_resolves_from_checkout_working_directory(self) -> None:
        self.assertEqual(ROOT / "contracts" / "kafka", resolve_contract_dir())

    def test_latest_by_key_uses_last_partition_order_observation(self) -> None:
        rows = [
            {"partition": 1, "offset": 3, "key": "a", "value": {"n": 1}},
            {"partition": 1, "offset": 4, "key": "a", "value": {"n": 2}},
            {"partition": 0, "offset": 9, "key": "b", "value": {"n": 3}},
        ]
        self.assertEqual(
            {"a": {"n": 2}, "b": {"n": 3}},
            latest_by_key(rows),
        )

    def test_operational_timestamps_are_removed_from_stable_comparison(self) -> None:
        value = {
            "event_id": "e1",
            "processed_at": "now",
            "detected_at": "later",
            "payload": {"ok": True},
        }
        self.assertEqual(
            {"event_id": "e1", "payload": {"ok": True}},
            stable_value(value),
        )

    def test_verifier_accepts_exact_outputs_and_recovery_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_valid_dump(root)
            evidence = root / "recovery.json"
            evidence.write_text(
                json.dumps(
                    {
                        "checkpoint_before_restart": 3,
                        "checkpoint_after_restart": 4,
                        "recovery_seconds": 12.5,
                        "raw_records_before_restart": 40,
                        "raw_records_after_restart": 219,
                        "metrics": {
                            "accepted": 140,
                            "duplicates": 225,
                            "source_marked_late": 2,
                            "watermark_late": 1,
                            "aggregate_ineligible": 1,
                            "invalid": 1,
                        },
                    }
                ),
                encoding="utf-8",
            )
            report = verify(FIXTURE, root, evidence)
        self.assertEqual("PASS", report["status"])
        self.assertEqual(217, report["unique_event_ids"])
        self.assertEqual(1, report["extra_watermark_late"])
        self.assertEqual(4, report["checkpoint_after_restart"])

    def test_verifier_rejects_duplicate_committed_clean_event(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_valid_dump(root)
            path = root / "marketing.clean.customer-events.v1.jsonl"
            first = path.read_text(encoding="utf-8").splitlines()[0]
            row = json.loads(first)
            row["offset"] = 999
            with path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(row) + "\n")
            with self.assertRaisesRegex(ValueError, "must occur once"):
                verify(FIXTURE, root)

    def test_verifier_requires_the_exact_watermark_late_probe(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_valid_dump(root)
            path = root / "marketing.clean.customer-events.v1.jsonl"
            rows = [
                line
                for line in path.read_text(encoding="utf-8").splitlines()
                if "00000000-0000-5000-8000-000000000099" not in line
            ]
            path.write_text("\n".join(rows) + "\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "watermark-late probe"):
                verify(FIXTURE, root)

    def test_verifier_rejects_revenue_moved_between_campaigns(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_valid_dump(root)
            revenue_fields = (
                "order_count",
                "refund_count",
                "gross_revenue_minor",
                "refund_amount_minor",
                "net_revenue_minor",
            )
            for topic in (
                "marketing.gold.revenue-metrics.v1",
                "marketing.gold.campaign-metrics.v1",
            ):
                path = root / f"{topic}.jsonl"
                rows = [json.loads(line) for line in path.read_text().splitlines()]
                populated = [
                    index
                    for index, row in enumerate(rows)
                    if row["value"]["gross_revenue_minor"] > 0
                ]
                left, right = populated[:2]
                for field in revenue_fields:
                    rows[left]["value"][field], rows[right]["value"][field] = (
                        rows[right]["value"][field],
                        rows[left]["value"][field],
                    )
                path.write_text(
                    "".join(json.dumps(row) + "\n" for row in rows),
                    encoding="utf-8",
                )

            with self.assertRaisesRegex(ValueError, "revenue mismatch"):
                verify(FIXTURE, root)

    def test_verifier_checks_session_and_dlq_contract_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_valid_dump(root)
            session_path = root / "marketing.clean.sessions.v1.jsonl"
            sessions = [json.loads(line) for line in session_path.read_text().splitlines()]
            sessions[0]["value"].pop("status")
            session_path.write_text(
                "".join(json.dumps(row) + "\n" for row in sessions),
                encoding="utf-8",
            )
            dlq_path = root / "marketing.dlq.flink-validation.v1.jsonl"
            dlq = json.loads(dlq_path.read_text())
            dlq["value"]["source_topic"] = "wrong.topic"
            dlq_path.write_text(json.dumps(dlq) + "\n", encoding="utf-8")

            with self.assertRaisesRegex(
                ValueError, "output schema mismatch|session mismatch|DLQ mismatch"
            ):
                verify(FIXTURE, root)

    def test_verifier_rejects_outputs_that_do_not_match_registered_schemas(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_valid_dump(root)
            path = root / "marketing.clean.customer-events.v1.jsonl"
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            rows[0]["value"].pop("processed_at")
            path.write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "output schema mismatch"):
                verify(FIXTURE, root)

    def test_verifier_compares_session_payload_id_with_kafka_key(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_valid_dump(root)
            path = root / "marketing.clean.sessions.v1.jsonl"
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            rows[0]["value"]["session_id"] = "wrong-session"
            path.write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "session mismatch"):
                verify(FIXTURE, root)

    def test_probe_writer_reuses_one_event_for_watermark_and_makes_late_id_unique(self) -> None:
        oracle = derive_fixture_oracle(FIXTURE)
        with tempfile.TemporaryDirectory() as temporary:
            probes = write_probe_records(FIXTURE, Path(temporary))
            frontier = json.loads(probes["frontier"].read_text(encoding="utf-8"))
            late = json.loads(probes["late"].read_text(encoding="utf-8"))
            malformed = probes["malformed"].read_text(encoding="utf-8")
        self.assertIn(frontier["event_id"], oracle.unique_events)
        self.assertEqual("2030-01-01T00:00:00+00:00", frontier["event_time"])
        self.assertNotIn(late["event_id"], oracle.unique_events)
        self.assertEqual("2025-01-01T00:00:00+00:00", late["event_time"])
        self.assertEqual("not-json\n", malformed)


if __name__ == "__main__":
    unittest.main()
