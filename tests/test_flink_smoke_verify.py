from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from marketpulse_mock.flink_smoke_verify import (
    derive_fixture_oracle,
    latest_by_key,
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
        clean = [
            (
                event_id,
                {
                    "event_id": event_id,
                    "late_status": "source_marked_late" if row["is_late"] else "on_time",
                    "eligible_for_aggregates": True,
                },
            )
            for event_id, row in oracle.unique_events.items()
        ]
        clean.append(
            (
                "00000000-0000-5000-8000-000000000099",
                {
                    "event_id": "00000000-0000-5000-8000-000000000099",
                    "late_status": "watermark_late",
                    "eligible_for_aggregates": False,
                },
            )
        )
        self._write_rows(root, "marketing.clean.customer-events.v1", clean)
        self._write_rows(
            root,
            "marketing.clean.sessions.v1",
            [(key, {"session_id": key, **value}) for key, value in oracle.session_metrics.items()],
        )
        for topic in (
            "marketing.gold.funnel-metrics.v1",
            "marketing.gold.revenue-metrics.v1",
            "marketing.gold.campaign-metrics.v1",
        ):
            self._write_rows(
                root,
                topic,
                [(key, {"metric_key": key, **value}) for key, value in oracle.campaign_metrics.items()],
            )
        self._write_rows(
            root,
            "marketing.dlq.flink-validation.v1",
            [("error-1", {"error_id": "error-1", "reason_code": "MALFORMED_JSON"})],
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
