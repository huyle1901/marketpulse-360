from __future__ import annotations

import base64
import copy
import hashlib
import json
import unittest
from datetime import UTC, datetime

from marketpulse_lakehouse.bronze import (
    ALLOWED_KAFKA_TOPICS,
    KafkaOffsetManifest,
    KafkaOffsetRange,
)
from marketpulse_lakehouse.evidence import checksum_sorted_encoded_rows
from marketpulse_lakehouse.verify import (
    EVIDENCE_TABLES,
    compare_manifest_tables,
    evidence_table_from_mapping,
    rebuild_namespace,
    validate_evidence_manifest,
    validate_physical_schema,
    parse_input_provenance,
    validate_provenance_observations,
    validate_offset_manifest_hashes,
    validate_recomputed_blocks,
    validate_snapshot_ancestry,
)


def _table(name: str, snapshot: int, checksum: str = "abc") -> dict[str, object]:
    return {
        "table": name,
        "physical_table": name,
        "snapshot_id": snapshot,
        "later_snapshot_id": snapshot + 100,
        "schema_fingerprint": "schema",
        "row_count": 1,
        "checksum_algorithm": "canonical-json-rowset-v1",
        "business_columns": ["id"],
        "checksum": checksum,
        "reconciliations": (
            {"gross_amount_minor": 8, "refund_amount_minor": 2}
            if name == "silver.customer_events" else {"rows": 1}
        ),
    }


def _blob(value: bytes) -> dict[str, object]:
    return {
        "base64": base64.b64encode(value).decode("ascii"),
        "sha256": hashlib.sha256(value).hexdigest(),
        "size_bytes": len(value),
    }


def _manifest() -> dict[str, object]:
    silver = EVIDENCE_TABLES[:5]
    gold = EVIDENCE_TABLES[5:]
    baseline = {name: index + 100 for index, name in enumerate(EVIDENCE_TABLES)}
    later = {name: value + 100 for name, value in baseline.items()}
    return {
        "schema_version": 1,
        "checksum_algorithm": "canonical-json-rowset-v1",
        "run_id": "proof-1",
        "inputs": {
            "bronze_snapshots": {
                "bronze.kafka_records": 11,
                "bronze.source_files": 13,
                "bronze.source_file_rows": 12,
            },
            "silver_snapshots": {name: baseline[name] for name in silver},
            "gold_snapshots": {name: baseline[name] for name in gold},
            "later_silver_snapshots": {name: later[name] for name in silver},
            "later_gold_snapshots": {name: later[name] for name in gold},
            "offset_manifest": _blob(
                KafkaOffsetManifest(
                    cluster_id="fixture-cluster",
                    captured_at=datetime(2026, 1, 1, tzinfo=UTC),
                    allowed_topics=tuple(sorted(ALLOWED_KAFKA_TOPICS)),
                    ranges=tuple(
                        KafkaOffsetRange(topic, 0, 0, 0)
                        for topic in sorted(ALLOWED_KAFKA_TOPICS)
                    ),
                ).to_json().encode()
            ),
            "file_manifest": _blob(b'{"files":[]}'),
        },
        "tables": {
            name: _table(name, baseline[name]) for name in EVIDENCE_TABLES
        },
        "quality": {"accepted": 10, "dlq": 2},
        "revenue": {
            "source": {"gross": 10, "refund": 2, "net": 8},
            "governed": {"gross": 8, "refund": 2, "net": 6},
            "attribution": {
                model: {"gross": 8, "refund": 2, "net": 6}
                for model in ("first_touch", "last_touch", "linear")
            },
        },
    }


class _Type:
    def __init__(self, value: str) -> None:
        self.value = value

    def simpleString(self) -> str:
        return self.value


class _Field:
    def __init__(self, name: str, kind: str, nullable: bool) -> None:
        self.name = name
        self.dataType = _Type(kind)
        self.nullable = nullable


class _Schema:
    def __init__(self, fields: list[_Field]) -> None:
        self.fields = fields


class LakehouseVerifyTests(unittest.TestCase):
    def test_streaming_checksum_requires_sorted_rows(self) -> None:
        self.assertEqual(
            checksum_sorted_encoded_rows([b'["a"]', b'["b"]']),
            "0c6776f2ab802ce3a644cc4743b27a813ccd9fd749173bef9aeee72dca3f814f",
        )
        with self.assertRaisesRegex(ValueError, "not sorted"):
            checksum_sorted_encoded_rows([b"b", b"a"])

    def test_manifest_requires_complete_snapshot_maps_and_provenance(self) -> None:
        manifest = _manifest()
        validate_evidence_manifest(manifest)

        broken = copy.deepcopy(manifest)
        broken["tables"].pop("gold.funnel_daily")  # type: ignore[union-attr]
        with self.assertRaisesRegex(ValueError, "table coverage"):
            validate_evidence_manifest(broken)

        broken = copy.deepcopy(manifest)
        broken["inputs"]["later_gold_snapshots"]["gold.funnel_daily"] += 1  # type: ignore[index]
        with self.assertRaisesRegex(ValueError, "snapshot map mismatch"):
            validate_evidence_manifest(broken)

        broken = copy.deepcopy(manifest)
        broken["inputs"]["offset_manifest"]["sha256"] = "0" * 64  # type: ignore[index]
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            validate_evidence_manifest(broken)

        broken = copy.deepcopy(manifest)
        broken["inputs"]["offset_manifest"] = _blob(b"not-json")  # type: ignore[index]
        with self.assertRaisesRegex(ValueError, "malformed"):
            validate_evidence_manifest(broken)

    def test_self_consistent_provenance_mutation_must_still_match_bronze_rows(self) -> None:
        manifest = _manifest()
        inputs = manifest["inputs"]
        assert isinstance(inputs, dict)
        offsets, files, file_bytes = parse_input_provenance(inputs)
        validate_provenance_observations(
            offset_manifest=offsets,
            expected_files=files,
            kafka_rows=(),
            source_rows=(),
            source_files=(),
            file_manifest_sha256=hashlib.sha256(file_bytes).hexdigest(),
        )
        changed = KafkaOffsetManifest(
            cluster_id=offsets.cluster_id,
            captured_at=offsets.captured_at,
            allowed_topics=offsets.allowed_topics,
            ranges=tuple(
                KafkaOffsetRange(item.topic, item.partition, 0, 1)
                if index == 0 else item
                for index, item in enumerate(offsets.ranges)
            ),
        )
        mutated = copy.deepcopy(inputs)
        mutated["offset_manifest"] = _blob(changed.to_json().encode())
        parsed_changed, parsed_files, parsed_file_bytes = parse_input_provenance(mutated)
        validate_offset_manifest_hashes(offsets, (offsets.sha256,))
        with self.assertRaisesRegex(ValueError, "hash does not match Bronze"):
            validate_offset_manifest_hashes(parsed_changed, (offsets.sha256,))
        mutated_files = copy.deepcopy(inputs)
        changed_file_bytes = json.dumps(
            {
                "files": [
                    {
                        "dataset": "crm",
                        "relative_path": "crm/date=2026-01-01/part.csv.gz",
                        "sha256": "a" * 64,
                        "row_count": 1,
                    }
                ]
            }
        ).encode()
        mutated_files["file_manifest"] = _blob(changed_file_bytes)
        parsed_offsets, parsed_files, parsed_file_bytes = parse_input_provenance(
            mutated_files
        )
        with self.assertRaisesRegex(ValueError, "file manifest rows"):
            validate_provenance_observations(
                offset_manifest=parsed_offsets,
                expected_files=parsed_files,
                kafka_rows=(),
                source_rows=(),
                source_files=(),
                file_manifest_sha256=hashlib.sha256(parsed_file_bytes).hexdigest(),
            )

    def test_recomputed_quality_and_revenue_must_match_exactly(self) -> None:
        manifest = _manifest()
        self.assertEqual(
            validate_recomputed_blocks(
                manifest,
                quality=manifest["quality"],  # type: ignore[arg-type]
                revenue=manifest["revenue"],  # type: ignore[arg-type]
            ),
            (),
        )
        self.assertIn(
            "quality evidence differs",
            validate_recomputed_blocks(
                manifest,
                quality={"accepted": 9, "dlq": 2},
                revenue=manifest["revenue"],  # type: ignore[arg-type]
            )[0],
        )
        mutated = copy.deepcopy(manifest["revenue"])
        mutated["source"] = {"gross": 12, "refund": 2, "net": 10}  # type: ignore[index]
        self.assertIn(
            "revenue evidence differs",
            validate_recomputed_blocks(
                manifest,
                quality=manifest["quality"],  # type: ignore[arg-type]
                revenue=mutated,  # type: ignore[arg-type]
            )[0],
        )

    def test_silver_financial_reconciliation_cannot_count_refund_gross_context(self) -> None:
        manifest = _manifest()
        manifest["tables"]["silver.customer_events"]["reconciliations"]["gross_amount_minor"] = 10  # type: ignore[index]
        with self.assertRaisesRegex(ValueError, "Silver financial reconciliation"):
            validate_evidence_manifest(manifest)

    def test_snapshot_proof_uses_parent_ancestry_not_timestamp_adjacency(self) -> None:
        validate_snapshot_ancestry(
            {10: None, 20: 10, 30: 20, 25: 10}, baseline=10, later=30, head=30
        )
        with self.assertRaisesRegex(ValueError, "not an ancestor"):
            validate_snapshot_ancestry(
                {10: None, 20: 10, 25: 10}, baseline=20, later=25, head=25
            )
        with self.assertRaisesRegex(ValueError, "current table head"):
            validate_snapshot_ancestry(
                {10: None, 20: 10, 30: 20}, baseline=10, later=20, head=30
            )

    def test_physical_schema_rejects_wrong_type_and_nullability(self) -> None:
        valid = _Schema([
            _Field("event_date", "date", False),
            _Field("campaign_id", "string", False),
            _Field("sessions", "long", False),
            _Field("page_view_sessions", "long", False),
            _Field("view_item_sessions", "long", False),
            _Field("add_to_cart_sessions", "long", False),
            _Field("checkout_sessions", "long", False),
            _Field("purchase_sessions", "long", False),
        ])
        validate_physical_schema(valid, "gold.funnel_daily")
        wrong_type = copy.deepcopy(valid)
        wrong_type.fields[2].dataType = _Type("int")
        with self.assertRaisesRegex(ValueError, "physical schema mismatch"):
            validate_physical_schema(wrong_type, "gold.funnel_daily")
        wrong_nullability = copy.deepcopy(valid)
        wrong_nullability.fields[0].nullable = True
        with self.assertRaisesRegex(ValueError, "physical schema mismatch"):
            validate_physical_schema(wrong_nullability, "gold.funnel_daily")

    def test_table_comparison_ignores_physical_snapshot_identity(self) -> None:
        expected = _table("gold.funnel_daily", 10)
        rebuilt = {
            **expected,
            "physical_table": "rebuild_proof.funnel_daily",
            "snapshot_id": 99,
            "later_snapshot_id": 100,
        }
        self.assertEqual(evidence_table_from_mapping(expected).snapshot_id, 10)
        self.assertEqual(compare_manifest_tables({"x": expected}, {"x": rebuilt}), ())
        mismatched = {"x": {**rebuilt, "checksum": "changed"}}
        self.assertIn(
            "checksum expected abc, actual changed",
            compare_manifest_tables({"x": expected}, mismatched)[0],
        )

    def test_rebuild_namespace_is_safe_deterministic_and_identity_scoped(self) -> None:
        value = rebuild_namespace("Run 2026/10/12", "proof-a")
        self.assertEqual(value, rebuild_namespace("Run 2026/10/12", "proof-a"))
        self.assertNotEqual(value, rebuild_namespace("Run 2026/10/12", "proof-b"))
        self.assertTrue(value.startswith("rebuild_run_2026_10_12_"))
        with self.assertRaisesRegex(ValueError, "run_id"):
            rebuild_namespace("---")


if __name__ == "__main__":
    unittest.main()
