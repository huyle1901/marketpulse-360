from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from marketpulse_batch.certification import validate_dbt_artifacts
from marketpulse_lakehouse.contracts import TABLE_CONTRACTS


class DbtArtifactTests(unittest.TestCase):
    def artifacts(self, root: Path) -> None:
        names = ("funnel_daily", "attribution_daily", "customer_360", "cohort_retention",
                 "campaign_performance_daily", "campaigns", "customers", "campaign_dashboard")
        nodes = {f"model.marketpulse_serving.{name}": {"resource_type": "model", "name": name,
                 "schema": "mp_release_" + "a" * 64 + "_1"} for name in names}
        gates = ("financial_equations", "attribution_conservation", "cohort_invariants", "funnel_invariants",
                 "consent_eligibility", "campaign_relationships", "customer_relationships", "accepted_domains")
        nodes.update({f"test.marketpulse_serving.{name}": {"resource_type": "test", "name": name} for name in gates})
        for logical_name, contract in TABLE_CONTRACTS.items():
            model = logical_name.split(".")[1]
            if model not in names:
                continue
            for generic, columns in (("unique_combination", list(contract.key_fields)),
                                     ("required_fields", [c.name for c in contract.columns if not c.nullable and not c.operational])):
                uid = f"test.marketpulse_serving.{generic}_{model}"
                nodes[uid] = {"resource_type": "test", "name": f"{generic}_{model}",
                              "test_metadata": {"name": generic, "kwargs": {"columns": columns}},
                              "depends_on": {"nodes": [f"model.marketpulse_serving.{model}"]}}
        nodes["test.marketpulse_serving.unique_dashboard"] = {
            "resource_type": "test", "name": "unique_dashboard",
            "test_metadata": {"name": "unique_combination", "kwargs": {"columns": ["event_date", "campaign_id"]}},
            "depends_on": {"nodes": ["model.marketpulse_serving.campaign_dashboard"]}}
        metadata = {"invocation_id": "00000000-0000-0000-0000-000000000001"}
        (root / "manifest.json").write_text(json.dumps({"metadata": metadata, "nodes": nodes}))
        results = [{"unique_id": uid, "status": "success" if node["resource_type"] == "model" else "pass"}
                   for uid, node in nodes.items()]
        (root / "run_results.json").write_text(json.dumps({"metadata": metadata, "results": results}))

    def test_missing_dbt_result_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)
            self.artifacts(path)
            results = json.loads((path / "run_results.json").read_text())
            results["results"].pop()
            (path / "run_results.json").write_text(json.dumps(results))
            with self.assertRaises(ValueError):
                validate_dbt_artifacts(path, "mp_release_" + "a" * 64 + "_1")

    def test_failed_relationship_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)
            self.artifacts(path)
            results = json.loads((path / "run_results.json").read_text())
            next(r for r in results["results"] if "campaign_relationships" in r["unique_id"])["status"] = "fail"
            (path / "run_results.json").write_text(json.dumps(results))
            with self.assertRaisesRegex(ValueError, "dbt"):
                validate_dbt_artifacts(path, "mp_release_" + "a" * 64 + "_1")

    def test_complete_successful_artifacts_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)
            self.artifacts(path)
            self.assertEqual(validate_dbt_artifacts(path, "mp_release_" + "a" * 64 + "_1"),
                             "00000000-0000-0000-0000-000000000001")

    def test_different_candidate_or_invocation_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)
            self.artifacts(path)
            with self.assertRaises(ValueError):
                validate_dbt_artifacts(path, "mp_release_" + "b" * 64 + "_1")
            results = json.loads((path / "run_results.json").read_text())
            results["metadata"]["invocation_id"] = "different"
            (path / "run_results.json").write_text(json.dumps(results))
            with self.assertRaises(ValueError):
                validate_dbt_artifacts(path, "mp_release_" + "a" * 64 + "_1")

    def test_missing_unique_test_gate_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)
            self.artifacts(path)
            payload = json.loads((path / "manifest.json").read_text())
            payload["nodes"].pop("test.marketpulse_serving.unique_combination_customers")
            (path / "manifest.json").write_text(json.dumps(payload))
            with self.assertRaises(ValueError):
                validate_dbt_artifacts(path, "mp_release_" + "a" * 64 + "_1")
