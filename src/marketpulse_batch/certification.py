"""Fail-closed dbt artifact and complete serving-content certification."""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, localcontext
from pathlib import Path
from typing import Any

from marketpulse_lakehouse.contracts import TABLE_CONTRACTS
from .canonical import clickhouse_type, rowset_checksum
from .contracts import PreparationReceipt, validate_digest
from .export import EXPORTED_RELATIONS, ExportManifest, business_schema

SERVING_MODELS = tuple(name.split(".")[1] for name in EXPORTED_RELATIONS) + ("campaign_dashboard",)
SEMANTIC_GATES = {"financial_equations", "attribution_conservation", "cohort_invariants", "funnel_invariants",
                  "consent_eligibility", "campaign_relationships", "customer_relationships", "accepted_domains"}


def validate_dbt_artifacts(path: Path, database: str) -> str:
    try:
        manifest = json.loads((path / "manifest.json").read_bytes())
        results = json.loads((path / "run_results.json").read_bytes())
        invocation = results["metadata"]["invocation_id"]
        uuid.UUID(invocation)
        if manifest["metadata"]["invocation_id"] != invocation:
            raise ValueError("dbt artifact invocation mismatch")
        nodes = manifest["nodes"]
        model_nodes = {uid: node for uid, node in nodes.items() if node["resource_type"] == "model"}
        if set(node["name"] for node in model_nodes.values()) != set(SERVING_MODELS):
            raise ValueError("dbt model set is incomplete")
        if any(node.get("schema") != database for node in model_nodes.values()):
            raise ValueError("dbt artifacts belong to another candidate")
        tests = {uid: node for uid, node in nodes.items() if node["resource_type"] == "test"}
        if not SEMANTIC_GATES <= {node["name"] for node in tests.values()}:
            raise ValueError("dbt semantic test gates missing")
        for uid, node in model_nodes.items():
            model = node["name"]
            logical_name = next((name for name in EXPORTED_RELATIONS if name.split(".")[1] == model), None)
            requirements = {"unique_combination": list(TABLE_CONTRACTS[logical_name].key_fields)
                            if logical_name else ["event_date", "campaign_id"]}
            if logical_name:
                requirements["required_fields"] = [name for name, _, nullable in business_schema(logical_name) if not nullable]
            for generic, columns in requirements.items():
                if not any(test.get("test_metadata", {}).get("name") == generic
                           and uid in test.get("depends_on", {}).get("nodes", [])
                           and test["test_metadata"].get("kwargs", {}).get("columns") == columns
                           for test in tests.values()):
                    raise ValueError("dbt key/required-field gate declaration missing")
        by_id = {}
        for result in results["results"]:
            uid = result["unique_id"]
            if uid in by_id:
                raise ValueError("duplicate dbt result")
            by_id[uid] = result["status"]
        for uid in model_nodes:
            if by_id.get(uid) != "success":
                raise ValueError("dbt model missing or unsuccessful")
        for uid in tests:
            if by_id.get(uid) != "pass":
                raise ValueError("dbt test missing or unsuccessful")
        return invocation
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        raise ValueError("dbt artifacts do not prove a complete successful build") from error


@dataclass(frozen=True, slots=True)
class Certification:
    identity: str
    database: str
    proof_digest: str
    manifest_digest: str
    dbt_invocation_id: str
    checked_relations: tuple[str, ...]
    proof_path: Path | None = None

    def __post_init__(self) -> None:
        for name in ("identity", "proof_digest", "manifest_digest"):
            validate_digest(getattr(self, name), name)
        if re.fullmatch(rf"mp_release_{self.identity}_[1-9][0-9]*", self.database) is None:
            raise ValueError("certification database does not belong to identity")
        uuid.UUID(self.dbt_invocation_id)
        if tuple(sorted(self.checked_relations)) != EXPORTED_RELATIONS:
            raise ValueError("certification must check all seven relations")


def _ratio(numerator: int, denominator: int) -> Decimal | None:
    if denominator == 0:
        return None
    with localcontext() as context:
        context.prec = 80
        return (Decimal(numerator) / Decimal(denominator)).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)


def _dashboard_proof(client: Any, database: str, *, columns: tuple[tuple[str, str, bool], ...],
                     row_count: int, checksum: str) -> dict[str, object]:
    names = tuple(name for name, _, _ in columns)
    with client.query_rows_stream(f"SELECT {','.join(names)} FROM {database}.campaign_dashboard",
                                  query_tz="UTC", tz_mode="aware") as stream:
        observed = rowset_checksum(columns, (dict(zip(names, row)) for row in stream))
    if observed != (row_count, checksum):
        raise ValueError("dashboard base fields do not equal certified performance")
    query = (f"SELECT d.campaign_id,d.impressions,d.clicks,d.spend_minor,d.ctr,d.cpc_minor,"
             f"d.campaign_name,d.campaign_status,c.name,c.status FROM {database}.campaign_dashboard d "
             f"LEFT JOIN {database}.campaigns c ON d.campaign_id=c.campaign_id")
    count = 0
    with client.query_rows_stream(query, settings={"join_use_nulls": 1}) as stream:
        for count, row in enumerate(stream, start=1):
            campaign, impressions, clicks, spend, ctr, cpc, name, status, reference_name, reference_status = row
            expected_name = "Unattributed" if campaign == "__unattributed__" else reference_name
            if (ctr, cpc, name, status) != (_ratio(clicks, impressions), _ratio(spend, clicks), expected_name, reference_status):
                raise ValueError("dashboard independent ratio/dimension oracle failed")
    if count != row_count:
        raise ValueError("dashboard row-count oracle failed")
    return {"row_count": count, "base_checksum": observed[1], "ratio_policy": "decimal6-truncate-toward-zero"}


def certify_candidate(client: Any, receipt: PreparationReceipt, manifest: ExportManifest,
                      database: str, dbt_artifacts: Path) -> Certification:
    if manifest.identity != receipt.identity or re.fullmatch(rf"mp_release_{receipt.identity}_[1-9][0-9]*", database) is None:
        raise ValueError("candidate/export identity mismatch")
    invocation = validate_dbt_artifacts(dbt_artifacts, database)
    checks = []
    for relation in manifest.relations:
        if receipt.output_snapshots.get(relation.name) != relation.snapshot_id:
            raise ValueError("export does not use receipt snapshots")
        model = relation.name.split(".")[1]
        actual = client.query(f"DESCRIBE TABLE {database}.{model}").result_rows
        expected = [(name, clickhouse_type(kind, nullable).replace(" ", "")) for name, kind, nullable in relation.columns]
        if [(row[0], row[1].replace(" ", "")) for row in actual] != expected:
            raise ValueError(f"served schema mismatch: {model}")
        keys = ",".join(TABLE_CONTRACTS[relation.name].key_fields)
        duplicate_keys = client.query(f"SELECT count() FROM (SELECT {keys} FROM {database}.{model} "
                                      f"GROUP BY {keys} HAVING count()>1)").first_row[0]
        if duplicate_keys:
            raise ValueError(f"served duplicate key: {model}")
        names = [name for name, _, _ in relation.columns]
        with client.query_rows_stream(f"SELECT * FROM {database}.{model}", query_tz="UTC", tz_mode="aware") as stream:
            count, checksum = rowset_checksum(relation.columns, (dict(zip(names, row)) for row in stream))
        if (count, checksum) != (relation.row_count, relation.business_checksum):
            raise ValueError(f"served business rowset mismatch: {model}")
        checks.append({"relation": relation.name, "snapshot_id": relation.snapshot_id,
                       "row_count": count, "checksum": checksum, "schema": expected})
    performance = next(r for r in manifest.relations if r.name == "gold.campaign_performance_daily")
    dashboard = _dashboard_proof(client, database, columns=performance.columns,
                                 row_count=performance.row_count, checksum=performance.business_checksum)
    proof = {"version": 1, "identity": receipt.identity, "database": database,
             "preparation_digest": receipt.proof_digest, "export_digest": manifest.digest,
             "dbt_invocation_id": invocation,
             "dbt_manifest_sha256": hashlib.sha256((dbt_artifacts / "manifest.json").read_bytes()).hexdigest(),
             "dbt_results_sha256": hashlib.sha256((dbt_artifacts / "run_results.json").read_bytes()).hexdigest(),
             "relations": checks, "dashboard": dashboard}
    content = json.dumps(proof, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(content).hexdigest()
    path = dbt_artifacts / "certification.json"
    try:
        with path.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        if path.read_bytes() != content:
            raise ValueError("immutable certification proof conflict")
    return Certification(receipt.identity, database, digest, manifest.digest, invocation,
                         tuple(r.name for r in manifest.relations), path.resolve())


def read_certification(path: Path) -> Certification:
    try:
        content = path.read_bytes()
        proof = json.loads(content)
        return Certification(proof["identity"], proof["database"], hashlib.sha256(content).hexdigest(),
                             proof["export_digest"], proof["dbt_invocation_id"],
                             tuple(row["relation"] for row in proof["relations"]), path.resolve())
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise ValueError("cannot read persisted certification proof") from error


def verify_certification_current(client: Any, receipt: PreparationReceipt, certification: Certification) -> None:
    path = certification.proof_path
    if path is None:
        raise ValueError("publication requires persisted certification proof")
    observed = read_certification(path)
    if observed != certification or certification.identity != receipt.identity:
        raise ValueError("persisted certification proof identity/digest mismatch")
    proof = json.loads(path.read_bytes())
    if proof.get("version") != 1 or proof.get("preparation_digest") != receipt.proof_digest:
        raise ValueError("certification proof preparation mismatch")
    invocation = validate_dbt_artifacts(path.parent, certification.database)
    if invocation != certification.dbt_invocation_id:
        raise ValueError("certification dbt invocation mismatch")
    for artifact, key in (("manifest.json", "dbt_manifest_sha256"), ("run_results.json", "dbt_results_sha256")):
        if hashlib.sha256((path.parent / artifact).read_bytes()).hexdigest() != proof.get(key):
            raise ValueError("certification dbt artifacts changed")
    for relation in proof["relations"]:
        name = relation["relation"]
        if receipt.output_snapshots.get(name) != relation["snapshot_id"]:
            raise ValueError("certification snapshots changed")
        columns = business_schema(name)
        model = name.split(".")[1]
        physical = client.query(f"DESCRIBE TABLE {certification.database}.{model}").result_rows
        expected = [(field, clickhouse_type(kind, nullable).replace(" ", "")) for field, kind, nullable in columns]
        if [(row[0], row[1].replace(" ", "")) for row in physical] != expected:
            raise ValueError("certified table schema changed")
        fields = [field for field, _, _ in columns]
        with client.query_rows_stream(f"SELECT * FROM {certification.database}.{model}", query_tz="UTC", tz_mode="aware") as stream:
            actual = rowset_checksum(columns, (dict(zip(fields, row)) for row in stream))
        if actual != (relation["row_count"], relation["checksum"]):
            raise ValueError("certified table business rowset changed")
    performance = next(r for r in proof["relations"] if r["relation"] == "gold.campaign_performance_daily")
    dashboard = _dashboard_proof(client, certification.database,
                                 columns=business_schema("gold.campaign_performance_daily"),
                                 row_count=performance["row_count"], checksum=performance["checksum"])
    if dashboard != proof["dashboard"]:
        raise ValueError("certified dashboard proof changed")
