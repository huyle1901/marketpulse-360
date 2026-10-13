"""Reconcile live workloads against pinned export rows and emit benchmark evidence."""

from __future__ import annotations

import json
import tempfile
from datetime import date
from pathlib import Path
from typing import Any
from collections.abc import Iterator

from .benchmark import benchmark_queries, normalized_rows
from .queries import QueryRequest, WORKLOADS, run_query
from .oracles import fixture_oracle
from .export import ExportManifest, json_rows
from .copying import _download
from .jobs import local_receipt_path
from .ledger import BatchLedger, PublicationReservation, CertifiedBundle
from .resources import clickhouse_client
from .settings import BatchSettings
from .certification import Certification, read_certification, verify_certification_current
from .publication import resolve_active
from marketpulse_lakehouse.source_files import create_s3_client

RELATION_FOR_QUERY = {"campaign_overview": "gold.campaign_performance_daily", "daily_funnel": "gold.funnel_daily",
                      "attribution_comparison": "gold.attribution_daily", "cohort_retention": "gold.cohort_retention",
                      "customer_drill": "gold.customer_360", "top_campaigns": "gold.campaign_performance_daily"}


def validate_release_binding(release: PublicationReservation, bundle: CertifiedBundle,
                             cert: Certification, manifest: ExportManifest) -> None:
    for field in ("identity", "database", "proof_digest"):
        if getattr(release, field) != getattr(bundle, field) or getattr(release, field) != getattr(cert, field):
            raise ValueError(f"active release binding mismatch: {field}")
    if manifest.identity != release.identity or manifest.digest != cert.manifest_digest:
        raise ValueError("active export binding mismatch")


def export_rows(manifest: ExportManifest, relation_name: str, s3: Any) -> Iterator[dict[str, object]]:
    relation = next(row for row in manifest.relations if row.name == relation_name)
    with tempfile.TemporaryDirectory(prefix="marketpulse-query-oracle-") as root:
        path = Path(root) / "part.json"
        for obj in relation.objects:
            _download(s3, obj, path, relation.columns)
            yield from json_rows(path)


def verify_serving(settings: BatchSettings, *, from_date: date, to_date: date,
                   warmups: int = 20, samples: int = 100) -> dict[str, object]:
    client = clickhouse_client(settings)
    try:
        release = resolve_active(client)
        ledger = BatchLedger(settings.ledger_dsn)
        ledger.initialize()
        receipt, _ = ledger.get_receipt(release.identity)
        bundle = ledger.get_certified_bundle(release.identity)
        if bundle is None:
            raise ValueError("active release has no certified artifact bundle")
        cert = read_certification(local_receipt_path(bundle.certification_uri))
        verify_certification_current(client, receipt, cert)
        manifest = ExportManifest.from_payload(json.loads(local_receipt_path(bundle.export_manifest_uri).read_bytes()))
        validate_release_binding(release, bundle, cert, manifest)
        s3 = create_s3_client()
        currencies = {row["currency"] for row in export_rows(manifest, "silver.campaigns", s3)}
        if len(currencies) != 1:
            raise ValueError("fixture proof requires one declared currency")
        currency = next(iter(currencies))
        customer_ids = (row["customer_id"] for row in export_rows(manifest, "gold.customer_360", s3) if row["customer_id"] is not None)
        customer_id = min(customer_ids, default=None)
        if customer_id is None:
            raise ValueError("fixture proof requires an identified customer drill-down")
        requests = [QueryRequest(name, from_date, to_date, currency if name in
                    {"campaign_overview", "attribution_comparison", "customer_drill", "top_campaigns"} else None,
                    customer_id if name == "customer_drill" else None) for name in WORKLOADS]
        expected = {}
        for request in requests:
            expected[request.workload] = fixture_oracle(request, export_rows(manifest, RELATION_FOR_QUERY[request.workload], s3), currency=currency)
            actual = run_query(client, release, request)
            if normalized_rows(actual) != normalized_rows(expected[request.workload]):
                raise ValueError(f"independent query oracle mismatch: {request.workload}")
        report = benchmark_queries(client, release, requests, warmups=warmups, samples=samples, expected=expected)
        report["source_row_counts"] = {relation.name: relation.row_count for relation in manifest.relations}
        report["source_snapshots"] = dict(receipt.output_snapshots)
        report["model_digest"] = receipt.model_digest
        report["export_digest"] = manifest.digest
        report["dbt_invocation_id"] = cert.dbt_invocation_id
        report["query_interval"] = {"from": from_date.isoformat(), "to": to_date.isoformat(), "bounds": "[from,to)"}
        report["oracle_method"] = "independent Python aggregation of pinned typed export objects"
        report["financial_totals"] = expected["campaign_overview"]
        report["accepted"] = all(row["failures"] == 0 and row["p95_ms"] is not None and row["p95_ms"] <= 500
                                  for row in report["workloads"].values())
        return report
    finally:
        client.close()
