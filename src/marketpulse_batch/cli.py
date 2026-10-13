"""Operator tools for the local certified marketing serving workflow."""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from .settings import load_settings
from .queries import QueryRequest, WORKLOADS, run_query
from .resources import clickhouse_client, run_spark_operation
from .publication import resolve_active, publish_certified
from .proof import verify_serving
from .jobs import prepare_refresh, rebuild_receipt, local_receipt_path
from .contracts import BatchSelection
from .receipts import read_receipt
from .ledger import BatchLedger
from .export import ExportManifest
from .copying import load_candidate
from .dbt_runner import run_dbt_build
from .certification import read_certification, certify_candidate
from marketpulse_lakehouse.source_files import create_s3_client


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    query = commands.add_parser("query")
    query.add_argument("workload", choices=WORKLOADS)
    for target in (query, commands.add_parser("benchmark"), commands.add_parser("verify")):
        target.add_argument("--from-date", type=date.fromisoformat, required=True)
        target.add_argument("--to-date", type=date.fromisoformat, required=True)
        target.add_argument("--output", type=Path)
    query.add_argument("--currency")
    query.add_argument("--customer-id")
    query.add_argument("--limit", type=int, default=20)
    refresh = commands.add_parser("refresh")
    refresh.add_argument("--day", type=date.fromisoformat, required=True)
    rebuild = commands.add_parser("rebuild")
    rebuild.add_argument("--receipt", type=Path, required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("--receipt", type=Path, required=True)
    publish.add_argument("--certification", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        request = QueryRequest(args.workload, args.from_date, args.to_date, args.currency, args.customer_id, args.limit) if args.command == "query" else None
        settings = load_settings(os.environ)
        ledger = BatchLedger(settings.ledger_dsn)
        if args.command in {"benchmark", "verify"}:
            report = verify_serving(settings, from_date=args.from_date, to_date=args.to_date)
            if not report["accepted"]:
                raise ValueError("serving correctness/latency acceptance failed")
        elif args.command == "rebuild":
            receipt = rebuild_receipt(read_receipt(args.receipt), settings=settings)
            report = {"identity": receipt.identity, "namespace": receipt.namespace, "proof_digest": receipt.proof_digest, "published": False}
        else:
            client = clickhouse_client(settings)
            try:
                if args.command == "query":
                    release = resolve_active(client)
                    report = {"generation": release.generation, "database": release.database,
                              "workload": request.workload, "rows": run_query(client, release, request)}
                elif args.command == "publish":
                    ledger.initialize()
                    release = publish_certified(client, ledger, read_receipt(args.receipt), read_certification(args.certification))
                    report = {"generation": release.generation, "database": release.database}
                else:
                    receipt = prepare_refresh(BatchSelection(args.day), settings=settings, ledger=ledger)
                    bundle = ledger.get_certified_bundle(receipt.identity)
                    if bundle:
                        cert = read_certification(local_receipt_path(bundle.certification_uri))
                    else:
                        directory = settings.state_path / "cli-refresh" / uuid.uuid4().hex
                        directory.mkdir(parents=True)
                        receipt_path = local_receipt_path(ledger.get_receipt(receipt.identity)[1])
                        value = run_spark_operation(settings, "export", {"receipt_path": str(receipt_path),
                            "output_prefix": f"s3://marketpulse-lakehouse/batch-exports/{receipt.identity}/{uuid.uuid4().hex}"})
                        manifest = ExportManifest.from_payload(value)
                        manifest_path = directory / "export_manifest.json"
                        manifest_path.write_text(json.dumps(value, sort_keys=True))
                        database = load_candidate(client, manifest, attempt=ledger.allocate_attempt(receipt.identity, "copy"), s3=create_s3_client())
                        target = run_dbt_build(database, settings=settings, artifact_root=directory / "dbt")
                        cert = certify_candidate(client, receipt, manifest, database, target)
                        ledger.record_certified_bundle(receipt.identity, database, cert.proof_digest,
                                                         manifest_path.as_uri(), cert.proof_path.as_uri())
                    release = publish_certified(client, ledger, receipt, cert)
                    report = {"identity": receipt.identity, "generation": release.generation, "database": release.database}
            finally:
                client.close()
        content = json.dumps(report, sort_keys=True, indent=2, default=str) + "\n"
        if getattr(args, "output", None):
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(content)
        print(content, end="")
        return 0
    except ValueError as error:
        print(f"marketpulse-batch: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
