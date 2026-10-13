from __future__ import annotations

import copy
import unittest

from marketpulse_batch.evidence import validate_evidence
from marketpulse_batch.proof import validate_release_binding


class ServingEvidenceTests(unittest.TestCase):
    def test_active_release_must_match_certification_and_export(self):
        from types import SimpleNamespace
        release = SimpleNamespace(identity="a", database="release_a", proof_digest="proof_a")
        bundle = SimpleNamespace(**vars(release))
        cert = SimpleNamespace(**vars(release), manifest_digest="export_a")
        manifest = SimpleNamespace(identity="a", digest="export_a")
        validate_release_binding(release, bundle, cert, manifest)
        for obj, field in ((release, "database"), (bundle, "proof_digest"), (cert, "identity"), (manifest, "digest")):
            original = getattr(obj, field)
            setattr(obj, field, "wrong")
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "binding"):
                validate_release_binding(release, bundle, cert, manifest)
            setattr(obj, field, original)

    def report(self):
        return {"accepted": True, "identity": "a" * 64, "database": "mp_release_" + "a" * 64 + "_1",
                "generation": 1, "proof_digest": "b" * 64, "export_digest": "c" * 64,
                "model_digest": "d" * 64, "source_snapshots": {"gold.funnel_daily": 123},
                "source_row_counts": {"gold.funnel_daily": 12}, "financial_totals": [{"net_revenue_minor": 22193000}],
                "query_interval": {"from": "2026-01-01", "to": "2026-01-03", "bounds": "[from,to)"},
                "query_cache": "disabled", "percentile_method": "nearest_rank",
                "workloads": {name: {"warmups": 20, "samples": 100, "successful_samples": 100,
                                     "failures": 0, "correctness_checked": True, "p95_ms": 3.0}
                              for name in ("campaign_overview", "daily_funnel", "attribution_comparison",
                                           "cohort_retention", "customer_drill", "top_campaigns")}}

    def test_restart_report_rejects_financial_inflation(self):
        before = self.report()
        after = copy.deepcopy(before)
        validate_evidence(before, after)
        after["financial_totals"][0]["net_revenue_minor"] += 1
        with self.assertRaisesRegex(ValueError, "financial_totals"):
            validate_evidence(before, after)

    def test_restart_cannot_change_release_or_snapshot(self):
        for field, value in (("generation", 2), ("source_snapshots", {"gold.funnel_daily": 124}),
                             ("source_row_counts", {"gold.funnel_daily": 13})):
            after = self.report() | {field: value}
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_evidence(self.report(), after)

    def test_report_rejects_unmeasured_or_incorrect_workloads(self):
        for field, value in (("samples", 99), ("warmups", 0), ("failures", 1),
                             ("correctness_checked", False), ("p95_ms", 501), ("p95_ms", float("nan"))):
            report = self.report()
            report["workloads"]["campaign_overview"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_evidence(report)


import os
import uuid
from unittest.mock import patch


@unittest.skipUnless(os.environ.get("BATCH_TEST_RECOVERY"), "opt-in real PostgreSQL/ClickHouse lost-ack proof")
class LostAcknowledgementTests(unittest.TestCase):
    def test_uncertain_ack_restart_no_inflation(self):
        from dataclasses import replace
        import psycopg
        from marketpulse_batch.settings import load_settings
        from marketpulse_batch.resources import clickhouse_client
        from marketpulse_batch.ledger import BatchLedger
        from marketpulse_batch.publication import PublicationJournal, publish_certified, resolve_active
        from marketpulse_batch.certification import read_certification
        from marketpulse_batch.jobs import local_receipt_path

        settings = load_settings(os.environ)
        client = clickhouse_client(settings)
        try:
            active = resolve_active(client)
            real = BatchLedger(settings.ledger_dsn)
            receipt, uri = real.get_receipt(active.identity)
            bundle = real.get_certified_bundle(active.identity)
            cert = read_certification(local_receipt_path(bundle.certification_uri))
            dsn = replace(settings, ledger_database="mp05_test").ledger_dsn
            with psycopg.connect(settings.ledger_dsn, autocommit=True) as connection:
                if not connection.execute("SELECT 1 FROM pg_database WHERE datname = %s", ("mp05_test",)).fetchone():
                    connection.execute("CREATE DATABASE mp05_test")
            # Only the dedicated test database is reset. Active release DBs are read-only.
            with psycopg.connect(dsn) as connection:
                if connection.execute("SELECT current_database()").fetchone()[0] != "mp05_test":
                    raise RuntimeError("recovery proof requires mp05_test")
                connection.execute("DROP SCHEMA IF EXISTS marketpulse_ops CASCADE")
            ledger = BatchLedger(dsn)
            ledger.initialize()
            ledger.record_receipt(receipt, uri)
            journal = PublicationJournal(client, "mp05_test_recovery_" + uuid.uuid4().hex)

            class LostAck:
                def reserve_publication(self, *args):
                    return ledger.reserve_publication(*args)

                def acknowledge(self, row):
                    raise ConnectionError("injected acknowledgement loss")

            with patch("marketpulse_batch.publication.PublicationJournal", return_value=journal):
                with self.assertRaises(ConnectionError):
                    publish_certified(client, LostAck(), receipt, cert)
                published = journal.resolve()
                # Reconstruct clients as after a process restart; the shell proof also restarts servers.
                restarted = BatchLedger(dsn)
                recovered = publish_certified(client, restarted, receipt, cert)
            self.assertEqual(recovered, published)
            self.assertEqual(client.query(f"SELECT count() FROM {journal.table}").first_row[0], 1)
            self.assertEqual(resolve_active(client), active)
        finally:
            client.close()
