from __future__ import annotations

import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date

from marketpulse_batch.contracts import BatchSelection, PreparationReceipt
from marketpulse_batch.receipts import preparation_identity
from marketpulse_batch.ledger import BatchLedger


@unittest.skipUnless(os.environ.get("BATCH_TEST_DSN"), "opt-in isolated PostgreSQL integration")
class BatchLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        import psycopg
        self.dsn = os.environ["BATCH_TEST_DSN"]
        # Destructive cleanup is test-owned and requires explicit dedicated DB marker.
        with psycopg.connect(self.dsn) as connection:
            database = connection.execute("SELECT current_database()").fetchone()[0]
            if database != "mp05_test":
                raise RuntimeError("ledger tests require dedicated mp05_test database")
            connection.execute("DROP SCHEMA IF EXISTS marketpulse_ops CASCADE")
        self.ledger = BatchLedger(self.dsn)
        self.ledger.initialize()

    def receipt(self, day: date = date(2026, 1, 1), snapshot: int = 101) -> PreparationReceipt:
        bronze = {"bronze.kafka_records": snapshot}
        identity = preparation_identity(BatchSelection(day), bronze, "a" * 64)
        return PreparationReceipt(identity, day, "a" * 64, bronze, {"gold.funnel_daily": 201},
                                  f"mp_batch_{identity}_1", "b" * 64)

    def reserve(self, receipt: PreparationReceipt, digest: str = "c" * 64):
        self.ledger.record_receipt(receipt, f"file:///batch-state/{receipt.identity}.json")
        return self.ledger.reserve_publication(receipt, f"mp_release_{receipt.identity}_1", digest)

    def test_reservation_retry_same_generation(self) -> None:
        receipt = self.receipt()
        first = self.reserve(receipt)
        self.assertEqual(first.generation, 1)
        self.assertEqual(self.reserve(receipt), first)
        self.assertEqual(first.proof_digest, "c" * 64)
        self.ledger.acknowledge(first)
        self.assertEqual(BatchLedger(self.dsn).get_receipt(receipt.identity)[0], receipt)

    def test_reservation_payload_conflict(self) -> None:
        receipt = self.receipt()
        self.reserve(receipt)
        with self.assertRaisesRegex(ValueError, "conflict"):
            self.reserve(receipt, "d" * 64)
        with self.assertRaisesRegex(ValueError, "conflict"):
            self.ledger.record_receipt(replace(receipt, proof_digest="e" * 64), "file:///other.json")

    def test_older_day_rejected(self) -> None:
        newer = self.receipt(date(2026, 1, 2))
        self.reserve(newer)
        with self.assertRaisesRegex(ValueError, "older"):
            self.reserve(self.receipt())
        self.assertEqual(self.reserve(self.receipt(date(2026, 1, 3))).generation, 2)

    def test_concurrent_reservations_serialized(self) -> None:
        receipts = [self.receipt(snapshot=101 + n) for n in range(8)]
        for receipt in receipts:
            self.ledger.record_receipt(receipt, f"file:///batch-state/{receipt.identity}.json")
        with ThreadPoolExecutor(max_workers=8) as pool:
            rows = list(pool.map(lambda r: self.ledger.reserve_publication(r, f"mp_release_{r.identity}_1", "c" * 64), receipts))
        self.assertEqual(sorted(r.generation for r in rows), list(range(1, 9)))

    def test_acknowledgement_rejects_unreserved_payload(self) -> None:
        row = self.reserve(self.receipt())
        with self.assertRaises(ValueError):
            self.ledger.acknowledge(replace(row, proof_digest="d" * 64))

    def test_certified_bundle_reusable_and_immutable(self) -> None:
        receipt = self.receipt()
        self.ledger.record_receipt(receipt, "file:///batch-state/receipt.json")
        database = f"mp_release_{receipt.identity}_1"
        self.assertIsNone(self.ledger.get_certified_bundle(receipt.identity))
        self.ledger.record_certified_bundle(receipt.identity, database, "c" * 64,
                                             "file:///batch-state/export.json", "file:///batch-state/certification.json")
        bundle = self.ledger.get_certified_bundle(receipt.identity)
        self.assertEqual(bundle.database, database)
        self.assertEqual(bundle.certification_uri, "file:///batch-state/certification.json")
        with self.assertRaisesRegex(ValueError, "conflict"):
            self.ledger.record_certified_bundle(receipt.identity, database, "d" * 64,
                                                 "file:///batch-state/export.json", "file:///batch-state/certification.json")
