from __future__ import annotations

import os
import unittest
import uuid
from dataclasses import replace
from datetime import date

from marketpulse_batch.ledger import PublicationReservation
from marketpulse_batch.publication import PublicationJournal, NoPublishedRelease


@unittest.skipUnless(os.environ.get("BATCH_TEST_CLICKHOUSE"), "opt-in real ClickHouse journal integration")
class PublicationJournalTests(unittest.TestCase):
    def setUp(self) -> None:
        import clickhouse_connect
        from marketpulse_batch.settings import load_settings
        settings = load_settings(os.environ)
        self.client = clickhouse_connect.get_client(host=settings.clickhouse_host, port=settings.clickhouse_port,
                                                   username=settings.clickhouse_user, password=settings.clickhouse_password)
        self.journal = PublicationJournal(self.client, database=f"mp05_test_journal_{uuid.uuid4().hex}")
        self.journal.initialize()

    def row(self, generation: int) -> PublicationReservation:
        identity = f"{generation:064x}"
        return PublicationReservation(generation, identity, f"mp_release_{identity}_1", date(2026, 1, 1), "c" * 64)

    def test_old_inflight_after_lock_loss_cannot_rollback(self) -> None:
        self.journal.append(self.row(2))
        self.journal.append(self.row(1))
        self.assertEqual(self.journal.resolve(), self.row(2))

    def test_identical_journal_duplicate_collapses(self) -> None:
        row = self.row(1)
        self.journal.append(row)
        self.journal.append(row)
        self.assertTrue(self.journal.contains(row))
        self.assertEqual(self.journal.resolve(), row)

    def test_conflicting_generation_fails(self) -> None:
        row = self.row(1)
        self.journal.append(row)
        self.journal.append(replace(row, proof_digest="d" * 64))
        with self.assertRaisesRegex(ValueError, "conflict"):
            self.journal.resolve()

    def test_pending_generation_does_not_hide_active(self) -> None:
        with self.assertRaises(NoPublishedRelease):
            self.journal.resolve()
        self.journal.append(self.row(1))
        # Merely reserving generation 2 in a ledger does not add a journal row.
        self.assertFalse(self.journal.contains(self.row(2)))
        self.assertEqual(self.journal.resolve(), self.row(1))

    def test_one_identity_cannot_change_generation(self) -> None:
        row = self.row(1)
        self.journal.append(row)
        self.journal.append(replace(row, generation=2))
        with self.assertRaisesRegex(ValueError, "conflict"):
            self.journal.resolve()
