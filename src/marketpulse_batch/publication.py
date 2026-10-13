"""Monotonic publication: late or repeated deliveries cannot roll back readers."""

from __future__ import annotations

import re
from typing import Any

from .ledger import PublicationReservation
from .ledger import BatchLedger
from .contracts import PreparationReceipt
from .certification import Certification, verify_certification_current

JOURNAL_COLUMNS = ("generation", "identity", "database_name", "logical_day", "proof_digest")


class NoPublishedRelease(ValueError):
    """No certified dataset has been published yet."""


class PublicationJournal:
    def __init__(self, client: Any, database: str = "marketpulse_serving"):
        if re.fullmatch(r"[a-z][a-z0-9_]{0,127}", database) is None:
            raise ValueError("invalid owned publication control database")
        self.client = client
        self.database = database

    @property
    def table(self) -> str:
        return f"{self.database}.publication_journal"

    def initialize(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database} ENGINE=Atomic")
        self.client.command(f"CREATE TABLE IF NOT EXISTS {self.table} ("
                            "generation UInt64, identity String, database_name String, logical_day Date32, "
                            "proof_digest String) ENGINE=MergeTree ORDER BY (generation,identity)")

    def append(self, row: PublicationReservation) -> None:
        self.client.insert(self.table,
                           [(row.generation, row.identity, row.database, row.logical_day, row.proof_digest)],
                           column_names=JOURNAL_COLUMNS, settings={"async_insert": 0})

    def contains(self, row: PublicationReservation) -> bool:
        observed = self.client.query(f"SELECT DISTINCT {','.join(JOURNAL_COLUMNS)} FROM {self.table} "
                                     "WHERE generation={generation:UInt64}", parameters={"generation": row.generation}).result_rows
        expected = (row.generation, row.identity, row.database, row.logical_day, row.proof_digest)
        if any(tuple(value) != expected for value in observed):
            raise ValueError("publication generation payload conflict")
        return bool(observed)

    def resolve(self) -> PublicationReservation:
        for key, payload in (("generation", "identity,database_name,logical_day,proof_digest"),
                             ("identity", "generation,database_name,logical_day,proof_digest")):
            conflicts = self.client.query(f"SELECT count() FROM (SELECT {key} FROM {self.table} "
                                           f"GROUP BY {key} HAVING uniqExact(tuple({payload}))>1)").first_row[0]
            if conflicts:
                raise ValueError("publication journal payload conflict")
        rows = self.client.query(f"SELECT DISTINCT {','.join(JOURNAL_COLUMNS)} FROM {self.table} "
                                  "ORDER BY generation DESC LIMIT 1").result_rows
        if not rows:
            raise NoPublishedRelease("no certified release has been published")
        return PublicationReservation(*rows[0])


def resolve_active(client: Any) -> PublicationReservation:
    return PublicationJournal(client).resolve()


def publish_certified(client: Any, ledger: BatchLedger, receipt: PreparationReceipt,
                      certification: Certification) -> PublicationReservation:
    verify_certification_current(client, receipt, certification)
    reservation = ledger.reserve_publication(receipt, certification.database, certification.proof_digest)
    journal = PublicationJournal(client)
    journal.initialize()
    if not journal.contains(reservation):
        journal.append(reservation)
    ledger.acknowledge(reservation)
    return reservation
