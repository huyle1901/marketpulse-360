"""Transactional operational ledger; analytical business rows never live here."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from importlib.resources import files
from typing import Any

from .contracts import PreparationReceipt, validate_digest
from .receipts import receipt_from_payload, receipt_payload

PUBLISHER_LOCK = 36005


@dataclass(frozen=True, slots=True)
class PublicationReservation:
    generation: int
    identity: str
    database: str
    logical_day: date
    proof_digest: str

    def __post_init__(self) -> None:
        validate_digest(self.identity, "identity")
        validate_digest(self.proof_digest, "proof_digest")
        if type(self.generation) is not int or self.generation < 1:
            raise ValueError("invalid publication generation")
        if type(self.logical_day) is not date:
            raise ValueError("invalid publication day")
        if re.fullmatch(rf"mp_release_{self.identity}_[1-9][0-9]*", self.database) is None:
            raise ValueError("candidate database does not belong to identity")


def _reservation(row: tuple[Any, ...]) -> PublicationReservation:
    return PublicationReservation(*row)


class BatchLedger:
    def __init__(self, dsn: str):
        if not dsn:
            raise ValueError("ledger DSN must be configured")
        self._dsn = dsn

    def _connect(self) -> Any:
        import psycopg
        return psycopg.connect(self._dsn)

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("SELECT pg_advisory_xact_lock(%s)", (PUBLISHER_LOCK,))
            connection.execute(files("marketpulse_batch").joinpath("sql/ledger.sql").read_text())

    def record_receipt(self, receipt: PreparationReceipt, uri: str) -> None:
        payload = receipt_payload(receipt)
        if not uri.startswith(("file:///", "s3://")):
            raise ValueError("receipt URI must be a persisted local/S3 object")
        with self._connect() as connection:
            connection.execute("SELECT pg_advisory_xact_lock(%s)", (PUBLISHER_LOCK,))
            connection.execute(
                "INSERT INTO marketpulse_ops.preparations(identity, receipt, receipt_uri) "
                "VALUES (%s,%s::jsonb,%s) ON CONFLICT(identity) DO NOTHING",
                (receipt.identity, json.dumps(payload), uri),
            )
            existing = connection.execute(
                "SELECT receipt,receipt_uri FROM marketpulse_ops.preparations WHERE identity=%s",
                (receipt.identity,),
            ).fetchone()
            if existing != (payload, uri):
                raise ValueError("immutable preparation receipt conflict")

    def get_receipt(self, identity: str) -> tuple[PreparationReceipt, str]:
        validate_digest(identity, "identity")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT receipt,receipt_uri FROM marketpulse_ops.preparations WHERE identity=%s", (identity,)
            ).fetchone()
            if row is None:
                raise ValueError("unknown preparation receipt")
            return receipt_from_payload(row[0]), row[1]

    def allocate_attempt(self, identity: str, stage: str) -> int:
        validate_digest(identity, "identity")
        if stage not in {"preparation", "copy", "rebuild"}:
            raise ValueError("invalid attempt stage")
        with self._connect() as connection:
            connection.execute("SELECT pg_advisory_xact_lock(%s)", (PUBLISHER_LOCK,))
            row = connection.execute(
                "INSERT INTO marketpulse_ops.attempts(identity,stage,attempt) VALUES(%s,%s,1) "
                "ON CONFLICT(identity,stage) DO UPDATE SET attempt=marketpulse_ops.attempts.attempt+1 "
                "RETURNING attempt", (identity, stage),
            ).fetchone()
            return row[0]

    def reserve_publication(self, receipt: PreparationReceipt, database: str, proof_digest: str) -> PublicationReservation:
        # Validate owned identifiers and certification digest before touching SQL.
        proposal = PublicationReservation(1, receipt.identity, database, receipt.logical_day, proof_digest)
        with self._connect() as connection:
            connection.execute("SELECT pg_advisory_xact_lock(%s)", (PUBLISHER_LOCK,))
            prepared = connection.execute(
                "SELECT receipt FROM marketpulse_ops.preparations WHERE identity=%s", (receipt.identity,)
            ).fetchone()
            if prepared is None or prepared[0] != receipt_payload(receipt):
                raise ValueError("publication requires the exact recorded preparation")
            row = connection.execute(
                "SELECT generation,identity,database_name,logical_day,proof_digest "
                "FROM marketpulse_ops.publications WHERE identity=%s", (receipt.identity,)
            ).fetchone()
            if row is not None:
                existing = _reservation(row)
                if (existing.database, existing.logical_day, existing.proof_digest) != (
                    proposal.database, proposal.logical_day, proposal.proof_digest
                ):
                    raise ValueError("immutable publication payload conflict")
                return existing
            high_day = connection.execute("SELECT max(logical_day) FROM marketpulse_ops.publications").fetchone()[0]
            if high_day is not None and receipt.logical_day < high_day:
                raise ValueError("older logical-day publication rejected")
            row = connection.execute(
                "INSERT INTO marketpulse_ops.publications(identity,database_name,logical_day,proof_digest) "
                "VALUES(%s,%s,%s,%s) RETURNING generation,identity,database_name,logical_day,proof_digest",
                (receipt.identity, database, receipt.logical_day, proof_digest),
            ).fetchone()
            return _reservation(row)

    def acknowledge(self, reservation: PublicationReservation) -> None:
        with self._connect() as connection:
            row = connection.execute(
                "UPDATE marketpulse_ops.publications SET acknowledged_at=COALESCE(acknowledged_at,now()) "
                "WHERE generation=%s AND identity=%s AND database_name=%s AND logical_day=%s AND proof_digest=%s "
                "RETURNING generation",
                (reservation.generation, reservation.identity, reservation.database,
                 reservation.logical_day, reservation.proof_digest),
            ).fetchone()
            if row is None:
                raise ValueError("publication acknowledgement payload conflict")
