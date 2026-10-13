"""Exact transport types and disk-backed multiset equality checks."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import tempfile
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path

Columns = tuple[tuple[str, str, bool], ...]
_DECIMAL = re.compile(r"decimal\(([1-9][0-9]*),([0-9]+)\)")


def clickhouse_type(type_name: str, nullable: bool) -> str:
    mapping = {"string": "String", "int": "Int32", "long": "Int64", "boolean": "Bool",
               "date": "Date32", "timestamp": "DateTime64(6,'UTC')"}
    result = mapping.get(type_name)
    decimal = _DECIMAL.fullmatch(type_name)
    if decimal:
        precision, scale = map(int, decimal.groups())
        if precision > 38 or scale > precision:
            raise ValueError("unsupported decimal precision/scale")
        result = f"Decimal({precision},{scale})"
    if result is None:
        raise ValueError(f"unsupported serving type: {type_name}")
    return f"Nullable({result})" if nullable else result


def _value(kind: str, value: object) -> object:
    if kind == "string" and isinstance(value, str):
        return value
    if kind in ("int", "long") and type(value) is int:
        bits = 32 if kind == "int" else 64
        if -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
            return str(value)
    if kind == "boolean" and type(value) is bool:
        return value
    if kind == "date":
        try:
            day = value if type(value) is date else date.fromisoformat(value) if isinstance(value, str) else None
            if day is not None and date(1900, 1, 1) <= day <= date(2299, 12, 31):
                return day.isoformat()
        except ValueError:
            pass
    if kind == "timestamp":
        try:
            stamp = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else None
            if stamp is not None and stamp.tzinfo is not None:
                stamp = stamp.astimezone(UTC)
                if datetime(1900, 1, 1, tzinfo=UTC) <= stamp < datetime(2300, 1, 1, tzinfo=UTC):
                    return stamp.isoformat(timespec="microseconds").replace("+00:00", "Z")
        except ValueError:
            pass
    decimal = _DECIMAL.fullmatch(kind)
    if decimal and isinstance(value, (Decimal, str, int)) and type(value) is not bool:
        precision, scale = map(int, decimal.groups())
        try:
            with localcontext() as context:
                context.prec = 80
                number = Decimal(value)
                quantized = number.quantize(Decimal(1).scaleb(-scale))
                if number.is_finite() and number == quantized and abs(number) < Decimal(10) ** (precision - scale):
                    if not number:
                        quantized = abs(quantized)  # -0 and 0 are the same decimal business value.
                    return format(quantized, f".{scale}f")
        except (InvalidOperation, ValueError):
            pass
    raise ValueError(f"invalid or out-of-range {kind} value")


def canonical_row(columns: Columns, row: Mapping[str, object]) -> bytes:
    names = tuple(name for name, _, _ in columns)
    if len(names) != len(set(names)) or set(row) != set(names):
        raise ValueError("row columns differ from declared serving schema")
    values = []
    for name, kind, nullable in columns:
        clickhouse_type(kind, nullable)
        value = row[name]
        if value is None:
            if not nullable:
                raise ValueError(f"required column is null: {name}")
            values.append(None)
        else:
            values.append(_value(kind, value))
    return json.dumps(values, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()


def rowset_checksum(columns: Columns, rows: Iterable[Mapping[str, object]]) -> tuple[int, str]:
    """Hash sorted row hashes and multiplicities with bounded RAM, retaining duplicates."""
    with tempfile.TemporaryDirectory(prefix="marketpulse-rowset-") as root:
        with sqlite3.connect(Path(root) / "hashes.sqlite") as database:
            database.execute("PRAGMA cache_size=-2048")
            database.execute("CREATE TABLE hashes(digest BLOB PRIMARY KEY, n INTEGER NOT NULL)")
            count = 0
            for count, row in enumerate(rows, start=1):
                digest = hashlib.sha256(canonical_row(columns, row)).digest()
                database.execute("INSERT INTO hashes VALUES(?,1) ON CONFLICT(digest) DO UPDATE SET n=n+1", (digest,))
                if count % 10000 == 0:
                    database.commit()
            database.commit()
            result = hashlib.sha256(b"marketpulse-rowset-v1\0")
            for digest, multiplicity in database.execute("SELECT digest,n FROM hashes ORDER BY digest"):
                result.update(digest)
                result.update(multiplicity.to_bytes(8, "big"))
            return count, result.hexdigest()
