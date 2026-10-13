"""Version-bound preparation/backfill contracts, independent of Dagster imports."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from urllib.parse import unquote, urlparse

from .contracts import PreparationReceipt
from .contracts import BatchSelection
from .receipts import read_receipt, write_receipt, preparation_identity
from .settings import BatchSettings
from .ledger import BatchLedger, UnknownPreparationReceipt
from .resources import run_spark_operation
import json
import uuid


def ordered_asset_outputs(output_keys: Mapping[str, str], dependencies: Mapping[str, set[str]]) -> tuple[str, ...]:
    if len(set(output_keys.values())) != len(output_keys):
        raise ValueError("duplicate reused asset output keys")
    pending = dict(output_keys)
    ordered = []
    while pending:
        unresolved = set(pending.values())
        ready = sorted(name for name, key in pending.items() if not dependencies.get(key, set()) & unresolved)
        if not ready:
            raise ValueError("cyclic reused asset outputs")
        for name in ready:
            ordered.append(name)
            del pending[name]
    return tuple(ordered)


def model_digest(root: Path = Path("/opt/marketpulse")) -> str:
    paths = [path for path in (root / "src/marketpulse_lakehouse").glob("*.py")
             if path.name not in ("cli.py", "__main__.py", "__init__.py")]
    paths += [path for name in ("contracts.py", "receipts.py", "canonical.py", "export.py", "copying.py", "certification.py")
              if (path := root / "src/marketpulse_batch" / name).is_file()]
    paths += [path for path in (root / "dbt/marketpulse_serving").rglob("*")
              if path.is_file() and path.suffix in (".sql", ".yml", ".yaml")
              and not set(path.relative_to(root / "dbt/marketpulse_serving").parts) & {"target", "logs", "dbt_packages"}]
    if not paths:
        raise ValueError("no model source files available")
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def local_receipt_path(uri: str) -> Path:
    if not isinstance(uri, str):
        raise ValueError("receipt mapping requires one explicit local URI per day")
    parsed = urlparse(uri)
    if parsed.scheme != "file" or parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError("receipt mapping requires an explicit file:/// URI")
    path = Path(unquote(parsed.path))
    if not path.is_absolute():
        raise ValueError("receipt URI must be absolute")
    return path


def resolve_rebuild_receipts(days: Sequence[date], mapping: Mapping[str, str]) -> tuple[PreparationReceipt, ...]:
    if not days or len(days) != len(set(days)) or any(type(day) is not date for day in days):
        raise ValueError("backfill days must be nonempty and distinct")
    if not isinstance(mapping, Mapping):
        raise ValueError("backfill requires an explicit receipt mapping")
    receipts = []
    for day in days:
        uri = mapping.get(day.isoformat())
        if uri is None:
            raise ValueError("missing explicit backfill receipt mapping")
        receipt = read_receipt(local_receipt_path(uri))
        if receipt.logical_day != day:
            raise ValueError("backfill receipt logical day mismatch")
        receipts.append(receipt)
    return tuple(receipts)


def prepare_from_pinned(selection: BatchSelection, bronze_snapshots: Mapping[str, int], *,
                        settings: BatchSettings, ledger: BatchLedger) -> PreparationReceipt:
    digest = model_digest()
    identity = preparation_identity(selection, bronze_snapshots, digest)
    try:
        existing, uri = ledger.get_receipt(identity)
    except UnknownPreparationReceipt:
        pass
    else:
        if read_receipt(local_receipt_path(uri)) != existing:
            raise ValueError("persisted preparation receipt changed")
        return existing
    attempt = ledger.allocate_attempt(identity, "preparation")
    return _build_preparation(selection, dict(bronze_snapshots), digest, identity, attempt, settings, ledger, record=True)


def _build_preparation(selection: BatchSelection, bronze: dict[str, int], digest: str, identity: str, attempt: int,
                        settings: BatchSettings, ledger: BatchLedger, *, record: bool) -> PreparationReceipt:
    namespace = f"mp_batch_{identity}_{attempt}"
    run_id = f"batch-{uuid.uuid4().hex}"
    silver = run_spark_operation(settings, "silver", {"run_id": run_id, "namespace": namespace, "bronze_snapshots": bronze})
    silver_snapshots = {f"silver.{name}": value for name, value in silver["snapshots"].items()}
    gold = run_spark_operation(settings, "gold", {"run_id": run_id, "namespace": namespace, "identity": identity,
                                                "model_digest": digest, "bronze_snapshots": bronze,
                                                "silver_snapshots": silver_snapshots})
    receipt = PreparationReceipt(identity, selection.logical_day, digest, bronze, gold["output_snapshots"],
                                 namespace, gold["proof_digest"], selection.required_platforms)
    directory = settings.state_path / "preparations" / identity / str(attempt)
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "proof.json").write_text(json.dumps(gold["proof"], sort_keys=True, separators=(",", ":")))
    path = directory / "receipt.json"
    write_receipt(receipt, path)
    if record:
        ledger.record_receipt(receipt, path.as_uri())
    return receipt


def prepare_refresh(selection: BatchSelection, *, settings: BatchSettings, ledger: BatchLedger) -> PreparationReceipt:
    ledger.initialize()
    run_id = f"refresh-{uuid.uuid4().hex}"
    run_spark_operation(settings, "ingest_files", {"source_root": str(settings.source_root), "run_id": run_id,
                                                  "selection": {"logical_day": selection.logical_day.isoformat(),
                                                                "required_platforms": selection.required_platforms}})
    run_spark_operation(settings, "bounded_kafka", {"run_id": run_id, "bootstrap_servers": settings.kafka_bootstrap_servers})
    bronze = run_spark_operation(settings, "pin_bronze", {})["bronze_snapshots"]
    return prepare_from_pinned(selection, bronze, settings=settings, ledger=ledger)


def rebuild_receipt(receipt: PreparationReceipt, *, settings: BatchSettings) -> PreparationReceipt:
    if model_digest() != receipt.model_digest:
        raise ValueError("recorded model version unavailable; use its compatible source revision")
    ledger = BatchLedger(settings.ledger_dsn)
    ledger.initialize()
    # Shared namespace counter prevents rebuild attempt 1 from colliding with preparation 1.
    attempt = ledger.allocate_attempt(receipt.identity, "preparation")
    rebuilt = _build_preparation(BatchSelection(receipt.logical_day, receipt.required_platforms),
                                 dict(receipt.bronze_snapshots), receipt.model_digest, receipt.identity,
                                 attempt, settings, ledger, record=False)
    if rebuilt.proof_digest != receipt.proof_digest:
        raise ValueError("historical rebuild business proof differs from recorded preparation")
    return rebuilt
