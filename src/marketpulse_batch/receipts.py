"""Canonical preparation identities and exclusive immutable receipt persistence."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from datetime import date
from pathlib import Path

from .contracts import BatchSelection, PreparationReceipt, snapshot_map, validate_digest


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def preparation_identity(selection: BatchSelection, bronze_snapshots: Mapping[str, int], model_digest: str) -> str:
    validate_digest(model_digest, "model_digest")
    return hashlib.sha256(_canonical({
        "version": 1, "logical_day": selection.logical_day.isoformat(),
        "required_platforms": selection.required_platforms,
        "bronze_snapshots": dict(snapshot_map(bronze_snapshots, "bronze")),
        "model_digest": model_digest,
    })).hexdigest()


def receipt_payload(receipt: PreparationReceipt) -> dict[str, object]:
    expected = preparation_identity(BatchSelection(receipt.logical_day, receipt.required_platforms),
                                    receipt.bronze_snapshots, receipt.model_digest)
    if expected != receipt.identity:
        raise ValueError("receipt identity does not match pinned inputs")
    return {
        "version": 1, "identity": receipt.identity, "logical_day": receipt.logical_day.isoformat(),
        "model_digest": receipt.model_digest, "bronze_snapshots": dict(receipt.bronze_snapshots),
        "output_snapshots": dict(receipt.output_snapshots), "namespace": receipt.namespace,
        "proof_digest": receipt.proof_digest, "required_platforms": list(receipt.required_platforms),
    }


def receipt_from_payload(value: object) -> PreparationReceipt:
    expected = {"version", "identity", "logical_day", "model_digest", "bronze_snapshots",
                "output_snapshots", "namespace", "proof_digest", "required_platforms"}
    try:
        if not isinstance(value, dict) or set(value) != expected or type(value["version"]) is not int or value["version"] != 1:
            raise ValueError("invalid receipt fields/version")
        if not isinstance(value["required_platforms"], list):
            raise ValueError("invalid receipt platforms")
        receipt = PreparationReceipt(
            identity=value["identity"], logical_day=date.fromisoformat(value["logical_day"]),
            model_digest=value["model_digest"], bronze_snapshots=value["bronze_snapshots"],
            output_snapshots=value["output_snapshots"], namespace=value["namespace"],
            proof_digest=value["proof_digest"], required_platforms=tuple(value["required_platforms"]),
        )
        receipt_payload(receipt)
        return receipt
    except (TypeError, KeyError, ValueError) as error:
        raise ValueError("invalid preparation receipt") from error


def read_receipt(path: Path) -> PreparationReceipt:
    try:
        return receipt_from_payload(json.loads(Path(path).read_bytes()))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("cannot read preparation receipt") from error


def write_receipt(receipt: PreparationReceipt, path: Path) -> None:
    path = Path(path)
    content = _canonical(receipt_payload(receipt)) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".receipt-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)  # Exclusive creation; competing writers cannot overwrite.
        except FileExistsError:
            if read_receipt(path) != receipt:
                raise ValueError("immutable receipt conflict")
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
