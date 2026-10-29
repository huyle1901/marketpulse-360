"""Fail-closed, acknowledged replay cursor for a bounded demo window."""

from __future__ import annotations

import fcntl
import hashlib
import itertools
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from .kafka_client import KafkaPublisher
from .replay_pacing import ReplayController, ReplayPacer
from .replay_reader import iter_replay_records


@dataclass(frozen=True, slots=True)
class ReplayWindowSummary:
    committed: int
    published_this_run: int
    complete: bool
    last_event_time: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "committed": self.committed,
            "published_this_run": self.published_this_run,
            "complete": self.complete,
            "last_event_time": self.last_event_time,
        }


def _write_state(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=".replay-", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def replay_window(
    root: Path,
    state_path: Path,
    from_date: date,
    to_date: date,
    publisher: KafkaPublisher,
    pacer: ReplayPacer,
    controller: ReplayController,
    *,
    target_id: str,
    batch_size: int = 500,
    initialize: bool = False,
    flush_timeout: float = 30.0,
    include_dlq: bool = False,
) -> ReplayWindowSummary:
    """Publish only confirmed batches; an interrupted batch requires reconciliation.

    The durable pending marker is written before the first send. If a process dies
    after broker acknowledgement but before its cursor commit, the next process
    refuses to republish that ambiguous range. A normal stop finishes its current
    batch and resumes from the confirmed cursor.
    """
    if type(from_date) is not date or type(to_date) is not date or from_date >= to_date:
        raise ValueError("invalid UTC replay [from_date,to_date) window")
    if not target_id or batch_size <= 0 or flush_timeout <= 0:
        raise ValueError("target_id, batch_size and flush_timeout must be positive")
    root, state_path = Path(root), Path(state_path)
    if state_path.is_symlink():
        raise ValueError("replay state must not be a symbolic link")
    manifest_sha256 = hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest()
    identity = {
        "version": 1,
        "manifest_sha256": manifest_sha256,
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "target_id": target_id,
        "include_dlq": include_dlq,
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with state_path.with_suffix(state_path.suffix + ".lock").open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("replay window is already owned by another process") from error
        if state_path.exists():
            state = json.loads(state_path.read_text(encoding="utf-8"))
            if not isinstance(state, dict) or any(state.get(key) != value for key, value in identity.items()):
                raise ValueError("replay world/window/target identity changed")
        elif initialize:
            state = {**identity, "next_index": 0, "pending": None,
                     "complete": False, "last_event_time": None}
            _write_state(state_path, state)
        else:
            raise ValueError("replay state is absent; initialize only after history preparation")
        if state.get("pending") is not None:
            raise ValueError("ambiguous broker acknowledgement; reconcile pending replay range before resuming")
        committed = state.get("next_index")
        if type(committed) is not int or committed < 0 or type(state.get("complete")) is not bool:
            raise ValueError("invalid replay cursor state")
        if state["complete"]:
            return ReplayWindowSummary(committed, 0, True, state.get("last_event_time"))

        records = iter(iter_replay_records(root, include_dlq=include_dlq, from_date=from_date, to_date=to_date))
        for _ in range(committed):
            if next(records, None) is None:
                raise ValueError("replay cursor exceeds the validated source window")
        published = 0
        while not controller.stopped:
            batch = list(itertools.islice(records, batch_size))
            if not batch:
                state["complete"] = True
                _write_state(state_path, state)
                break
            for ordinal, record in enumerate(batch, start=committed):
                pacer.wait(record.event_time, ordinal)
                if controller.stopped:
                    return ReplayWindowSummary(committed, published, False, state.get("last_event_time"))
            state["pending"] = {"start": committed, "end": committed + len(batch)}
            _write_state(state_path, state)
            acknowledgements: list[Exception | None] = []
            try:
                for record in batch:
                    publisher.publish(record, acknowledgements.append, cancelled=lambda: False)
                remaining = publisher.flush(flush_timeout)
            except Exception as error:
                try:
                    publisher.flush(flush_timeout)
                except Exception:
                    pass
                raise ValueError("replay batch publication failed; pending acknowledgement is ambiguous") from error
            if remaining or len(acknowledgements) != len(batch) or any(error is not None for error in acknowledgements):
                raise ValueError("replay batch acknowledgement is ambiguous; reconcile before resuming")
            committed += len(batch)
            published += len(batch)
            state["next_index"] = committed
            state["last_event_time"] = batch[-1].event_time.isoformat()
            state["pending"] = None
            _write_state(state_path, state)
        return ReplayWindowSummary(committed, published, bool(state["complete"]), state.get("last_event_time"))
