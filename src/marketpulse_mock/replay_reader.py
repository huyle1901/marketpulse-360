"""Bounded-memory merge reader for MP-01 replayable source files."""

from __future__ import annotations

import gzip
import heapq
import json
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterator, TextIO

from .kafka_contracts import TOPIC_CONTRACTS, record_key
from .validator import validate_dataset


@dataclass(frozen=True, slots=True)
class ReplayRecord:
    dataset: str
    topic: str
    key: str
    value: bytes
    event_id: str
    event_time: datetime
    headers: tuple[tuple[str, bytes], ...]
    source_file: str


@dataclass(slots=True)
class _Cursor:
    dataset: str
    relative_path: str
    stream: TextIO
    ordinal: int = 0

    def next_row(self) -> dict[str, object] | None:
        for line in self.stream:
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError(f"row is not an object: {self.relative_path}")
            self.ordinal += 1
            return row
        return None


def iter_replay_records(
    root: Path,
    include_dlq: bool = False,
    *,
    from_date: date | None = None,
    to_date: date | None = None,
) -> Iterator[ReplayRecord]:
    """Validate and merge replayable parts in deterministic event-time order."""

    if (from_date is not None and type(from_date) is not date) or (to_date is not None and type(to_date) is not date) or (from_date is not None and to_date is not None and from_date >= to_date):
        raise ValueError("invalid UTC replay [from_date,to_date) window")

    root = Path(root)
    report = validate_dataset(root)
    if not report.valid:
        raise ValueError("dataset validation failed: " + "; ".join(report.errors))
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    allowed = {"web_events", "orders", "email_events"}
    if include_dlq:
        allowed.add("quality_cases")
    files_by_date: dict[date, list[dict[str, object]]] = {}
    for item in manifest["files"]:
        if item.get("dataset") not in allowed:
            continue
        relative_path = str(item["relative_path"])
        partition = _partition_date(relative_path)
        if (from_date is not None and partition < from_date) or (to_date is not None and partition >= to_date):
            continue
        files_by_date.setdefault(partition, []).append(item)

    for partition in sorted(files_by_date):
        yield from _merge_day(root, sorted(files_by_date[partition], key=lambda item: str(item["relative_path"])), partition)


def _partition_date(relative_path: str) -> date:
    matches = [part[5:] for part in Path(relative_path).parts if part.startswith("date=")]
    if len(matches) != 1:
        raise ValueError(f"replay file needs one date partition: {relative_path}")
    try:
        return date.fromisoformat(matches[0])
    except ValueError as error:
        raise ValueError(f"invalid replay date partition: {relative_path}") from error


def _merge_day(root: Path, files: list[dict[str, object]], partition: date) -> Iterator[ReplayRecord]:
    with ExitStack() as stack:
        cursors: list[_Cursor] = []
        heap: list[tuple[datetime, str, str, int, int, dict[str, object]]] = []
        for cursor_index, item in enumerate(files):
            relative_path = str(item["relative_path"])
            stream = stack.enter_context(
                gzip.open(root / relative_path, "rt", encoding="utf-8")
            )
            cursor = _Cursor(str(item["dataset"]), relative_path, stream)
            cursors.append(cursor)
            row = cursor.next_row()
            if row is not None:
                _push(heap, cursor_index, cursor, row)

        while heap:
            _, _, _, _, cursor_index, row = heapq.heappop(heap)
            cursor = cursors[cursor_index]
            candidate = _to_replay_record(cursor.dataset, cursor.relative_path, row)
            if candidate.event_time.date() != partition:
                raise ValueError(f"event time differs from replay partition: {cursor.relative_path}")
            yield candidate
            next_row = cursor.next_row()
            if next_row is not None:
                _push(heap, cursor_index, cursor, next_row)


def _push(
    heap: list[tuple[datetime, str, str, int, int, dict[str, object]]],
    cursor_index: int,
    cursor: _Cursor,
    row: dict[str, object],
) -> None:
    event_time = _parse_event_time(row)
    heapq.heappush(
        heap,
        (
            event_time,
            cursor.dataset,
            cursor.relative_path,
            cursor.ordinal,
            cursor_index,
            row,
        ),
    )


def _to_replay_record(
    dataset: str,
    relative_path: str,
    row: dict[str, object],
) -> ReplayRecord:
    if dataset == "quality_cases":
        value = {
            "case_id": row["case_id"],
            "event_time": row["event_time"],
            "source_dataset": row["original_dataset"],
            "source_topic": TOPIC_CONTRACTS[str(row["original_dataset"])].topic,
            "reason_code": row["expected_error_code"],
            "failed_record": row["record"],
        }
        event_id = str(row["case_id"])
    else:
        value = row
        event_id = str(row["event_id"])
    contract = TOPIC_CONTRACTS[dataset]
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return ReplayRecord(
        dataset=dataset,
        topic=contract.topic,
        key=record_key(dataset, value),
        value=encoded,
        event_id=event_id,
        event_time=_parse_event_time(row),
        headers=(
            ("dataset", dataset.encode("utf-8")),
            ("schema-version", b"1"),
            ("event-id", event_id.encode("utf-8")),
            ("source-file", relative_path.encode("utf-8")),
        ),
        source_file=relative_path,
    )


def _parse_event_time(row: dict[str, object]) -> datetime:
    try:
        return datetime.fromisoformat(
            str(row["event_time"]).replace("Z", "+00:00")
        )
    except (KeyError, ValueError) as error:
        raise ValueError(f"invalid replay event_time: {row.get('event_time')}") from error
