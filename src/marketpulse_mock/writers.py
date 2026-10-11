"""Deterministic, rotating gzip writers for source-shaped datasets."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, TextIO

from .journeys import SourceRecord


CSV_FIELDS: dict[str, tuple[str, ...]] = {
    "master/customers": (
        "customer_id",
        "email",
        "phone_token",
        "created_at",
        "country",
        "city",
        "loyalty_tier",
        "consent_analytics",
        "consent_marketing",
    ),
    "master/products": (
        "product_id",
        "sku",
        "name",
        "category",
        "brand",
        "unit_price_minor",
        "currency",
    ),
    "master/campaigns": (
        "campaign_id",
        "platform",
        "platform_campaign_id",
        "name",
        "objective",
        "status",
        "start_date",
        "end_date",
        "daily_budget_minor",
        "currency",
    ),
    "master/ads": (
        "ad_id",
        "campaign_id",
        "platform_ad_id",
        "name",
        "creative_format",
        "landing_page_url",
    ),
    "google_ads": (
        "segments_date",
        "campaign_id",
        "impressions",
        "clicks",
        "cost_micros",
        "conversions",
        "conversions_value",
    ),
    "tiktok_ads": (
        "stat_time_day",
        "campaign_id",
        "impressions",
        "clicks",
        "spend",
        "conversion",
        "total_purchase_value",
    ),
    "crm": (
        "snapshot_date",
        "customer_id",
        "email",
        "loyalty_tier",
        "lifetime_value_minor",
        "last_order_at",
        "consent_analytics",
        "consent_marketing",
    ),
}

JSONL_DATASETS = {
    "meta_ads",
    "web_events",
    "orders",
    "email_events",
    "quality_cases",
    "ground_truth",
}


@dataclass(frozen=True, slots=True)
class FileStat:
    dataset: str
    relative_path: str
    format: str
    row_count: int
    byte_size: int
    sha256: str
    min_event_time: str
    max_event_time: str


@dataclass(slots=True)
class _PartState:
    dataset: str
    partition_date: str | None
    part_number: int
    path: Path
    relative_path: str
    format: str
    stream: TextIO
    raw_stream: BinaryIO
    csv_writer: csv.DictWriter[str] | None
    row_count: int = 0
    min_event_time: datetime | None = None
    max_event_time: datetime | None = None


class DatasetWriter:
    """Write records to deterministic source-specific files with row rotation."""

    def __init__(self, root: Path, rows_per_file: int) -> None:
        if rows_per_file <= 0:
            raise ValueError("rows_per_file must be positive")
        self.root = Path(root)
        self.rows_per_file = rows_per_file
        self._parts: dict[tuple[str, str | None], _PartState] = {}
        self._next_part: dict[tuple[str, str | None], int] = {}
        self._last_event_time: dict[tuple[str, str | None], datetime] = {}
        self._stats: list[FileStat] = []
        self._closed = False

    def write(self, record: SourceRecord) -> None:
        if self._closed:
            raise RuntimeError("writer is closed")
        if record.event_time.tzinfo is None:
            raise ValueError("event_time must be timezone-aware")
        event_time = record.event_time.astimezone(timezone.utc)
        partition_date = (
            None if record.dataset.startswith("master/") else event_time.date().isoformat()
        )
        key = (record.dataset, partition_date)
        previous = self._last_event_time.get(key)
        if previous is not None and event_time < previous:
            raise ValueError(
                f"{record.dataset} requires nondecreasing event_time within partition"
            )
        self._last_event_time[key] = event_time

        state = self._parts.get(key)
        if state is None or state.row_count >= self.rows_per_file:
            if state is not None:
                self._stats.append(self._close_part(state))
            part_number = self._next_part.get(key, 0)
            state = self._open_part(
                record.dataset, partition_date, part_number
            )
            self._parts[key] = state
            self._next_part[key] = part_number + 1

        if state.format == "csv":
            assert state.csv_writer is not None
            state.csv_writer.writerow(record.payload)
        else:
            state.stream.write(
                json.dumps(
                    record.payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
            state.stream.write("\n")
        state.row_count += 1
        state.min_event_time = state.min_event_time or event_time
        state.max_event_time = event_time

    def close(self) -> tuple[FileStat, ...]:
        if self._closed:
            return tuple(sorted(self._stats, key=lambda stat: stat.relative_path))
        for state in self._parts.values():
            self._stats.append(self._close_part(state))
        self._parts.clear()
        self._closed = True
        return tuple(sorted(self._stats, key=lambda stat: stat.relative_path))

    def __enter__(self) -> DatasetWriter:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _open_part(
        self,
        dataset: str,
        partition_date: str | None,
        part_number: int,
    ) -> _PartState:
        file_format = _dataset_format(dataset)
        relative_path = _relative_path(
            dataset, partition_date, part_number, file_format
        )
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        raw_stream = path.open("wb")
        gzip_stream = gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=raw_stream,
            compresslevel=6,
            mtime=0,
        )
        text_stream = io.TextIOWrapper(
            gzip_stream, encoding="utf-8", newline="", write_through=False
        )
        csv_writer: csv.DictWriter[str] | None = None
        if file_format == "csv":
            csv_writer = csv.DictWriter(
                text_stream,
                fieldnames=CSV_FIELDS[dataset],
                extrasaction="raise",
                lineterminator="\n",
            )
            csv_writer.writeheader()
        return _PartState(
            dataset=dataset,
            partition_date=partition_date,
            part_number=part_number,
            path=path,
            relative_path=relative_path,
            format=file_format,
            stream=text_stream,
            raw_stream=raw_stream,
            csv_writer=csv_writer,
        )

    def _close_part(self, state: _PartState) -> FileStat:
        try:
            state.stream.close()
        finally:
            state.raw_stream.close()
        content = state.path.read_bytes()
        assert state.min_event_time is not None
        assert state.max_event_time is not None
        return FileStat(
            dataset=state.dataset,
            relative_path=state.relative_path,
            format=state.format,
            row_count=state.row_count,
            byte_size=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            min_event_time=_iso(state.min_event_time),
            max_event_time=_iso(state.max_event_time),
        )


def _dataset_format(dataset: str) -> str:
    if dataset in CSV_FIELDS:
        return "csv"
    if dataset in JSONL_DATASETS:
        return "jsonl"
    raise ValueError(f"unsupported dataset {dataset!r}")


def _relative_path(
    dataset: str,
    partition_date: str | None,
    part_number: int,
    file_format: str,
) -> str:
    extension = f"{file_format}.gz"
    if dataset.startswith("master/"):
        base = dataset.removeprefix("master/")
        filename = (
            f"{base}.{extension}"
            if part_number == 0
            else f"{base}-part-{part_number:05d}.{extension}"
        )
        return f"master/{filename}"
    assert partition_date is not None
    if dataset == "ground_truth":
        filename = (
            f"campaign_metrics.{extension}"
            if part_number == 0
            else f"campaign_metrics-part-{part_number:05d}.{extension}"
        )
    else:
        filename = f"part-{part_number:05d}.{extension}"
    return f"{dataset}/date={partition_date}/{filename}"


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
