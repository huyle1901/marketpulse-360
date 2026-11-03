"""Fail-closed daily source selection without modifying archive lineage."""

from __future__ import annotations

from datetime import date, datetime, timedelta

from marketpulse_lakehouse.source_files import (
    ALLOWED_SOURCE_DATASETS, SourceArchivePlan, iter_source_file_rows, verify_source_archive_object,
)
from .contracts import BatchSelection, REPORT_PLATFORMS

DAY_FIELDS = {
    "google_ads": ("segments_date",),
    "meta_ads": ("date_start", "date_stop"),
    "tiktok_ads": ("stat_time_day",),
    "shopee_ads": ("date",),
    "crm": ("snapshot_date",),
}


def source_day(dataset: str, value: str) -> date:
    """Parse a source row's day: Shopee reports are day-first, the rest ISO."""
    if dataset == "shopee_ads":
        return datetime.strptime(value, "%d/%m/%Y").date()
    return date.fromisoformat(value)


def _row_matches_day(dataset: str, row: dict, field: str, day: date) -> bool:
    value = row.get(field)
    if not isinstance(value, str):
        return False
    expected = f"{day:%d/%m/%Y}" if dataset == "shopee_ads" else day.isoformat()
    return value == expected


def _require_declared_platforms(plan: SourceArchivePlan, required_platforms: tuple[str, ...]) -> None:
    """Fail closed when the world has campaigns on a platform whose report the stack skips.

    Otherwise that platform's orders and revenue would be certified with no spend.
    """
    for source in plan.source_files:
        if source.source_dataset != "master/campaigns":
            continue
        verify_source_archive_object(source)
        platforms = {f"{row['platform']}_ads" for row in iter_source_file_rows(source)}
        if undeclared := sorted(platforms - set(required_platforms)):
            raise ValueError(
                f"campaigns name {', '.join(undeclared)} but the stack does not require that report; "
                "add it to BATCH_REQUIRED_PLATFORMS"
            )


def select_range_sources(plan: SourceArchivePlan, from_date: date, to_date: date,
                         required_platforms: tuple[str, ...]) -> SourceArchivePlan:
    """Pin complete daily ad/CRM inputs in one half-open history window."""
    if type(from_date) is not date or type(to_date) is not date or from_date >= to_date:
        raise ValueError("invalid UTC source [from_date,to_date) window")
    if (to_date - from_date).days > 731:
        raise ValueError("source window exceeds 731 days")
    required_platforms = BatchSelection(from_date, required_platforms).required_platforms
    _require_declared_platforms(plan, required_platforms)
    selected = []
    observed: dict[date, set[str]] = {}
    for source in plan.source_files:
        if source.source_dataset.startswith("master/"):
            selected.append(source)
            continue
        if source.source_dataset not in DAY_FIELDS:
            raise ValueError(f"unsupported batch source: {source.source_dataset}")
        day = source.source_date
        if day is None:
            raise ValueError(f"missing daily date partition: {source.source_file}")
        if not from_date <= day < to_date:
            continue
        if source.source_dataset in REPORT_PLATFORMS and source.source_dataset not in required_platforms:
            continue
        verify_source_archive_object(source)
        count = 0
        for count, row in enumerate(iter_source_file_rows(source), start=1):
            for field in DAY_FIELDS[source.source_dataset]:
                if not _row_matches_day(source.source_dataset, row, field, day):
                    raise ValueError(f"source date mismatch: {source.source_file}:{count}:{field}")
        if count != source.row_count:
            raise ValueError(f"row-count mismatch: {source.source_file}")
        if count:
            observed.setdefault(day, set()).add(source.source_dataset)
        selected.append(source)
    required = {"crm", *required_platforms}
    for offset in range((to_date - from_date).days):
        day = from_date + timedelta(days=offset)
        if missing := required - observed.get(day, set()):
            raise ValueError(f"missing or empty daily sources for {day}: {', '.join(sorted(missing))}")
    manifests = tuple(obj for obj in plan.archive_objects if obj.source_dataset == "_manifest")
    if len(manifests) != 1:
        raise ValueError("source plan must contain exactly one immutable manifest")
    files = tuple(selected)
    return SourceArchivePlan(plan.manifest_sha256, (*manifests, *files), files)


def select_daily_sources(plan: SourceArchivePlan, selection: BatchSelection) -> SourceArchivePlan:
    _require_declared_platforms(plan, selection.required_platforms)
    selected = []
    observed: set[str] = set()
    for source in plan.source_files:
        if source.source_dataset.startswith("master/"):
            selected.append(source)
            continue
        if source.source_dataset not in DAY_FIELDS:
            raise ValueError(f"unsupported batch source: {source.source_dataset}")
        if source.source_date is None:
            raise ValueError(f"missing daily date partition: {source.source_file}")
        if source.source_date != selection.logical_day:
            continue
        if source.source_dataset in REPORT_PLATFORMS and source.source_dataset not in selection.required_platforms:
            continue
        verify_source_archive_object(source)
        count = 0
        for count, row in enumerate(iter_source_file_rows(source), start=1):
            for field in DAY_FIELDS[source.source_dataset]:
                if not _row_matches_day(source.source_dataset, row, field, selection.logical_day):
                    raise ValueError(f"source date mismatch: {source.source_file}:{count}:{field}")
        if count != source.row_count:
            raise ValueError(f"row-count mismatch: {source.source_file}")
        if count:
            observed.add(source.source_dataset)
        selected.append(source)
    missing = ({"crm", *selection.required_platforms} - observed)
    if missing:
        raise ValueError(f"missing or empty daily sources: {', '.join(sorted(missing))}")
    files = tuple(selected)
    manifests = tuple(obj for obj in plan.archive_objects if obj.source_dataset == "_manifest")
    if len(manifests) != 1:
        raise ValueError("source plan must contain exactly one immutable manifest")
    return SourceArchivePlan(plan.manifest_sha256, (*manifests, *files), files)


def select_source_datasets(plan: SourceArchivePlan, datasets: tuple[str, ...]) -> SourceArchivePlan:
    if not datasets or len(datasets) != len(set(datasets)) or not set(datasets) <= ALLOWED_SOURCE_DATASETS:
        raise ValueError("dataset selection must be distinct allowlisted source names")
    files = tuple(source for source in plan.source_files if source.source_dataset in datasets)
    missing = set(datasets) - {source.source_dataset for source in files}
    if missing:
        raise ValueError(f"missing selected source datasets: {','.join(sorted(missing))}")
    manifests = tuple(source for source in plan.archive_objects if source.source_dataset == "_manifest")
    if len(manifests) != 1:
        raise ValueError("source plan must contain exactly one immutable manifest")
    return SourceArchivePlan(plan.manifest_sha256, (*manifests, *files), files)
