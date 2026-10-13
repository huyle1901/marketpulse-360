"""Fail-closed daily source selection without modifying archive lineage."""

from __future__ import annotations

from marketpulse_lakehouse.source_files import (
    SourceArchivePlan, iter_source_file_rows, verify_source_archive_object,
)
from .contracts import BatchSelection, REPORT_PLATFORMS

DAY_FIELDS = {
    "google_ads": ("segments_date",),
    "meta_ads": ("date_start", "date_stop"),
    "tiktok_ads": ("stat_time_day",),
    "crm": ("snapshot_date",),
}


def select_daily_sources(plan: SourceArchivePlan, selection: BatchSelection) -> SourceArchivePlan:
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
                if row.get(field) != selection.logical_day.isoformat():
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
