"""End-to-end assembly of deterministic MarketPulse mock datasets."""

from __future__ import annotations

import json
import shutil
from dataclasses import asdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable

from . import __version__
from .config import GenerationConfig
from .journeys import SourceRecord, iter_day_records
from .manifest import Manifest
from .masters import generate_master_data
from .models import Ad, Campaign, Customer, MasterData, Product
from .writers import DatasetWriter, FileStat


SCHEMA_VERSIONS = {
    "master/customers": 1,
    "master/products": 1,
    "master/campaigns": 1,
    "master/ads": 1,
    "google_ads": 1,
    "meta_ads": 1,
    "tiktok_ads": 1,
    "web_events": 1,
    "orders": 1,
    "crm": 1,
    "email_events": 1,
    "quality_cases": 1,
    "ground_truth": 1,
}


def generate_dataset(
    config: GenerationConfig,
    overwrite: bool = False,
) -> Manifest:
    """Generate a complete dataset and deterministic manifest."""

    output_dir = _prepare_output(config.output_dir, overwrite)
    masters = generate_master_data(config)
    writer = DatasetWriter(output_dir, config.rows_per_file)
    anomaly_counts = {
        "duplicate_events": 0,
        "late_events": 0,
        "invalid_cases": 0,
        "no_analytics_consent_customers": sum(
            not customer.consent_analytics for customer in masters.customers
        ),
        "bot_events": 0,
        "refund_events": 0,
    }
    totals = {
        "impressions": 0,
        "clicks": 0,
        "spend_minor": 0,
        "orders": 0,
        "gross_revenue_minor": 0,
        "refunds_minor": 0,
        "net_revenue_minor": 0,
    }
    previous_event_ids: dict[str, str] = {}

    try:
        for record in _master_records(config, masters):
            writer.write(record)
        for day_index in range(config.profile.days):
            for record in iter_day_records(config, masters, day_index):
                _observe_record(
                    record,
                    previous_event_ids,
                    anomaly_counts,
                    totals,
                )
                writer.write(record)
            writer.seal_day(config.start_date + timedelta(days=day_index))
        file_stats = writer.close()
    except BaseException:
        writer.close()
        raise

    datasets = _dataset_counts(file_stats)
    missing_datasets = set(SCHEMA_VERSIONS) - set(datasets)
    if missing_datasets:
        missing = ", ".join(sorted(missing_datasets))
        raise RuntimeError(f"generation produced no records for: {missing}")

    end_date = config.start_date + timedelta(days=config.profile.days - 1)
    manifest = Manifest(
        generator_version=__version__,
        synthetic=True,
        profile=config.profile.name,
        seed=config.seed,
        start_date=config.start_date.isoformat(),
        end_date=end_date.isoformat(),
        configuration={
            "days": config.profile.days,
            "customers": config.profile.customers,
            "campaigns": config.profile.campaigns,
            "products": config.profile.products,
            "impressions_per_day": config.profile.impressions_per_day,
            "rows_per_file": config.rows_per_file,
            "currency": "VND",
            "timezone": "UTC",
            "anomaly_rates": asdict(config.anomalies),
        },
        schema_versions=dict(SCHEMA_VERSIONS),
        datasets=datasets,
        anomalies=anomaly_counts,
        totals=totals,
        files=file_stats,
    )
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def _prepare_output(output_dir: Path, overwrite: bool) -> Path:
    requested_output = Path(output_dir).expanduser()
    if requested_output.is_symlink():
        raise ValueError("output directory must not be a symbolic link")
    output = requested_output.resolve()
    if output.exists() and any(output.iterdir()):
        if not overwrite:
            raise FileExistsError(f"output directory is not empty: {output}")
        _assert_safe_overwrite_target(output)
        shutil.rmtree(output)
    output.mkdir(parents=True, exist_ok=True)
    return output


def _assert_safe_overwrite_target(output: Path) -> None:
    protected = {Path("/").resolve(), Path.home().resolve(), Path.cwd().resolve()}
    contains_protected_path = any(
        protected_path == output or output in protected_path.parents
        for protected_path in protected
    )
    if contains_protected_path or len(output.parts) < 3:
        raise ValueError(f"refusing unsafe overwrite target: {output}")


def _master_records(
    config: GenerationConfig,
    masters: MasterData,
) -> Iterable[SourceRecord]:
    event_time = datetime.combine(config.start_date, time.min, tzinfo=timezone.utc)
    groups: tuple[
        tuple[str, tuple[Customer | Product | Campaign | Ad, ...]], ...
    ] = (
        ("master/customers", masters.customers),
        ("master/products", masters.products),
        ("master/campaigns", masters.campaigns),
        ("master/ads", masters.ads),
    )
    for dataset, entities in groups:
        for entity in entities:
            yield SourceRecord(
                dataset=dataset,
                event_time=event_time,
                payload=_json_ready(asdict(entity)),
            )


def _json_ready(value: object) -> object:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def _observe_record(
    record: SourceRecord,
    previous_event_ids: dict[str, str],
    anomaly_counts: dict[str, int],
    totals: dict[str, int],
) -> None:
    if record.dataset in {"web_events", "orders", "email_events"}:
        event_id = str(record.payload["event_id"])
        if previous_event_ids.get(record.dataset) == event_id:
            anomaly_counts["duplicate_events"] += 1
        previous_event_ids[record.dataset] = event_id
        anomaly_counts["late_events"] += int(
            bool(record.payload.get("is_late", False))
        )
    if record.dataset == "quality_cases":
        anomaly_counts["invalid_cases"] += 1
    if record.dataset == "web_events":
        payload = record.payload.get("payload", {})
        if isinstance(payload, dict):
            anomaly_counts["bot_events"] += int(
                bool(payload.get("is_bot", False))
            )
    if (
        record.dataset == "orders"
        and record.payload.get("event_name") == "order_refunded"
    ):
        anomaly_counts["refund_events"] += 1
    if record.dataset == "ground_truth":
        for key in totals:
            totals[key] += int(record.payload[key])


def _dataset_counts(file_stats: tuple[FileStat, ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for stat in file_stats:
        counts[stat.dataset] = counts.get(stat.dataset, 0) + stat.row_count
    return dict(sorted(counts.items()))
