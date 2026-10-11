"""Streaming integrity and reconciliation validation for generated datasets."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator


REPLAYABLE_DATASETS = {"web_events", "orders", "email_events"}
EXPECTED_SCHEMA_VERSIONS = {
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
EVENT_CONTRACTS = {
    "web_events": (
        {"web", "mobile"},
        {"page_view", "view_item", "add_to_cart", "begin_checkout", "purchase"},
    ),
    "orders": ({"commerce"}, {"order_created", "order_refunded"}),
    "email_events": (
        {"email"},
        {"email_sent", "email_delivered", "email_opened", "email_clicked", "email_unsubscribed"},
    ),
}
REQUIRED_EVENT_FIELDS = {
    "event_id",
    "event_name",
    "event_time",
    "ingested_at",
    "source",
    "schema_version",
    "customer_id",
    "anonymous_id",
    "session_id",
    "campaign_id",
    "platform_campaign_id",
    "click_id",
    "consent_analytics",
    "is_late",
    "payload",
}


@dataclass(frozen=True, slots=True)
class ValidationReport:
    errors: tuple[str, ...]
    files_checked: int
    rows_checked: int

    @property
    def valid(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, object]:
        return {
            "valid": self.valid,
            "files_checked": self.files_checked,
            "rows_checked": self.rows_checked,
            "errors": list(self.errors),
        }


def validate_dataset(root: Path) -> ValidationReport:
    """Validate file integrity, schemas, references, and reconciled metrics."""

    root = Path(root)
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        return ValidationReport(
            errors=(f"manifest not found: {manifest_path}",),
            files_checked=0,
            rows_checked=0,
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return ValidationReport(
            errors=(f"invalid manifest: {error}",),
            files_checked=0,
            rows_checked=0,
        )

    errors: list[str] = []
    if manifest.get("schema_versions") != EXPECTED_SCHEMA_VERSIONS:
        errors.append(
            "schema_versions mismatch: "
            f"manifest={manifest.get('schema_versions')} expected={EXPECTED_SCHEMA_VERSIONS}"
        )
    verified_files: list[dict[str, object]] = []
    files_checked = 0
    for item in manifest.get("files", []):
        relative_path = str(item.get("relative_path", ""))
        path = root / relative_path
        if not path.is_file():
            errors.append(f"missing file: {relative_path}")
            continue
        try:
            content = path.read_bytes()
        except OSError as error:
            errors.append(f"cannot read {relative_path}: {error}")
            continue
        actual_hash = hashlib.sha256(content).hexdigest()
        if actual_hash != item.get("sha256"):
            errors.append(f"checksum mismatch: {relative_path}")
            continue
        if len(content) != item.get("byte_size"):
            errors.append(f"byte-size mismatch: {relative_path}")
            continue
        verified_files.append(item)
        files_checked += 1

    master = _load_master_indexes(root, verified_files, errors)
    semantic = _SemanticState(master=master, errors=errors)
    actual_dataset_counts: dict[str, int] = {}
    rows_checked = 0
    last_event_times: dict[tuple[str, str], datetime] = {}

    last_partition: str | None = None
    for item in sorted(verified_files, key=_validation_order):
        dataset = str(item["dataset"])
        relative_path = str(item["relative_path"])
        partition = _partition_from_path(relative_path)
        if partition is not None and last_partition not in {None, partition}:
            semantic.finish_partition(last_partition)
        if partition is not None:
            last_partition = partition
        path = root / relative_path
        row_count = 0
        file_min_event_time: datetime | None = None
        file_max_event_time: datetime | None = None
        try:
            for row in _iter_rows(path, str(item["format"])):
                row_count += 1
                rows_checked += 1
                if dataset in REPLAYABLE_DATASETS:
                    event_time = _validate_event_envelope(
                        dataset, relative_path, row, master, errors
                    )
                    if event_time is not None:
                        order_key = (dataset, partition or "")
                        previous_event_time = last_event_times.get(order_key)
                        if previous_event_time is not None and event_time < previous_event_time:
                            errors.append(
                                f"nondecreasing event_time violated: {relative_path}"
                            )
                        last_event_times[order_key] = event_time
                        file_min_event_time = file_min_event_time or event_time
                        file_max_event_time = event_time
                semantic.observe(dataset, relative_path, row)
        except (OSError, EOFError, gzip.BadGzipFile, UnicodeDecodeError, json.JSONDecodeError) as error:
            errors.append(f"cannot parse {relative_path}: {error}")
            continue
        if row_count != item.get("row_count"):
            errors.append(
                f"row-count mismatch: {relative_path}; manifest={item.get('row_count')} actual={row_count}"
            )
        if file_min_event_time is not None and file_max_event_time is not None:
            actual_bounds = (
                _iso(file_min_event_time),
                _iso(file_max_event_time),
            )
            manifest_bounds = (
                item.get("min_event_time"),
                item.get("max_event_time"),
            )
            if actual_bounds != manifest_bounds:
                errors.append(
                    f"time-bound mismatch: {relative_path}; "
                    f"manifest={manifest_bounds} actual={actual_bounds}"
                )
        actual_dataset_counts[dataset] = (
            actual_dataset_counts.get(dataset, 0) + row_count
        )

    expected_dataset_counts = manifest.get("datasets", {})
    if actual_dataset_counts != expected_dataset_counts:
        errors.append(
            "dataset row-count mismatch: "
            f"manifest={expected_dataset_counts} actual={actual_dataset_counts}"
        )

    if last_partition is not None:
        semantic.finish_partition(last_partition)
    semantic.finish(manifest)
    return ValidationReport(
        errors=tuple(dict.fromkeys(errors)),
        files_checked=files_checked,
        rows_checked=rows_checked,
    )


@dataclass(slots=True)
class _MasterIndexes:
    customer_ids: set[str]
    product_ids: set[str]
    campaign_ids: set[str]
    vendor_to_campaign: dict[str, str]
    no_analytics_consent_customers: int


@dataclass(slots=True)
class _SemanticState:
    master: _MasterIndexes
    errors: list[str]
    previous_event_ids: dict[str, str] = field(default_factory=dict)
    purchases: dict[str, tuple[object, ...]] = field(default_factory=dict)
    orders: dict[str, tuple[object, ...]] = field(default_factory=dict)
    vendor_reports: dict[tuple[str, str], tuple[int, int, int, int, int]] = field(default_factory=dict)
    truth: dict[tuple[str, str], tuple[int, int, int, int, int]] = field(default_factory=dict)
    truth_totals: dict[str, int] = field(default_factory=dict)
    anomalies: dict[str, int] = field(default_factory=dict)
    order_metrics: dict[tuple[str, str], list[int]] = field(default_factory=dict)
    refund_truth: dict[tuple[str, str], int] = field(default_factory=dict)
    session_stages: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.truth_totals = {
            "impressions": 0,
            "clicks": 0,
            "spend_minor": 0,
            "orders": 0,
            "gross_revenue_minor": 0,
            "refunds_minor": 0,
            "net_revenue_minor": 0,
        }
        self.anomalies = {
            "duplicate_events": 0,
            "late_events": 0,
            "invalid_cases": 0,
            "no_analytics_consent_customers": self.master.no_analytics_consent_customers,
            "bot_events": 0,
            "refund_events": 0,
        }

    def observe(
        self,
        dataset: str,
        relative_path: str,
        row: dict[str, object],
    ) -> None:
        self._observe_anomalies(dataset, row)
        if dataset in REPLAYABLE_DATASETS:
            event_id = str(row.get("event_id", ""))
            if self.previous_event_ids.get(dataset) == event_id:
                return
            self.previous_event_ids[dataset] = event_id

        if dataset == "web_events":
            self._observe_web(relative_path, row)
        elif dataset == "orders":
            self._observe_order(relative_path, row)
        elif dataset == "email_events":
            self._check_customer(relative_path, row.get("customer_id"))
        elif dataset == "crm":
            self._check_customer(relative_path, row.get("customer_id"))
        elif dataset in {"google_ads", "meta_ads", "tiktok_ads"}:
            self._observe_vendor(dataset, relative_path, row)
        elif dataset == "ground_truth":
            self._observe_truth(relative_path, row)
        elif dataset == "quality_cases":
            self._observe_quality(relative_path, row)

    def _observe_anomalies(
        self,
        dataset: str,
        row: dict[str, object],
    ) -> None:
        if dataset in REPLAYABLE_DATASETS:
            event_id = str(row.get("event_id", ""))
            if self.previous_event_ids.get(dataset) == event_id:
                self.anomalies["duplicate_events"] += 1
            self.anomalies["late_events"] += int(
                row.get("is_late") is True
            )
        if dataset == "quality_cases":
            self.anomalies["invalid_cases"] += 1
        if dataset == "web_events":
            payload = row.get("payload")
            if isinstance(payload, dict):
                self.anomalies["bot_events"] += int(
                    payload.get("is_bot") is True
                )
        if dataset == "orders" and row.get("event_name") == "order_refunded":
            self.anomalies["refund_events"] += 1

    def finish_partition(self, day: str) -> None:
        if self.purchases != self.orders:
            missing_orders = sorted(set(self.purchases) - set(self.orders))[:3]
            missing_purchases = sorted(set(self.orders) - set(self.purchases))[:3]
            differing = sorted(
                order_id
                for order_id in set(self.purchases) & set(self.orders)
                if self.purchases[order_id] != self.orders[order_id]
            )[:3]
            self.errors.append(
                "purchase/order mismatch: "
                f"missing_orders={missing_orders} missing_purchases={missing_purchases} differing={differing}"
            )

        self.purchases.clear()
        self.orders.clear()
        self.previous_event_ids.clear()
        self.session_stages.clear()

        all_keys = {
            key
            for key in set(self.truth) | set(self.vendor_reports)
            if key[0] == day
        }
        for key in sorted(all_keys):
            expected = self.truth.get(key)
            actual = self.vendor_reports.get(key)
            if expected != actual:
                self.errors.append(
                    "vendor/ground-truth mismatch: "
                    f"date={key[0]} campaign_id={key[1]} truth={expected} vendor={actual}"
                )

        truth_keys = {key for key in self.truth if key[0] == day}
        event_keys = {key for key in self.order_metrics if key[0] == day}
        for key in sorted(truth_keys | event_keys):
            truth = self.truth.get(key)
            expected = None if truth is None else (truth[3], truth[4], int(self._truth_refunds(key)))
            actual_values = self.order_metrics.get(key, [0, 0, 0])
            actual = tuple(actual_values)
            if expected != actual:
                self.errors.append(
                    "orders/ground-truth mismatch: "
                    f"date={key[0]} campaign_id={key[1]} truth={expected} events={actual}"
                )

        for mapping in (
            self.truth,
            self.vendor_reports,
            self.order_metrics,
            self.refund_truth,
        ):
            for key in [candidate for candidate in mapping if candidate[0] == day]:
                del mapping[key]

    def finish(self, manifest: dict[str, object]) -> None:

        manifest_totals = manifest.get("totals", {})
        if self.truth_totals != manifest_totals:
            self.errors.append(
                "manifest/ground-truth totals mismatch: "
                f"manifest={manifest_totals} truth={self.truth_totals}"
            )

        manifest_anomalies = manifest.get("anomalies", {})
        if self.anomalies != manifest_anomalies:
            self.errors.append(
                "anomaly-count mismatch: "
                f"manifest={manifest_anomalies} actual={self.anomalies}"
            )

    def _truth_refunds(self, key: tuple[str, str]) -> int:
        return self.refund_truth.get(key, 0)

    def _observe_web(self, path: str, row: dict[str, object]) -> None:
        self._check_customer(path, row.get("customer_id"))
        self._check_campaign(path, row.get("campaign_id"))
        payload = row.get("payload")
        if not isinstance(payload, dict):
            self.errors.append(f"invalid payload object: {path}")
            return
        session_id = str(row.get("session_id", ""))
        event_name = str(row.get("event_name", ""))
        stage = {
            "page_view": 0,
            "view_item": 1,
            "add_to_cart": 2,
            "begin_checkout": 3,
            "purchase": 4,
        }.get(event_name)
        previous_stage = self.session_stages.get(session_id, -1)
        if stage is not None:
            required_previous = max(0, stage - 1)
            if stage > 0 and previous_stage < required_previous:
                self.errors.append(
                    f"broken funnel for session_id {session_id}: "
                    f"event={event_name} previous_stage={previous_stage}: {path}"
                )
            self.session_stages[session_id] = max(previous_stage, stage)
        product_id = payload.get("product_id")
        if product_id is not None and str(product_id) not in self.master.product_ids:
            self.errors.append(f"unknown product_id {product_id}: {path}")
        if row.get("event_name") == "purchase":
            order_id = str(payload.get("order_id", ""))
            self.purchases[order_id] = (
                int(payload.get("amount_minor", -1)),
                row.get("customer_id"),
                row.get("session_id"),
                row.get("campaign_id"),
                row.get("click_id"),
            )

    def _observe_order(self, path: str, row: dict[str, object]) -> None:
        self._check_customer(path, row.get("customer_id"))
        self._check_campaign(path, row.get("campaign_id"))
        payload = row.get("payload")
        if not isinstance(payload, dict):
            self.errors.append(f"invalid payload object: {path}")
            return
        attributed = payload.get("attributed_campaign_id")
        self._check_campaign(path, attributed)
        for item in payload.get("items", []):
            if isinstance(item, dict):
                product_id = item.get("product_id")
                if str(product_id) not in self.master.product_ids:
                    self.errors.append(f"unknown product_id {product_id}: {path}")
        refund_amount = int(payload.get("refund_amount_minor", -1))
        gross_amount = int(payload.get("gross_amount_minor", -1))
        if row.get("event_name") == "order_refunded" and not (
            0 < refund_amount <= gross_amount
        ):
            self.errors.append(
                f"invalid refund amount {refund_amount} for gross {gross_amount}: {path}"
            )
        if row.get("event_name") == "order_created":
            order_id = str(payload.get("order_id", ""))
            self.orders[order_id] = (
                int(payload.get("gross_amount_minor", -1)),
                row.get("customer_id"),
                row.get("session_id"),
                payload.get("attributed_campaign_id"),
                row.get("click_id"),
            )
            key = (_event_day(row), str(payload.get("attributed_campaign_id", "")))
            metrics = self.order_metrics.setdefault(key, [0, 0, 0])
            metrics[0] += 1
            metrics[1] += gross_amount
        elif row.get("event_name") == "order_refunded":
            key = (_event_day(row), str(payload.get("attributed_campaign_id", "")))
            metrics = self.order_metrics.setdefault(key, [0, 0, 0])
            metrics[2] += refund_amount

    def _observe_vendor(
        self,
        dataset: str,
        path: str,
        row: dict[str, object],
    ) -> None:
        vendor_id = str(row.get("campaign_id", ""))
        campaign_id = self.master.vendor_to_campaign.get(vendor_id)
        if campaign_id is None:
            self.errors.append(f"unknown platform campaign_id {vendor_id}: {path}")
            return
        if dataset == "google_ads":
            day = str(row["segments_date"])
            values = (
                int(row["impressions"]),
                int(row["clicks"]),
                int(row["cost_micros"]) // 1_000_000,
                int(row["conversions"]),
                int(row["conversions_value"]),
            )
        elif dataset == "meta_ads":
            day = str(row["date_start"])
            actions = row.get("actions", [])
            action_values = row.get("action_values", [])
            values = (
                int(row["impressions"]),
                int(row["clicks"]),
                int(row["spend"]),
                int(actions[0]["value"]),
                int(action_values[0]["value"]),
            )
        else:
            day = str(row["stat_time_day"])
            values = (
                int(row["impressions"]),
                int(row["clicks"]),
                int(row["spend"]),
                int(row["conversion"]),
                int(row["total_purchase_value"]),
            )
        self.vendor_reports[(day, campaign_id)] = values

    def _observe_truth(self, path: str, row: dict[str, object]) -> None:
        campaign_id = str(row.get("campaign_id", ""))
        self._check_campaign(path, campaign_id)
        day = str(row.get("date", ""))
        self.truth[(day, campaign_id)] = (
            int(row["impressions"]),
            int(row["clicks"]),
            int(row["spend_minor"]),
            int(row["orders"]),
            int(row["gross_revenue_minor"]),
        )
        self.refund_truth[(day, campaign_id)] = int(row["refunds_minor"])
        for key in self.truth_totals:
            self.truth_totals[key] += int(row[key])

    def _observe_quality(self, path: str, row: dict[str, object]) -> None:
        expected = row.get("expected_error_code")
        record = row.get("record")
        if expected not in {
            "MISSING_REQUIRED_FIELD",
            "UNSUPPORTED_SCHEMA_VERSION",
            "ORPHAN_CLICK_ID",
        } or not isinstance(record, dict):
            self.errors.append(f"invalid quality case metadata: {path}")
            return
        missing = REQUIRED_EVENT_FIELDS - set(record)
        if expected == "MISSING_REQUIRED_FIELD" and missing != {"event_id"}:
            self.errors.append(f"quality case has unexpected missing fields {sorted(missing)}: {path}")
        elif expected != "MISSING_REQUIRED_FIELD" and missing:
            self.errors.append(f"quality case is missing unrelated fields {sorted(missing)}: {path}")
        if expected == "UNSUPPORTED_SCHEMA_VERSION" and record.get("schema_version") == 1:
            self.errors.append(f"quality case does not have unsupported schema: {path}")
        if expected == "ORPHAN_CLICK_ID" and not str(record.get("click_id", "")).startswith("orphan-"):
            self.errors.append(f"quality case does not have orphan click_id: {path}")

    def _check_customer(self, path: str, value: object) -> None:
        if value is not None and str(value) not in self.master.customer_ids:
            self.errors.append(f"unknown customer_id {value}: {path}")

    def _check_campaign(self, path: str, value: object) -> None:
        if value is not None and str(value) not in self.master.campaign_ids:
            self.errors.append(f"unknown campaign_id {value}: {path}")


def _load_master_indexes(
    root: Path,
    verified_files: list[dict[str, object]],
    errors: list[str],
) -> _MasterIndexes:
    customer_ids: set[str] = set()
    product_ids: set[str] = set()
    campaign_ids: set[str] = set()
    vendor_to_campaign: dict[str, str] = {}
    no_analytics_consent_customers = 0
    ads: list[tuple[str, str]] = []
    for item in verified_files:
        dataset = str(item["dataset"])
        if not dataset.startswith("master/"):
            continue
        path = root / str(item["relative_path"])
        for row in _iter_rows(path, str(item["format"])):
            if dataset == "master/customers":
                customer_ids.add(str(row["customer_id"]))
                no_analytics_consent_customers += int(
                    str(row["consent_analytics"]).lower() == "false"
                )
            elif dataset == "master/products":
                product_ids.add(str(row["product_id"]))
            elif dataset == "master/campaigns":
                campaign_id = str(row["campaign_id"])
                campaign_ids.add(campaign_id)
                vendor_to_campaign[str(row["platform_campaign_id"])] = campaign_id
            elif dataset == "master/ads":
                ads.append((str(row["ad_id"]), str(row["campaign_id"])))
    for ad_id, campaign_id in ads:
        if campaign_id not in campaign_ids:
            errors.append(f"ad {ad_id} references unknown campaign_id {campaign_id}")
    return _MasterIndexes(
        customer_ids=customer_ids,
        product_ids=product_ids,
        campaign_ids=campaign_ids,
        vendor_to_campaign=vendor_to_campaign,
        no_analytics_consent_customers=no_analytics_consent_customers,
    )


def _validate_event_envelope(
    dataset: str,
    path: str,
    row: dict[str, object],
    master: _MasterIndexes,
    errors: list[str],
) -> datetime | None:
    missing = REQUIRED_EVENT_FIELDS - set(row)
    if missing:
        errors.append(f"missing event fields {sorted(missing)}: {path}")
        return None
    if row.get("schema_version") != 1:
        errors.append(f"unsupported schema_version {row.get('schema_version')}: {path}")
    allowed_sources, allowed_events = EVENT_CONTRACTS[dataset]
    if row.get("source") not in allowed_sources:
        errors.append(f"invalid source {row.get('source')!r} for {dataset}: {path}")
    if row.get("event_name") not in allowed_events:
        errors.append(
            f"invalid event_name {row.get('event_name')!r} for {dataset}: {path}"
        )
    if not isinstance(row.get("consent_analytics"), bool):
        errors.append(f"consent_analytics must be boolean: {path}")
    if not isinstance(row.get("is_late"), bool):
        errors.append(f"is_late must be boolean: {path}")
    if not isinstance(row.get("payload"), dict):
        errors.append(f"payload must be an object: {path}")
    try:
        event_time = datetime.fromisoformat(
            str(row["event_time"]).replace("Z", "+00:00")
        )
    except ValueError:
        errors.append(f"invalid event_time {row.get('event_time')}: {path}")
        return None
    if event_time.utcoffset() != timedelta(0):
        errors.append(f"event_time must be UTC: {path}")
    try:
        ingested_at = datetime.fromisoformat(
            str(row["ingested_at"]).replace("Z", "+00:00")
        )
        if ingested_at.utcoffset() != timedelta(0):
            errors.append(f"ingested_at must be UTC: {path}")
        if ingested_at < event_time:
            errors.append(f"ingested_at precedes event_time: {path}")
    except ValueError:
        errors.append(f"invalid ingested_at {row.get('ingested_at')}: {path}")
    partition = next(
        (
            component.removeprefix("date=")
            for component in Path(path).parts
            if component.startswith("date=")
        ),
        None,
    )
    if partition != event_time.date().isoformat():
        errors.append(
            f"partition date mismatch: partition={partition} event_time={event_time.date()}: {path}"
        )
    return event_time


def _iter_rows(path: Path, file_format: str) -> Iterator[dict[str, object]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as stream:
        if file_format == "csv":
            for row in csv.DictReader(stream):
                yield dict(row)
        elif file_format == "jsonl":
            for line in stream:
                if line.strip():
                    row = json.loads(line)
                    if not isinstance(row, dict):
                        raise json.JSONDecodeError("row is not an object", line, 0)
                    yield row
        else:
            raise ValueError(f"unsupported file format {file_format!r}")


def _partition_from_path(relative_path: str) -> str | None:
    return next(
        (
            component.removeprefix("date=")
            for component in Path(relative_path).parts
            if component.startswith("date=")
        ),
        None,
    )


def _validation_order(item: dict[str, object]) -> tuple[int, str, int, str]:
    relative_path = str(item.get("relative_path", ""))
    dataset = str(item.get("dataset", ""))
    partition = _partition_from_path(relative_path)
    dataset_order = {
        "web_events": 0,
        "orders": 1,
        "email_events": 2,
        "crm": 3,
        "google_ads": 4,
        "meta_ads": 4,
        "tiktok_ads": 4,
        "ground_truth": 5,
        "quality_cases": 6,
    }.get(dataset, 7)
    return (0 if partition is None else 1, partition or "", dataset_order, relative_path)


def _event_day(row: dict[str, object]) -> str:
    return str(row.get("event_time", ""))[:10]


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
