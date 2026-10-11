"""Streaming integrity and reconciliation validation for generated datasets."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator


REPLAYABLE_DATASETS = {"web_events", "orders", "email_events"}
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

    for item in verified_files:
        dataset = str(item["dataset"])
        relative_path = str(item["relative_path"])
        path = root / relative_path
        row_count = 0
        last_event_time: datetime | None = None
        try:
            for row in _iter_rows(path, str(item["format"])):
                row_count += 1
                rows_checked += 1
                if dataset in REPLAYABLE_DATASETS:
                    event_time = _validate_event_envelope(
                        dataset, relative_path, row, master, errors
                    )
                    if event_time is not None:
                        if last_event_time is not None and event_time < last_event_time:
                            errors.append(
                                f"nondecreasing event_time violated: {relative_path}"
                            )
                        last_event_time = event_time
                semantic.observe(dataset, relative_path, row)
        except (OSError, EOFError, gzip.BadGzipFile, UnicodeDecodeError, json.JSONDecodeError) as error:
            errors.append(f"cannot parse {relative_path}: {error}")
            continue
        if row_count != item.get("row_count"):
            errors.append(
                f"row-count mismatch: {relative_path}; manifest={item.get('row_count')} actual={row_count}"
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


@dataclass(slots=True)
class _SemanticState:
    master: _MasterIndexes
    errors: list[str]
    seen_event_ids: set[tuple[str, str]] = None  # type: ignore[assignment]
    purchases: dict[str, tuple[int, object, object]] = None  # type: ignore[assignment]
    orders: dict[str, tuple[int, object, object]] = None  # type: ignore[assignment]
    vendor_reports: dict[tuple[str, str], tuple[int, int, int, int]] = None  # type: ignore[assignment]
    truth: dict[tuple[str, str], tuple[int, int, int, int]] = None  # type: ignore[assignment]
    truth_totals: dict[str, int] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        self.seen_event_ids = set()
        self.purchases = {}
        self.orders = {}
        self.vendor_reports = {}
        self.truth = {}
        self.truth_totals = {
            "impressions": 0,
            "clicks": 0,
            "spend_minor": 0,
            "orders": 0,
            "gross_revenue_minor": 0,
            "refunds_minor": 0,
            "net_revenue_minor": 0,
        }

    def observe(
        self,
        dataset: str,
        relative_path: str,
        row: dict[str, object],
    ) -> None:
        if dataset in REPLAYABLE_DATASETS:
            event_id = str(row.get("event_id", ""))
            event_key = (dataset, event_id)
            if event_key in self.seen_event_ids:
                return
            self.seen_event_ids.add(event_key)

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

    def finish(self, manifest: dict[str, object]) -> None:
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

        all_keys = set(self.truth) | set(self.vendor_reports)
        for key in sorted(all_keys):
            expected = self.truth.get(key)
            actual = self.vendor_reports.get(key)
            if expected != actual:
                self.errors.append(
                    "vendor/ground-truth mismatch: "
                    f"date={key[0]} campaign_id={key[1]} truth={expected} vendor={actual}"
                )

        manifest_totals = manifest.get("totals", {})
        if self.truth_totals != manifest_totals:
            self.errors.append(
                "manifest/ground-truth totals mismatch: "
                f"manifest={manifest_totals} truth={self.truth_totals}"
            )

    def _observe_web(self, path: str, row: dict[str, object]) -> None:
        self._check_customer(path, row.get("customer_id"))
        self._check_campaign(path, row.get("campaign_id"))
        payload = row.get("payload")
        if not isinstance(payload, dict):
            self.errors.append(f"invalid payload object: {path}")
            return
        product_id = payload.get("product_id")
        if product_id is not None and str(product_id) not in self.master.product_ids:
            self.errors.append(f"unknown product_id {product_id}: {path}")
        if row.get("event_name") == "purchase":
            order_id = str(payload.get("order_id", ""))
            self.purchases[order_id] = (
                int(payload.get("amount_minor", -1)),
                row.get("customer_id"),
                row.get("session_id"),
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
        if row.get("event_name") == "order_created":
            order_id = str(payload.get("order_id", ""))
            self.orders[order_id] = (
                int(payload.get("gross_amount_minor", -1)),
                row.get("customer_id"),
                row.get("session_id"),
            )

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
                int(actions[0]["value"]),
                int(action_values[0]["value"]),
            )
        else:
            day = str(row["stat_time_day"])
            values = (
                int(row["impressions"]),
                int(row["clicks"]),
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
            int(row["orders"]),
            int(row["gross_revenue_minor"]),
        )
        for key in self.truth_totals:
            self.truth_totals[key] += int(row[key])

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
    ads: list[tuple[str, str]] = []
    for item in verified_files:
        dataset = str(item["dataset"])
        if not dataset.startswith("master/"):
            continue
        path = root / str(item["relative_path"])
        for row in _iter_rows(path, str(item["format"])):
            if dataset == "master/customers":
                customer_ids.add(str(row["customer_id"]))
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
    try:
        return datetime.fromisoformat(str(row["event_time"]).replace("Z", "+00:00"))
    except ValueError:
        errors.append(f"invalid event_time {row.get('event_time')}: {path}")
        return None


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

