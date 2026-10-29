"""Ordered Iceberg table contracts shared by builders and evidence checks."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ColumnContract:
    name: str
    iceberg_type: str
    nullable: bool = True
    operational: bool = False


@dataclass(frozen=True, slots=True)
class TableContract:
    name: str
    columns: tuple[ColumnContract, ...]
    key_fields: tuple[str, ...]
    partition_by: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        names = tuple(column.name for column in self.columns)
        if not self.name or "." not in self.name:
            raise ValueError("table name must be namespace-qualified")
        if len(names) != len(set(names)):
            raise ValueError(f"duplicate columns in {self.name}")
        missing_keys = set(self.key_fields).difference(names)
        if missing_keys:
            raise ValueError(f"unknown key fields in {self.name}: {sorted(missing_keys)}")

    @property
    def business_columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns if not column.operational)


def spark_schema_columns(schema: object) -> tuple[tuple[str, str, bool], ...]:
    """Return a stable name/type/nullability representation of a Spark schema."""

    fields = getattr(schema, "fields", None)
    if not isinstance(fields, (list, tuple)):
        raise TypeError("Spark schema must expose a fields sequence")
    columns: list[tuple[str, str, bool]] = []
    for field in fields:
        data_type = getattr(field, "dataType", None)
        simple_string = getattr(data_type, "simpleString", None)
        if not callable(simple_string):
            raise TypeError("Spark field data type must expose simpleString()")
        kind = str(simple_string()).lower()
        kind = {"bigint": "long", "integer": "int"}.get(kind, kind)
        columns.append((str(field.name), kind, bool(field.nullable)))
    return tuple(columns)


def contract_schema_columns(contract: TableContract) -> tuple[tuple[str, str, bool], ...]:
    return tuple(
        (column.name, column.iceberg_type.lower(), column.nullable)
        for column in contract.columns
    )


def _c(
    name: str,
    iceberg_type: str,
    nullable: bool = True,
    *,
    operational: bool = False,
) -> ColumnContract:
    return ColumnContract(name, iceberg_type, nullable, operational)


TABLE_CONTRACTS: dict[str, TableContract] = {
    contract.name: contract
    for contract in (
        TableContract(
            "bronze.kafka_records",
            (
                _c("ingest_id", "string", False),
                _c("source_topic", "string", False),
                _c("source_partition", "int", False),
                _c("source_offset", "long", False),
                _c("kafka_timestamp", "timestamp", False),
                _c("kafka_timestamp_type", "string", False),
                _c("key_base64", "string"),
                _c("key_sha256", "string"),
                _c("headers_json", "string", False),
                _c("payload_base64", "string"),
                _c("payload_text", "string"),
                _c("payload_sha256", "string"),
                _c("ingestion_run_id", "string", False, operational=True),
                _c("ingestion_manifest_sha256", "string", operational=True),
                _c("ingested_at", "timestamp", False, operational=True),
            ),
            ("ingest_id",),
            ("days(kafka_timestamp)",),
        ),
        TableContract(
            "bronze.source_files",
            (
                _c("file_sha256", "string", False),
                _c("source_dataset", "string", False),
                _c("source_file", "string", False),
                _c("source_object_uri", "string", False),
                _c("compressed_size_bytes", "long", False),
                _c("manifest_sha256", "string", False),
                _c("ingestion_run_id", "string", False, operational=True),
                _c("archived_at", "timestamp", False, operational=True),
            ),
            ("file_sha256",),
            ("source_dataset",),
        ),
        TableContract(
            "bronze.source_file_rows",
            (
                _c("ingest_id", "string", False),
                _c("source_dataset", "string", False),
                _c("source_file", "string", False),
                _c("source_object_uri", "string", False),
                _c("row_number", "long", False),
                _c("source_date", "date"),
                _c("file_sha256", "string", False),
                _c("payload_json", "string", False),
                _c("payload_sha256", "string", False),
                _c("ingestion_run_id", "string", False, operational=True),
                _c("ingested_at", "timestamp", False, operational=True),
            ),
            ("ingest_id",),
            ("source_dataset", "source_date"),
        ),
        TableContract(
            "silver.customer_events",
            (
                _c("event_id", "string", False),
                _c("event_name", "string", False),
                _c("event_time", "timestamp", False),
                _c("ingested_at", "timestamp", False),
                _c("event_date", "date", False),
                _c("source", "string", False),
                _c("channel", "string", False),
                _c("customer_id", "string"),
                _c("anonymous_id", "string"),
                _c("profile_key", "string", False),
                _c("session_id", "string", False),
                _c("campaign_id", "string"),
                _c("campaign_key", "string"),
                _c("click_id", "string"),
                _c("consent_analytics", "boolean", False),
                _c("late_status", "string", False),
                _c("is_bot_session", "boolean", False),
                _c("order_id", "string"),
                _c("gross_amount_minor", "long"),
                _c("refund_amount_minor", "long"),
                _c("currency", "string"),
                _c("payload_json", "string", False),
                _c("source_payload_sha256", "string", False),
                _c("lineage_topic", "string", False),
                _c("lineage_partition", "int", False),
                _c("lineage_offset", "long", False),
            ),
            ("event_id",),
            ("months(event_date)",),
        ),
        TableContract(
            "silver.customers",
            (
                _c("customer_id", "string", False),
                _c("created_at", "timestamp", False),
                _c("snapshot_date", "date", False),
                _c("loyalty_tier", "string", False),
                _c("lifetime_value_minor", "long", False),
                _c("last_order_at", "timestamp"),
                _c("consent_analytics", "boolean", False),
                _c("consent_marketing", "boolean", False),
                _c("country", "string", False),
                _c("city", "string", False),
                _c("source_file", "string", False),
                _c("source_row_number", "long", False),
            ),
            ("customer_id",),
        ),
        TableContract(
            "silver.campaigns",
            (
                _c("campaign_id", "string", False),
                _c("platform", "string", False),
                _c("platform_campaign_id", "string", False),
                _c("name", "string", False),
                _c("objective", "string", False),
                _c("status", "string", False),
                _c("start_date", "date", False),
                _c("end_date", "date", False),
                _c("daily_budget_minor", "long", False),
                _c("currency", "string", False),
                _c("ad_count", "long", False),
                _c("source_file", "string", False),
                _c("source_row_number", "long", False),
            ),
            ("campaign_id",),
        ),
        TableContract(
            "silver.products",
            (
                _c("product_id", "string", False),
                _c("sku", "string", False),
                _c("name", "string", False),
                _c("category", "string", False),
                _c("brand", "string", False),
                _c("unit_price_minor", "long", False),
                _c("currency", "string", False),
                _c("source_file", "string", False),
                _c("source_row_number", "long", False),
            ),
            ("product_id",),
        ),
        TableContract(
            "silver.ad_spend_daily",
            (
                _c("spend_key", "string", False),
                _c("spend_date", "date", False),
                _c("campaign_id", "string", False),
                _c("platform", "string", False),
                _c("platform_campaign_id", "string", False),
                _c("impressions", "long", False),
                _c("clicks", "long", False),
                _c("conversions", "long", False),
                _c("spend_minor", "long", False),
                _c("conversion_value_minor", "long", False),
                _c("currency", "string", False),
                _c("source_file", "string", False),
                _c("source_row_number", "long", False),
            ),
            ("spend_key",),
            ("months(spend_date)",),
        ),
        TableContract(
            "silver.quality_summary",
            (
                _c("event_date", "date", False),
                _c("source_topic", "string", False),
                _c("reason_code", "string", False),
                _c("record_count", "long", False),
                _c("distinct_event_count", "long", False),
                _c("run_id", "string", False, operational=True),
            ),
            ("event_date", "source_topic", "reason_code"),
            ("months(event_date)",),
        ),
        TableContract(
            "gold.funnel_daily",
            (
                _c("event_date", "date", False),
                _c("campaign_id", "string", False),
                _c("sessions", "long", False),
                _c("page_view_sessions", "long", False),
                _c("view_item_sessions", "long", False),
                _c("add_to_cart_sessions", "long", False),
                _c("checkout_sessions", "long", False),
                _c("purchase_sessions", "long", False),
            ),
            ("event_date", "campaign_id"),
            ("months(event_date)",),
        ),
        TableContract(
            "gold.attribution_daily",
            (
                _c("model", "string", False),
                _c("event_date", "date", False),
                _c("campaign_id", "string", False),
                _c("is_attributed", "boolean", False),
                _c("allocated_order_count", "decimal(18,6)", False),
                _c("allocated_gross_minor", "long", False),
                _c("allocated_refund_minor", "long", False),
                _c("allocated_net_minor", "long", False),
            ),
            ("model", "event_date", "campaign_id"),
            ("months(event_date)",),
        ),
        TableContract(
            "gold.customer_360",
            (
                _c("profile_key", "string", False),
                _c("customer_id", "string"),
                _c("anonymous_id", "string"),
                _c("profile_type", "string", False),
                _c("first_activity_at", "timestamp", False),
                _c("last_activity_at", "timestamp", False),
                _c("event_count", "long", False),
                _c("session_count", "long", False),
                _c("order_count", "long", False),
                _c("refund_count", "long", False),
                _c("gross_revenue_minor", "long", False),
                _c("refund_amount_minor", "long", False),
                _c("net_revenue_minor", "long", False),
                _c("acquisition_campaign_id", "string"),
                _c("cohort_month", "date", False),
                _c("consent_analytics", "boolean", False),
                _c("consent_marketing", "boolean"),
            ),
            ("profile_key",),
        ),
        TableContract(
            "gold.cohort_retention",
            (
                _c("cohort_month", "date", False),
                _c("activity_month", "date", False),
                _c("profile_type", "string", False),
                _c("month_number", "int", False),
                _c("cohort_size", "long", False),
                _c("active_profiles", "long", False),
                _c("retained_rate", "decimal(18,6)", False),
            ),
            ("cohort_month", "activity_month", "profile_type"),
            ("months(cohort_month)",),
        ),
        TableContract(
            "gold.campaign_performance_daily",
            (
                _c("event_date", "date", False),
                _c("campaign_id", "string", False),
                _c("platform", "string"),
                _c("currency", "string", False),
                _c("impressions", "long", False),
                _c("clicks", "long", False),
                _c("vendor_conversions", "long", False),
                _c("spend_minor", "long", False),
                _c("sessions", "long", False),
                _c("page_view_sessions", "long", False),
                _c("view_item_sessions", "long", False),
                _c("add_to_cart_sessions", "long", False),
                _c("checkout_sessions", "long", False),
                _c("purchase_sessions", "long", False),
                _c("orders", "long", False),
                _c("refunds", "long", False),
                _c("gross_revenue_minor", "long", False),
                _c("refund_amount_minor", "long", False),
                _c("net_revenue_minor", "long", False),
                _c("first_touch_gross_minor", "long", False),
                _c("first_touch_refund_minor", "long", False),
                _c("first_touch_net_minor", "long", False),
                _c("last_touch_gross_minor", "long", False),
                _c("last_touch_refund_minor", "long", False),
                _c("last_touch_net_minor", "long", False),
                _c("linear_gross_minor", "long", False),
                _c("linear_refund_minor", "long", False),
                _c("linear_net_minor", "long", False),
                _c("source_roas", "decimal(18,6)"),
                _c("first_touch_roas", "decimal(18,6)"),
                _c("last_touch_roas", "decimal(18,6)"),
                _c("linear_roas", "decimal(18,6)"),
            ),
            ("event_date", "campaign_id"),
            ("months(event_date)",),
        ),
        TableContract(
            "gold.commerce_daily",
            (
                _c("event_date", "date", False),
                _c("product_id", "string", False),
                _c("device_type", "string", False),
                _c("currency", "string", False),
                _c("product_name", "string", False),
                _c("category", "string", False),
                _c("brand", "string", False),
                _c("orders", "long", False),
                _c("units", "long", False),
                _c("refunds", "long", False),
                _c("gross_revenue_minor", "long", False),
                _c("refund_amount_minor", "long", False),
                _c("net_revenue_minor", "long", False),
            ),
            ("event_date", "product_id", "device_type", "currency"),
            ("months(event_date)",),
        ),
    )
}
