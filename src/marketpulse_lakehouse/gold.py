"""Deterministic marketing Gold model transformations."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from functools import reduce
import uuid
from typing import Any, Iterable, Mapping, Sequence

from .attribution import Touch, allocate_minor_units
from .bronze import _table_statement
from .contracts import TABLE_CONTRACTS, contract_schema_columns, spark_schema_columns
from .silver import validate_namespace


def _month(value: date) -> date:
    return date(value.year, value.month, 1)


def _required_date(row: Mapping[str, object], field: str) -> date:
    value = row.get(field)
    if not isinstance(value, date) or isinstance(value, datetime):
        raise ValueError(f"{field} must be a date")
    return value


def _required_datetime(row: Mapping[str, object], field: str) -> datetime:
    value = row.get(field)
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{field} must be a timezone-aware datetime")
    return value


def build_funnel_daily(
    events: Iterable[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    stages = {
        "page_view": "page_view_sessions",
        "view_item": "view_item_sessions",
        "add_to_cart": "add_to_cart_sessions",
        "begin_checkout": "checkout_sessions",
        "purchase": "purchase_sessions",
    }
    grouped: dict[tuple[date, str], dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    for event in events:
        if event.get("source") not in {"web", "mobile"} or event.get("is_bot_session") is True:
            continue
        event_date = _required_date(event, "event_date")
        campaign_id = str(event.get("campaign_id") or "__unattributed__")
        session_id = str(event.get("session_id") or "")
        if not session_id:
            raise ValueError("funnel event has no session_id")
        current = grouped[(event_date, campaign_id)]
        current["sessions"].add(session_id)
        stage = stages.get(str(event.get("event_name")))
        if stage:
            current[stage].add(session_id)
    fields = (
        "sessions",
        "page_view_sessions",
        "view_item_sessions",
        "add_to_cart_sessions",
        "checkout_sessions",
        "purchase_sessions",
    )
    return tuple(
        {"event_date": key[0], "campaign_id": key[1]}
        | {field: len(values[field]) for field in fields}
        for key, values in sorted(grouped.items())
    )


def _unique_touches(events: Sequence[Mapping[str, object]]) -> dict[str, list[Touch]]:
    by_profile: dict[str, dict[str, Touch]] = defaultdict(dict)
    for event in events:
        if (
            event.get("source") not in {"web", "mobile"}
            or event.get("event_name") != "page_view"
            or event.get("is_bot_session") is True
            or not event.get("click_id")
            or not event.get("campaign_id")
        ):
            continue
        profile = str(event.get("profile_key") or "")
        touch = Touch(
            click_id=str(event["click_id"]),
            campaign_id=str(event["campaign_id"]),
            event_time=_required_datetime(event, "event_time"),
            event_id=str(event.get("event_id") or ""),
        )
        previous = by_profile[profile].get(touch.click_id)
        if previous is None or (
            touch.event_time,
            touch.campaign_id,
            touch.event_id,
        ) < (previous.event_time, previous.campaign_id, previous.event_id):
            by_profile[profile][touch.click_id] = touch
    return {
        profile: sorted(
            values.values(),
            key=lambda item: (
                item.event_time,
                item.campaign_id,
                item.click_id,
                item.event_id,
            ),
        )
        for profile, values in by_profile.items()
    }


def _amount_allocations(amount: int, touches: Sequence[Touch]) -> list[tuple[str, int]]:
    return [(campaign, value) for _click, campaign, value in allocate_minor_units(amount, touches)]


def _exact_division_expression(source_column: str) -> str:
    if source_column not in {"order_count_micros", "gross_minor", "refund_minor"}:
        raise ValueError("unsupported allocation column")
    return f"`{source_column}` DIV `touch_count`"


def build_attribution_daily(
    events: Iterable[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    materialized = list(events)
    touches_by_profile = _unique_touches(materialized)
    orders = {
        str(event.get("order_id")): event
        for event in materialized
        if event.get("event_name") == "order_created" and event.get("order_id")
    }
    grouped: dict[tuple[str, date, str, bool], dict[str, object]] = defaultdict(
        lambda: {
            "order_count_micros": 0,
            "gross": 0,
            "refund": 0,
        }
    )
    for event in sorted(
        materialized,
        key=lambda item: (_required_datetime(item, "event_time"), str(item.get("event_id"))),
    ):
        event_name = event.get("event_name")
        if event_name not in {"order_created", "order_refunded"}:
            continue
        order_id = str(event.get("order_id") or "")
        original = event if event_name == "order_created" else orders.get(order_id)
        if original is None:
            raise ValueError(f"refund references absent order: {order_id}")
        order_time = _required_datetime(original, "event_time")
        candidates = [
            touch
            for touch in touches_by_profile.get(str(original.get("profile_key")), [])
            if order_time - timedelta(days=30) <= touch.event_time <= order_time
        ]
        event_date = _required_date(event, "event_date")
        is_order = event_name == "order_created"
        gross = int(event.get("gross_amount_minor") or 0) if is_order else 0
        refund = int(event.get("refund_amount_minor") or 0) if not is_order else 0
        order_count_micros = 1_000_000 if is_order else 0
        for model in ("first_touch", "last_touch", "linear"):
            if not candidates:
                allocations = [("__unattributed__", gross, refund, order_count_micros)]
                attributed = False
            elif model == "first_touch":
                allocations = [(candidates[0].campaign_id, gross, refund, order_count_micros)]
                attributed = True
            elif model == "last_touch":
                allocations = [(candidates[-1].campaign_id, gross, refund, order_count_micros)]
                attributed = True
            else:
                gross_parts = _amount_allocations(gross, candidates)
                refund_parts = _amount_allocations(refund, candidates)
                count_parts = _amount_allocations(order_count_micros, candidates)
                allocations = [
                    (gross_parts[index][0], gross_parts[index][1], refund_parts[index][1], count_parts[index][1])
                    for index in range(len(candidates))
                ]
                attributed = True
            for campaign_id, gross_value, refund_value, count_micros in allocations:
                current = grouped[(model, event_date, campaign_id, attributed)]
                current["order_count_micros"] = int(current["order_count_micros"]) + count_micros
                current["gross"] = int(current["gross"]) + gross_value
                current["refund"] = int(current["refund"]) + refund_value
    rows = []
    for key, value in sorted(grouped.items()):
        gross = int(value["gross"])
        refund = int(value["refund"])
        rows.append(
            {
                "model": key[0],
                "event_date": key[1],
                "campaign_id": key[2],
                "is_attributed": key[3],
                "allocated_order_count": (
                    Decimal(int(value["order_count_micros"])) / Decimal(1_000_000)
                ).quantize(Decimal("0.000001")),
                "allocated_gross_minor": gross,
                "allocated_refund_minor": refund,
                "allocated_net_minor": gross - refund,
            }
        )
    return tuple(rows)


def build_customer_360(
    events: Iterable[Mapping[str, object]],
    customers: Iterable[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    customer_map = {str(row["customer_id"]): row for row in customers}
    grouped: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for event in events:
        grouped[str(event.get("profile_key"))].append(event)
    rows = []
    for profile_key, values in sorted(grouped.items()):
        ordered = sorted(values, key=lambda row: (_required_datetime(row, "event_time"), str(row.get("event_id"))))
        customer_id = profile_key.removeprefix("customer:") if profile_key.startswith("customer:") else None
        anonymous_id = profile_key.removeprefix("anonymous:") if profile_key.startswith("anonymous:") else None
        customer = customer_map.get(customer_id or "")
        acquisition = next((row.get("campaign_id") for row in ordered if row.get("campaign_id")), None)
        gross = sum(
            int(row.get("gross_amount_minor") or 0) for row in ordered
            if row.get("event_name") == "order_created"
        )
        refund = sum(
            int(row.get("refund_amount_minor") or 0) for row in ordered
            if row.get("event_name") == "order_refunded"
        )
        created_at = customer.get("created_at") if customer else None
        cohort = _month(created_at.date()) if isinstance(created_at, datetime) else _month(_required_date(ordered[0], "event_date"))
        rows.append(
            {
                "profile_key": profile_key,
                "customer_id": customer_id,
                "anonymous_id": anonymous_id,
                "profile_type": "customer" if customer_id else ("anonymous" if anonymous_id else "session"),
                "first_activity_at": _required_datetime(ordered[0], "event_time"),
                "last_activity_at": _required_datetime(ordered[-1], "event_time"),
                "event_count": len(ordered),
                "session_count": len({str(row.get("session_id")) for row in ordered}),
                "order_count": sum(row.get("event_name") == "order_created" for row in ordered),
                "refund_count": sum(row.get("event_name") == "order_refunded" for row in ordered),
                "gross_revenue_minor": gross,
                "refund_amount_minor": refund,
                "net_revenue_minor": gross - refund,
                "acquisition_campaign_id": acquisition,
                "cohort_month": cohort,
                "consent_analytics": True,
                "consent_marketing": customer.get("consent_marketing") if customer else None,
            }
        )
    return tuple(rows)


def build_cohort_retention(
    events: Iterable[Mapping[str, object]],
    profiles: Iterable[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    profile_map = {str(row["profile_key"]): row for row in profiles}
    population: dict[tuple[date, str], set[str]] = defaultdict(set)
    activity: dict[tuple[date, date, str], set[str]] = defaultdict(set)
    for profile_key, profile in profile_map.items():
        population[(_required_date(profile, "cohort_month"), str(profile["profile_type"]))].add(profile_key)
    for event in events:
        profile_key = str(event["profile_key"])
        profile = profile_map[profile_key]
        cohort = _required_date(profile, "cohort_month")
        active_month = _month(_required_date(event, "event_date"))
        activity[(cohort, active_month, str(profile["profile_type"]))].add(profile_key)
    rows = []
    for key, active in sorted(activity.items()):
        cohort_size = len(population[(key[0], key[2])])
        month_number = (key[1].year - key[0].year) * 12 + key[1].month - key[0].month
        rows.append(
            {
                "cohort_month": key[0],
                "activity_month": key[1],
                "profile_type": key[2],
                "month_number": month_number,
                "cohort_size": cohort_size,
                "active_profiles": len(active),
                "retained_rate": (Decimal(len(active)) / Decimal(cohort_size)).quantize(
                    Decimal("0.000001"), rounding=ROUND_HALF_UP
                ),
            }
        )
    return tuple(rows)


def _ratio(numerator: int, denominator: int) -> Decimal | None:
    if denominator == 0:
        return None
    return (Decimal(numerator) / Decimal(denominator)).quantize(
        Decimal("0.000001"), rounding=ROUND_HALF_UP
    )


def build_campaign_performance_daily(
    spend: Iterable[Mapping[str, object]],
    funnel: Iterable[Mapping[str, object]],
    events: Iterable[Mapping[str, object]],
    attribution: Iterable[Mapping[str, object]],
    campaigns: Iterable[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    campaign_map = {str(row["campaign_id"]): row for row in campaigns}
    values: dict[tuple[date, str], dict[str, object]] = defaultdict(dict)
    for row in spend:
        key = (_required_date(row, "spend_date"), str(row["campaign_id"]))
        values[key]["spend"] = row
    for row in funnel:
        values[(_required_date(row, "event_date"), str(row["campaign_id"]))]["funnel"] = row
    source: dict[tuple[date, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for event in events:
        if event.get("event_name") not in {"order_created", "order_refunded"}:
            continue
        key = (_required_date(event, "event_date"), str(event.get("campaign_id") or "__unattributed__"))
        if event.get("event_name") == "order_created":
            source[key]["orders"] += 1
            source[key]["gross"] += int(event.get("gross_amount_minor") or 0)
        else:
            source[key]["refunds"] += 1
            source[key]["refund"] += int(event.get("refund_amount_minor") or 0)
        values[key]
    modeled: dict[tuple[date, str], dict[str, Mapping[str, object]]] = defaultdict(dict)
    for row in attribution:
        key = (_required_date(row, "event_date"), str(row["campaign_id"]))
        modeled[key][str(row["model"])] = row
        values[key]
    rows = []
    funnel_fields = (
        "sessions", "page_view_sessions", "view_item_sessions", "add_to_cart_sessions",
        "checkout_sessions", "purchase_sessions",
    )
    for key in sorted(values):
        spend_row = values[key].get("spend", {})
        funnel_row = values[key].get("funnel", {})
        source_row = source[key]
        campaign = campaign_map.get(key[1], {})
        spend_minor = int(spend_row.get("spend_minor", 0))
        gross = source_row["gross"]
        refund = source_row["refund"]
        row: dict[str, object] = {
            "event_date": key[0], "campaign_id": key[1],
            "platform": spend_row.get("platform") or campaign.get("platform"),
            "currency": spend_row.get("currency") or campaign.get("currency") or "VND",
            "impressions": int(spend_row.get("impressions", 0)),
            "clicks": int(spend_row.get("clicks", 0)),
            "vendor_conversions": int(spend_row.get("conversions", 0)),
            "spend_minor": spend_minor,
            **{field: int(funnel_row.get(field, 0)) for field in funnel_fields},
            "orders": source_row["orders"], "refunds": source_row["refunds"],
            "gross_revenue_minor": gross, "refund_amount_minor": refund,
            "net_revenue_minor": gross - refund,
        }
        for model, prefix in (("first_touch", "first_touch"), ("last_touch", "last_touch"), ("linear", "linear")):
            model_row = modeled[key].get(model, {})
            mg = int(model_row.get("allocated_gross_minor", 0))
            mr = int(model_row.get("allocated_refund_minor", 0))
            row[f"{prefix}_gross_minor"] = mg
            row[f"{prefix}_refund_minor"] = mr
            row[f"{prefix}_net_minor"] = mg - mr
            row[f"{prefix}_roas"] = _ratio(mg - mr, spend_minor)
        row["source_roas"] = _ratio(gross - refund, spend_minor)
        rows.append(row)
    return tuple(rows)


_GOLD_TABLES = (
    "funnel_daily",
    "attribution_daily",
    "customer_360",
    "cohort_retention",
    "campaign_performance_daily",
)


def _event_amount(event_name: str, source_column: str) -> Any:
    """Count money at its financial event, not repeated order context on refunds."""

    from pyspark.sql import functions as F

    return F.when(
        F.col("event_name") == event_name,
        F.coalesce(F.col(source_column), F.lit(0)),
    ).otherwise(F.lit(0))


def _qualified(namespace: str, table: str) -> str:
    return f"lakehouse.`{validate_namespace(namespace)}`.`{table}`"


def _ensure_gold_tables(spark: Any, namespace: str) -> None:
    namespace = validate_namespace(namespace)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.`{namespace}`")
    for table in _GOLD_TABLES:
        statement = _table_statement("lakehouse", TABLE_CONTRACTS[f"gold.{table}"])
        spark.sql(statement.replace("`gold`", f"`{namespace}`", 1))


def _replace_gold_table(spark: Any, namespace: str, table: str, frame: Any) -> int:
    view = f"gold_build_{table}_{uuid.uuid4().hex}"
    columns = [column.name for column in TABLE_CONTRACTS[f"gold.{table}"].columns]
    frame.select(*columns).createOrReplaceTempView(view)
    try:
        spark.sql(
            f"INSERT OVERWRITE {_qualified(namespace, table)} "
            + "SELECT "
            + ", ".join(f"`{column}`" for column in columns)
            + f" FROM `{view}`"
        )
        return int(frame.count())
    finally:
        spark.catalog.dropTempView(view)


def _current_snapshot(spark: Any, namespace: str, table: str) -> int | None:
    rows = spark.sql(
        f"SELECT snapshot_id FROM {_qualified(namespace, table)}.refs "
        "WHERE name = 'main'"
    ).collect()
    if not rows:
        return None
    if len(rows) != 1:
        raise ValueError(f"Iceberg main ref is ambiguous: {namespace}.{table}")
    return int(rows[0]["snapshot_id"])


def _read_silver(
    spark: Any, namespace: str, table: str, snapshot_id: int
) -> Any:
    if snapshot_id < 1:
        raise ValueError(f"Silver snapshot ID must be positive: {table}")
    return spark.sql(
        f"SELECT * FROM {_qualified(namespace, table)} VERSION AS OF {snapshot_id}"
    )


def _funnel_frame(events: Any) -> Any:
    from pyspark.sql import functions as F

    source = events.filter(
        F.col("source").isin("web", "mobile") & ~F.col("is_bot_session")
    ).withColumn("campaign_id", F.coalesce("campaign_id", F.lit("__unattributed__")))
    stages = {
        "page_view_sessions": "page_view",
        "view_item_sessions": "view_item",
        "add_to_cart_sessions": "add_to_cart",
        "checkout_sessions": "begin_checkout",
        "purchase_sessions": "purchase",
    }
    expressions = [F.countDistinct("session_id").cast("long").alias("sessions")]
    expressions.extend(
        F.countDistinct(
            F.when(F.col("event_name") == event_name, F.col("session_id"))
        )
        .cast("long")
        .alias(column)
        for column, event_name in stages.items()
    )
    return source.groupBy("event_date", "campaign_id").agg(*expressions)


def _attribution_frame(events: Any) -> Any:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    touch_order = Window.partitionBy("profile_key", "click_id").orderBy(
        "event_time", "campaign_id", "click_id", "event_id"
    )
    touches = (
        events.filter(
            F.col("source").isin("web", "mobile")
            & (F.col("event_name") == "page_view")
            & ~F.col("is_bot_session")
            & F.col("click_id").isNotNull()
            & F.col("campaign_id").isNotNull()
        )
        .withColumn("touch_dedup_rank", F.row_number().over(touch_order))
        .filter(F.col("touch_dedup_rank") == 1)
        .select(
            "profile_key",
            "click_id",
            "campaign_id",
            F.col("event_time").alias("touch_time"),
            F.col("event_id").alias("touch_event_id"),
        )
    )
    orders = events.filter(F.col("event_name") == "order_created").select(
        "order_id",
        F.col("profile_key").alias("order_profile_key"),
        F.col("event_time").alias("order_time"),
        F.col("event_id").alias("order_event_id"),
        F.col("event_date").alias("order_date"),
        F.coalesce("gross_amount_minor", F.lit(0)).cast("long").alias("order_gross"),
    )
    invalid_order = orders.filter(F.col("order_id").isNull()).limit(1)
    duplicate_order = orders.groupBy("order_id").count().filter(F.col("count") != 1).limit(1)
    if invalid_order.count() or duplicate_order.count():
        raise ValueError("Gold attribution requires unique non-null order_id values")
    refunds = events.filter(F.col("event_name") == "order_refunded").select(
        "order_id",
        F.col("event_id").alias("refund_event_id"),
        F.col("event_date").alias("refund_date"),
        F.coalesce("refund_amount_minor", F.lit(0)).cast("long").alias("refund_amount"),
    )
    orphan_refund = refunds.join(orders.select("order_id"), "order_id", "left_anti").limit(1)
    if orphan_refund.count():
        raise ValueError("refund references absent order")
    transactions = orders.select(
        F.col("order_event_id").alias("transaction_id"),
        "order_id",
        F.col("order_profile_key").alias("profile_key"),
        "order_time",
        F.col("order_date").alias("event_date"),
        F.col("order_gross").alias("gross_minor"),
        F.lit(0).cast("long").alias("refund_minor"),
        F.lit(1_000_000).cast("long").alias("order_count_micros"),
    ).unionByName(
        refunds.join(orders, "order_id", "inner").select(
            F.col("refund_event_id").alias("transaction_id"),
            "order_id",
            F.col("order_profile_key").alias("profile_key"),
            "order_time",
            F.col("refund_date").alias("event_date"),
            F.lit(0).cast("long").alias("gross_minor"),
            F.col("refund_amount").alias("refund_minor"),
            F.lit(0).cast("long").alias("order_count_micros"),
        )
    )
    candidates = transactions.join(
        touches,
        (transactions.profile_key == touches.profile_key)
        & (touches.touch_time <= transactions.order_time)
        & (touches.touch_time >= transactions.order_time - F.expr("INTERVAL 30 DAYS")),
        "left",
    ).drop(touches.profile_key)
    chronological = Window.partitionBy("transaction_id").orderBy(
        F.col("touch_time").asc_nulls_last(),
        F.col("campaign_id").asc_nulls_last(),
        F.col("click_id").asc_nulls_last(),
        F.col("touch_event_id").asc_nulls_last(),
    )
    reverse = Window.partitionBy("transaction_id").orderBy(
        F.col("touch_time").desc_nulls_last(),
        F.col("campaign_id").desc_nulls_last(),
        F.col("click_id").desc_nulls_last(),
        F.col("touch_event_id").desc_nulls_last(),
    )
    partition = Window.partitionBy("transaction_id")
    ranked = (
        candidates.withColumn("touch_rank", F.row_number().over(chronological))
        .withColumn("reverse_rank", F.row_number().over(reverse))
        .withColumn("touch_count", F.count("click_id").over(partition).cast("long"))
    )

    def selected(model: str, rank_column: str) -> Any:
        return ranked.filter(F.col(rank_column) == 1).select(
            F.lit(model).alias("model"),
            "event_date",
            F.coalesce("campaign_id", F.lit("__unattributed__")).alias("campaign_id"),
            F.col("campaign_id").isNotNull().alias("is_attributed"),
            "order_count_micros",
            F.col("gross_minor").alias("allocated_gross_minor"),
            F.col("refund_minor").alias("allocated_refund_minor"),
        )

    linear = ranked.select(
        F.lit("linear").alias("model"),
        "event_date",
        F.coalesce("campaign_id", F.lit("__unattributed__")).alias("campaign_id"),
        F.col("campaign_id").isNotNull().alias("is_attributed"),
        *[
            F.when(F.col("touch_count") == 0, F.col(source_column))
            .otherwise(
                F.expr(_exact_division_expression(source_column)).cast("long")
                + F.when(
                    F.col("touch_rank")
                    <= F.pmod(F.col(source_column), F.col("touch_count")),
                    F.lit(1),
                ).otherwise(F.lit(0))
            )
            .cast("long")
            .alias(target_column)
            for source_column, target_column in (
                ("order_count_micros", "order_count_micros"),
                ("gross_minor", "allocated_gross_minor"),
                ("refund_minor", "allocated_refund_minor"),
            )
        ],
    )
    allocations = (
        selected("first_touch", "touch_rank")
        .unionByName(selected("last_touch", "reverse_rank"))
        .unionByName(linear)
    )
    return (
        allocations.groupBy("model", "event_date", "campaign_id", "is_attributed")
        .agg(
            F.sum("order_count_micros").alias("order_count_micros"),
            F.sum("allocated_gross_minor").cast("long").alias("allocated_gross_minor"),
            F.sum("allocated_refund_minor").cast("long").alias("allocated_refund_minor"),
        )
        .withColumn(
            "allocated_order_count",
            (F.col("order_count_micros") / F.lit(1_000_000)).cast("decimal(18,6)"),
        )
        .withColumn(
            "allocated_net_minor",
            F.col("allocated_gross_minor") - F.col("allocated_refund_minor"),
        )
        .select(
            *[column.name for column in TABLE_CONTRACTS["gold.attribution_daily"].columns]
        )
    )


def _customer_frames(events: Any, customers: Any) -> tuple[Any, Any]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    acquisition = (
        events.filter(F.col("campaign_id").isNotNull())
        .withColumn(
            "acquisition_rank",
            F.row_number().over(
                Window.partitionBy("profile_key").orderBy("event_time", "event_id")
            ),
        )
        .filter(F.col("acquisition_rank") == 1)
        .select(
            "profile_key",
            F.col("campaign_id").alias("acquisition_campaign_id"),
        )
    )
    aggregates = events.groupBy("profile_key").agg(
        F.min("event_time").alias("first_activity_at"),
        F.max("event_time").alias("last_activity_at"),
        F.count(F.lit(1)).cast("long").alias("event_count"),
        F.countDistinct("session_id").cast("long").alias("session_count"),
        F.sum(F.when(F.col("event_name") == "order_created", 1).otherwise(0))
        .cast("long")
        .alias("order_count"),
        F.sum(F.when(F.col("event_name") == "order_refunded", 1).otherwise(0))
        .cast("long")
        .alias("refund_count"),
        F.sum(_event_amount("order_created", "gross_amount_minor"))
        .cast("long")
        .alias("gross_revenue_minor"),
        F.sum(_event_amount("order_refunded", "refund_amount_minor"))
        .cast("long")
        .alias("refund_amount_minor"),
    )
    profiles = (
        aggregates.withColumn(
            "customer_id",
            F.when(
                F.col("profile_key").startswith("customer:"),
                F.substring("profile_key", len("customer:") + 1, 1_000_000),
            ),
        )
        .withColumn(
            "anonymous_id",
            F.when(
                F.col("profile_key").startswith("anonymous:"),
                F.substring("profile_key", len("anonymous:") + 1, 1_000_000),
            ),
        )
        .withColumn(
            "profile_type",
            F.when(F.col("customer_id").isNotNull(), F.lit("customer"))
            .when(F.col("anonymous_id").isNotNull(), F.lit("anonymous"))
            .otherwise(F.lit("session")),
        )
        .join(
            customers.select("customer_id", "created_at", "consent_marketing"),
            "customer_id",
            "left",
        )
        .join(acquisition, "profile_key", "left")
        .withColumn(
            "cohort_month",
            F.trunc(F.coalesce(F.to_date("created_at"), F.to_date("first_activity_at")), "month"),
        )
        .withColumn("consent_analytics", F.lit(True))
        .withColumn(
            "net_revenue_minor",
            F.col("gross_revenue_minor") - F.col("refund_amount_minor"),
        )
        .select(*[column.name for column in TABLE_CONTRACTS["gold.customer_360"].columns])
    )
    population = profiles.groupBy("cohort_month", "profile_type").agg(
        F.countDistinct("profile_key").cast("long").alias("cohort_size")
    )
    activity = (
        events.select(
            "profile_key", F.trunc("event_date", "month").alias("activity_month")
        )
        .distinct()
        .join(profiles.select("profile_key", "cohort_month", "profile_type"), "profile_key")
        .groupBy("cohort_month", "activity_month", "profile_type")
        .agg(F.countDistinct("profile_key").cast("long").alias("active_profiles"))
        .join(population, ["cohort_month", "profile_type"], "inner")
        .withColumn(
            "month_number",
            F.months_between("activity_month", "cohort_month").cast("int"),
        )
        .withColumn(
            "retained_rate",
            (F.col("active_profiles") / F.col("cohort_size")).cast("decimal(18,6)"),
        )
        .select(*[column.name for column in TABLE_CONTRACTS["gold.cohort_retention"].columns])
    )
    return profiles, activity


def _campaign_performance_frame(
    spend: Any, funnel: Any, events: Any, attribution: Any, campaigns: Any
) -> Any:
    from pyspark.sql import functions as F

    spend_daily = spend.withColumnRenamed("spend_date", "event_date")
    source = (
        events.filter(F.col("event_name").isin("order_created", "order_refunded"))
        .withColumn("campaign_id", F.coalesce("campaign_id", F.lit("__unattributed__")))
        .groupBy("event_date", "campaign_id")
        .agg(
            F.sum(F.when(F.col("event_name") == "order_created", 1).otherwise(0))
            .cast("long")
            .alias("orders"),
            F.sum(F.when(F.col("event_name") == "order_refunded", 1).otherwise(0))
            .cast("long")
            .alias("refunds"),
            F.sum(_event_amount("order_created", "gross_amount_minor"))
            .cast("long")
            .alias("gross_revenue_minor"),
            F.sum(_event_amount("order_refunded", "refund_amount_minor"))
            .cast("long")
            .alias("refund_amount_minor"),
        )
    )
    modeled = attribution.groupBy("event_date", "campaign_id").agg(
        *[
            F.sum(
                F.when(F.col("model") == model, F.col(source_column)).otherwise(0)
            )
            .cast("long")
            .alias(f"{prefix}_{suffix}")
            for model, prefix in (
                ("first_touch", "first_touch"),
                ("last_touch", "last_touch"),
                ("linear", "linear"),
            )
            for source_column, suffix in (
                ("allocated_gross_minor", "gross_minor"),
                ("allocated_refund_minor", "refund_minor"),
                ("allocated_net_minor", "net_minor"),
            )
        ]
    )
    keys = (
        spend_daily.select("event_date", "campaign_id")
        .unionByName(funnel.select("event_date", "campaign_id"))
        .unionByName(source.select("event_date", "campaign_id"))
        .unionByName(modeled.select("event_date", "campaign_id"))
        .distinct()
    )
    frame = (
        keys.join(spend_daily, ["event_date", "campaign_id"], "left")
        .join(funnel, ["event_date", "campaign_id"], "left")
        .join(source, ["event_date", "campaign_id"], "left")
        .join(modeled, ["event_date", "campaign_id"], "left")
        .join(
            campaigns.select(
                "campaign_id",
                F.col("platform").alias("campaign_platform"),
                F.col("currency").alias("campaign_currency"),
            ),
            "campaign_id",
            "left",
        )
        .withColumn("platform", F.coalesce("platform", "campaign_platform"))
        .withColumn("currency", F.coalesce("currency", "campaign_currency", F.lit("VND")))
    )
    additive = [
        "impressions", "clicks", "conversions", "spend_minor", "sessions",
        "page_view_sessions", "view_item_sessions", "add_to_cart_sessions",
        "checkout_sessions", "purchase_sessions", "orders", "refunds",
        "gross_revenue_minor", "refund_amount_minor", "first_touch_gross_minor",
        "first_touch_refund_minor", "first_touch_net_minor", "last_touch_gross_minor",
        "last_touch_refund_minor", "last_touch_net_minor", "linear_gross_minor",
        "linear_refund_minor", "linear_net_minor",
    ]
    frame = frame.fillna({column: 0 for column in additive}).withColumnRenamed(
        "conversions", "vendor_conversions"
    ).withColumn(
        "net_revenue_minor", F.col("gross_revenue_minor") - F.col("refund_amount_minor")
    )

    def roas(numerator: str) -> Any:
        return F.when(F.col("spend_minor") == 0, F.lit(None)).otherwise(
            (F.col(numerator) / F.col("spend_minor")).cast("decimal(18,6)")
        )

    return (
        frame.withColumn("source_roas", roas("net_revenue_minor"))
        .withColumn("first_touch_roas", roas("first_touch_net_minor"))
        .withColumn("last_touch_roas", roas("last_touch_net_minor"))
        .withColumn("linear_roas", roas("linear_net_minor"))
        .select(
            *[
                column.name
                for column in TABLE_CONTRACTS["gold.campaign_performance_daily"].columns
            ]
        )
    )


def build_gold(
    spark: Any,
    *,
    silver_snapshots: Mapping[str, int],
    run_id: str,
    source_namespace: str = "silver",
    target_namespace: str = "gold",
) -> dict[str, object]:
    """Build every Gold model from explicit immutable Silver snapshots."""

    if not run_id:
        raise ValueError("run_id must not be empty")
    source_namespace = validate_namespace(source_namespace)
    target_namespace = validate_namespace(target_namespace)
    expected = {name for name in TABLE_CONTRACTS if name.startswith("silver.")}
    if set(silver_snapshots) != expected:
        raise ValueError("Silver snapshots must contain exactly: " + ", ".join(sorted(expected)))

    from pyspark.storagelevel import StorageLevel

    _ensure_gold_tables(spark, target_namespace)
    events = _read_silver(
        spark, source_namespace, "customer_events", silver_snapshots["silver.customer_events"]
    ).persist(StorageLevel.MEMORY_AND_DISK)
    customers = _read_silver(
        spark, source_namespace, "customers", silver_snapshots["silver.customers"]
    )
    campaigns = _read_silver(
        spark, source_namespace, "campaigns", silver_snapshots["silver.campaigns"]
    )
    spend = _read_silver(
        spark, source_namespace, "ad_spend_daily", silver_snapshots["silver.ad_spend_daily"]
    )
    events.count()
    funnel = _funnel_frame(events).persist(StorageLevel.MEMORY_AND_DISK)
    attribution = _attribution_frame(events).persist(StorageLevel.MEMORY_AND_DISK)
    profiles, retention = _customer_frames(events, customers)
    profiles = profiles.persist(StorageLevel.MEMORY_AND_DISK)
    performance = _campaign_performance_frame(
        spend, funnel, events, attribution, campaigns
    )
    frames = {
        "funnel_daily": funnel,
        "attribution_daily": attribution,
        "customer_360": profiles,
        "cohort_retention": retention,
        "campaign_performance_daily": performance,
    }
    try:
        _validate_gold_frames(frames)
        counts = {
            table: _replace_gold_table(spark, target_namespace, table, frame)
            for table, frame in frames.items()
        }
    finally:
        for frame in (events, *frames.values()):
            frame.unpersist(blocking=False)
    snapshots = {
        table: _current_snapshot(spark, target_namespace, table) for table in frames
    }
    return {
        "command": "build-gold",
        "run_id": run_id,
        "source_namespace": source_namespace,
        "target_namespace": target_namespace,
        "silver_snapshots": dict(silver_snapshots),
        "counts": counts,
        "snapshots": snapshots,
    }


def _validate_gold_frames(frames: Mapping[str, Any]) -> None:
    """Validate every Gold result before the first table is overwritten."""

    from pyspark.sql import functions as F

    if set(frames) != set(_GOLD_TABLES):
        raise ValueError("Gold frame coverage is incomplete")
    for table in _GOLD_TABLES:
        frame = frames[table]
        contract = TABLE_CONTRACTS[f"gold.{table}"]
        actual = spark_schema_columns(frame.schema)
        actual_types = tuple((name, kind) for name, kind, _nullable in actual)
        required = [column.name for column in contract.columns if not column.nullable]
        null_condition = reduce(
            lambda left, right: left | right,
            (F.col(name).isNull() for name in required),
            F.lit(False),
        )
        has_null_required = bool(
            required and frame.filter(null_condition).limit(1).count()
        )
        has_duplicate_key = bool(
            contract.key_fields and frame.groupBy(*contract.key_fields).count().filter(
            F.col("count") > 1
        ).limit(1).count()
        )
        _validate_gold_observations(
            table,
            actual_types=actual_types,
            has_null_required=has_null_required,
            has_duplicate_key=has_duplicate_key,
        )


def _validate_gold_observations(
    table: str,
    *,
    actual_types: tuple[tuple[str, str], ...],
    has_null_required: bool,
    has_duplicate_key: bool,
) -> None:
    contract = TABLE_CONTRACTS[f"gold.{table}"]
    expected_types = tuple(
        (name, kind) for name, kind, _nullable in contract_schema_columns(contract)
    )
    if actual_types != expected_types:
        raise ValueError(f"Gold frame schema mismatch: gold.{table}")
    if has_null_required:
        raise ValueError(f"Gold frame contains null required values: gold.{table}")
    if has_duplicate_key:
        raise ValueError(f"Gold frame contains duplicate keys: gold.{table}")
