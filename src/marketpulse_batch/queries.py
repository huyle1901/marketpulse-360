"""Six allowlisted marketing workloads against one request-pinned release."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .ledger import PublicationReservation

WORKLOADS = ("campaign_overview", "daily_funnel", "attribution_comparison", "cohort_retention", "customer_drill", "top_campaigns")


@dataclass(frozen=True, slots=True)
class QueryRequest:
    workload: str
    from_date: date
    to_date: date
    currency: str | None = None
    customer_id: str | None = None
    limit: int = 20

    def __post_init__(self) -> None:
        if self.workload not in WORKLOADS:
            raise ValueError("unknown marketing workload")
        if any(type(day) is not date or not date(1900, 1, 1) <= day <= date(2299, 12, 31) for day in (self.from_date, self.to_date)) or self.from_date >= self.to_date:
            raise ValueError("query requires a valid UTC [from,to) date interval")
        if type(self.limit) is not int or not 1 <= self.limit <= 1000:
            raise ValueError("query limit must be between 1 and 1000")
        if self.currency is not None and re.fullmatch(r"[A-Z]{3}", self.currency) is None:
            raise ValueError("currency must be a three-letter code")
        if self.workload == "customer_drill" and (not isinstance(self.customer_id, str) or not self.customer_id or len(self.customer_id) > 256):
            raise ValueError("customer drill requires a bounded customer identifier")


def build_query(release: PublicationReservation, request: QueryRequest, *, single_currency: str | None = None) -> tuple[str, dict[str, object]]:
    database = release.database  # Validated owned identifier in PublicationReservation.
    params = {"from_date": request.from_date, "to_date": request.to_date, "limit": request.limit}
    window = "event_date >= {from_date:Date32} AND event_date < {to_date:Date32}"
    currency_filter = ""
    if request.currency is not None:
        params["currency"] = request.currency
        currency_filter = " AND currency={currency:String}"
    if request.workload == "campaign_overview":
        sql = (f"SELECT currency,uniqExactIf(campaign_id,campaign_id!='__unattributed__') AS campaigns,"
               "sum(toInt128(impressions)) AS impressions,sum(toInt128(clicks)) AS clicks,"
               "sum(toInt128(spend_minor)) AS spend_minor,sum(toInt128(gross_revenue_minor)) AS gross_revenue_minor,"
               "sum(toInt128(refund_amount_minor)) AS refund_amount_minor,sum(toInt128(net_revenue_minor)) AS net_revenue_minor "
               f"FROM {database}.campaign_performance_daily WHERE {window}{currency_filter} GROUP BY currency ORDER BY currency")
    elif request.workload == "daily_funnel":
        sql = f"SELECT * FROM {database}.funnel_daily WHERE {window} ORDER BY event_date,campaign_id LIMIT {{limit:UInt32}}"
    elif request.workload == "cohort_retention":
        sql = (f"SELECT * FROM {database}.cohort_retention WHERE activity_month >= {{from_date:Date32}} "
               "AND activity_month < {to_date:Date32} ORDER BY cohort_month,activity_month,profile_type LIMIT {limit:UInt32}")
    elif request.workload == "top_campaigns":
        sql = (f"SELECT campaign_id,currency,sum(toInt128(spend_minor)) AS spend_minor,"
               "sum(toInt128(net_revenue_minor)) AS net_revenue_minor "
               f"FROM {database}.campaign_performance_daily WHERE {window}{currency_filter} "
               "GROUP BY campaign_id,currency ORDER BY net_revenue_minor DESC,campaign_id,currency LIMIT {limit:UInt32}")
    else:
        if single_currency is None:
            raise ValueError("customer/attribution financial contracts require a single-currency snapshot")
        params["dataset_currency"] = single_currency
        params["currency"] = request.currency or single_currency
        if request.workload == "attribution_comparison":
            sql = ("SELECT model,{dataset_currency:String} AS currency,sum(allocated_order_count) AS allocated_order_count,"
                   "sum(toInt128(allocated_gross_minor)) AS gross_revenue_minor,"
                   "sum(toInt128(allocated_refund_minor)) AS refund_amount_minor,"
                   "sum(toInt128(allocated_net_minor)) AS net_revenue_minor "
                   f"FROM {database}.attribution_daily WHERE {window} AND {{currency:String}}={{dataset_currency:String}} "
                   "GROUP BY model ORDER BY model")
        else:
            params["customer_id"] = request.customer_id
            sql = (f"SELECT *,{{dataset_currency:String}} AS currency,'snapshot_lifetime' AS financial_scope "
                   f"FROM {database}.customer_360 WHERE customer_id={{customer_id:String}} "
                   "AND last_activity_at >= toDateTime64({from_date:Date32},6,'UTC') "
                   "AND last_activity_at < toDateTime64({to_date:Date32},6,'UTC') "
                   "AND {currency:String}={dataset_currency:String} ORDER BY profile_key LIMIT {limit:UInt32}")
    return sql, params


def run_query(client: Any, release: PublicationReservation, request: QueryRequest) -> list[dict[str, object]]:
    currency = None
    if request.workload in {"attribution_comparison", "customer_drill"}:
        currencies = client.query(f"SELECT DISTINCT currency FROM {release.database}.campaigns "
                                  f"UNION DISTINCT SELECT DISTINCT currency FROM {release.database}.campaign_performance_daily",
                                  settings={"use_query_cache": 0}).result_rows
        if len(currencies) != 1:
            raise ValueError("MP-04 customer/attribution aggregates do not support mixed currencies")
        currency = currencies[0][0]
    sql, parameters = build_query(release, request, single_currency=currency)
    result = client.query(sql, parameters=parameters, query_tz="UTC", tz_mode="aware", settings={"use_query_cache": 0})
    return [dict(zip(result.column_names, row)) for row in result.result_rows]
