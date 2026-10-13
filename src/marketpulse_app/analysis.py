"""Certified stage reach and attribution reads; no new attribution policy."""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

from .analytics import Analytics, wire
from .contracts import CampaignSetting, DashboardFilter
from .math import ratio

STAGES = ("sessions", "page_view_sessions", "view_item_sessions", "add_to_cart_sessions", "checkout_sessions", "purchase_sessions")
MODELS = ("first_touch", "last_touch", "linear")
DEFINITIONS = {
    "first_touch": "Earliest unique eligible paid click in the inclusive 30-day window ending at original purchase time.",
    "last_touch": "Latest unique eligible paid click in the inclusive 30-day window ending at original purchase time.",
    "linear": "Equal credit across unique eligible clicks; integral money/count micros remainders follow event-time/campaign/click/event order.",
}
MONEY = ("gross_revenue_minor", "refund_amount_minor", "net_revenue_minor")
ALLOCATED = ("allocated_gross_minor", "allocated_refund_minor", "allocated_net_minor")


class AnalysisError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def campaign_dimension(campaign_id: str, dimensions: Mapping[str, Mapping[str, object]]) -> dict[str, object]:
    row = dimensions.get(campaign_id)
    return {"campaign_id": campaign_id, "campaign_name": row["name"] if row else "Unattributed" if campaign_id == "__unattributed__" else campaign_id,
            "channel": row["platform"] if row else "unattributed" if campaign_id == "__unattributed__" else "unmapped"}


def summarize_funnel(rows: Sequence[Mapping[str, object]], dimensions: Sequence[Mapping[str, object]]) -> dict[str, object]:
    lookup = {row["campaign_id"]: row for row in dimensions}
    totals = dict.fromkeys(STAGES, 0)
    campaigns = {}
    channels = {}
    keys = set()
    for row in rows:
        key = (row["event_date"], row["campaign_id"])
        if key in keys or any(type(row[name]) is not int or row[name] < 0 for name in STAGES) or any(row[name] > row["sessions"] for name in STAGES[1:]):
            raise AnalysisError("reconciliation_failed")
        keys.add(key)
        dimension = campaign_dimension(row["campaign_id"], lookup)
        campaign = campaigns.setdefault(row["campaign_id"], dimension | dict.fromkeys(STAGES, 0))
        channel = channels.setdefault(dimension["channel"], {"channel": dimension["channel"]} | dict.fromkeys(STAGES, 0))
        for name in STAGES:
            totals[name] += row[name]
            campaign[name] += row[name]
            channel[name] += row[name]
    for row in [*campaigns.values(), *channels.values()]:
        row["reach_ratios"] = {name: ratio(row[name], row["sessions"]) for name in STAGES[1:]}
    return wire({"totals": totals, "reach_ratios": {name: ratio(totals[name], totals["sessions"]) for name in STAGES[1:]},
        "channels": sorted(channels.values(), key=lambda row: row["channel"]),
        "campaigns": sorted(campaigns.values(), key=lambda row: row["campaign_id"]),
        "unit": "campaign-day session incidences", "definition": "Stage reach, not a strict sequential funnel; sessions may span multiple campaign/day buckets."})


def _attribution_rollup(rows: Sequence[Mapping[str, object]], dimensions: Sequence[Mapping[str, object]]) -> dict[str, object]:
    lookup = {row["campaign_id"]: row for row in dimensions}
    empty = lambda: {**dict.fromkeys(MONEY, 0), "allocated_order_count": Decimal("0.000000")}
    totals = {model: empty() for model in MODELS}
    campaigns, channels = {}, {}
    for row in rows:
        model = row["model"]
        if model not in MODELS or row["allocated_gross_minor"]-row["allocated_refund_minor"] != row["allocated_net_minor"]:
            raise AnalysisError("reconciliation_failed")
        dimension = campaign_dimension(row["campaign_id"], lookup)
        campaign = campaigns.setdefault((model, row["campaign_id"]), dimension | {"model": model} | empty())
        channel = channels.setdefault((model, dimension["channel"]), {"model": model, "channel": dimension["channel"]} | empty())
        for target in (totals[model], campaign, channel):
            for name, source in zip(MONEY, ALLOCATED):
                target[name] += row[source]
            target["allocated_order_count"] += Decimal(row["allocated_order_count"])
    for row in [*totals.values(), *campaigns.values(), *channels.values()]:
        row["allocated_order_count"] = format(row["allocated_order_count"], ".6f")
    for row in [*campaigns.values(), *channels.values()]:
        row["net_contribution_ratio"] = ratio(row["net_revenue_minor"], totals[row["model"]]["net_revenue_minor"])
    return {"models": [{"model": model, "definition": DEFINITIONS[model], **totals[model]} for model in MODELS],
            "channels": sorted(channels.values(), key=lambda row: (MODELS.index(row["model"]), row["channel"])),
            "campaigns": sorted(campaigns.values(), key=lambda row: (MODELS.index(row["model"]), row["campaign_id"]))}


def summarize_attribution(rows: Sequence[Mapping[str, object]], baseline: Mapping[str, int],
                          dimensions: Sequence[Mapping[str, object]]) -> dict[str, object]:
    result = _attribution_rollup(rows, dimensions)
    models = {row["model"] for row in rows}
    if (rows and models != set(MODELS)) or baseline["gross_revenue_minor"]-baseline["refund_amount_minor"] != baseline["net_revenue_minor"]:
        raise AnalysisError("reconciliation_failed")
    if any(any(row[name] != baseline[name] for name in MONEY) for row in result["models"]):
        raise AnalysisError("reconciliation_failed")
    return wire(result | {"reconciliation": {"balanced": True, "scope": "All campaigns before dimension filters", "source_totals": dict(baseline),
        "model_totals": [{key: row[key] for key in ("model", *MONEY)} for row in result["models"]]}})


def filter_sql(filters: DashboardFilter, settings: Sequence[CampaignSetting], *, attribution: bool = False,
               dataset_currency: str | None = None) -> tuple[str, dict[str, object]]:
    clauses = ["a.event_date>={from_date:Date32}", "a.event_date<{to_date:Date32}"]
    params = {"from_date": filters.from_date, "to_date": filters.to_date}
    for name in ("platform", "objective", "campaign_id"):
        value = getattr(filters, name)
        if value is not None:
            params[name] = value
            alias = "a" if name == "campaign_id" else "c"
            clauses.append(f"{alias}.{name}={{{name}:String}}")
    if filters.market:
        params["market_campaigns"] = [row.campaign_id for row in settings if row.market == filters.market]
        clauses.append("a.campaign_id IN {market_campaigns:Array(String)}")
    if filters.currency:
        params["currency"] = filters.currency
        if attribution:
            params["dataset_currency"] = dataset_currency
            clauses.append("{currency:String}={dataset_currency:String}")
        else:
            clauses.append("c.currency={currency:String}")
    return " AND ".join(clauses), params


def provenance(session: Analytics, model: str, rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    maximum = max((row["event_date"] for row in rows), default=None)
    through = datetime.combine(maximum+timedelta(days=1), time.min, timezone.utc) if maximum else None
    now = datetime.now(timezone.utc)
    return session.provenance() | {"model": "gold."+model, "serving_relation": model,
        "data_through": through.isoformat() if through else None, "timestamp_resolution": "day", "observed_at": now.isoformat(),
        "historical": maximum is not None and maximum < now.date()-timedelta(days=1)}


def single_currency(session: Analytics) -> str:
    rows = session.client.query(f"SELECT DISTINCT currency FROM {session.release.database}.campaigns "
        f"UNION DISTINCT SELECT DISTINCT currency FROM {session.release.database}.campaign_performance_daily",
        settings={"use_query_cache": 0}).result_rows
    if len(rows) != 1 or not rows[0][0]:
        raise AnalysisError("unsupported_currency")
    return rows[0][0]


class AnalysisService:
    def __init__(self, analytics: Analytics):
        self.analytics = analytics

    def funnel(self, filters: DashboardFilter, settings: Sequence[CampaignSetting]) -> dict[str, object]:
        session = self.analytics.pin()
        where, params = filter_sql(filters, settings)
        rows = session._query(f"SELECT a.* FROM {session.release.database}.funnel_daily a "
            f"LEFT JOIN {session.release.database}.campaigns c USING(campaign_id) WHERE {where}", params)
        return summarize_funnel(rows, session.campaigns()) | {"provenance": provenance(session, "funnel_daily", rows)}

    def attribution(self, filters: DashboardFilter, settings: Sequence[CampaignSetting]) -> dict[str, object]:
        session = self.analytics.pin()
        currency = single_currency(session)
        params = {"from_date": filters.from_date, "to_date": filters.to_date, "currency": filters.currency or currency}
        window = "event_date>={from_date:Date32} AND event_date<{to_date:Date32}"
        all_rows = session._query(f"SELECT * FROM {session.release.database}.attribution_daily WHERE {window}", params) if params["currency"] == currency else []
        baseline = session._query("SELECT sum(toInt128(gross_revenue_minor)) AS gross_revenue_minor,"
            "sum(toInt128(refund_amount_minor)) AS refund_amount_minor,sum(toInt128(net_revenue_minor)) AS net_revenue_minor "
            f"FROM {session.release.database}.campaign_performance_daily WHERE {window} AND currency={{currency:String}}", params)[0]
        dimensions = session.campaigns()
        global_result = summarize_attribution(all_rows, baseline, dimensions)
        where, bound = filter_sql(filters, settings, attribution=True, dataset_currency=currency)
        rows = session._query(f"SELECT a.* FROM {session.release.database}.attribution_daily a "
            f"LEFT JOIN {session.release.database}.campaigns c USING(campaign_id) WHERE {where}", bound)
        return wire(_attribution_rollup(rows, dimensions) | {"global_reconciliation": global_result["reconciliation"],
            "currency": currency, "definitions": DEFINITIONS, "provenance": provenance(session, "attribution_daily", rows)})
