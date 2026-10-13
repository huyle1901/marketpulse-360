"""Independent streaming Python oracles for the documented fixture workloads."""

from __future__ import annotations

import heapq
from collections.abc import Iterable
from decimal import Decimal

from .queries import QueryRequest


def fixture_oracle(request: QueryRequest, rows: Iterable[dict[str, object]], *, currency: str = "VND") -> list[dict[str, object]]:
    start, end = request.from_date.isoformat(), request.to_date.isoformat()
    if request.workload == "daily_funnel":
        return heapq.nsmallest(request.limit, (row for row in rows if start <= str(row["event_date"]) < end),
                               key=lambda row: (row["event_date"], row["campaign_id"]))
    if request.workload == "cohort_retention":
        return heapq.nsmallest(request.limit, (row for row in rows if start <= str(row["activity_month"]) < end),
                               key=lambda row: (row["cohort_month"], row["activity_month"], row["profile_type"]))
    if request.workload == "customer_drill":
        if request.currency and request.currency != currency:
            return []
        selected = (row | {"currency": currency, "financial_scope": "snapshot_lifetime"} for row in rows
                    if row["customer_id"] == request.customer_id and start <= str(row["last_activity_at"])[:10] < end)
        return heapq.nsmallest(request.limit, selected, key=lambda row: row["profile_key"])
    groups = {}
    campaigns = {}
    for row in rows:
        if not start <= str(row["event_date"]) < end:
            continue
        if request.workload == "attribution_comparison":
            if request.currency and request.currency != currency:
                continue
            key = row["model"]
            target = groups.setdefault(key, {"model": key, "currency": currency, "allocated_order_count": Decimal("0.000000"),
                                              "gross_revenue_minor": 0, "refund_amount_minor": 0, "net_revenue_minor": 0})
            target["allocated_order_count"] += Decimal(row["allocated_order_count"])
            for source, dest in (("allocated_gross_minor", "gross_revenue_minor"), ("allocated_refund_minor", "refund_amount_minor"),
                                 ("allocated_net_minor", "net_revenue_minor")):
                target[dest] += row[source]
        elif request.workload == "campaign_overview":
            if request.currency and request.currency != row["currency"]:
                continue
            key = row["currency"]
            fields = ("impressions", "clicks", "spend_minor", "gross_revenue_minor", "refund_amount_minor", "net_revenue_minor")
            target = groups.setdefault(key, {"currency": key, **{field: 0 for field in fields}})
            for field in fields:
                target[field] += row[field]
            if row["campaign_id"] != "__unattributed__":
                campaigns.setdefault(key, set()).add(row["campaign_id"])
        else:
            if request.currency and request.currency != row["currency"]:
                continue
            key = (row["campaign_id"], row["currency"])
            target = groups.setdefault(key, {"campaign_id": key[0], "currency": key[1], "spend_minor": 0, "net_revenue_minor": 0})
            for field in ("spend_minor", "net_revenue_minor"):
                target[field] += row[field]
    if request.workload == "campaign_overview":
        return [groups[key] | {"campaigns": len(campaigns.get(key, set()))} for key in sorted(groups)]
    if request.workload == "attribution_comparison":
        return [groups[key] for key in sorted(groups)]
    return sorted(groups.values(), key=lambda row: (-row["net_revenue_minor"], row["campaign_id"], row["currency"]))[:request.limit]
