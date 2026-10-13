"""Pure exact calculations; never imply forecasts from absent daily data."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date, timedelta
from decimal import Decimal, ROUND_DOWN, localcontext

from .contracts import AlertRule, CampaignSetting, integer


def ratio(numerator: int | Decimal, denominator: int | Decimal) -> str | None:
    if not denominator:
        return None
    with localcontext() as context:
        context.prec = 80
        return format((Decimal(numerator) / Decimal(denominator)).quantize(Decimal("0.000001"), rounding=ROUND_DOWN), "f")


def pacing(setting: CampaignSetting, daily_spend: Mapping[date, int], today: date) -> dict[str, object]:
    days = (setting.period_end - setting.period_start).days
    closed = min(max((today - setting.period_start).days, 0), days)
    rows = {day: amount for day, amount in daily_spend.items()
            if setting.period_start <= day < min(today, setting.period_end)}
    for amount in rows.values():
        integer(amount)
    spend = sum(rows.values())
    prefix = 0
    while prefix < closed and setting.period_start + timedelta(days=prefix) in rows:
        prefix += 1
    gap = any(day >= setting.period_start + timedelta(days=prefix) for day in rows)
    usable = prefix > 0 and not gap
    with localcontext() as context:
        context.prec = 80
        expected = Decimal(setting.budget_minor) * prefix / days if usable else None
        forecast = int(Decimal(spend) * days / prefix) if usable else None
        return {"status": "insufficient_data" if not usable else "complete" if prefix == closed else "incomplete",
                "completed_days": str(prefix), "period_days": str(days), "spend_minor": str(spend),
                "budget_minor": str(setting.budget_minor), "remaining_minor": str(setting.budget_minor - spend),
                "expected_spend_minor": ratio(expected, 1) if expected is not None else None,
                "pace_ratio": ratio(spend, expected) if expected is not None else None,
                "forecast_minor": str(forecast) if forecast is not None else None,
                "data_through": max(rows).isoformat() if rows else None,
                "assumption": "Closed daily spend, projected as of data_through"}


def simulate(baselines: Mapping[str, tuple[int, int]], allocations: Mapping[str, int], total_budget_minor: int) -> dict[str, object]:
    integer(total_budget_minor, 1)
    if not 1 <= len(allocations) <= 100:
        raise ValueError("scenario needs 1..100 campaigns")
    for amount in allocations.values():
        integer(amount)
    if sum(allocations.values()) != total_budget_minor:
        raise ValueError("allocations must sum exactly to total budget")
    rows = []
    total = 0
    complete = True
    with localcontext() as context:
        context.prec = 80
        for campaign_id, amount in allocations.items():
            spend, net = baselines.get(campaign_id, (0, 0))
            projected = int(Decimal(amount) * net / spend) if spend > 0 else 0 if amount == 0 else None
            complete = complete and projected is not None
            total += projected or 0
            rows.append({"campaign_id": campaign_id, "allocated_budget_minor": str(amount),
                         "historical_spend_minor": str(spend), "historical_net_minor": str(net),
                         "historical_net_roas": ratio(net, spend),
                         "projected_net_minor": str(projected) if projected is not None else None,
                         "status": "available" if projected is not None else "insufficient_data"})
    return {"rows": rows, "total_budget_minor": str(total_budget_minor),
            "projected_net_minor": str(total) if complete else None,
            "assumption": "Linear scenario using historical net ROAS; not a prediction"}


def evaluate_rule(rule: AlertRule, operands: Mapping[str, object]) -> dict[str, object]:
    numerator = denominator = None
    if rule.kind == "refund_share":
        numerator, denominator = operands.get("refund_amount_minor"), operands.get("gross_revenue_minor")
    elif rule.kind == "spend_spike":
        numerator, denominator = operands.get("spend_minor"), operands.get("previous_spend_minor")
    elif rule.kind == "budget_overrun":
        numerator, denominator = operands.get("spend_minor"), operands.get("budget_minor")
    usable = type(numerator) is int and type(denominator) is int and denominator > 0
    triggered = usable and Decimal(numerator) > rule.threshold * Decimal(denominator)
    outcome = "insufficient_data" if not usable else "triggered" if triggered else "clear"
    return {"outcome": outcome, "kind": rule.kind, "threshold": str(rule.threshold),
            "observed_ratio": ratio(numerator, denominator) if usable else None,
            "operands": {key: str(value) if type(value) is int else value for key, value in operands.items()},
            "comparator": ">", "explanation": f"{rule.kind}: observed numerator must exceed {rule.threshold} × baseline; {outcome}"}
