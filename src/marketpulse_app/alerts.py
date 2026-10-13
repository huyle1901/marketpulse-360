"""Explainable certified-data alerts with immutable, version-bound identities."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from typing import Any

from .contracts import AlertRule, CampaignSetting
from .math import evaluate_rule


def build_evaluations(provenance: Mapping[str, object], campaigns: Sequence[Mapping[str, object]],
                      daily: Sequence[Mapping[str, object]], settings: Sequence[CampaignSetting],
                      rules: Sequence[AlertRule], today: date) -> list[tuple[str, dict[str, object]]]:
    dimensions = {row["campaign_id"]: row for row in campaigns}
    configured = {row.campaign_id: row for row in settings}
    closed = [row for row in daily if row["event_date"] < today]
    latest = max((row["event_date"] for row in closed), default=None)
    results = []
    for rule in rules:
        if not rule.enabled or rule.campaign_id not in dimensions:
            continue
        currency = dimensions[rule.campaign_id]["currency"]
        campaign_rows = {row["event_date"]: row for row in closed
                         if row["campaign_id"] == rule.campaign_id and row["currency"] == currency}
        setting = configured.get(rule.campaign_id)
        operands = {}
        start, end = latest, latest + timedelta(days=1) if latest else None
        if rule.kind == "budget_overrun":
            start, end = (setting.period_start, setting.period_end) if setting else (None, None)
            if setting and setting.currency == currency and setting.period_end <= today:
                dates = [start + timedelta(days=i) for i in range((end-start).days)]
                if all(day in campaign_rows for day in dates):
                    operands = {"spend_minor": sum(campaign_rows[day]["spend_minor"] for day in dates),
                                "budget_minor": setting.budget_minor}
        elif latest in campaign_rows:
            row = campaign_rows[latest]
            if rule.kind == "refund_share":
                operands = {"gross_revenue_minor": row["gross_revenue_minor"], "refund_amount_minor": row["refund_amount_minor"]}
            elif latest - timedelta(days=1) in campaign_rows:
                operands = {"spend_minor": row["spend_minor"],
                            "previous_spend_minor": campaign_rows[latest-timedelta(days=1)]["spend_minor"]}
                start = latest - timedelta(days=1)
        result = evaluate_rule(rule, operands)
        evidence = result | {"rule_id": rule.rule_id, "rule_version": str(rule.version), "campaign_id": rule.campaign_id,
            "setting_version": str(setting.version if setting else 0), "currency": currency,
            "from_date": start.isoformat() if start else None, "to_date": end.isoformat() if end else None,
            "provenance": dict(provenance), "model": "gold.campaign_performance_daily"}
        # Operands are deterministic within this immutable release/versions/window.
        identity = {key: evidence[key] for key in ("rule_id", "rule_version", "setting_version", "from_date", "to_date")}
        identity["release_identity"] = provenance["identity"]
        digest = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        results.append((digest, evidence))
    return results


def evaluate_alerts(analytics: Any, store: Any, today: date) -> dict[str, object]:
    session = analytics.pin()
    evaluations = build_evaluations(session.provenance(), session.campaigns(), session.all_daily(),
                                    store.settings(), store.rules(), today)
    inserted = sum(store.record_evaluation(key, evidence) for key, evidence in evaluations)
    return {"evaluated": str(len(evaluations)), "new_evaluations": str(inserted),
            "triggered": str(sum(row[1]["outcome"] == "triggered" for row in evaluations)),
            "provenance": session.provenance()}
