"""Allowlisted analytical reads pinned to one certified MP-05 release."""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

from marketpulse_batch.ledger import PublicationReservation
from marketpulse_batch.publication import NoPublishedRelease, resolve_active
from .contracts import CampaignSetting, DashboardFilter
from .math import ratio

SUM_FIELDS = ("impressions", "clicks", "spend_minor", "orders", "gross_revenue_minor", "refund_amount_minor", "net_revenue_minor")


def wire(value: Any) -> Any:
    """Keep all integers exact across JSON/JavaScript; versions are also strings."""
    if type(value) is int:
        return str(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: wire(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [wire(item) for item in value]
    return value


class Analytics:
    def __init__(self, client: Any, live_store: Any, *, release: PublicationReservation | None = None):
        self.client, self.live_store, self.release = client, live_store, release

    def pin(self) -> Analytics:
        if self.release:
            return self
        exists = self.client.query("SELECT count() FROM system.tables WHERE database='marketpulse_serving' AND name='publication_journal'").first_row[0]
        if not exists:
            raise NoPublishedRelease("no certified release published")
        return Analytics(self.client, self.live_store, release=resolve_active(self.client))

    def provenance(self) -> dict[str, object]:
        session = self.pin()
        return {"generation": str(session.release.generation), "identity": session.release.identity,
                "proof_digest": session.release.proof_digest, "logical_day": session.release.logical_day.isoformat(),
                "model": "gold.campaign_performance_daily", "serving_relation": "campaign_dashboard"}

    def _query(self, sql: str, parameters: dict[str, object] | None = None) -> list[dict[str, object]]:
        result = self.client.query(sql, parameters=parameters or {}, query_tz="UTC", tz_mode="aware", settings={"use_query_cache": 0})
        return [dict(zip(result.column_names, row)) for row in result.result_rows]

    def campaigns(self) -> list[dict[str, object]]:
        session = self.pin()
        return session._query(f"SELECT * FROM {session.release.database}.campaigns ORDER BY campaign_id")

    def all_daily(self) -> list[dict[str, object]]:
        session = self.pin()
        return session._query(f"SELECT * FROM {session.release.database}.campaign_performance_daily ORDER BY event_date,campaign_id,currency")

    def daily(self, filters: DashboardFilter, settings: Sequence[CampaignSetting] = ()) -> list[dict[str, object]]:
        session = self.pin()
        where = ["p.event_date>={from_date:Date32}", "p.event_date<{to_date:Date32}"]
        parameters: dict[str, object] = {"from_date": filters.from_date, "to_date": filters.to_date}
        for field, alias in (("platform", "c"), ("objective", "c"), ("campaign_id", "p"), ("currency", "p")):
            value = getattr(filters, field)
            if value is not None:
                parameters[field] = value
                where.append(f"{alias}.{field}={{{field}:String}}")
        if filters.market:
            parameters["market_campaigns"] = [setting.campaign_id for setting in settings if setting.market == filters.market]
            where.append("p.campaign_id IN {market_campaigns:Array(String)}")
        return session._query(f"SELECT p.*,c.objective FROM {session.release.database}.campaign_dashboard p "
            f"LEFT JOIN {session.release.database}.campaigns c USING(campaign_id) WHERE {' AND '.join(where)} "
            "ORDER BY p.event_date,p.campaign_id,p.currency", parameters)

    def dashboard(self, filters: DashboardFilter, settings: Sequence[CampaignSetting]) -> dict[str, object]:
        session = self.pin()
        daily = session.daily(filters, settings)
        campaigns = session.campaigns()
        configured = {row.campaign_id: row for row in settings}
        totals = defaultdict(lambda: {field: 0 for field in SUM_FIELDS})
        groups = {}
        for row in daily:
            key = (row["campaign_id"], row["currency"])
            if key not in groups:
                setting = configured.get(row["campaign_id"])
                groups[key] = {"campaign_id": key[0], "currency": key[1], "campaign_name": row.get("campaign_name"),
                    "platform": row.get("platform"), "objective": row.get("objective"),
                    "configured_market": setting.market if setting else None, **{field: 0 for field in SUM_FIELDS}}
            for field in SUM_FIELDS:
                groups[key][field] += row[field]
                totals[row["currency"]][field] += row[field]
        for row in groups.values():
            row.update(ctr=ratio(row["clicks"], row["impressions"]), cpc_minor=ratio(row["spend_minor"], row["clicks"]),
                       source_roas=ratio(row["net_revenue_minor"], row["spend_minor"]))
        now = datetime.now(timezone.utc)
        maximum = max((row["event_date"] for row in daily), default=None)
        through = datetime.combine(maximum + timedelta(days=1), time.min, timezone.utc) if maximum else None
        provenance = session.provenance() | {"max_event_date": maximum.isoformat() if maximum else None,
            "data_through": through.isoformat() if through else None, "timestamp_resolution": "day", "observed_at": now.isoformat()}
        age = int((now-through).total_seconds()) if through else None
        freshness = {"age_seconds": str(age) if age is not None else None, "stale": age is None or age > 300,
                     "historical": maximum is not None and maximum < now.date()-timedelta(days=1)}
        streaming: dict[str, object] = {"status": "unavailable", "rows": [], "source": "marketing.gold.campaign-metrics.v1"}
        if session.live_store is not None:
            exists = session.client.query("SELECT count() FROM system.tables WHERE database='marketpulse_live' AND name='campaign_snapshots'").first_row[0]
            if exists:
                live = session.live_store.latest()
                dimensions = {row["campaign_id"]: row for row in campaigns}
                rows = []
                for item in live:
                    row = item["payload"]
                    dimension = dimensions.get(row["campaign_id"])
                    setting = configured.get(row["campaign_id"])
                    if not filters.from_date <= date.fromisoformat(row["event_date"]) < filters.to_date:
                        continue
                    if filters.campaign_id and row["campaign_id"] != filters.campaign_id:
                        continue
                    if filters.currency and row["currency"] != filters.currency:
                        continue
                    if any(getattr(filters, field) and (not dimension or dimension[field] != getattr(filters, field)) for field in ("platform", "objective")):
                        continue
                    if filters.market and (not dimension or not setting or setting.market != filters.market):
                        continue
                    from .streaming import utc_timestamp
                    event_age = int((now-utc_timestamp(row["max_event_time"])).total_seconds())
                    rows.append(wire(row | {"received_at": item["received_at"], "partition": item["partition"], "offset": item["offset"],
                        "mapped": dimension is not None, "event_age_seconds": event_age,
                        "transport_age_seconds": int((now-item["received_at"]).total_seconds()), "stale": event_age > 300}))
                partitions = sorted({int(row["partition"]) for row in rows})
                bounds = [{"partition": str(partition),
                    "from_offset": str(min(int(row["offset"]) for row in rows if int(row["partition"]) == partition)),
                    "to_offset": str(max(int(row["offset"]) for row in rows if int(row["partition"]) == partition)+1)} for partition in partitions]
                streaming = streaming | {"status": "available" if rows else "empty", "rows": rows,
                    "source_identity": session.live_store.source_identity(), "offset_bounds": bounds,
                    "offset_scope": "Bounds of latest selected keyed snapshots; gaps may exist"}
        return wire({"filters": vars_filter(filters), "certified": {"rows": list(groups.values()),
            "totals": [{"currency": currency, **values} for currency, values in sorted(totals.items())],
            "provenance": provenance, "freshness": freshness}, "streaming": streaming, "campaigns": campaigns})


def vars_filter(filters: DashboardFilter) -> dict[str, object]:
    from dataclasses import asdict
    return asdict(filters)
