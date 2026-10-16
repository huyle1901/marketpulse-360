"""FastAPI campaign operations with strict inputs and safe HTTP error boundaries."""
from __future__ import annotations

import os
import re
import uuid
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from marketpulse_batch.publication import NoPublishedRelease
from .alerts import evaluate_alerts
from .analytics import wire
from .contracts import AlertRule, CampaignSetting, DashboardFilter, MAX_MONEY
from .math import pacing, simulate
from .store import VersionConflict


class SettingInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    market: str | None = None
    currency: str
    period_start: date
    period_end: date
    budget_minor: str = Field(pattern=r"^[0-9]+$", max_length=19)
    expected_version: int = Field(default=0, strict=True, ge=0, le=MAX_MONEY)


class RuleInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    campaign_id: str
    kind: str
    threshold: str = Field(pattern=r"^[0-9]+(?:\.[0-9]+)?$", max_length=50)
    enabled: bool = Field(default=True, strict=True)
    expected_version: int = Field(default=0, strict=True, ge=0, le=MAX_MONEY)


class AllocationInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    campaign_id: str
    budget_minor: str = Field(pattern=r"^[0-9]+$", max_length=19)


class SimulationInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    from_date: date
    to_date: date
    currency: str
    total_budget_minor: str = Field(pattern=r"^[0-9]+$", max_length=19)
    allocations: list[AllocationInput] = Field(min_length=1, max_length=100)


def create_app(analytics: Any, store: Any) -> FastAPI:
    app = FastAPI(title="MarketPulse 360 Campaign Operations", version="0.6.0")

    @app.middleware("http")
    async def cache_boundary(request: Request, call_next):
        try:
            response = await call_next(request)
        except Exception:
            response = JSONResponse({"code": "dependency_unavailable", "message": "Service dependency unavailable"}, status_code=503)
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(NoPublishedRelease)
    async def unpublished(request, error):
        return JSONResponse({"code": "no_published_release", "message": "Publish a certified dataset first"}, status_code=409)

    @app.exception_handler(VersionConflict)
    async def conflict(request, error):
        return JSONResponse({"code": "version_conflict", "message": "Reload the setting or rule before saving"}, status_code=409)

    @app.exception_handler(ValueError)
    async def invalid(request, error):
        return JSONResponse({"code": "invalid_input", "message": "Input does not meet the campaign operation contract"}, status_code=422)

    @app.exception_handler(RequestValidationError)
    async def validation(request, error):
        return JSONResponse({"code": "invalid_input", "message": "Check dates, amounts and required fields"}, status_code=422)

    def campaign(session, campaign_id: str, currency: str | None = None):
        found = next((row for row in session.campaigns() if row["campaign_id"] == campaign_id), None)
        if found is None or currency is not None and found["currency"] != currency:
            raise ValueError("unknown campaign or currency mismatch")
        return found

    @app.get("/api/v1/health/live")
    def live():
        return {"status": "ok"}

    @app.get("/api/v1/health/ready")
    def ready():
        analytics.client.query("SELECT 1")
        with store.connect() as connection:
            connection.execute("SELECT 1")
        return {"status": "ready"}

    @app.get("/api/v1/dashboard")
    def dashboard(from_date: date | None = None, to_date: date | None = None,
                  platform: str | None = None, campaign_id: str | None = None,
                  objective: str | None = None, market: str | None = None, currency: str | None = None):
        session = analytics.pin()
        if from_date is None or to_date is None:
            rows = session.all_daily()
            end = max((row["event_date"] for row in rows), default=date.today()) + timedelta(days=1)
            start = max(min((row["event_date"] for row in rows), default=end-timedelta(days=1)), end-timedelta(days=366))
            from_date, to_date = from_date or start, to_date or end
        return session.dashboard(DashboardFilter(from_date, to_date, platform, campaign_id, objective, market, currency), store.settings())

    @app.get("/api/v1/campaign-settings")
    def campaign_settings():
        return wire([asdict(row) for row in store.settings()])

    @app.put("/api/v1/campaign-settings/{campaign_id}")
    def save_setting(campaign_id: str, body: SettingInput):
        session = analytics.pin()
        campaign(session, campaign_id, body.currency)
        setting = CampaignSetting(campaign_id, body.market, body.currency, body.period_start, body.period_end, int(body.budget_minor))
        return wire(asdict(store.save_setting(setting, body.expected_version)))

    @app.get("/api/v1/alert-rules")
    def rules():
        return wire([asdict(row) for row in store.rules()])

    def save_rule(rule_id: str, body: RuleInput):
        session = analytics.pin()
        campaign(session, body.campaign_id)
        rule = AlertRule(rule_id, body.campaign_id, body.kind, Decimal(body.threshold), body.enabled)
        return wire(asdict(store.save_rule(rule, body.expected_version)))

    @app.post("/api/v1/alert-rules")
    def create_rule(body: RuleInput):
        if body.expected_version != 0:
            raise ValueError("new rule requires expected version 0")
        return save_rule(str(uuid.uuid4()), body)

    @app.put("/api/v1/alert-rules/{rule_id}")
    def update_rule(rule_id: str, body: RuleInput):
        return save_rule(rule_id, body)

    @app.post("/api/v1/alerts/evaluate")
    def evaluate():
        return evaluate_alerts(analytics, store, datetime.now(timezone.utc).date())

    @app.get("/api/v1/alerts")
    def alerts():
        return store.alerts()

    @app.get("/api/v1/budget-pacing/{campaign_id}")
    def budget_pacing(campaign_id: str):
        session = analytics.pin()
        setting = next((row for row in store.settings() if row.campaign_id == campaign_id), None)
        if setting is None:
            raise HTTPException(404, "Configure a campaign period budget first")
        campaign(session, campaign_id, setting.currency)
        rows = session.daily(DashboardFilter(setting.period_start, setting.period_end, campaign_id=campaign_id, currency=setting.currency))
        result = pacing(setting, {row["event_date"]: row["spend_minor"] for row in rows}, datetime.now(timezone.utc).date())
        return result | {"campaign_id": campaign_id, "currency": setting.currency, "setting_version": str(setting.version),
                         "period_start": setting.period_start.isoformat(), "period_end": setting.period_end.isoformat(),
                         "provenance": session.provenance()}

    @app.post("/api/v1/budget-simulation")
    def simulation(body: SimulationInput):
        session = analytics.pin()
        filters = DashboardFilter(body.from_date, body.to_date, currency=body.currency)
        allocations = {row.campaign_id: int(row.budget_minor) for row in body.allocations}
        if len(allocations) != len(body.allocations):
            raise ValueError("duplicate campaign allocations")
        for campaign_id in allocations:
            campaign(session, campaign_id, body.currency)
        baselines = {}
        for row in session.daily(filters):
            spend, net = baselines.get(row["campaign_id"], (0, 0))
            baselines[row["campaign_id"]] = (spend+row["spend_minor"], net+row["net_revenue_minor"])
        result = simulate(baselines, allocations, int(body.total_budget_minor))
        return result | {"currency": body.currency, "from_date": body.from_date.isoformat(), "to_date": body.to_date.isoformat(),
                         "provenance": session.provenance()}

    from .analysis_api import register_analysis_routes
    register_analysis_routes(app, analytics, store)
    from .lifecycle_api import register_lifecycle_routes
    register_lifecycle_routes(app, analytics, store)
    return app


def runtime_app() -> FastAPI:
    from .settings import clients, load_settings
    analytics, store = clients(load_settings(os.environ))
    store.initialize()
    from .segment_store import SegmentStore
    SegmentStore(store).initialize()
    return create_app(analytics, store)
