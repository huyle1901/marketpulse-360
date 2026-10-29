"""Dependency-free, strict application input contracts."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

MAX_MONEY = 2**63 - 1


def identifier(value: str) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > 256:
        raise ValueError("invalid identifier")


def integer(value: int, minimum: int = 0) -> None:
    if type(value) is not int or not minimum <= value <= MAX_MONEY:
        raise ValueError("integer amount out of range")


def interval(start: date, end: date) -> None:
    if (type(start) is not date or type(end) is not date or
            not date(1900, 1, 1) <= start < end <= date(2299, 12, 31) or
            not 1 <= (end - start).days <= 366):
        raise ValueError("requires UTC [from,to) interval of 1..366 days")


def read_interval(start: date, end: date) -> None:
    """Validate a historical analytical selection without expanding write periods."""
    if (type(start) is not date or type(end) is not date or
            not date(1900, 1, 1) <= start < end <= date(2299, 12, 31) or
            not 1 <= (end - start).days <= 731):
        raise ValueError("requires UTC [from,to) interval of 1..731 days")


def code(value: str | None, length: int) -> None:
    if value is not None and (not isinstance(value, str) or re.fullmatch(rf"[A-Z]{{{length}}}", value) is None):
        raise ValueError("invalid currency/market code")


@dataclass(frozen=True, slots=True)
class DashboardFilter:
    from_date: date
    to_date: date
    platform: str | None = None
    campaign_id: str | None = None
    objective: str | None = None
    market: str | None = None
    currency: str | None = None

    def __post_init__(self) -> None:
        read_interval(self.from_date, self.to_date)
        if self.platform is not None and self.platform not in {"google", "meta", "tiktok"}:
            raise ValueError("unsupported platform")
        if self.objective is not None and self.objective not in {"awareness", "traffic", "conversions"}:
            raise ValueError("unsupported objective")
        if self.campaign_id is not None:
            identifier(self.campaign_id)
        code(self.market, 2)
        code(self.currency, 3)


@dataclass(frozen=True, slots=True)
class CampaignSetting:
    campaign_id: str
    market: str | None
    currency: str
    period_start: date
    period_end: date
    budget_minor: int
    version: int = 0

    def __post_init__(self) -> None:
        identifier(self.campaign_id)
        code(self.market, 2)
        code(self.currency, 3)
        if self.currency is None:
            raise ValueError("currency required")
        interval(self.period_start, self.period_end)
        integer(self.budget_minor, 1)
        integer(self.version)


@dataclass(frozen=True, slots=True)
class AlertRule:
    rule_id: str
    campaign_id: str
    kind: str
    threshold: Decimal
    enabled: bool = True
    version: int = 0

    def __post_init__(self) -> None:
        identifier(self.rule_id)
        identifier(self.campaign_id)
        integer(self.version)
        if type(self.enabled) is not bool or not isinstance(self.threshold, Decimal) or not self.threshold.is_finite():
            raise ValueError("invalid rule threshold/enabled")
        if self.kind == "refund_share" and 0 <= self.threshold <= 1:
            return
        if self.kind == "spend_spike" and 1 < self.threshold <= MAX_MONEY:
            return
        if self.kind == "budget_overrun" and self.threshold == 1:
            return
        raise ValueError("unsupported rule or threshold")
