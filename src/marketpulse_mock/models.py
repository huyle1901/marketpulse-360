"""Immutable domain models shared by generator components."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class Customer:
    customer_id: str
    email: str
    phone_token: str
    created_at: datetime
    country: str
    city: str
    loyalty_tier: str
    consent_analytics: bool
    consent_marketing: bool


@dataclass(frozen=True, slots=True)
class Product:
    product_id: str
    sku: str
    name: str
    category: str
    brand: str
    unit_price_minor: int
    currency: str = "VND"


@dataclass(frozen=True, slots=True)
class Campaign:
    campaign_id: str
    platform: str
    platform_campaign_id: str
    name: str
    objective: str
    status: str
    start_date: date
    end_date: date
    daily_budget_minor: int
    currency: str = "VND"


@dataclass(frozen=True, slots=True)
class Ad:
    ad_id: str
    campaign_id: str
    platform_ad_id: str
    name: str
    creative_format: str
    landing_page_url: str


@dataclass(frozen=True, slots=True)
class MasterData:
    customers: tuple[Customer, ...]
    products: tuple[Product, ...]
    campaigns: tuple[Campaign, ...]
    ads: tuple[Ad, ...]
