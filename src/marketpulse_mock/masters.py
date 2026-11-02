"""Deterministic synthetic marketing master-data generation."""

from __future__ import annotations

import math
from datetime import datetime, time, timedelta, timezone

from .config import GenerationConfig
from .ids import stable_id, stable_int
from .models import Ad, Campaign, Customer, MasterData, Product

PLATFORMS = ("google", "meta", "tiktok")
CATEGORIES = ("electronics", "home", "beauty", "fashion", "sports")
BRANDS = ("Aster", "Lotus", "Nova", "Orbit", "Pulse")
CITIES = (
    ("VN", "Ho Chi Minh City"),
    ("VN", "Ha Noi"),
    ("VN", "Da Nang"),
    ("VN", "Can Tho"),
    ("VN", "Hai Phong"),
)
OBJECTIVES = ("awareness", "traffic", "conversions")
CREATIVE_FORMATS = ("image", "video", "carousel")


def generate_master_data(config: GenerationConfig) -> MasterData:
    """Generate all master entities required by a dataset run."""

    return MasterData(
        customers=_generate_customers(config),
        products=_generate_products(config),
        campaigns=_generate_campaigns(config),
        ads=_generate_ads(config),
    )


def _generate_customers(config: GenerationConfig) -> tuple[Customer, ...]:
    customers: list[Customer] = []
    tiers = ("bronze", "silver", "gold")
    for ordinal in range(config.profile.customers):
        customer_id = stable_id(config.seed, "customer", ordinal)
        country, city = _customer_city(config, ordinal)
        phone_digits = stable_int(
            config.seed, "customer-phone", ordinal, modulo=10_000_000_000
        )
        customers.append(
            Customer(
                customer_id=customer_id,
                email=(
                    f"customer-{customer_id[:12]}@marketpulse.example.test"
                ),
                phone_token=f"synthetic-{phone_digits:010d}",
                created_at=_customer_created_at(config, ordinal),
                country=country,
                city=city,
                loyalty_tier=tiers[
                    stable_int(config.seed, "customer-tier", ordinal, modulo=3)
                ],
                consent_analytics=not _is_scheduled_rate(
                    ordinal, config.anomalies.no_analytics_consent
                ),
                consent_marketing=(ordinal + 1) % 10 != 0,
            )
        )
    return tuple(customers)


def _customer_city(config: GenerationConfig, ordinal: int) -> tuple[str, str]:
    cities = config.scenario.cities
    if cities is None:
        return CITIES[
            stable_int(config.seed, "customer-city", ordinal, modulo=len(CITIES))
        ]
    total = sum(weight for _, _, weight in cities)
    point = stable_int(config.seed, "customer-city", ordinal, modulo=total)
    cumulative = 0
    for country, city, weight in cities:
        cumulative += weight
        if point < cumulative:
            return country, city
    raise AssertionError("unreachable: point is below the total weight")


def _customer_created_at(config: GenerationConfig, ordinal: int) -> datetime:
    anchor = datetime.combine(config.start_date, time.min, tzinfo=timezone.utc)
    scenario = config.scenario
    before_share = scenario.customers_before_start_share
    if before_share is None:
        days_ago = 30 + stable_int(config.seed, "customer-age", ordinal, modulo=700)
        return anchor - timedelta(days=days_ago)
    split = stable_int(config.seed, "customer-join", ordinal, modulo=1000)
    if split < round(before_share * 1000):
        days_ago = 1 + stable_int(
            config.seed, "customer-join-before", ordinal, modulo=730
        )
        return anchor - timedelta(days=days_ago)
    # Inverse CDF of the linear ramp density 1 + a*t on [0, 1], a = ramp - 1.
    fraction = (
        stable_int(config.seed, "customer-join-t", ordinal, modulo=1_000_000)
        / 1_000_000
    )
    slope = scenario.join_ramp - 1.0
    if slope == 0:
        t = fraction
    else:
        t = (-1 + math.sqrt(1 + 2 * slope * fraction * (1 + slope / 2))) / slope
    story_days = config.profile.full_days
    day = min(int(t * story_days), story_days - 1)
    return anchor + timedelta(days=day)


def _is_scheduled_rate(ordinal: int, rate: float) -> bool:
    if rate <= 0:
        return False
    interval = max(1, round(1 / rate))
    return (ordinal + 1) % interval == 0


def _generate_products(config: GenerationConfig) -> tuple[Product, ...]:
    products: list[Product] = []
    for ordinal in range(config.profile.products):
        category = CATEGORIES[ordinal % len(CATEGORIES)]
        brand = BRANDS[
            stable_int(config.seed, "product-brand", ordinal, modulo=len(BRANDS))
        ]
        price_step = stable_int(config.seed, "product-price", ordinal, modulo=190)
        products.append(
            Product(
                product_id=stable_id(config.seed, "product", ordinal),
                sku=f"MP-{ordinal + 1:06d}",
                name=f"{brand} {category.title()} {ordinal + 1}",
                category=category,
                brand=brand,
                unit_price_minor=99_000 + price_step * 10_000,
            )
        )
    return tuple(products)


def _generate_campaigns(config: GenerationConfig) -> tuple[Campaign, ...]:
    campaigns: list[Campaign] = []
    generation_end = config.start_date + timedelta(
        days=config.profile.full_days - 1
    )
    platform_prefix = {"google": "G", "meta": "M", "tiktok": "T"}
    for ordinal in range(config.profile.campaigns):
        platform = PLATFORMS[ordinal % len(PLATFORMS)]
        objective = OBJECTIVES[
            stable_int(config.seed, "campaign-objective", ordinal, modulo=3)
        ]
        campaigns.append(
            Campaign(
                campaign_id=stable_id(config.seed, "campaign", ordinal),
                platform=platform,
                platform_campaign_id=(
                    f"{platform_prefix[platform]}-{config.seed}-{ordinal + 1:05d}"
                ),
                name=f"{platform.title()} {objective.title()} {ordinal + 1}",
                objective=objective,
                status="active",
                start_date=config.start_date - timedelta(days=14),
                end_date=generation_end + timedelta(days=14),
                daily_budget_minor=(
                    1_000_000
                    + stable_int(
                        config.seed, "campaign-budget", ordinal, modulo=20
                    )
                    * 1_000_000
                ),
            )
        )
    return tuple(campaigns)


def _generate_ads(config: GenerationConfig) -> tuple[Ad, ...]:
    ads: list[Ad] = []
    platform_prefix = {"google": "GA", "meta": "MA", "tiktok": "TA"}
    for campaign_ordinal in range(config.profile.campaigns):
        platform = PLATFORMS[campaign_ordinal % len(PLATFORMS)]
        campaign_id = stable_id(config.seed, "campaign", campaign_ordinal)
        for creative_ordinal in range(2):
            ordinal = campaign_ordinal * 2 + creative_ordinal
            ads.append(
                Ad(
                    ad_id=stable_id(config.seed, "ad", ordinal),
                    campaign_id=campaign_id,
                    platform_ad_id=(
                        f"{platform_prefix[platform]}-{config.seed}-{ordinal + 1:06d}"
                    ),
                    name=f"Creative {campaign_ordinal + 1}-{creative_ordinal + 1}",
                    creative_format=CREATIVE_FORMATS[
                        stable_int(
                            config.seed, "ad-format", ordinal, modulo=3
                        )
                    ],
                    landing_page_url=(
                        f"https://shop.marketpulse.example.test/c/{campaign_id}"
                    ),
                )
            )
    return tuple(ads)
