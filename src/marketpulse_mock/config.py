"""Configuration contracts for deterministic dataset generation."""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True, slots=True)
class ScaleProfile:
    """Capacity settings for a named generator profile.

    ``days`` is the generated window. ``story_days`` is the length of the full
    story that window was cut from (None: the window is the whole story);
    story-wide distributions such as customer join dates and campaign flights
    follow ``story_days`` so a shorter window is the start of the same world.
    """

    name: str
    days: int
    customers: int
    campaigns: int
    products: int
    impressions_per_day: int
    rows_per_file: int
    scenario: str = "flat"
    story_days: int | None = None

    def __post_init__(self) -> None:
        for field in fields(self):
            if field.name in ("name", "scenario", "story_days"):
                continue
            value = getattr(self, field.name)
            if value <= 0:
                raise ValueError(f"{field.name} must be positive")
        if self.story_days is not None and self.story_days < self.days:
            raise ValueError("story_days must be at least days")
        if self.scenario not in SCENARIOS:
            choices = ", ".join(SCENARIOS)
            raise ValueError(
                f"unknown scenario {self.scenario!r}; choose one of: {choices}"
            )

    @property
    def full_days(self) -> int:
        """Length of the full story: ``story_days``, or ``days`` when unset."""

        return self.days if self.story_days is None else self.story_days


@dataclass(frozen=True, slots=True)
class Scenario:
    """World-shaping settings that make a profile's data varied.

    ``flat`` keeps the legacy uniform behaviour; its optional fields are
    None/0 and its ``join_ramp`` is the neutral 1.0.
    """

    name: str
    """Registry key, e.g. ``flat`` or ``vietnam-retail``."""

    cities: tuple[tuple[str, str, int], ...] | None = None
    """``(country, city, weight)`` entries for a weighted customer city draw;
    None keeps the legacy uniform five cities. Weights must be positive."""

    customers_before_start_share: float | None = None
    """Share (0-1) of customers created in the 730 days before the start;
    the rest join during the story. None keeps the legacy 30-730 day ages."""

    join_ramp: float = 1.0
    """Joins per day at the end of the story relative to the start (>= 0);
    1.0 is a flat rate, 2.0 doubles it."""

    tenure_months: float | None = None
    """Months after which a customer's session weight halves,
    ``1 / (1 + months / tenure_months)``; None picks customers uniformly."""

    returning_device_share: float = 0.0
    """Share (0-1) of anonymous paid sessions that reuse an earlier device;
    organic and direct visits always use a fresh one."""

    returning_window_days: int = 0
    """How many previous days (>= 0) a returning device is drawn from;
    0 disables returning devices."""

    platforms: tuple[str, ...] = ("google", "meta", "tiktok")
    """Ad platforms campaigns cycle through, in order; the campaign count
    must be divisible by their number."""

    platform_spend_share: Mapping[str, int] | None = None
    """Relative spend share per platform (positive; exactly the ``platforms``);
    None keeps the legacy round-robin allocation."""

    objective_spend_weight: Mapping[str, float] | None = None
    """Spend multiplier per campaign objective (positive) splitting a
    platform's spend between its campaigns; None (or an unlisted objective)
    weighs 1.0."""

    personas: Mapping[str, tuple[float, float, float]] | None = None
    """``(cost, ctr, strength)`` multipliers per platform (three positive
    floats; exactly the ``platforms``); None keeps every platform identical."""

    calendar_rhythm: bool = False
    """Size each UTC day and draw session times from the Vietnam retail
    calendar (``calendar.utc_hour_weights``); False keeps the legacy fixed
    daily count and evenly spaced session times."""

    device_strength: Mapping[str, float] | None = None
    """Purchase-strength multiplier per device (``desktop``, ``mobile``,
    ``tablet``; positive); None leaves every device's strength unchanged."""

    email_lifecycle: bool = False
    """Email every marketing-consented customer from their join date until
    they unsubscribe (welcome, promo and newsletter sends, see
    ``email_schedule``); False keeps the legacy fixed 2 % email list."""

    def __post_init__(self) -> None:
        if not self.platforms:
            raise ValueError("platforms must not be empty")
        if self.platform_spend_share is not None:
            for platform, share in self.platform_spend_share.items():
                if platform not in self.platforms:
                    raise ValueError(
                        f"platform_spend_share names unknown platform {platform!r}"
                    )
                if share <= 0:
                    raise ValueError(
                        f"platform_spend_share for {platform} must be positive"
                    )
            _require_every_platform(
                "platform_spend_share", self.platform_spend_share, self.platforms
            )
        if self.objective_spend_weight is not None:
            for objective, weight in self.objective_spend_weight.items():
                if weight <= 0:
                    raise ValueError(
                        f"objective_spend_weight for {objective} must be positive"
                    )
        if self.personas is not None:
            for platform, persona in self.personas.items():
                if platform not in self.platforms:
                    raise ValueError(f"personas names unknown platform {platform!r}")
                if len(persona) != 3 or any(value <= 0 for value in persona):
                    raise ValueError(
                        f"personas for {platform} must be three positive numbers"
                    )
            _require_every_platform("personas", self.personas, self.platforms)
        if self.device_strength is not None:
            if set(self.device_strength) != {"desktop", "mobile", "tablet"}:
                raise ValueError(
                    "device_strength must name exactly desktop, mobile and tablet"
                )
            if any(value <= 0 for value in self.device_strength.values()):
                raise ValueError("device_strength values must be positive")
        if self.cities is not None:
            if not self.cities:
                raise ValueError("cities must not be empty when set")
            for _country, city, weight in self.cities:
                if weight <= 0:
                    raise ValueError(f"city weight for {city} must be positive")
        share = self.customers_before_start_share
        if share is not None and not 0.0 <= share <= 1.0:
            raise ValueError("customers_before_start_share must be between 0 and 1")
        if self.join_ramp < 0:
            raise ValueError("join_ramp must not be negative")
        if self.tenure_months is not None and self.tenure_months <= 0:
            raise ValueError("tenure_months must be positive")
        if not 0.0 <= self.returning_device_share <= 1.0:
            raise ValueError("returning_device_share must be between 0 and 1")
        if self.returning_window_days < 0:
            raise ValueError("returning_window_days must not be negative")
        if self.email_lifecycle and not self.calendar_rhythm:
            # Email-referred visits are drawn only on the calendar branch.
            raise ValueError("email_lifecycle requires calendar_rhythm")


def _require_every_platform(
    name: str, mapping: Mapping[str, object], platforms: tuple[str, ...]
) -> None:
    missing = [platform for platform in platforms if platform not in mapping]
    if missing:
        raise ValueError(f"{name} is missing platforms: {', '.join(missing)}")


SCENARIOS: Mapping[str, Scenario] = MappingProxyType(
    {
        "flat": Scenario("flat"),
        "vietnam-retail": Scenario(
            "vietnam-retail",
            cities=(
                ("VN", "Ho Chi Minh City", 28),
                ("VN", "Ha Noi", 24),
                ("VN", "Bien Hoa", 6),
                ("VN", "Thu Dau Mot", 6),
                ("VN", "Hai Phong", 6),
                ("VN", "Da Nang", 6),
                ("VN", "Can Tho", 5),
                ("VN", "Nha Trang", 4),
                ("VN", "Hue", 3),
                ("VN", "Vung Tau", 3),
                ("VN", "Quy Nhon", 3),
                ("VN", "Buon Ma Thuot", 3),
                ("VN", "Ha Long", 3),
            ),
            customers_before_start_share=0.4,
            join_ramp=2.0,
            tenure_months=6.0,
            returning_device_share=0.3,
            returning_window_days=60,
            platforms=("google", "meta", "tiktok", "shopee"),
            platform_spend_share=MappingProxyType(
                {"google": 30, "meta": 28, "tiktok": 24, "shopee": 18}
            ),
            objective_spend_weight=MappingProxyType(
                {"conversions": 1.3, "traffic": 1.0, "awareness": 0.7}
            ),
            personas=MappingProxyType(
                {
                    "google": (1.4, 0.8, 1.3),
                    "meta": (1.0, 1.0, 1.0),
                    "tiktok": (0.6, 1.6, 0.5),
                    "shopee": (1.0, 1.1, 1.6),
                }
            ),
            calendar_rhythm=True,
            device_strength=MappingProxyType(
                {"mobile": 0.85, "desktop": 1.15, "tablet": 1.0}
            ),
            email_lifecycle=True,
        ),
    }
)


@dataclass(frozen=True, slots=True)
class AnomalyRates:
    """Rates used to schedule deterministic data-quality cases."""

    duplicate_event: float
    late_event: float
    invalid_case: float
    no_analytics_consent: float
    bot_session: float
    refund: float

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{field.name} must be between 0 and 1")


DEFAULT_ANOMALY_RATES = AnomalyRates(
    duplicate_event=0.005,
    late_event=0.01,
    invalid_case=0.002,
    no_analytics_consent=0.05,
    bot_session=0.002,
    refund=0.05,
)


PROFILES: Mapping[str, ScaleProfile] = MappingProxyType(
    {
        "fixture": ScaleProfile("fixture", 2, 100, 6, 50, 500, 1_000),
        "smoke": ScaleProfile("smoke", 3, 2_000, 12, 200, 10_000, 50_000),
        "demo": ScaleProfile("demo", 14, 50_000, 30, 1_000, 150_000, 100_000),
        "benchmark": ScaleProfile(
            "benchmark", 30, 250_000, 60, 5_000, 1_000_000, 250_000
        ),
        "story-demo": ScaleProfile(
            "story-demo",
            638,
            3_000,
            16,
            100,
            4_000,
            50_000,
            "vietnam-retail",
        ),
    }
)


@dataclass(frozen=True, slots=True)
class GenerationConfig:
    """Resolved, immutable inputs for a single generator run."""

    profile: ScaleProfile
    seed: int
    output_dir: Path
    start_date: date
    rows_per_file: int
    anomalies: AnomalyRates = DEFAULT_ANOMALY_RATES

    def __post_init__(self) -> None:
        if self.rows_per_file <= 0:
            raise ValueError("rows_per_file must be positive")
        platform_count = len(self.scenario.platforms)
        if self.profile.campaigns % platform_count:
            raise ValueError(
                f"campaigns must be divisible by {platform_count}"
            )

    @property
    def scenario(self) -> Scenario:
        return SCENARIOS[self.profile.scenario]


CALENDAR_CAPACITY_FACTOR = 3
"""Impression ordinals reserved per day for calendar scenarios, as a multiple
of ``impressions_per_day``; the largest daily factor is about 2.07."""


def impression_capacity(config: GenerationConfig) -> int:
    """Return the fixed per-day impression ordinal space.

    It bounds every day's impression count, is the stride of event ordinals
    and is the first overflow (join-day) ordinal.
    """

    base = config.profile.impressions_per_day
    if config.scenario.calendar_rhythm:
        return base * CALENDAR_CAPACITY_FACTOR
    return base


def first_non_paid_ordinal(config: GenerationConfig) -> int:
    """Return the first ordinal of a day's non-paid (organic, direct) sessions.

    Impression ordinals fill ``[0, impression_capacity)`` and overflow
    join-day ordinals ``[impression_capacity, impression_capacity + customers)``;
    non-paid ordinals follow both, so no session shares an ordinal.
    """

    return impression_capacity(config) + config.profile.customers


def first_email_ordinal(config: GenerationConfig) -> int:
    """Return the first ordinal of a day's email-referred sessions.

    A day has at most ``impression_capacity + customers`` paid sessions and
    adds a third of them as non-paid visits, so reserving that whole span
    after ``first_non_paid_ordinal`` keeps every non-paid ordinal below this
    one on any day. An email session's ordinal is this plus its recipient's
    index in ``MasterData.customers`` (one email per customer per day), so
    email ordinals fill ``[first_email_ordinal, first_email_ordinal + customers)``.
    """

    return (
        first_non_paid_ordinal(config)
        + impression_capacity(config)
        + config.profile.customers
    )


def resolve_config(
    profile_name: str,
    seed: int,
    output_dir: Path,
    rows_per_file: int | None = None,
    start_date: date | None = None,
    days: int | None = None,
) -> GenerationConfig:
    """Resolve a named profile plus safe CLI overrides.

    ``days`` (1..profile.days) generates only the first ``days`` days of the
    profile's story: customer join dates and campaign flights still come from
    the full-length profile (``ScaleProfile.story_days``), so a 30-day
    story-demo window holds the real story's first 30 days of joiners rather
    than the whole join history squeezed into 30 days.
    """

    try:
        profile = PROFILES[profile_name]
    except KeyError as error:
        choices = ", ".join(PROFILES)
        raise ValueError(
            f"unknown profile {profile_name!r}; choose one of: {choices}"
        ) from error

    if days is not None:
        if not 1 <= days <= profile.days:
            raise ValueError(f"days must be between 1 and {profile.days}")
        profile = replace(profile, days=days, story_days=profile.full_days)

    if profile_name == "story-demo" and start_date not in (None, date(2025, 1, 1)):
        raise ValueError("story-demo uses one fixed UTC world starting 2025-01-01")

    return GenerationConfig(
        profile=profile,
        seed=seed,
        output_dir=Path(output_dir),
        start_date=start_date or (date(2025, 1, 1) if profile_name == "story-demo" else date(2026, 1, 1)),
        rows_per_file=(
            profile.rows_per_file if rows_per_file is None else rows_per_file
        ),
    )
