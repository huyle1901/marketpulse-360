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
        if self.campaigns % 3:
            raise ValueError("campaigns must be divisible by 3")
        if self.story_days is not None and self.story_days < self.days:
            raise ValueError("story_days must be at least days")

    @property
    def full_days(self) -> int:
        """Length of the full story: ``story_days``, or ``days`` when unset."""

        return self.days if self.story_days is None else self.story_days


@dataclass(frozen=True, slots=True)
class Scenario:
    """World-shaping settings that make a profile's data varied.

    ``flat`` keeps the legacy uniform behaviour; optional fields are None/0.
    ``cities`` holds ``(country, city, weight)`` entries when set.
    """

    name: str
    cities: tuple[tuple[str, str, int], ...] | None = None
    customers_before_start_share: float | None = None
    join_ramp: float = 0.0
    tenure_months: float | None = None
    returning_device_share: float = 0.0
    returning_window_days: int = 0


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
            12,
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

    @property
    def scenario(self) -> Scenario:
        return SCENARIOS[self.profile.scenario]


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
