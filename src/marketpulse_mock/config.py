"""Configuration contracts for deterministic dataset generation."""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True, slots=True)
class ScaleProfile:
    """Capacity settings for a named generator profile."""

    name: str
    days: int
    customers: int
    campaigns: int
    products: int
    impressions_per_day: int
    rows_per_file: int

    def __post_init__(self) -> None:
        for field in fields(self):
            if field.name == "name":
                continue
            value = getattr(self, field.name)
            if value <= 0:
                raise ValueError(f"{field.name} must be positive")
        if self.campaigns % 3:
            raise ValueError("campaigns must be divisible by 3")


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


def resolve_config(
    profile_name: str,
    seed: int,
    output_dir: Path,
    rows_per_file: int | None = None,
    start_date: date | None = None,
) -> GenerationConfig:
    """Resolve a named profile plus safe CLI overrides."""

    try:
        profile = PROFILES[profile_name]
    except KeyError as error:
        choices = ", ".join(PROFILES)
        raise ValueError(
            f"unknown profile {profile_name!r}; choose one of: {choices}"
        ) from error

    return GenerationConfig(
        profile=profile,
        seed=seed,
        output_dir=Path(output_dir),
        start_date=start_date or date(2026, 1, 1),
        rows_per_file=(
            profile.rows_per_file if rows_per_file is None else rows_per_file
        ),
    )
