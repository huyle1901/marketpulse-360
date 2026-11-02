"""Per-day choice of which customer, if any, owns a generated session."""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date
from itertools import accumulate
from types import MappingProxyType
from typing import Iterable, Mapping

from .config import GenerationConfig
from .ids import stable_int
from .models import Customer

DAYS_PER_MONTH = 365.25 / 12
"""Average Gregorian month length used to turn tenure days into months."""

WEIGHT_SCALE = 1_000_000
"""Integer scale for tenure weights so the cumulative table stays exact."""

IDENTIFIED_PERCENT = 70
"""Share of sessions (percent) that belong to a known customer."""


def is_identified(
    config: GenerationConfig, day_index: int, impression_ordinal: int
) -> bool:
    """Return whether a session belongs to a known customer (the 70 % rule)."""

    return (
        stable_int(
            config.seed,
            "identified",
            day_index,
            impression_ordinal,
            modulo=100,
        )
        < IDENTIFIED_PERCENT
    )


def tenure_weight(tenure_days: int, tenure_months: float) -> int:
    """Return ``1 / (1 + months_since_join / tenure_months)`` as a scaled integer."""

    months = tenure_days / DAYS_PER_MONTH
    return max(1, round(WEIGHT_SCALE / (1 + months / tenure_months)))


@dataclass(frozen=True, slots=True)
class CustomerPicker:
    """Deterministic session-to-customer assignment for one UTC day.

    ``flat`` scenarios (no ``tenure_months``) pick uniformly from every
    customer, exactly as the legacy generator did, and seat no joiners.
    Otherwise only customers who have joined by ``day`` are eligible, weighted
    by tenure, and the day's ``joiners`` take the first identified sessions in
    order. Joiners left over
    once those sessions run out own the overflow ordinals, which start at
    ``impressions_per_day``.
    """

    config: GenerationConfig
    day: date
    day_index: int
    eligible: tuple[Customer, ...]
    cumulative_weights: tuple[int, ...] | None
    joiners: tuple[Customer, ...]
    joiner_sessions: Mapping[int, Customer]
    overflow_ordinals: tuple[int, ...]

    @classmethod
    def for_day(
        cls,
        config: GenerationConfig,
        customers: tuple[Customer, ...],
        day: date,
        *,
        session_ordinals: Iterable[int],
    ) -> CustomerPicker:
        """Build the picker for ``day``.

        ``session_ordinals`` are the day's impression ordinals that become paid
        sessions, in ascending order; only as many are read as needed to seat
        the joiners.
        """

        day_index = (day - config.start_date).days
        tenure_months = config.scenario.tenure_months
        if tenure_months is None:
            return cls(
                config, day, day_index, customers, None, (), MappingProxyType({}), ()
            )

        eligible = tuple(c for c in customers if c.created_at.date() <= day)
        cumulative = tuple(
            accumulate(
                tenure_weight((day - c.created_at.date()).days, tenure_months)
                for c in eligible
            )
        )
        joiners = tuple(c for c in eligible if c.created_at.date() == day)

        joiner_sessions: dict[int, Customer] = {}
        waiting = iter(joiners)
        next_joiner = next(waiting, None)
        if next_joiner is not None:
            for ordinal in session_ordinals:
                if not is_identified(config, day_index, ordinal):
                    continue
                joiner_sessions[ordinal] = next_joiner
                next_joiner = next(waiting, None)
                if next_joiner is None:
                    break
        overflow: list[int] = []
        first_overflow = config.profile.impressions_per_day
        while next_joiner is not None:
            ordinal = first_overflow + len(overflow)
            joiner_sessions[ordinal] = next_joiner
            overflow.append(ordinal)
            next_joiner = next(waiting, None)

        return cls(
            config,
            day,
            day_index,
            eligible,
            cumulative,
            joiners,
            MappingProxyType(joiner_sessions),
            tuple(overflow),
        )

    def pick(self, day_index: int, impression_ordinal: int) -> Customer | None:
        """Return the session's customer, or None for an anonymous session."""

        if day_index != self.day_index:
            raise ValueError(
                f"picker for day {self.day_index} asked about day {day_index}"
            )
        joiner = self.joiner_sessions.get(impression_ordinal)
        if joiner is not None:
            return joiner
        if not is_identified(self.config, day_index, impression_ordinal):
            return None
        if self.cumulative_weights is None:
            index = stable_int(
                self.config.seed,
                "session-customer",
                day_index,
                impression_ordinal,
                modulo=len(self.eligible),
            )
            return self.eligible[index]
        if not self.cumulative_weights:
            return None
        point = stable_int(
            self.config.seed,
            "session-customer",
            day_index,
            impression_ordinal,
            modulo=self.cumulative_weights[-1],
        )
        return self.eligible[bisect_right(self.cumulative_weights, point)]
