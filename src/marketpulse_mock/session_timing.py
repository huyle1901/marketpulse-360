"""Story and legacy session timing: each day's volume and session times."""

from __future__ import annotations

import heapq
from bisect import bisect_right
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from itertools import chain
from typing import Iterator

from .calendar import HOURLY_WEIGHTS, VIETNAM, is_double_day, utc_hour_weights
from .config import GenerationConfig, impression_capacity
from .customer_picker import CustomerPicker
from .ids import stable_int


LEGACY_SESSION_SPAN_SECONDS = 86_000
"""Non-calendar impressions are spaced evenly over the day's first 86,000 s."""

LEGACY_OVERFLOW_SPAN_MS = 300_000
"""Non-calendar overflow sessions share the 300 s after the last impression."""

HOUR_DRAW_RESOLUTION = 1_000_000
"""Integer scale of the hourly inverse-CDF draw."""

LAST_HOUR_SECONDS = 3_480
"""Seconds drawn in UTC hour 23 (23:00:00-23:57:59): a session plus its
funnel (at most 62 s) ends before the 23:59:59 reports, so no day's records
cross midnight."""


def _utc_day(config: GenerationConfig, day_index: int) -> date:
    return config.start_date + timedelta(days=day_index)


@lru_cache(maxsize=128)
def _hour_thresholds(utc_day: date) -> tuple[int, ...]:
    """Return the cumulative per-UTC-hour draw thresholds of a calendar day."""

    weights = utc_hour_weights(utc_day)
    total = sum(weights)
    running = 0.0
    thresholds: list[int] = []
    for weight in weights:
        running += weight
        thresholds.append(round(running / total * HOUR_DRAW_RESOLUTION))
    thresholds[-1] = HOUR_DRAW_RESOLUTION
    return tuple(thresholds)


@lru_cache(maxsize=128)
def _double_day_hours(utc_day: date) -> tuple[bool, ...]:
    """Return, per UTC hour, whether its Vietnam date is a double day."""

    start = datetime.combine(utc_day, time.min, tzinfo=timezone.utc)
    return tuple(
        is_double_day((start + timedelta(hours=hour)).astimezone(VIETNAM).date())
        for hour in range(24)
    )


@lru_cache(maxsize=128)
def _day_factor(utc_day: date) -> float:
    """Return a calendar day's summed hour weights relative to a plain day."""

    return sum(utc_hour_weights(utc_day)) / sum(HOURLY_WEIGHTS)


def _day_impressions(config: GenerationConfig, day_index: int) -> int:
    """Return the day's impression count (ordinals ``0..count-1``).

    Calendar scenarios scale ``impressions_per_day`` by the day's summed
    hourly weights relative to a plain weekday; others keep the fixed count.
    """

    base = config.profile.impressions_per_day
    if not config.scenario.calendar_rhythm:
        return base
    count = round(base * _day_factor(_utc_day(config, day_index)))
    capacity = impression_capacity(config)
    if count > capacity:
        raise ValueError(
            f"day {day_index} needs {count} impressions; capacity is {capacity}"
        )
    return count


def _session_hour(
    config: GenerationConfig, day_index: int, impression_ordinal: int
) -> int:
    """Draw a calendar session's UTC hour by inverse CDF of the hour weights."""

    draw = stable_int(
        config.seed,
        "impression-hour",
        day_index,
        impression_ordinal,
        modulo=HOUR_DRAW_RESOLUTION,
    )
    return bisect_right(_hour_thresholds(_utc_day(config, day_index)), draw)


def _session_time(
    config: GenerationConfig,
    day_index: int,
    impression_ordinal: int,
    *,
    overflow_count: int = 0,
) -> datetime:
    """Return when an impression (or overflow join-day session) happens.

    Calendar scenarios draw the UTC hour from ``utc_hour_weights`` and a
    second inside it, overflow ordinals alike. Others keep the legacy
    even spacing, with overflow sessions packed after 86,000 s; only that
    legacy overflow packing needs the day's ``overflow_count``.
    """

    day_start = datetime.combine(
        _utc_day(config, day_index), time.min, tzinfo=timezone.utc
    )
    if config.scenario.calendar_rhythm:
        hour = _session_hour(config, day_index, impression_ordinal)
        second = stable_int(
            config.seed,
            "impression-second",
            day_index,
            impression_ordinal,
            modulo=LAST_HOUR_SECONDS if hour == 23 else 3_600,
        )
        return day_start + timedelta(hours=hour, seconds=second)
    return day_start + _legacy_offset(
        impression_ordinal, config.profile.impressions_per_day, overflow_count
    )


def _legacy_offset(
    impression_ordinal: int, count: int, overflow_count: int
) -> timedelta:
    """Return a non-calendar session's offset from midnight.

    Impressions are spaced evenly over 86,000 s; overflow sessions share the
    300 s after them, which needs the day's ``overflow_count``.
    """

    if impression_ordinal < count:
        return timedelta(
            seconds=(impression_ordinal * LEGACY_SESSION_SPAN_SECONDS) // count
        )
    if overflow_count <= 0:
        raise ValueError("a legacy overflow session needs the day's overflow_count")
    return timedelta(
        seconds=LEGACY_SESSION_SPAN_SECONDS,
        milliseconds=(
            (impression_ordinal - count) * LEGACY_OVERFLOW_SPAN_MS
        ) // overflow_count,
    )


def _impression_times(
    config: GenerationConfig, day_index: int
) -> list[tuple[datetime, int]]:
    """Return ``(time, ordinal)`` for the day's impressions, sorted."""

    return sorted(
        (_session_time(config, day_index, ordinal), ordinal)
        for ordinal in range(_day_impressions(config, day_index))
    )


class _LegacyTimeline(Sequence[tuple[datetime, int]]):
    """A non-calendar day's ``(time, ordinal)`` entries, computed on demand.

    Legacy times rise with the ordinal and overflow sessions follow every
    impression, so the entries are already sorted; computing them lazily keeps
    a million-impression ``flat`` day from holding a day-sized list. Entries
    equal ``_session_time``; the day's constants are computed once because
    attribution reads millions of entries on a ``benchmark`` day.
    """

    def __init__(
        self,
        config: GenerationConfig,
        day_index: int,
        overflow_ordinals: tuple[int, ...],
    ) -> None:
        self._day_start = datetime.combine(
            _utc_day(config, day_index), time.min, tzinfo=timezone.utc
        )
        self._count = config.profile.impressions_per_day
        self._overflow = overflow_ordinals
        self._overflow_count = len(overflow_ordinals)
        self._length = self._count + self._overflow_count

    def __len__(self) -> int:
        return self._length

    def __iter__(self) -> Iterator[tuple[datetime, int]]:
        day_start, count, overflow_count = (
            self._day_start, self._count, self._overflow_count
        )
        for ordinal in chain(range(count), self._overflow):
            yield day_start + _legacy_offset(ordinal, count, overflow_count), ordinal

    def __getitem__(self, position: int) -> tuple[datetime, int]:  # type: ignore[override]
        if not 0 <= position < self._length:
            raise IndexError(position)
        count = self._count
        ordinal = position if position < count else self._overflow[position - count]
        return (
            self._day_start
            + _legacy_offset(ordinal, count, self._overflow_count),
            ordinal,
        )


def _session_timeline(
    config: GenerationConfig, day_index: int, picker: CustomerPicker
) -> Sequence[tuple[datetime, int]]:
    """Return the day's impressions and overflow sessions in time order.

    Ties keep ascending ordinals, matching ``_impression_times``.
    """

    if not config.scenario.calendar_rhythm:
        return _LegacyTimeline(config, day_index, picker.overflow_ordinals)
    overflow_count = len(picker.overflow_ordinals)
    overflow = sorted(
        (
            _session_time(
                config, day_index, ordinal, overflow_count=overflow_count
            ),
            ordinal,
        )
        for ordinal in picker.overflow_ordinals
    )
    return list(heapq.merge(_impression_times(config, day_index), overflow))
