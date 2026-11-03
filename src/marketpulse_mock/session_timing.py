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
from .config import (
    GenerationConfig,
    first_email_ordinal,
    first_non_paid_ordinal,
    impression_capacity,
)
from .customer_picker import CustomerPicker
from .email_schedule import EmailSchedule, message_events
from .ids import stable_int
from .models import MasterData


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
    config: GenerationConfig,
    day_index: int,
    ordinal: int,
    namespace: str = "impression",
) -> int:
    """Draw a calendar session's UTC hour by inverse CDF of the hour weights.

    ``namespace`` keeps independent draws for each kind of session:
    ``impression`` for paid (and overflow) ordinals, ``visit`` for non-paid.
    """

    draw = stable_int(
        config.seed,
        f"{namespace}-hour",
        day_index,
        ordinal,
        modulo=HOUR_DRAW_RESOLUTION,
    )
    return bisect_right(_hour_thresholds(_utc_day(config, day_index)), draw)


def _drawn_time(
    config: GenerationConfig, day_index: int, ordinal: int, namespace: str
) -> datetime:
    """Return a calendar session time: a drawn UTC hour and a second inside it.

    UTC hour 23 draws only its first ``LAST_HOUR_SECONDS``.
    """

    hour = _session_hour(config, day_index, ordinal, namespace)
    second = stable_int(
        config.seed,
        f"{namespace}-second",
        day_index,
        ordinal,
        modulo=LAST_HOUR_SECONDS if hour == 23 else 3_600,
    )
    day_start = datetime.combine(
        _utc_day(config, day_index), time.min, tzinfo=timezone.utc
    )
    return day_start + timedelta(hours=hour, seconds=second)


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

    if config.scenario.calendar_rhythm:
        return _drawn_time(config, day_index, impression_ordinal, "impression")
    day_start = datetime.combine(
        _utc_day(config, day_index), time.min, tzinfo=timezone.utc
    )
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


def _non_paid_timeline(
    config: GenerationConfig, day_index: int, count: int
) -> list[tuple[datetime, int]]:
    """Return ``(time, ordinal)`` for the day's ``count`` non-paid sessions, sorted.

    Ordinals start at ``first_non_paid_ordinal``; times are drawn like paid
    sessions' under the ``visit`` namespace. Only calendar scenarios have
    non-paid sessions.
    """

    if count and not config.scenario.calendar_rhythm:
        raise ValueError("only calendar scenarios have non-paid sessions")
    first = first_non_paid_ordinal(config)
    return sorted(
        (_drawn_time(config, day_index, ordinal, "visit"), ordinal)
        for ordinal in range(first, first + count)
    )


EMAIL_VISIT_DELAY_SECONDS = 2 * 3600
"""An email-referred visit starts 0-2 h (whole seconds) after its click."""

SESSION_FUNNEL_SECONDS = 62
"""A session's last event (a refund) follows its start by at most 62 s."""

EMAIL_VISIT_END = timedelta(hours=23, minutes=58)
"""Every email visit, funnel included, ends before 23:58 of its UTC day."""


def _email_timeline(
    config: GenerationConfig, masters: MasterData, day_index: int
) -> list[tuple[datetime, int]]:
    """Return ``(time, ordinal)`` for the day's email-referred visits, sorted.

    Each ``email_clicked`` of the day's sends (``message_events``, the same
    times the email events carry) starts one visit by its recipient within
    ``EMAIL_VISIT_DELAY_SECONDS``; the ordinal is ``first_email_ordinal``
    plus the recipient's customer index. Sends end by 03:00 UTC and clicks
    follow within 8.5 h, so visits start before 14:00 UTC; a visit whose
    funnel could reach ``EMAIL_VISIT_END`` raises instead of crossing midnight.
    """

    if not (config.scenario.calendar_rhythm and config.scenario.email_lifecycle):
        raise ValueError("only calendar email-lifecycle scenarios have email visits")
    day = _utc_day(config, day_index)
    end = datetime.combine(day, time.min, tzinfo=timezone.utc) + EMAIL_VISIT_END
    first = first_email_ordinal(config)
    visits: list[tuple[datetime, int]] = []
    for send in EmailSchedule.build(config, masters).sends_on(day):
        if not send.clicked:
            continue
        clicked_at = dict(message_events(config.seed, day, send))["email_clicked"]
        visit_time = clicked_at + timedelta(
            seconds=stable_int(
                config.seed,
                "email-visit",
                day,
                send.customer.customer_id,
                modulo=EMAIL_VISIT_DELAY_SECONDS,
            )
        )
        if visit_time + timedelta(seconds=SESSION_FUNNEL_SECONDS) >= end:
            raise AssertionError(f"email visit at {visit_time} leaves its UTC day")
        visits.append((visit_time, first + send.ordinal))
    visits.sort()
    return visits
