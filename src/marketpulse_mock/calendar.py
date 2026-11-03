"""Vietnam retail calendar: hourly shape and day multipliers for the story world."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

VIETNAM = timezone(timedelta(hours=7))

# Relative traffic weight per Vietnam-time hour (GMT+7, no DST), index 0-23.
HOURLY_WEIGHTS: tuple[float, ...] = (
    1.0, 0.6, 0.4, 0.3, 0.3, 0.4, 0.8, 1.4, 2.0, 2.4, 2.6, 2.8,
    3.4, 3.2, 2.6, 2.4, 2.4, 2.6, 3.0, 3.6, 4.2, 4.4, 4.0, 2.4,
)

WEEKEND_MULTIPLIER = 1.15
DOUBLE_DAY_MULTIPLIER = 1.8
BLACK_FRIDAY = date(2025, 11, 28)
BLACK_FRIDAY_MULTIPLIER = 1.6
PRE_TET_MULTIPLIER = 1.4
TET_MULTIPLIER = 0.5

# Tet (lunar new year) day by Gregorian year.
TET_DAYS: dict[int, date] = {
    2025: date(2025, 1, 29),
    2026: date(2026, 2, 17),
    2027: date(2027, 2, 6),
}


def is_double_day(vn_date: date) -> bool:
    return vn_date.month == vn_date.day


def _days_to_tet(vn_date: date) -> int | None:
    """Days until the nearest upcoming-or-current Tet day within range, else None."""
    for tet in TET_DAYS.values():
        delta = (tet - vn_date).days
        if -4 <= delta <= 10:
            return delta
    return None


def is_promo_day(vn_date: date) -> bool:
    if is_double_day(vn_date):
        return True
    days = _days_to_tet(vn_date)
    return days is not None and 1 <= days <= 3


def day_multiplier(vn_date: date) -> float:
    multiplier = 1.0
    if vn_date.weekday() >= 5:
        multiplier *= WEEKEND_MULTIPLIER
    if is_double_day(vn_date):
        multiplier *= DOUBLE_DAY_MULTIPLIER
    if vn_date == BLACK_FRIDAY:
        multiplier *= BLACK_FRIDAY_MULTIPLIER
    days = _days_to_tet(vn_date)
    if days is not None:
        if -3 <= days <= 1:
            # Eve through the fourth day; overrides the pre-Tet window.
            multiplier *= TET_MULTIPLIER
        elif 2 <= days <= 10:
            multiplier *= PRE_TET_MULTIPLIER
    return multiplier


def utc_hour_weights(utc_day: date) -> tuple[float, ...]:
    start = datetime(utc_day.year, utc_day.month, utc_day.day, tzinfo=timezone.utc)
    weights: list[float] = []
    for hour in range(24):
        local = (start + timedelta(hours=hour)).astimezone(VIETNAM)
        weights.append(HOURLY_WEIGHTS[local.hour] * day_multiplier(local.date()))
    return tuple(weights)
