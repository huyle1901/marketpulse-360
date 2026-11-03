"""Story-world email lifecycle: who gets which email on which day.

Every marketing-consented customer is in the audience from their join date
until they unsubscribe. A customer who joins inside the window gets a
``welcome`` on the join day; the whole audience gets a ``promo`` on Vietnam
sale days (``calendar.is_promo_day``) and a ``newsletter`` on other Vietnam
Tuesdays. A customer gets at most one email a day (welcome > promo >
newsletter). Sends go out 08:00-10:00 Vietnam time (01:00-03:00 UTC), so a
send's UTC day is also its Vietnam date.

Each send's outcome is a stable hash of ``(seed, kind, day, customer_id)``;
the unsubscribe outcome ends the customer's sends, so a schedule is built by
walking each customer's sends once.
"""

from __future__ import annotations

from bisect import bisect_right
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Literal, Mapping

from .calendar import is_promo_day
from .config import GenerationConfig
from .ids import stable_int
from .models import Customer, MasterData

EmailType = Literal["welcome", "newsletter", "promo"]

RATE_BASIS = 10_000
"""Rates below are in basis points of delivered emails."""

OPEN_AND_CLICK_RATES: Mapping[str, tuple[int, int]] = {
    "welcome": (5_500, 1_200),
    "newsletter": (2_800, 250),
    "promo": (3_200, 450),
}
"""``(open, click)`` basis points per email type; a click is always an open."""

UNSUBSCRIBE_RATE = 50
"""Basis points of delivered emails that unsubscribe (0.5 %)."""

SEND_WINDOW_START = timedelta(hours=1)
SEND_WINDOW_SECONDS = 2 * 3600
"""Sends go out in ``[01:00, 03:00)`` UTC, i.e. 08:00-10:00 Vietnam time."""

DELIVERY_DELAY_SECONDS = 30
OPEN_DELAY_SECONDS = 8 * 3600
CLICK_DELAY_SECONDS = 30 * 60
UNSUBSCRIBE_DELAY_SECONDS = 5 * 60

NEWSLETTER_WEEKDAY = 1  # Tuesday


@dataclass(frozen=True, slots=True)
class EmailSend:
    """One email to one customer on one day, with its outcome.

    ``ordinal`` is the customer's index in ``MasterData.customers``; it keys
    the message's ids. Every send is delivered.
    """

    customer: Customer
    email_type: EmailType
    ordinal: int = 0
    opened: bool = False
    clicked: bool = False
    unsubscribed: bool = False


class EmailSchedule:
    """A window's email sends by day plus each customer's unsubscribe day."""

    __slots__ = ("_sends", "_unsubscribed")

    def __init__(
        self,
        sends: Mapping[date, tuple[EmailSend, ...]],
        unsubscribed: Mapping[str, date],
    ) -> None:
        self._sends = sends
        self._unsubscribed = unsubscribed

    @classmethod
    def build(cls, config: GenerationConfig, masters: MasterData) -> EmailSchedule:
        """Return (and cache) the schedule for ``config``'s window.

        The cache is a small LRU keyed by seed, profile name, window length
        and start date, valid only for the same ``masters`` object.
        """

        key = (
            config.seed,
            config.profile.name,
            config.profile.days,
            config.start_date,
        )
        cached = _CACHE.get(key)
        if cached is not None and cached[0] is masters:
            _CACHE.move_to_end(key)
            return cached[1]
        schedule = _compute(config, masters)
        _CACHE[key] = (masters, schedule)
        _CACHE.move_to_end(key)
        while len(_CACHE) > _CACHE_SIZE:
            _CACHE.popitem(last=False)
        return schedule

    def sends_on(self, day: date) -> tuple[EmailSend, ...]:
        """Return the day's sends in customer order."""

        return self._sends.get(day, ())

    def unsubscribed_on(self, customer_id: str) -> date | None:
        """Return the day of the customer's unsubscribing send, if any."""

        return self._unsubscribed.get(customer_id)


_CACHE: OrderedDict[
    tuple[int, str, int, date], tuple[MasterData, EmailSchedule]
] = OrderedDict()
_CACHE_SIZE = 4


def _compute(config: GenerationConfig, masters: MasterData) -> EmailSchedule:
    seed = config.seed
    start = config.start_date
    window = [start + timedelta(days=offset) for offset in range(config.profile.days)]
    end = window[-1]
    campaign_days: list[date] = []
    campaign_types: list[EmailType] = []
    for day in window:
        if is_promo_day(day):
            campaign_days.append(day)
            campaign_types.append("promo")
        elif day.weekday() == NEWSLETTER_WEEKDAY:
            campaign_days.append(day)
            campaign_types.append("newsletter")

    sends: dict[date, list[EmailSend]] = {}
    unsubscribed: dict[str, date] = {}
    for ordinal, customer in enumerate(masters.customers):
        if not customer.consent_marketing:
            continue
        joined = customer.created_at.date()
        if joined > end:
            continue
        plan: list[tuple[date, EmailType]] = []
        if joined >= start:
            plan.append((joined, "welcome"))
        # The join day already has the welcome (one email a day), and a
        # customer who joined earlier has no campaign day on or before it.
        first = bisect_right(campaign_days, joined)
        plan_tail = zip(campaign_days[first:], campaign_types[first:])
        for day, email_type in (*plan, *plan_tail):
            send = _send(seed, ordinal, customer, day, email_type)
            sends.setdefault(day, []).append(send)
            if send.unsubscribed:
                unsubscribed[customer.customer_id] = day
                break
    return EmailSchedule(
        {day: tuple(day_sends) for day, day_sends in sends.items()},
        unsubscribed,
    )


def _send(
    seed: int, ordinal: int, customer: Customer, day: date, email_type: EmailType
) -> EmailSend:
    open_rate, click_rate = OPEN_AND_CLICK_RATES[email_type]
    customer_id = customer.customer_id
    opened = stable_int(seed, "email-open", day, customer_id, modulo=RATE_BASIS) < open_rate
    # Drawn within opens so the click share of delivered emails is click_rate.
    clicked = opened and (
        stable_int(seed, "email-click", day, customer_id, modulo=open_rate) < click_rate
    )
    unsubscribed = (
        stable_int(seed, "email-unsub", day, customer_id, modulo=RATE_BASIS)
        < UNSUBSCRIBE_RATE
    )
    return EmailSend(customer, email_type, ordinal, opened, clicked, unsubscribed)


def message_events(
    seed: int, day: date, send: EmailSend
) -> tuple[tuple[str, datetime], ...]:
    """Return a send's ``(event_name, event_time)`` pairs in time order.

    Sent in 01:00-03:00 UTC; delivered 1-30 s later; opened up to 8 h after
    delivery; clicked up to 30 min after the open; unsubscribed up to 5 min
    after the message's last other event (delivery, open or click). Every
    delay is at least a second, so the events never tie; all of them stay
    before 12:00 UTC of ``day``.
    """

    customer_id = send.customer.customer_id

    def delay(kind: str, seconds: int) -> timedelta:
        return timedelta(
            seconds=1 + stable_int(seed, kind, day, customer_id, modulo=seconds)
        )

    sent = (
        datetime.combine(day, time.min, tzinfo=timezone.utc)
        + SEND_WINDOW_START
        + timedelta(
            seconds=stable_int(
                seed, "email-send-time", day, customer_id, modulo=SEND_WINDOW_SECONDS
            )
        )
    )
    events = [("email_sent", sent)]
    last = sent + delay("email-deliver-delay", DELIVERY_DELAY_SECONDS)
    events.append(("email_delivered", last))
    if send.opened:
        last += delay("email-open-delay", OPEN_DELAY_SECONDS)
        events.append(("email_opened", last))
        if send.clicked:
            last += delay("email-click-delay", CLICK_DELAY_SECONDS)
            events.append(("email_clicked", last))
    if send.unsubscribed:
        last += delay("email-unsub-delay", UNSUBSCRIBE_DELAY_SECONDS)
        events.append(("email_unsubscribed", last))
    return tuple(events)
