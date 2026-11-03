"""Correlated, streaming journey generation for marketing source systems."""

from __future__ import annotations

import heapq
import math
from bisect import bisect_left, bisect_right
from collections import OrderedDict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from operator import itemgetter
from typing import Iterator

from .config import (
    GenerationConfig,
    Scenario,
    first_non_paid_ordinal,
    impression_capacity,
)
from .customer_picker import CustomerPicker, is_identified
from .ids import stable_id, stable_int
from .models import Campaign, Customer, MasterData, Product
from .session_timing import (
    _day_impressions,
    _double_day_hours,
    _non_paid_timeline,
    _session_hour,
    _session_timeline,
    _utc_day,
)


@dataclass(frozen=True, slots=True)
class SourceRecord:
    dataset: str
    event_time: datetime
    payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class PaidTouch:
    click_id: str
    campaign_id: str
    platform: str
    event_time: datetime


@dataclass(frozen=True, slots=True)
class DailyCampaignTruth:
    day: date
    campaign_id: str
    platform: str
    impressions: int
    clicks: int
    spend_minor: int
    sessions: int
    orders: int
    gross_revenue_minor: int
    refunds_minor: int

    @property
    def net_revenue_minor(self) -> int:
        return self.gross_revenue_minor - self.refunds_minor


@dataclass(slots=True)
class _DailyCampaignAggregate:
    campaign: Campaign
    impressions: int = 0
    clicks: int = 0
    spend_minor: int = 0
    sessions: int = 0
    orders: int = 0
    gross_revenue_minor: int = 0
    refunds_minor: int = 0

    def freeze(self, day: date) -> DailyCampaignTruth:
        return DailyCampaignTruth(
            day=day,
            campaign_id=self.campaign.campaign_id,
            platform=self.campaign.platform,
            impressions=self.impressions,
            clicks=self.clicks,
            spend_minor=self.spend_minor,
            sessions=self.sessions,
            orders=self.orders,
            gross_revenue_minor=self.gross_revenue_minor,
            refunds_minor=self.refunds_minor,
        )


def last_paid_touch(
    touches: tuple[PaidTouch, ...] | list[PaidTouch],
    purchase_time: datetime,
    window_days: int = 7,
) -> PaidTouch | None:
    """Return the latest non-future paid click within the attribution window."""

    earliest = purchase_time - timedelta(days=window_days)
    eligible = [
        touch
        for touch in touches
        if earliest <= touch.event_time <= purchase_time
    ]
    return max(eligible, key=lambda touch: touch.event_time, default=None)


def iter_day_records(
    config: GenerationConfig,
    masters: MasterData,
    day_index: int,
) -> Iterator[SourceRecord]:
    """Yield one UTC day of bounded-memory, cross-source records."""

    if not 0 <= day_index < config.profile.days:
        raise ValueError("day_index is outside the configured profile")

    day = config.start_date + timedelta(days=day_index)
    day_start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    aggregates = {
        campaign.campaign_id: _DailyCampaignAggregate(campaign)
        for campaign in masters.campaigns
    }
    replayable_index = 0
    click_index = 0
    order_index = 0
    pending_records: list[tuple[datetime, int, SourceRecord]] = []
    pending_sequence = 0

    def with_anomalies(record: SourceRecord) -> Iterator[SourceRecord]:
        nonlocal replayable_index
        late = _is_scheduled(
            replayable_index,
            config.anomalies.late_event,
            forced_index=1,
        )
        duplicate = _is_scheduled(
            replayable_index,
            config.anomalies.duplicate_event,
            forced_index=0,
        )
        if late:
            delayed_payload = dict(record.payload)
            delayed_payload["ingested_at"] = _iso(record.event_time + timedelta(hours=6))
            delayed_payload["is_late"] = True
            record = replace(record, payload=delayed_payload)
        else:
            normal_payload = dict(record.payload)
            normal_payload["is_late"] = False
            record = replace(record, payload=normal_payload)
        replayable_index += 1
        yield record
        if duplicate:
            yield record

    def enqueue(record: SourceRecord) -> None:
        """Buffer a short-lived journey event for event-time ordered output."""

        nonlocal pending_sequence
        for candidate in with_anomalies(record):
            heapq.heappush(
                pending_records,
                (candidate.event_time, pending_sequence, candidate),
            )
            pending_sequence += 1

    def flush_until(cutoff: datetime) -> Iterator[SourceRecord]:
        while pending_records and pending_records[0][0] <= cutoff:
            yield heapq.heappop(pending_records)[2]

    yield from _iter_crm_snapshots(
        config, masters, day_index, day, day_start
    )

    for record in _iter_email_events(config, masters, day_index, day_start):
        yield from with_anomalies(record)

    capacity = impression_capacity(config)
    first_visit = first_non_paid_ordinal(config)
    # Calendar scenarios precompute (and cache) the day's paid decisions, which
    # non-paid attribution on later days reuses; ``flat`` keeps the lazy path.
    paid_day = (
        _paid_day(config, masters, day_index)
        if config.scenario.calendar_rhythm
        else None
    )
    if paid_day is None:
        picker = _customer_picker(config, masters, day_index)
        timeline = _session_timeline(config, day_index, picker)
        sessions: Iterable[tuple[datetime, int]] = timeline
    else:
        picker, timeline = paid_day.picker, paid_day.timeline
        visits = _non_paid_timeline(
            config,
            day_index,
            _non_paid_count(len(paid_day.session_ordinals)),
        )
        sessions = heapq.merge(timeline, visits)

    def record_impression(impression_ordinal: int) -> Campaign:
        campaign = (
            _pick_campaign(config, masters, day_index, impression_ordinal)
            if paid_day is None
            else paid_day.campaigns[impression_ordinal]
        )
        aggregate = aggregates[campaign.campaign_id]
        aggregate.impressions += 1
        aggregate.spend_minor += _impression_cost(
            config, campaign, day_index, impression_ordinal
        )
        return campaign

    def emit_session(
        ordinal: int,
        campaign: Campaign | None,
        event_time: datetime,
        traffic_source: str,
        strength: float,
    ) -> Iterator[SourceRecord]:
        """Generate one session and its funnel, orders and quality cases.

        A paid session has its ``campaign`` and a click; a non-paid one
        (``campaign`` None: organic, direct, later email) has neither, adds
        nothing to campaign aggregates and credits its order to the
        customer's latest paid click in the window, if any. ``strength``
        scales the late funnel.
        """

        nonlocal click_index, order_index
        paid = campaign is not None
        click_id: str | None = None
        touch: PaidTouch | None = None
        if campaign is not None:
            aggregate = aggregates[campaign.campaign_id]
            aggregate.clicks += 1
            aggregate.sessions += 1
            click_id = _click_id(config.seed, campaign.platform, day_index, ordinal)
            touch = PaidTouch(
                click_id=click_id,
                campaign_id=campaign.campaign_id,
                platform=campaign.platform,
                event_time=event_time,
            )
        id_scope, event_ordinal = _event_id_scope(config, day_index, ordinal)
        yield from flush_until(event_time)
        session_id = stable_id(config.seed, f"session-{day_index}", ordinal)
        anonymous_id = _anonymous_id(config, masters, day_index, ordinal)
        customer = picker.pick(day_index, ordinal)
        product = _select_product(config, masters.products, day_index, ordinal)
        device = ("desktop", "mobile", "tablet")[
            stable_int(config.seed, "device", day_index, ordinal, modulo=3)
        ]
        strength = _device_strength(config, device, strength)
        source = "mobile" if device == "mobile" else "web"
        # Story payloads name the session's traffic source; flat omits it.
        source_tag: dict[str, object] = (
            {"traffic_source": traffic_source}
            if config.scenario.calendar_rhythm
            else {}
        )

        def session_event(
            dataset: str,
            event_name: str,
            offset: int,
            at: datetime,
            event_campaign: Campaign | None,
            event_click_id: str | None,
            payload: dict[str, object],
        ) -> SourceRecord:
            return _event_record(
                config=config,
                ordinal=event_ordinal + offset,
                id_scope=id_scope,
                dataset=dataset,
                event_name=event_name,
                event_time=at,
                source="commerce" if dataset == "orders" else source,
                customer=customer,
                anonymous_id=anonymous_id,
                session_id=session_id,
                campaign=event_campaign,
                click_id=event_click_id,
                payload={**payload, **source_tag},
            )

        page_view = session_event(
            "web_events",
            "page_view",
            0,
            event_time,
            campaign,
            click_id,
            {
                "product_id": None,
                "page_url": (
                    f"https://shop.marketpulse.example.test/c/{campaign.campaign_id}"
                    if campaign is not None
                    else "https://shop.marketpulse.example.test/"
                ),
                "device_type": device,
                "market": "VN",
                "is_bot": paid
                and _is_scheduled(
                    click_index, config.anomalies.bot_session, forced_index=0
                ),
            },
        )
        enqueue(page_view)

        if page_view.payload["payload"]["is_bot"]:
            bot_view = session_event(
                "web_events",
                "page_view",
                1,
                event_time + timedelta(milliseconds=100),
                campaign,
                click_id,
                {
                    **page_view.payload["payload"],
                    "page_url": "https://shop.marketpulse.example.test/bot-probe",
                    "is_bot": True,
                },
            )
            enqueue(bot_view)

        funnel_score = stable_int(
            config.seed, "funnel", day_index, ordinal, modulo=1_000
        )
        if paid and click_index == 0:
            funnel_score = 0
        reaches_view, reaches_cart, reaches_checkout, purchase = _funnel_steps(
            config, campaign, funnel_score, strength
        )
        for reached, offset, seconds, event_name, page in (
            (reaches_view, 2, 2, "view_item", "product"),
            (reaches_cart, 3, 8, "add_to_cart", "cart"),
            (reaches_checkout, 4, 20, "begin_checkout", "checkout"),
        ):
            if reached:
                enqueue(
                    session_event(
                        "web_events",
                        event_name,
                        offset,
                        event_time + timedelta(seconds=seconds),
                        campaign,
                        click_id,
                        _web_payload(product, device, page, False),
                    )
                )

        if purchase:
            purchase_time = event_time + timedelta(seconds=60)
            if touch is not None:
                attributed_touch = _latest_purchase_touch(
                    config=config,
                    masters=masters,
                    picker=picker,
                    day_index=day_index,
                    impression_ordinal=ordinal,
                    customer=customer,
                    current_touch=touch,
                    purchase_time=purchase_time,
                    timeline=timeline,
                )
                if attributed_touch is None:
                    raise AssertionError("new paid touch must be attributable")
            else:
                attributed_touch = _non_paid_purchase_touch(
                    config, masters, day_index, customer, purchase_time
                )
            attributed_campaign = (
                None
                if attributed_touch is None
                else next(
                    candidate
                    for candidate in masters.campaigns
                    if candidate.campaign_id == attributed_touch.campaign_id
                )
            )
            attributed_click_id = (
                None if attributed_touch is None else attributed_touch.click_id
            )
            attributed_aggregate = (
                None
                if attributed_campaign is None
                else aggregates[attributed_campaign.campaign_id]
            )
            quantity = 1 + stable_int(
                config.seed,
                "quantity",
                day_index,
                ordinal,
                modulo=3,
            )
            amount_minor = product.unit_price_minor * quantity
            order_id = stable_id(config.seed, f"order-{day_index}", ordinal)
            if attributed_aggregate is not None:
                attributed_aggregate.orders += 1
                attributed_aggregate.gross_revenue_minor += amount_minor

            web_purchase = session_event(
                "web_events",
                "purchase",
                5,
                purchase_time,
                attributed_campaign,
                attributed_click_id,
                {
                    **_web_payload(product, device, "confirmation", False),
                    "order_id": order_id,
                    "amount_minor": amount_minor,
                    "currency": "VND",
                },
            )
            enqueue(web_purchase)

            order_created = session_event(
                "orders",
                "order_created",
                6,
                purchase_time + timedelta(seconds=1),
                attributed_campaign,
                attributed_click_id,
                {
                    "order_id": order_id,
                    "items": [
                        {
                            "product_id": product.product_id,
                            "quantity": quantity,
                            "unit_price_minor": product.unit_price_minor,
                        }
                    ],
                    "gross_amount_minor": amount_minor,
                    "refund_amount_minor": 0,
                    "currency": "VND",
                    "attributed_platform": (
                        None if attributed_touch is None else attributed_touch.platform
                    ),
                    "attributed_campaign_id": (
                        None
                        if attributed_touch is None
                        else attributed_touch.campaign_id
                    ),
                },
            )
            enqueue(order_created)

            refunded = _is_scheduled(
                order_index, config.anomalies.refund, forced_index=0
            )
            if refunded:
                if attributed_aggregate is not None:
                    attributed_aggregate.refunds_minor += amount_minor
                refund_event = session_event(
                    "orders",
                    "order_refunded",
                    7,
                    purchase_time + timedelta(seconds=2),
                    attributed_campaign,
                    attributed_click_id,
                    {
                        **order_created.payload["payload"],
                        "refund_amount_minor": amount_minor,
                    },
                )
                enqueue(refund_event)
            order_index += 1

        if campaign is None or click_id is None:
            return  # quality cases and the click counter follow paid clicks only
        if config.anomalies.invalid_case > 0 and (
            (day_index == 0 and click_index < 3)
            or click_index == 0
            or _is_scheduled(
                click_index, config.anomalies.invalid_case, forced_index=-1
            )
        ):
            yield _quality_case(
                config,
                day_index,
                click_index,
                event_time,
                campaign,
                click_id,
            )
        click_index += 1

    # Impressions, overflow join-day sessions (ordinals >= capacity: always a
    # forced paid session for a joiner left without one) and, for calendar
    # scenarios, non-paid visits (ordinals >= first_visit), in time order.
    for event_time, ordinal in sessions:
        if ordinal >= first_visit:
            yield from emit_session(
                ordinal,
                None,
                event_time,
                _non_paid_source(config, day_index, ordinal),
                NON_PAID_STRENGTH,
            )
            continue
        campaign = record_impression(ordinal)
        if paid_day is None:
            if ordinal < capacity and not _is_clicked(
                config, campaign, day_index, ordinal
            ):
                continue
        elif ordinal not in paid_day.session_ordinals:
            continue
        yield from emit_session(
            ordinal,
            campaign,
            event_time,
            "paid",
            _purchase_strength(config, campaign, day_index, ordinal),
        )

    yield from flush_until(datetime.max.replace(tzinfo=timezone.utc))

    report_time = day_start + timedelta(hours=23, minutes=59, seconds=59)
    for campaign in masters.campaigns:
        truth = aggregates[campaign.campaign_id].freeze(day)
        yield SourceRecord(
            dataset=f"{campaign.platform}_ads",
            event_time=report_time,
            payload=_vendor_report_payload(campaign, truth),
        )
        yield SourceRecord(
            dataset="ground_truth",
            event_time=report_time,
            payload={
                "date": day.isoformat(),
                "campaign_id": truth.campaign_id,
                "platform": truth.platform,
                "impressions": truth.impressions,
                "clicks": truth.clicks,
                "spend_minor": truth.spend_minor,
                "sessions": truth.sessions,
                "orders": truth.orders,
                "gross_revenue_minor": truth.gross_revenue_minor,
                "refunds_minor": truth.refunds_minor,
                "net_revenue_minor": truth.net_revenue_minor,
            },
        )


def _iter_crm_snapshots(
    config: GenerationConfig,
    masters: MasterData,
    day_index: int,
    day: date,
    day_start: datetime,
) -> Iterator[SourceRecord]:
    email_ordinals = {
        customer.customer_id: ordinal
        for ordinal, customer in _email_audience(masters, day)
    }
    for ordinal, customer in enumerate(masters.customers):
        if customer.created_at.date() > day:
            continue
        email_ordinal = email_ordinals.get(customer.customer_id)
        consent_marketing = customer.consent_marketing and not (
            email_ordinal is not None
            and _email_unsubscribed_before(config, email_ordinal, day_index)
        )
        yield SourceRecord(
            dataset="crm",
            event_time=day_start,
            payload={
                "snapshot_date": day.isoformat(),
                "customer_id": customer.customer_id,
                "email": customer.email,
                "loyalty_tier": customer.loyalty_tier,
                "lifetime_value_minor": stable_int(
                    config.seed, "ltv", ordinal, modulo=20_000_001
                ),
                "last_order_at": None,
                "consent_analytics": customer.consent_analytics,
                "consent_marketing": consent_marketing,
            },
        )


def _iter_email_events(
    config: GenerationConfig,
    masters: MasterData,
    day_index: int,
    day_start: datetime,
) -> Iterator[SourceRecord]:
    day = day_start.date()
    for ordinal, customer in _email_audience(masters, day):
        if _email_unsubscribed_before(config, ordinal, day_index):
            continue
        message_id = stable_id(config.seed, f"email-{day_index}", ordinal)
        session_id = stable_id(config.seed, f"email-session-{day_index}", ordinal)
        event_time = day_start + timedelta(hours=8, seconds=ordinal * 10)
        event_names = ["email_sent", "email_delivered"]
        score = stable_int(config.seed, "email", day_index, ordinal, modulo=100)
        if score < 60:
            event_names.append("email_opened")
        if score < 20:
            event_names.append("email_clicked")
        if score == 0:
            event_names.append("email_unsubscribed")
        for event_offset, event_name in enumerate(event_names):
            yield SourceRecord(
                dataset="email_events",
                event_time=event_time + timedelta(seconds=event_offset),
                payload=_envelope(
                    event_id=stable_id(
                        config.seed,
                        f"email-event-{day_index}-{ordinal}",
                        event_offset,
                    ),
                    event_name=event_name,
                    event_time=event_time + timedelta(seconds=event_offset),
                    source="email",
                    customer=customer,
                    anonymous_id=None,
                    session_id=session_id,
                    campaign=None,
                    click_id=None,
                    payload={
                        "message_id": message_id,
                        "lifecycle_campaign_id": f"LC-{day_index + 1:04d}",
                        "destination_url": (
                            "https://shop.marketpulse.example.test/email"
                            if event_name == "email_clicked"
                            else None
                        ),
                    },
                ),
            )


def _email_audience(
    masters: MasterData, day: date
) -> tuple[tuple[int, Customer], ...]:
    """Return ``(email_ordinal, customer)`` for members who have joined by ``day``.

    Ordinals index the fixed legacy audience, so they stay stable across days.
    """

    eligible = tuple(
        customer
        for customer in masters.customers
        if customer.consent_marketing
    )
    selected_count = max(1, len(eligible) // 50)
    return tuple(
        (ordinal, customer)
        for ordinal, customer in enumerate(eligible[:selected_count])
        if customer.created_at.date() <= day
    )


def _email_unsubscribed_before(
    config: GenerationConfig,
    email_ordinal: int,
    day_index: int,
) -> bool:
    return any(
        stable_int(
            config.seed,
            "email",
            previous_day,
            email_ordinal,
            modulo=100,
        )
        == 0
        for previous_day in range(day_index)
    )


def _event_record(
    *,
    config: GenerationConfig,
    ordinal: int,
    dataset: str,
    event_name: str,
    event_time: datetime,
    source: str,
    customer: Customer | None,
    anonymous_id: str | None,
    session_id: str | None,
    campaign: Campaign | None,
    click_id: str | None,
    payload: dict[str, object],
    id_scope: str = "",
) -> SourceRecord:
    """Build a journey event; its id hashes ``dataset``, ``event_name``,
    ``id_scope`` (see ``_event_id_scope``) and ``ordinal``."""

    return SourceRecord(
        dataset=dataset,
        event_time=event_time,
        payload=_envelope(
            event_id=stable_id(
                config.seed, f"{dataset}-{event_name}{id_scope}", ordinal
            ),
            event_name=event_name,
            event_time=event_time,
            source=source,
            customer=customer,
            anonymous_id=anonymous_id,
            session_id=session_id,
            campaign=campaign,
            click_id=click_id,
            payload=payload,
        ),
    )


def _envelope(
    *,
    event_id: str,
    event_name: str,
    event_time: datetime,
    source: str,
    customer: Customer | None,
    anonymous_id: str | None,
    session_id: str | None,
    campaign: Campaign | None,
    click_id: str | None,
    payload: dict[str, object],
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_name": event_name,
        "event_time": _iso(event_time),
        "ingested_at": _iso(event_time + timedelta(seconds=1)),
        "source": source,
        "schema_version": 1,
        "customer_id": customer.customer_id if customer else None,
        "anonymous_id": anonymous_id,
        "session_id": session_id,
        "campaign_id": campaign.campaign_id if campaign else None,
        "platform_campaign_id": (
            campaign.platform_campaign_id if campaign else None
        ),
        "click_id": click_id,
        "consent_analytics": customer.consent_analytics if customer else True,
        "payload": payload,
    }


def _web_payload(
    product: Product,
    device: str,
    page: str,
    is_bot: bool,
) -> dict[str, object]:
    return {
        "product_id": product.product_id,
        "page_url": f"https://shop.marketpulse.example.test/{page}/{product.product_id}",
        "device_type": device,
        "market": "VN",
        "is_bot": is_bot,
    }


def _quality_case(
    config: GenerationConfig,
    day_index: int,
    ordinal: int,
    event_time: datetime,
    campaign: Campaign,
    click_id: str,
) -> SourceRecord:
    error_codes = (
        "MISSING_REQUIRED_FIELD",
        "UNSUPPORTED_SCHEMA_VERSION",
        "ORPHAN_CLICK_ID",
    )
    error_code = error_codes[ordinal % len(error_codes)]
    invalid_record = _envelope(
        event_id=stable_id(config.seed, f"invalid-{day_index}", ordinal),
        event_name="page_view",
        event_time=event_time,
        source="web",
        customer=None,
        anonymous_id=stable_id(config.seed, f"invalid-anonymous-{day_index}", ordinal),
        session_id=stable_id(config.seed, f"invalid-session-{day_index}", ordinal),
        campaign=campaign,
        click_id=click_id,
        payload={
            "product_id": None,
            "page_url": "https://shop.marketpulse.example.test/quality-case",
            "device_type": "desktop",
            "market": "VN",
            "is_bot": False,
        },
    )
    invalid_record["is_late"] = False
    if error_code == "MISSING_REQUIRED_FIELD":
        invalid_record.pop("event_id")
    elif error_code == "UNSUPPORTED_SCHEMA_VERSION":
        invalid_record["schema_version"] = 999
    else:
        invalid_record["click_id"] = f"orphan-{click_id}"
    return SourceRecord(
        dataset="quality_cases",
        event_time=event_time,
        payload={
            "case_id": stable_id(config.seed, f"quality-{day_index}", ordinal),
            "event_time": _iso(event_time),
            "original_dataset": "web_events",
            "expected_error_code": error_code,
            "record": invalid_record,
        },
    )


def _vendor_report_payload(
    campaign: Campaign,
    truth: DailyCampaignTruth,
) -> dict[str, object]:
    day = truth.day.isoformat()
    common: dict[str, object] = {
        "campaign_id": campaign.platform_campaign_id,
        "impressions": truth.impressions,
        "clicks": truth.clicks,
    }
    if campaign.platform == "google":
        return {
            "segments_date": day,
            **common,
            "cost_micros": truth.spend_minor * 1_000_000,
            "conversions": truth.orders,
            "conversions_value": truth.gross_revenue_minor,
        }
    if campaign.platform == "meta":
        return {
            "date_start": day,
            "date_stop": day,
            "campaign_id": campaign.platform_campaign_id,
            "impressions": str(truth.impressions),
            "clicks": str(truth.clicks),
            "spend": str(truth.spend_minor),
            "actions": [
                {"action_type": "purchase", "value": str(truth.orders)}
            ],
            "action_values": [
                {
                    "action_type": "purchase",
                    "value": str(truth.gross_revenue_minor),
                }
            ],
        }
    if campaign.platform == "shopee":
        return {
            "date": f"{truth.day:%d/%m/%Y}",
            "ad_id": campaign.platform_campaign_id,
            "impression": truth.impressions,
            "clicks": truth.clicks,
            "expense": truth.spend_minor,
            "conversions": truth.orders,
            "gmv": truth.gross_revenue_minor,
        }
    return {
        "stat_time_day": day,
        **common,
        "spend": str(truth.spend_minor),
        "conversion": truth.orders,
        "total_purchase_value": str(truth.gross_revenue_minor),
    }


def _latest_purchase_touch(
    *,
    config: GenerationConfig,
    masters: MasterData,
    picker: CustomerPicker,
    day_index: int,
    impression_ordinal: int,
    customer: Customer | None,
    current_touch: PaidTouch,
    purchase_time: datetime,
    timeline: Sequence[tuple[datetime, int]],
) -> PaidTouch | None:
    """Resolve last paid click, including clicks during an overlapping journey.

    ``picker`` must be the one that assigned the day's sessions, so the later
    clicks credited here belong to the customer who actually made them.
    ``timeline`` is the day's ``_session_timeline``; the sessions emitted after
    this one up to ``purchase_time``, overflow join-day sessions included, are
    the candidates.
    """

    if customer is None:
        return current_touch
    touches = [current_touch]
    capacity = impression_capacity(config)
    start = bisect_right(timeline, (current_touch.event_time, impression_ordinal))
    for position in range(start, len(timeline)):
        candidate_time, candidate_ordinal = timeline[position]
        if candidate_time > purchase_time:
            break
        campaign = _pick_campaign(config, masters, day_index, candidate_ordinal)
        if candidate_ordinal < capacity and not _is_clicked(
            config, campaign, day_index, candidate_ordinal
        ):
            continue
        candidate_customer = picker.pick(day_index, candidate_ordinal)
        if candidate_customer != customer:
            continue
        touches.append(
            PaidTouch(
                click_id=_click_id(
                    config.seed,
                    campaign.platform,
                    day_index,
                    candidate_ordinal,
                ),
                campaign_id=campaign.campaign_id,
                platform=campaign.platform,
                event_time=candidate_time,
            )
        )
    return last_paid_touch(touches, purchase_time)


NON_PAID_STRENGTH = 0.8
"""Purchase strength of an organic or direct visit (neutral persona x 0.8)."""

ORGANIC_PERCENT = 60
"""Share (percent) of non-paid visits that are organic; the rest are direct."""

ATTRIBUTION_WINDOW_DAYS = 7
"""Days a paid click stays creditable (``last_paid_touch``'s default window)."""


def _non_paid_count(paid_sessions: int) -> int:
    """Return a day's organic plus direct visits: a quarter of all its sessions.

    ``paid_sessions`` counts clicked impressions and overflow join-day
    sessions; a third of them is a quarter of paid plus non-paid sessions.
    """

    return round(paid_sessions / 3)


def _non_paid_source(
    config: GenerationConfig, day_index: int, ordinal: int
) -> str:
    """Return ``organic`` (60 %) or ``direct`` (40 %) for a non-paid visit."""

    score = stable_int(config.seed, "traffic-source", day_index, ordinal, modulo=100)
    return "organic" if score < ORGANIC_PERCENT else "direct"


@dataclass(frozen=True, slots=True)
class _PaidDay:
    """A calendar day's paid decisions, computed once from the same helpers.

    ``campaigns`` maps every impression and overflow ordinal to its campaign;
    ``session_ordinals`` are the clicked impressions plus overflow sessions;
    ``touches`` maps a customer id to that customer's paid sessions as
    ``(time, ordinal)``, sorted like ``timeline``.
    """

    picker: CustomerPicker
    timeline: Sequence[tuple[datetime, int]]
    campaigns: Mapping[int, Campaign]
    session_ordinals: frozenset[int]
    touches: Mapping[str, tuple[tuple[datetime, int], ...]]


_PAID_DAY_CACHE: OrderedDict[
    tuple[GenerationConfig, int], tuple[MasterData, _PaidDay]
] = OrderedDict()
_PAID_DAY_CACHE_SIZE = 16
"""Days kept: a day's non-paid attribution reads it and the 7 days before."""


def _paid_day(
    config: GenerationConfig, masters: MasterData, day_index: int
) -> _PaidDay:
    """Return (and cache) a calendar day's paid sessions and touches.

    The cache is a bounded LRU keyed by config and day, valid only for the
    same ``masters`` object; a miss recomputes the same result, so
    ``iter_day_records`` stays deterministic in any call order.
    """

    key = (config, day_index)
    cached = _PAID_DAY_CACHE.get(key)
    if cached is not None and cached[0] is masters:
        _PAID_DAY_CACHE.move_to_end(key)
        return cached[1]
    campaigns: dict[int, Campaign] = {}
    clicked: list[int] = []
    for ordinal in range(_day_impressions(config, day_index)):
        campaign = _pick_campaign(config, masters, day_index, ordinal)
        campaigns[ordinal] = campaign
        if _is_clicked(config, campaign, day_index, ordinal):
            clicked.append(ordinal)
    picker = CustomerPicker.for_day(
        config,
        masters.customers,
        _utc_day(config, day_index),
        session_ordinals=clicked,
    )
    for ordinal in picker.overflow_ordinals:
        campaigns[ordinal] = _pick_campaign(config, masters, day_index, ordinal)
    session_ordinals = frozenset((*clicked, *picker.overflow_ordinals))
    timeline = _session_timeline(config, day_index, picker)
    touches: dict[str, list[tuple[datetime, int]]] = {}
    for entry in timeline:
        if entry[1] not in session_ordinals:
            continue
        customer = picker.pick(day_index, entry[1])
        if customer is not None:
            touches.setdefault(customer.customer_id, []).append(entry)
    paid_day = _PaidDay(
        picker=picker,
        timeline=timeline,
        campaigns=campaigns,
        session_ordinals=session_ordinals,
        touches={key: tuple(value) for key, value in touches.items()},
    )
    _PAID_DAY_CACHE[key] = (masters, paid_day)
    _PAID_DAY_CACHE.move_to_end(key)
    while len(_PAID_DAY_CACHE) > _PAID_DAY_CACHE_SIZE:
        _PAID_DAY_CACHE.popitem(last=False)
    return paid_day


def _non_paid_purchase_touch(
    config: GenerationConfig,
    masters: MasterData,
    day_index: int,
    customer: Customer | None,
    purchase_time: datetime,
) -> PaidTouch | None:
    """Return the customer's latest paid click in the window, or None.

    A non-paid visit has no click of its own, so its order credits the same
    customer's latest paid click at or before ``purchase_time`` within
    ``ATTRIBUTION_WINDOW_DAYS`` (the same day included). Clicks in the same
    second resolve to the lowest ordinal. Anonymous visits are unattributed.
    """

    if customer is None:
        return None
    earliest = purchase_time - timedelta(days=ATTRIBUTION_WINDOW_DAYS)
    oldest_day = max(0, day_index - ATTRIBUTION_WINDOW_DAYS)
    for touch_day in range(day_index, oldest_day - 1, -1):
        paid_day = _paid_day(config, masters, touch_day)
        entries = paid_day.touches.get(customer.customer_id)
        if not entries:
            continue
        end = bisect_right(entries, purchase_time, key=itemgetter(0))
        if end == 0:
            continue
        latest = entries[end - 1][0]
        if latest < earliest:
            return None
        touch_time, ordinal = entries[bisect_left(entries, latest, key=itemgetter(0))]
        campaign = paid_day.campaigns[ordinal]
        return PaidTouch(
            click_id=_click_id(config.seed, campaign.platform, touch_day, ordinal),
            campaign_id=campaign.campaign_id,
            platform=campaign.platform,
            event_time=touch_time,
        )
    return None


def _customer_picker(
    config: GenerationConfig,
    masters: MasterData,
    day_index: int,
) -> CustomerPicker:
    """Build the day's picker from the same click decisions the loop makes."""

    session_ordinals = (
        ordinal
        for ordinal in range(_day_impressions(config, day_index))
        if _is_clicked(
            config,
            _pick_campaign(config, masters, day_index, ordinal),
            day_index,
            ordinal,
        )
    )
    return CustomerPicker.for_day(
        config,
        masters.customers,
        config.start_date + timedelta(days=day_index),
        session_ordinals=session_ordinals,
    )


MAX_RETURNING_CANDIDATES = 8


def _fresh_anonymous_id(
    config: GenerationConfig, day_index: int, impression_ordinal: int
) -> str:
    return stable_id(config.seed, f"anonymous-{day_index}", impression_ordinal)


def _is_fresh_anonymous_session(
    config: GenerationConfig,
    masters: MasterData,
    day_index: int,
    impression_ordinal: int,
) -> bool:
    """Return whether an impression became a clicked, unidentified session."""

    return _is_clicked(
        config,
        _pick_campaign(config, masters, day_index, impression_ordinal),
        day_index,
        impression_ordinal,
    ) and not is_identified(config, day_index, impression_ordinal)


def _is_returning_session(
    config: GenerationConfig, day_index: int, impression_ordinal: int
) -> bool:
    """Return whether a session reuses an earlier device instead of a fresh one.

    Only anonymous sessions return: identified sessions, and overflow join-day
    sessions (ordinal >= ``impression_capacity``, always a joiner's), keep their
    own fresh device so no device id is shared between customers.
    """

    scenario = config.scenario
    if day_index == 0 or scenario.returning_window_days <= 0:
        return False
    if impression_ordinal >= impression_capacity(config) or is_identified(
        config, day_index, impression_ordinal
    ):
        return False
    return stable_int(
        config.seed,
        "returning-device",
        day_index,
        impression_ordinal,
        modulo=1_000,
    ) < round(scenario.returning_device_share * 1_000)


def _anonymous_id(
    config: GenerationConfig,
    masters: MasterData,
    day_index: int,
    impression_ordinal: int,
) -> str:
    """Return a fresh device id, or reuse one first seen in the return window."""

    scenario = config.scenario
    if _is_returning_session(config, day_index, impression_ordinal):
        for attempt in range(MAX_RETURNING_CANDIDATES):
            offset = stable_int(
                config.seed,
                "returning-day",
                day_index,
                impression_ordinal,
                attempt,
                modulo=min(scenario.returning_window_days, day_index),
            )
            earlier_day = day_index - 1 - offset
            impressions = _day_impressions(config, earlier_day)
            start = stable_int(
                config.seed,
                "returning-ordinal",
                day_index,
                impression_ordinal,
                attempt,
                modulo=impressions,
            )
            # Anonymous sessions are sparse among impressions, so scan
            # forward (wrapping) from a hashed start to the next one.
            for step in range(impressions):
                earlier_ordinal = (start + step) % impressions
                # Only a session that kept its fresh id guarantees the
                # device was really emitted on the earlier day.
                if _is_fresh_anonymous_session(
                    config, masters, earlier_day, earlier_ordinal
                ) and not _is_returning_session(
                    config, earlier_day, earlier_ordinal
                ):
                    return _fresh_anonymous_id(
                        config, earlier_day, earlier_ordinal
                    )
    return _fresh_anonymous_id(config, day_index, impression_ordinal)


NEUTRAL_PERSONA = (1.0, 1.0, 1.0)
"""``(cost, ctr, strength)`` multipliers of a platform without a persona."""

CAMPAIGN_WEIGHT_RESOLUTION = 1_000_000
"""Total integer weight the impression draw splits between campaigns."""

_CAMPAIGN_WEIGHT_CACHE: dict[
    tuple[str, tuple[Campaign, ...], float], tuple[tuple[int, ...], int]
] = {}
_CAMPAIGN_WEIGHT_CACHE_SIZE = 64


def _is_weighted(scenario: Scenario) -> bool:
    """Return whether impressions follow spend weights instead of round-robin."""

    return not (
        scenario.platform_spend_share is None
        and scenario.objective_spend_weight is None
        and scenario.personas is None
    )


def _persona(
    config: GenerationConfig, campaign: Campaign
) -> tuple[float, float, float]:
    """Return the ``(cost, ctr, strength)`` multipliers of a campaign's platform."""

    personas = config.scenario.personas
    return NEUTRAL_PERSONA if personas is None else personas[campaign.platform]


def _campaign_cumulative_weights(
    config: GenerationConfig,
    campaigns: tuple[Campaign, ...],
    shopee_boost: float,
) -> tuple[tuple[int, ...], int]:
    """Return cumulative integer impression weights and their total (cached).

    A platform's spend share (Shopee's times ``shopee_boost``) is split between
    its campaigns by objective weight; dividing by the platform's cost
    multiplier turns that spend into impressions, so spend follows the shares.
    """

    scenario = config.scenario
    key = (scenario.name, campaigns, shopee_boost)
    cached = _CAMPAIGN_WEIGHT_CACHE.get(key)
    if cached is not None:
        return cached
    objective_weights = scenario.objective_spend_weight or {}

    def objective_weight(campaign: Campaign) -> float:
        return objective_weights.get(campaign.objective, 1.0)

    platform_objective_totals: dict[str, float] = {}
    for campaign in campaigns:
        platform_objective_totals[campaign.platform] = platform_objective_totals.get(
            campaign.platform, 0.0
        ) + objective_weight(campaign)
    spend_shares = scenario.platform_spend_share
    raw: list[float] = []
    for campaign in campaigns:
        platform_share = (
            1.0 if spend_shares is None else float(spend_shares[campaign.platform])
        )
        if campaign.platform == "shopee":
            platform_share *= shopee_boost
        spend = (
            platform_share
            * objective_weight(campaign)
            / platform_objective_totals[campaign.platform]
        )
        raw.append(spend / _persona(config, campaign)[0])
    scale = CAMPAIGN_WEIGHT_RESOLUTION / sum(raw)
    cumulative: list[int] = []
    running = 0
    for weight in raw:
        running += max(1, round(weight * scale))
        cumulative.append(running)
    result = (tuple(cumulative), running)
    if len(_CAMPAIGN_WEIGHT_CACHE) >= _CAMPAIGN_WEIGHT_CACHE_SIZE:
        _CAMPAIGN_WEIGHT_CACHE.clear()
    _CAMPAIGN_WEIGHT_CACHE[key] = result
    return result


SHOPEE_DOUBLE_DAY_BOOST = 2.0
"""Shopee spend-share multiplier for an impression on a Vietnam double day."""

SHOPEE_DOUBLE_DAY_STRENGTH = 1.5
"""Shopee purchase-strength multiplier for a double-day session."""


def _shopee_boost(
    config: GenerationConfig, day_index: int, impression_ordinal: int
) -> float:
    """Return Shopee's spend-share boost for an impression's Vietnam date.

    The Vietnam date of ``_session_time`` depends only on its UTC hour (the
    offset is a whole seven hours), so the hour draw alone decides it.
    """

    if not config.scenario.calendar_rhythm:
        return 1.0
    double_hours = _double_day_hours(_utc_day(config, day_index))
    if not any(double_hours):
        return 1.0
    if double_hours[_session_hour(config, day_index, impression_ordinal)]:
        return SHOPEE_DOUBLE_DAY_BOOST
    return 1.0


def _purchase_strength(
    config: GenerationConfig,
    campaign: Campaign,
    day_index: int,
    impression_ordinal: int,
) -> float:
    """Return a session's persona strength, lifted for Shopee on double days."""

    strength = _persona(config, campaign)[2]
    if campaign.platform == "shopee" and _shopee_boost(
        config, day_index, impression_ordinal
    ) > 1.0:
        strength *= SHOPEE_DOUBLE_DAY_STRENGTH
    return strength


def _device_strength(
    config: GenerationConfig, device: str, strength: float
) -> float:
    """Return ``strength`` scaled by the device's multiplier (flat: unchanged)."""

    multipliers = config.scenario.device_strength
    return strength if multipliers is None else strength * multipliers[device]


def _pick_campaign(
    config: GenerationConfig,
    masters: MasterData,
    day_index: int,
    impression_ordinal: int,
) -> Campaign:
    """Return the paid campaign serving an impression.

    ``flat`` keeps the legacy round-robin allocation. A weighted scenario draws
    the campaign from spend weights (see ``_campaign_cumulative_weights``),
    with Shopee's share boosted on the impression's double day
    (``_shopee_boost``). Every caller deciding sessions, devices or
    attribution must use this so they agree on which campaign served an
    impression.
    """

    campaigns = masters.campaigns
    if not _is_weighted(config.scenario):
        return campaigns[impression_ordinal % len(campaigns)]
    cumulative, total = _campaign_cumulative_weights(
        config, campaigns, _shopee_boost(config, day_index, impression_ordinal)
    )
    draw = stable_int(
        config.seed,
        "impression-campaign",
        day_index,
        impression_ordinal,
        modulo=total,
    )
    return campaigns[bisect_right(cumulative, draw)]


def _impression_cost(
    config: GenerationConfig,
    campaign: Campaign,
    day_index: int,
    impression_ordinal: int,
) -> int:
    """Return one impression's cost: 100-1,000 minor units times the cost persona."""

    base = 100 + stable_int(
        config.seed,
        "impression-cost",
        day_index,
        impression_ordinal,
        modulo=901,
    )
    if config.scenario.personas is None:
        return base
    return round(base * _persona(config, campaign)[0])


LEGACY_FUNNEL_CUTOFFS = (850, 400, 250, 120)
"""Per-1,000 ``funnel_score`` cut-offs for view, cart, checkout, purchase."""

MAX_LATE_FUNNEL_RATE = 0.9


def _funnel_steps(
    config: GenerationConfig,
    campaign: Campaign | None,
    score: int,
    strength: float,
) -> tuple[bool, bool, bool, bool]:
    """Return which of view, cart, checkout and purchase a session reaches.

    ``score`` is the session's 0-999 funnel score. Without personas (``flat``)
    the legacy cut-offs apply and ``strength`` is ignored. Otherwise view and
    cart keep their cut-offs while checkout|cart = min(0.9, 0.625 * sqrt(m))
    and purchase|checkout = min(0.9, 0.48 * m) for ``m = strength``; the
    stages stay nested because all four compare the same score.
    """

    view_cutoff, cart_cutoff, checkout_cutoff, purchase_cutoff = (
        LEGACY_FUNNEL_CUTOFFS
    )
    if config.scenario.personas is not None:
        checkout_rate = min(MAX_LATE_FUNNEL_RATE, 0.625 * math.sqrt(strength))
        purchase_rate = min(MAX_LATE_FUNNEL_RATE, 0.48 * strength)
        checkout_cutoff = round(cart_cutoff * checkout_rate)
        purchase_cutoff = round(cart_cutoff * checkout_rate * purchase_rate)
    return (
        score < view_cutoff,
        score < cart_cutoff,
        score < checkout_cutoff,
        score < purchase_cutoff,
    )


def _is_clicked(
    config: GenerationConfig,
    campaign: Campaign,
    day_index: int,
    impression_ordinal: int,
) -> bool:
    click_score = stable_int(
        config.seed, "click", day_index, impression_ordinal, modulo=1_000
    )
    return click_score < _click_threshold(config, campaign)


def _event_ordinal(
    config: GenerationConfig,
    day_index: int,
    impression_ordinal: int,
) -> int:
    """Return the base event ordinal of a session; offsets 0-7 name its events.

    Days are ``impression_capacity`` ordinals apart. Overflow join-day sessions
    (ordinal >= that capacity) use negative bases so their event ids never
    collide with another day's impressions.
    """

    capacity = impression_capacity(config)
    if impression_ordinal < capacity:
        return (day_index * capacity + impression_ordinal) * 10
    overflow_position = impression_ordinal - capacity
    return -10 * (day_index * config.profile.customers + overflow_position + 1)


def _event_id_scope(
    config: GenerationConfig, day_index: int, ordinal: int
) -> tuple[str, int]:
    """Return a session's event-id scope and base ordinal (offsets 0-7 follow).

    Paid and overflow sessions keep ``_event_ordinal`` in the unscoped id
    space. Non-paid sessions (``ordinal >= first_non_paid_ordinal``) hash
    their ordinal under a per-day ``-visit-<day>`` scope, so their ids never
    meet another day's or another session's.
    """

    if ordinal >= first_non_paid_ordinal(config):
        return f"-visit-{day_index}", ordinal * 10
    return "", _event_ordinal(config, day_index, ordinal)


def _select_product(
    config: GenerationConfig,
    products: tuple[Product, ...],
    day_index: int,
    impression_ordinal: int,
) -> Product:
    index = stable_int(
        config.seed,
        "session-product",
        day_index,
        impression_ordinal,
        modulo=len(products),
    )
    return products[index]


def _click_threshold(config: GenerationConfig, campaign: Campaign) -> int:
    """Return the per-1,000 click cut-off: objective base times the CTR persona."""

    base = {"awareness": 45, "traffic": 90, "conversions": 70}[campaign.objective]
    if config.scenario.personas is None:
        return base
    return min(999, round(base * _persona(config, campaign)[1]))


def _click_id(
    seed: int,
    platform: str,
    day_index: int,
    impression_ordinal: int,
) -> str:
    prefix = {
        "google": "gclid",
        "meta": "fbclid",
        "tiktok": "ttclid",
        "shopee": "shpclid",
    }[platform]
    value = stable_id(seed, f"click-{day_index}", impression_ordinal).replace("-", "")
    return f"{prefix}-{value}"


def _is_scheduled(index: int, rate: float, forced_index: int) -> bool:
    if rate <= 0:
        return False
    if index == forced_index:
        return True
    interval = max(1, round(1 / rate))
    return (index + 1) % interval == 0


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
