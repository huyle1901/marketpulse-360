"""Correlated, streaming journey generation for marketing source systems."""

from __future__ import annotations

import heapq
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterator

from .config import GenerationConfig
from .ids import stable_id, stable_int
from .models import Campaign, Customer, MasterData, Product


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

    impression_count = config.profile.impressions_per_day
    for impression_ordinal in range(impression_count):
        campaign = masters.campaigns[
            impression_ordinal % len(masters.campaigns)
        ]
        aggregate = aggregates[campaign.campaign_id]
        aggregate.impressions += 1
        aggregate.spend_minor += 100 + stable_int(
            config.seed,
            "impression-cost",
            day_index,
            impression_ordinal,
            modulo=901,
        )

        click_score = stable_int(
            config.seed, "click", day_index, impression_ordinal, modulo=1_000
        )
        if click_score >= _click_threshold(campaign):
            continue

        aggregate.clicks += 1
        aggregate.sessions += 1
        event_time = day_start + timedelta(
            seconds=(impression_ordinal * 86_000) // max(1, impression_count)
        )
        yield from flush_until(event_time)
        session_id = stable_id(
            config.seed, f"session-{day_index}", impression_ordinal
        )
        anonymous_id = stable_id(
            config.seed, f"anonymous-{day_index}", impression_ordinal
        )
        click_id = _click_id(
            config.seed, campaign.platform, day_index, impression_ordinal
        )
        customer = _select_customer(
            config, masters.customers, day_index, impression_ordinal
        )
        product = _select_product(
            config, masters.products, day_index, impression_ordinal
        )
        device = ("desktop", "mobile", "tablet")[
            stable_int(
                config.seed, "device", day_index, impression_ordinal, modulo=3
            )
        ]
        touch = PaidTouch(
            click_id=click_id,
            campaign_id=campaign.campaign_id,
            platform=campaign.platform,
            event_time=event_time,
        )

        page_view = _event_record(
            config=config,
            ordinal=(day_index * impression_count + impression_ordinal) * 10,
            dataset="web_events",
            event_name="page_view",
            event_time=event_time,
            source="mobile" if device == "mobile" else "web",
            customer=customer,
            anonymous_id=anonymous_id,
            session_id=session_id,
            campaign=campaign,
            click_id=click_id,
            payload={
                "product_id": None,
                "page_url": (
                    f"https://shop.marketpulse.example.test/c/{campaign.campaign_id}"
                ),
                "device_type": device,
                "market": "VN",
                "is_bot": _is_scheduled(
                    click_index, config.anomalies.bot_session, forced_index=0
                ),
            },
        )
        enqueue(page_view)

        if page_view.payload["payload"]["is_bot"]:
            bot_view = _event_record(
                config=config,
                ordinal=(day_index * impression_count + impression_ordinal) * 10 + 1,
                dataset="web_events",
                event_name="page_view",
                event_time=event_time + timedelta(milliseconds=100),
                source=page_view.payload["source"],
                customer=customer,
                anonymous_id=anonymous_id,
                session_id=session_id,
                campaign=campaign,
                click_id=click_id,
                payload={
                    **page_view.payload["payload"],
                    "page_url": "https://shop.marketpulse.example.test/bot-probe",
                    "is_bot": True,
                },
            )
            enqueue(bot_view)

        funnel_score = stable_int(
            config.seed, "funnel", day_index, impression_ordinal, modulo=1_000
        )
        if click_index == 0:
            funnel_score = 0
        if funnel_score < 850:
            view_item = _event_record(
                config=config,
                ordinal=(day_index * impression_count + impression_ordinal) * 10 + 2,
                dataset="web_events",
                event_name="view_item",
                event_time=event_time + timedelta(seconds=2),
                source=page_view.payload["source"],
                customer=customer,
                anonymous_id=anonymous_id,
                session_id=session_id,
                campaign=campaign,
                click_id=click_id,
                payload=_web_payload(product, device, "product", False),
            )
            enqueue(view_item)

        if funnel_score < 400:
            add_to_cart = _event_record(
                config=config,
                ordinal=(day_index * impression_count + impression_ordinal) * 10 + 3,
                dataset="web_events",
                event_name="add_to_cart",
                event_time=event_time + timedelta(seconds=8),
                source=page_view.payload["source"],
                customer=customer,
                anonymous_id=anonymous_id,
                session_id=session_id,
                campaign=campaign,
                click_id=click_id,
                payload=_web_payload(product, device, "cart", False),
            )
            enqueue(add_to_cart)

        if funnel_score < 250:
            checkout = _event_record(
                config=config,
                ordinal=(day_index * impression_count + impression_ordinal) * 10 + 4,
                dataset="web_events",
                event_name="begin_checkout",
                event_time=event_time + timedelta(seconds=20),
                source=page_view.payload["source"],
                customer=customer,
                anonymous_id=anonymous_id,
                session_id=session_id,
                campaign=campaign,
                click_id=click_id,
                payload=_web_payload(product, device, "checkout", False),
            )
            enqueue(checkout)

        purchase = funnel_score < 120
        if purchase:
            purchase_time = event_time + timedelta(seconds=60)
            attributed_touch = _latest_purchase_touch(
                config=config,
                masters=masters,
                day_index=day_index,
                impression_ordinal=impression_ordinal,
                customer=customer,
                current_touch=touch,
                purchase_time=purchase_time,
            )
            if attributed_touch is None:
                raise AssertionError("new paid touch must be attributable")
            attributed_campaign = next(
                candidate
                for candidate in masters.campaigns
                if candidate.campaign_id == attributed_touch.campaign_id
            )
            attributed_aggregate = aggregates[attributed_campaign.campaign_id]
            quantity = 1 + stable_int(
                config.seed,
                "quantity",
                day_index,
                impression_ordinal,
                modulo=3,
            )
            amount_minor = product.unit_price_minor * quantity
            order_id = stable_id(
                config.seed, f"order-{day_index}", impression_ordinal
            )
            attributed_aggregate.orders += 1
            attributed_aggregate.gross_revenue_minor += amount_minor

            web_purchase = _event_record(
                config=config,
                ordinal=(day_index * impression_count + impression_ordinal) * 10 + 5,
                dataset="web_events",
                event_name="purchase",
                event_time=purchase_time,
                source=page_view.payload["source"],
                customer=customer,
                anonymous_id=anonymous_id,
                session_id=session_id,
                campaign=attributed_campaign,
                click_id=attributed_touch.click_id,
                payload={
                    **_web_payload(product, device, "confirmation", False),
                    "order_id": order_id,
                    "amount_minor": amount_minor,
                    "currency": "VND",
                },
            )
            enqueue(web_purchase)

            order_created = _event_record(
                config=config,
                ordinal=(day_index * impression_count + impression_ordinal) * 10 + 6,
                dataset="orders",
                event_name="order_created",
                event_time=purchase_time + timedelta(seconds=1),
                source="commerce",
                customer=customer,
                anonymous_id=anonymous_id,
                session_id=session_id,
                campaign=attributed_campaign,
                click_id=attributed_touch.click_id,
                payload={
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
                    "attributed_platform": attributed_touch.platform,
                    "attributed_campaign_id": attributed_touch.campaign_id,
                },
            )
            enqueue(order_created)

            refunded = _is_scheduled(
                order_index, config.anomalies.refund, forced_index=0
            )
            if refunded:
                attributed_aggregate.refunds_minor += amount_minor
                refund_event = _event_record(
                    config=config,
                    ordinal=(day_index * impression_count + impression_ordinal) * 10 + 7,
                    dataset="orders",
                    event_name="order_refunded",
                    event_time=purchase_time + timedelta(seconds=2),
                    source="commerce",
                    customer=customer,
                    anonymous_id=anonymous_id,
                    session_id=session_id,
                    campaign=attributed_campaign,
                    click_id=attributed_touch.click_id,
                    payload={
                        **order_created.payload["payload"],
                        "refund_amount_minor": amount_minor,
                    },
                )
                enqueue(refund_event)
            order_index += 1

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
        for ordinal, customer in enumerate(_email_audience(masters))
    }
    for ordinal, customer in enumerate(masters.customers):
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
    for ordinal, customer in enumerate(_email_audience(masters)):
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


def _email_audience(masters: MasterData) -> tuple[Customer, ...]:
    eligible = tuple(
        customer
        for customer in masters.customers
        if customer.consent_marketing
    )
    selected_count = max(1, len(eligible) // 50)
    return eligible[:selected_count]


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
) -> SourceRecord:
    return SourceRecord(
        dataset=dataset,
        event_time=event_time,
        payload=_envelope(
            event_id=stable_id(config.seed, f"{dataset}-{event_name}", ordinal),
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
    day_index: int,
    impression_ordinal: int,
    customer: Customer | None,
    current_touch: PaidTouch,
    purchase_time: datetime,
) -> PaidTouch | None:
    """Resolve last paid click, including clicks during an overlapping journey."""

    if customer is None:
        return current_touch
    touches = [current_touch]
    impression_count = config.profile.impressions_per_day
    day = config.start_date + timedelta(days=day_index)
    day_start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    for candidate_ordinal in range(impression_ordinal + 1, impression_count):
        candidate_time = day_start + timedelta(
            seconds=(candidate_ordinal * 86_000) // max(1, impression_count)
        )
        if candidate_time > purchase_time:
            break
        campaign = masters.campaigns[candidate_ordinal % len(masters.campaigns)]
        click_score = stable_int(
            config.seed,
            "click",
            day_index,
            candidate_ordinal,
            modulo=1_000,
        )
        if click_score >= _click_threshold(campaign):
            continue
        candidate_customer = _select_customer(
            config,
            masters.customers,
            day_index,
            candidate_ordinal,
        )
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


def _select_customer(
    config: GenerationConfig,
    customers: tuple[Customer, ...],
    day_index: int,
    impression_ordinal: int,
) -> Customer | None:
    identified = stable_int(
        config.seed,
        "identified",
        day_index,
        impression_ordinal,
        modulo=100,
    ) < 70
    if not identified:
        return None
    index = stable_int(
        config.seed,
        "session-customer",
        day_index,
        impression_ordinal,
        modulo=len(customers),
    )
    return customers[index]


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


def _click_threshold(campaign: Campaign) -> int:
    return {"awareness": 45, "traffic": 90, "conversions": 70}[
        campaign.objective
    ]


def _click_id(
    seed: int,
    platform: str,
    day_index: int,
    impression_ordinal: int,
) -> str:
    prefix = {"google": "gclid", "meta": "fbclid", "tiktok": "ttclid"}[
        platform
    ]
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
