"""Deterministic integer allocation primitives for attribution models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable


@dataclass(frozen=True, slots=True)
class Touch:
    click_id: str
    campaign_id: str
    event_time: datetime
    event_id: str

    def __post_init__(self) -> None:
        if not self.click_id or not self.campaign_id or not self.event_id:
            raise ValueError("touch identifiers must not be empty")
        if self.event_time.tzinfo is None:
            raise ValueError("touch event_time must be timezone-aware")


def allocate_minor_units(
    amount: int,
    touches: Iterable[Touch],
) -> tuple[tuple[str, str, int], ...]:
    if amount < 0:
        raise ValueError("amount must be non-negative")
    ordered = sorted(
        touches,
        key=lambda touch: (
            touch.event_time,
            touch.campaign_id,
            touch.click_id,
            touch.event_id,
        ),
    )
    if not ordered:
        raise ValueError("at least one touch is required")
    click_ids = [touch.click_id for touch in ordered]
    if len(click_ids) != len(set(click_ids)):
        raise ValueError("duplicate touch click_id")

    quotient, remainder = divmod(amount, len(ordered))
    return tuple(
        (
            touch.click_id,
            touch.campaign_id,
            quotient + (1 if index < remainder else 0),
        )
        for index, touch in enumerate(ordered)
    )
