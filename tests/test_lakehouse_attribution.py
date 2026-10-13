from __future__ import annotations

import unittest
from datetime import datetime, timezone

from marketpulse_lakehouse.attribution import Touch, allocate_minor_units


def _touch(click_id: str, campaign_id: str, second: int) -> Touch:
    return Touch(
        click_id=click_id,
        campaign_id=campaign_id,
        event_time=datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc),
        event_id=f"event-{click_id}",
    )


class AttributionAllocationTests(unittest.TestCase):
    def test_one_touch_receives_the_complete_amount(self) -> None:
        self.assertEqual(
            allocate_minor_units(100, [_touch("click-1", "campaign-1", 1)]),
            (("click-1", "campaign-1", 100),),
        )

    def test_two_touches_preserve_remainder_in_chronological_order(self) -> None:
        touches = [
            _touch("click-2", "campaign-2", 2),
            _touch("click-1", "campaign-1", 1),
        ]
        self.assertEqual(
            allocate_minor_units(101, touches),
            (
                ("click-1", "campaign-1", 51),
                ("click-2", "campaign-2", 50),
            ),
        )

    def test_three_touches_split_100_without_losing_a_unit(self) -> None:
        touches = [
            _touch("click-3", "campaign-3", 3),
            _touch("click-1", "campaign-1", 1),
            _touch("click-2", "campaign-2", 2),
        ]
        self.assertEqual(
            allocate_minor_units(100, touches),
            (
                ("click-1", "campaign-1", 34),
                ("click-2", "campaign-2", 33),
                ("click-3", "campaign-3", 33),
            ),
        )

    def test_allocations_are_exact_beyond_float_precision_and_at_long_max(self) -> None:
        touches = [
            _touch("click-1", "campaign-1", 1),
            _touch("click-2", "campaign-2", 2),
            _touch("click-3", "campaign-3", 3),
        ]
        for amount in (2**53 + 1, 2**63 - 1):
            allocations = allocate_minor_units(amount, touches)
            quotient, remainder = divmod(amount, 3)
            self.assertEqual(sum(value for _click, _campaign, value in allocations), amount)
            self.assertEqual(allocations[0][2], quotient + (remainder > 0))
            self.assertEqual(allocations[-1][2], quotient)

    def test_duplicate_click_identity_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate touch"):
            allocate_minor_units(
                100,
                [
                    _touch("click-1", "campaign-1", 1),
                    _touch("click-1", "campaign-2", 2),
                ],
            )

    def test_negative_amount_and_empty_touches_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "amount"):
            allocate_minor_units(-1, [_touch("click-1", "campaign-1", 1)])
        with self.assertRaisesRegex(ValueError, "touch"):
            allocate_minor_units(1, [])


if __name__ == "__main__":
    unittest.main()
