from __future__ import annotations

import tempfile
import unittest
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

from marketpulse_mock.config import GenerationConfig, ScaleProfile, resolve_config
from marketpulse_mock.generator import generate_dataset
from marketpulse_mock.ids import stable_id
from marketpulse_mock.journeys import SourceRecord, iter_day_records
from marketpulse_mock.masters import generate_master_data
from marketpulse_mock.models import Customer
from marketpulse_mock.validator import validate_dataset

DAYS_PER_MONTH = 365.25 / 12


def _generate_window(
    config: GenerationConfig,
) -> tuple[dict[str, Customer], list[SourceRecord]]:
    masters = generate_master_data(config)
    records = [
        record
        for day_index in range(config.profile.days)
        for record in iter_day_records(config, masters, day_index)
    ]
    return {c.customer_id: c for c in masters.customers}, records


def _customer_id(record: SourceRecord) -> str | None:
    value = record.payload.get("customer_id")
    return None if value is None else str(value)


def _page_views(records: list[SourceRecord]) -> list[SourceRecord]:
    return [
        r
        for r in records
        if r.dataset == "web_events" and r.payload["event_name"] == "page_view"
    ]


def _assert_attribution_matches_sessions(
    test: unittest.TestCase, records: list[SourceRecord]
) -> int:
    """Check every order against the generated clicks; return overlap count."""

    clicks: dict[tuple[str, date], list[tuple[datetime, str, str]]] = defaultdict(
        list
    )
    own_click: dict[str, str] = {}
    for r in _page_views(records):
        own_click[str(r.payload["session_id"])] = str(r.payload["click_id"])
        customer_id = _customer_id(r)
        if customer_id is None or r.payload["click_id"] is None:
            continue
        clicks[(customer_id, r.event_time.date())].append(
            (r.event_time, str(r.payload["campaign_id"]), str(r.payload["click_id"]))
        )
    purchase_times = {
        r.payload["payload"]["order_id"]: r.event_time
        for r in records
        if r.dataset == "web_events" and r.payload["event_name"] == "purchase"
    }
    checked = overlaps = 0
    for r in records:
        if r.dataset != "orders" or r.payload["event_name"] != "order_created":
            continue
        customer_id = _customer_id(r)
        if customer_id is None:
            continue
        purchase_time = purchase_times[r.payload["payload"]["order_id"]]
        eligible = [
            click
            for click in clicks[(customer_id, purchase_time.date())]
            if purchase_time - timedelta(days=7) <= click[0] <= purchase_time
        ]
        test.assertTrue(eligible, r.payload)
        latest = max(eligible)
        test.assertEqual(
            latest[1], r.payload["payload"]["attributed_campaign_id"], r.payload
        )
        test.assertEqual(latest[2], r.payload["click_id"], r.payload)
        if r.payload["click_id"] != own_click[str(r.payload["session_id"])]:
            overlaps += 1
        checked += 1
    test.assertGreater(checked, 0)
    return overlaps


class StoryJoinAndTenureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cls.config = resolve_config("story-demo", 360, Path(tmp), days=30)
        cls.customers, cls.records = _generate_window(cls.config)
        cls.start = cls.config.start_date
        cls.end = cls.start + timedelta(days=cls.config.profile.days - 1)

    def _joined_in_window(self) -> list[Customer]:
        return [
            c
            for c in self.customers.values()
            if self.start <= c.created_at.date() <= self.end
        ]

    def test_no_customer_event_before_join(self) -> None:
        checked = 0
        for record in self.records:
            customer_id = _customer_id(record)
            if customer_id is None:
                continue
            created_at = self.customers[customer_id].created_at
            self.assertGreaterEqual(
                record.event_time,
                created_at,
                (record.dataset, customer_id, record.event_time, created_at),
            )
            checked += 1
        self.assertGreater(checked, 0)

    def test_every_joiner_has_a_session_on_the_join_day(self) -> None:
        sessions = {
            (_customer_id(r), r.event_time.date()) for r in _page_views(self.records)
        }
        joiners = self._joined_in_window()
        self.assertGreater(len(joiners), 1000)
        missing = [
            c.customer_id
            for c in joiners
            if (c.customer_id, c.created_at.date()) not in sessions
        ]
        self.assertEqual([], missing)

    def test_tenure_decays_activity(self) -> None:
        sessions_by_day: dict[date, dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        seen_sessions: set[str] = set()
        for r in _page_views(self.records):
            customer_id = _customer_id(r)
            session_id = str(r.payload["session_id"])
            if customer_id is None or session_id in seen_sessions:
                continue
            seen_sessions.add(session_id)
            sessions_by_day[r.event_time.date()][customer_id] += 1

        totals = {"new": [0, 0], "old": [0, 0]}  # [sessions, customer-days]
        for offset in range(self.config.profile.days):
            day = self.start + timedelta(days=offset)
            for customer in self.customers.values():
                tenure_days = (day - customer.created_at.date()).days
                if tenure_days < 0:
                    continue
                months = tenure_days / DAYS_PER_MONTH
                group = "new" if months < 1 else "old" if months >= 12 else None
                if group is None:
                    continue
                totals[group][0] += sessions_by_day[day].get(customer.customer_id, 0)
                totals[group][1] += 1
        new_rate = totals["new"][0] / totals["new"][1]
        old_rate = totals["old"][0] / totals["old"][1]
        self.assertGreater(totals["old"][1], 0)
        self.assertLess(old_rate, 0.6 * new_rate, (old_rate, new_rate))

    def test_crm_snapshot_starts_on_join_day(self) -> None:
        snapshot_dates: dict[str, set[date]] = defaultdict(set)
        for r in self.records:
            if r.dataset == "crm":
                snapshot_dates[str(r.payload["customer_id"])].add(
                    date.fromisoformat(str(r.payload["snapshot_date"]))
                )
        last_day_joiners = 0
        for customer in self.customers.values():
            first = max(customer.created_at.date(), self.start)
            expected = {
                first + timedelta(days=n) for n in range((self.end - first).days + 1)
            }
            self.assertEqual(
                expected, snapshot_dates[customer.customer_id], customer.customer_id
            )
            last_day_joiners += customer.created_at.date() == self.end
        self.assertGreater(last_day_joiners, 0)

    def test_attribution_uses_the_same_picker(self) -> None:
        _assert_attribution_matches_sessions(self, self.records)


class CrowdedStoryWorldTests(unittest.TestCase):
    """Few customers and dense traffic make same-customer overlapping clicks common."""

    @classmethod
    def setUpClass(cls) -> None:
        profile = ScaleProfile(
            "crowded-story", 4, 12, 3, 10, 20_000, 1_000, "vietnam-retail"
        )
        cls.config = GenerationConfig(
            profile, 360, Path("unused"), date(2025, 1, 1), 1_000
        )
        cls.customers, cls.records = _generate_window(cls.config)

    def test_overlapping_clicks_attribute_to_the_picked_customer(self) -> None:
        overlaps = _assert_attribution_matches_sessions(self, self.records)
        self.assertGreater(overlaps, 0)

    def test_no_customer_event_before_join(self) -> None:
        for record in self.records:
            customer_id = _customer_id(record)
            if customer_id is not None:
                self.assertGreaterEqual(
                    record.event_time, self.customers[customer_id].created_at
                )


class JoinOverflowTests(unittest.TestCase):
    """More joiners than identified sessions: every joiner still gets a session."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.profile = ScaleProfile(
            "overflow-story", 3, 300, 3, 10, 60, 1_000, "vietnam-retail"
        )
        cls.config = GenerationConfig(
            cls.profile, 360, Path("unused"), date(2025, 1, 1), 1_000
        )
        cls.customers, cls.records = _generate_window(cls.config)

    def test_every_joiner_has_a_session_on_the_join_day(self) -> None:
        sessions = {
            (_customer_id(r), r.event_time.date()) for r in _page_views(self.records)
        }
        start = self.config.start_date
        joiners = [
            c
            for c in self.customers.values()
            if 0 <= (c.created_at.date() - start).days < self.profile.days
        ]
        # 60 impressions a day yield only a handful of clicked sessions, so
        # ~60 joiners a day can only be served by overflow join-day sessions.
        per_day = defaultdict(int)
        for c in joiners:
            per_day[c.created_at.date()] += 1
        self.assertGreater(min(per_day.values()), 20)
        missing = [
            c.customer_id
            for c in joiners
            if (c.customer_id, c.created_at.date()) not in sessions
        ]
        self.assertEqual([], missing)

    def test_attribution_matches_sessions(self) -> None:
        _assert_attribution_matches_sessions(self, self.records)

    def test_overflow_dataset_validates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "overflow"
            config = GenerationConfig(
                self.profile, 360, root, date(2025, 1, 1), 1_000
            )
            generate_dataset(config)
            report = validate_dataset(root)
        self.assertTrue(report.valid, report.errors[:5])


class ReturningDeviceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cls.config = resolve_config("story-demo", 360, Path(tmp), days=30)
        _, cls.records = _generate_window(cls.config)

    def _anonymous_views(self) -> list[tuple[date, str]]:
        return [
            (r.event_time.date(), str(r.payload["anonymous_id"]))
            for r in _page_views(self.records)
            if _customer_id(r) is None and r.payload["anonymous_id"] is not None
        ]

    def test_returning_share_matches_target_with_real_earlier_devices(self) -> None:
        first_seen: dict[str, date] = {}
        later = returning = 0
        start = self.config.start_date
        for day, anonymous_id in self._anonymous_views():
            if day > start:
                later += 1
                if anonymous_id in first_seen and first_seen[anonymous_id] < day:
                    returning += 1
            first_seen.setdefault(anonymous_id, day)
        self.assertGreater(later, 0)
        self.assertGreaterEqual(returning / later, 0.25)
        self.assertLessEqual(returning / later, 0.35)

    def test_day_zero_never_returns(self) -> None:
        impressions = self.config.profile.impressions_per_day
        fresh = {
            stable_id(self.config.seed, "anonymous-0", ordinal)
            for ordinal in range(impressions)
        }
        day_zero = [
            anonymous_id
            for day, anonymous_id in self._anonymous_views()
            if day == self.config.start_date
        ]
        self.assertGreater(len(day_zero), 0)
        self.assertTrue(set(day_zero) <= fresh)
        self.assertEqual(len(day_zero), len(set(day_zero)))

    def test_determinism_with_returning_devices(self) -> None:
        def stream(seed: int) -> list[tuple[str, str, str]]:
            with tempfile.TemporaryDirectory() as tmp:
                config = resolve_config("story-demo", seed, Path(tmp), days=30)
            _, records = _generate_window(config)
            return [
                (r.dataset, r.event_time.isoformat(), str(r.payload))
                for r in records
            ]

        self.assertEqual(stream(360), stream(360))
        self.assertNotEqual(stream(360), stream(361))


if __name__ == "__main__":
    unittest.main()
