from __future__ import annotations

import gzip
import hashlib
import json
import tempfile
import math
import unittest
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import median

from marketpulse_mock.calendar import HOURLY_WEIGHTS, VIETNAM, utc_hour_weights
from marketpulse_mock.config import (
    GenerationConfig,
    ScaleProfile,
    first_email_ordinal,
    first_non_paid_ordinal,
    impression_capacity,
    resolve_config,
)
from marketpulse_mock.email_schedule import EmailSchedule
from marketpulse_mock.generator import generate_dataset
from marketpulse_mock.ids import stable_id
from marketpulse_mock.journeys import (
    SourceRecord,
    _customer_picker,
    _day_impressions,
    _iter_email_events,
    iter_day_records,
)
from marketpulse_mock.masters import generate_master_data
from marketpulse_mock.models import Customer, MasterData
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


def _traffic_source(record: SourceRecord) -> object:
    payload = record.payload["payload"]
    assert isinstance(payload, dict)
    return payload.get("traffic_source")


def _non_paid_sessions_per_day(records: list[SourceRecord]) -> Counter[date]:
    """Count the organic and direct sessions starting on each UTC day."""

    return Counter(
        r.event_time.date()
        for r in _session_starts(records).values()
        if _traffic_source(r) in ("organic", "direct")
    )


def _assert_identified_sessions_keep_fresh_devices(
    test: unittest.TestCase,
    config: GenerationConfig,
    records: list[SourceRecord],
) -> int:
    """Check identified sessions use their own fresh device; return their count.

    Also checks that no device id is ever shared by two customers.
    """

    fresh_ids: dict[date, set[str]] = {}
    owners: dict[str, set[str]] = defaultdict(set)
    checked = 0
    # Impression ordinals, then overflow ordinals from the fixed capacity,
    # then the day's non-paid ordinals, then one email ordinal per customer.
    ordinals = impression_capacity(config) + config.profile.customers
    non_paid = _non_paid_sessions_per_day(records)
    email = range(
        first_email_ordinal(config),
        first_email_ordinal(config) + config.profile.customers,
    )
    for r in _page_views(records):
        customer_id = _customer_id(r)
        if customer_id is None:
            continue
        day = r.event_time.date()
        if day not in fresh_ids:
            day_index = (day - config.start_date).days
            first = first_non_paid_ordinal(config)
            fresh_ids[day] = {
                stable_id(config.seed, f"anonymous-{day_index}", ordinal)
                for ordinal in (
                    *range(ordinals), *range(first, first + non_paid[day]), *email
                )
            }
        anonymous_id = str(r.payload["anonymous_id"])
        test.assertTrue(
            anonymous_id in fresh_ids[day],
            f"{customer_id} on {day} reuses device {anonymous_id}",
        )
        owners[anonymous_id].add(customer_id)
        checked += 1
    shared = {device: ids for device, ids in owners.items() if len(ids) > 1}
    test.assertEqual({}, shared)
    return checked


def _assert_attribution_matches_sessions(
    test: unittest.TestCase, records: list[SourceRecord]
) -> int:
    """Check every order against the generated paid clicks; return overlap count.

    Paid orders and non-paid orders alike credit the customer's latest paid
    click in the 7 days up to the purchase (a non-paid order skips bot
    sessions' clicks); a non-paid order with no such click (or no customer)
    is unattributed.
    """

    clicks: dict[str, list[tuple[datetime, str, str]]] = defaultdict(list)
    bot_clicks: set[str] = set()
    own_click: dict[str, object] = {}
    session_source: dict[str, object] = {}
    for r in _page_views(records):
        session_id = str(r.payload["session_id"])
        own_click[session_id] = r.payload["click_id"]
        session_source.setdefault(session_id, _traffic_source(r))
        customer_id = _customer_id(r)
        if customer_id is None or r.payload["click_id"] is None:
            continue
        if r.payload["payload"]["is_bot"]:
            bot_clicks.add(str(r.payload["click_id"]))
        clicks[customer_id].append(
            (r.event_time, str(r.payload["campaign_id"]), str(r.payload["click_id"]))
        )
    for customer_clicks in clicks.values():
        customer_clicks.sort()
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
        session_id = str(r.payload["session_id"])
        paid = session_source[session_id] in (None, "paid")
        if customer_id is None:
            if not paid:
                test.assertIsNone(r.payload["click_id"], r.payload)
                test.assertIsNone(
                    r.payload["payload"]["attributed_campaign_id"], r.payload
                )
            continue
        purchase_time = purchase_times[r.payload["payload"]["order_id"]]
        customer_clicks = clicks[customer_id]
        eligible = customer_clicks[
            bisect_left(customer_clicks, (purchase_time - timedelta(days=7),)):
            bisect_right(customer_clicks, (purchase_time, "\uffff"))
        ]
        if not paid:
            eligible = [click for click in eligible if click[2] not in bot_clicks]
        if not paid and not eligible:
            test.assertIsNone(r.payload["campaign_id"], r.payload)
            test.assertIsNone(r.payload["click_id"], r.payload)
            test.assertIsNone(
                r.payload["payload"]["attributed_campaign_id"], r.payload
            )
            checked += 1
            continue
        test.assertTrue(eligible, r.payload)
        # Session times have one-second resolution, so a customer can click
        # twice in the latest second; either click is then the latest.
        latest_time = eligible[-1][0]
        latest = {click[1:] for click in eligible if click[0] == latest_time}
        test.assertIn(
            (
                r.payload["payload"]["attributed_campaign_id"],
                r.payload["click_id"],
            ),
            latest,
            r.payload,
        )
        if paid and r.payload["click_id"] != own_click[session_id]:
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
        # The first 30 days of the story hold ~2 joiners a day.
        self.assertGreater(len(joiners), 30)
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
            "crowded-story", 4, 12, 4, 10, 20_000, 1_000, "vietnam-retail"
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
            "overflow-story", 3, 300, 4, 10, 60, 1_000, "vietnam-retail"
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
        masters = generate_master_data(self.config)
        for day_index in range(self.profile.days):
            picker = _customer_picker(self.config, masters, day_index)
            self.assertTrue(picker.overflow_ordinals, day_index)
        missing = [
            c.customer_id
            for c in joiners
            if (c.customer_id, c.created_at.date()) not in sessions
        ]
        self.assertEqual([], missing)

    def test_attribution_matches_sessions(self) -> None:
        _assert_attribution_matches_sessions(self, self.records)

    def test_event_ids_are_unique_across_the_world(self) -> None:
        # The duplicate_event anomaly re-emits an identical copy (same id and
        # payload); any other repeated id is a collision.
        payloads: dict[str, dict[str, object]] = {}
        checked = 0
        for r in self.records:
            event_id = r.payload.get("event_id")
            if not isinstance(event_id, str):
                continue
            checked += 1
            first = payloads.setdefault(event_id, r.payload)
            self.assertEqual(first, r.payload, event_id)
        self.assertGreater(checked, 0)
        self.assertLess(len(payloads), checked)  # duplicates were exercised

    def test_joiner_sessions_keep_fresh_devices(self) -> None:
        checked = _assert_identified_sessions_keep_fresh_devices(
            self, self.config, self.records
        )
        self.assertGreater(checked, 0)

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
        # Only paid sessions return; organic, direct and email visits keep
        # fresh ids. A duplicate anomaly repeats a view, so count it once.
        views = {str(r.payload["event_id"]): r for r in _page_views(self.records)}
        return [
            (r.event_time.date(), str(r.payload["anonymous_id"]))
            for r in views.values()
            if _customer_id(r) is None
            and r.payload["anonymous_id"] is not None
            and r.payload["click_id"] is not None
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

    def test_identified_sessions_keep_fresh_devices(self) -> None:
        checked = _assert_identified_sessions_keep_fresh_devices(
            self, self.config, self.records
        )
        self.assertGreater(checked, 1000)

    def test_day_zero_never_returns(self) -> None:
        impressions = _day_impressions(self.config, 0)
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


STORY_SPEND_SHARES = {"google": 30, "meta": 28, "tiktok": 24, "shopee": 18}
TRUTH_TOTALS = ("impressions", "clicks", "spend_minor", "sessions", "orders")


class ShopeeReportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cls.config = resolve_config("story-demo", 360, Path(tmp), days=3)
        cls.masters, cls.records = (
            generate_master_data(cls.config),
            _generate_window(cls.config)[1],
        )

    def test_shopee_report_reconciles_with_ground_truth(self) -> None:
        by_vendor = {c.platform_campaign_id: c for c in self.masters.campaigns}
        truth = {
            (str(r.payload["date"]), str(r.payload["campaign_id"])): r.payload
            for r in self.records
            if r.dataset == "ground_truth"
        }
        reports = [r for r in self.records if r.dataset == "shopee_ads"]
        self.assertGreater(len(reports), 0)
        for report in reports:
            p = report.payload
            self.assertEqual(
                {"date", "ad_id", "impression", "clicks", "expense",
                 "conversions", "gmv"},
                set(p),
            )
            campaign = by_vendor[str(p["ad_id"])]
            day, month, year = str(p["date"]).split("/")
            expected = truth[(f"{year}-{month}-{day}", campaign.campaign_id)]
            self.assertEqual("shopee", campaign.platform)
            self.assertEqual(expected["impressions"], p["impression"])
            self.assertEqual(expected["clicks"], p["clicks"])
            self.assertEqual(expected["spend_minor"], p["expense"])
            self.assertEqual(expected["orders"], p["conversions"])
            self.assertEqual(expected["gross_revenue_minor"], p["gmv"])

    def test_shopee_dates_are_day_first(self) -> None:
        dates = {
            str(r.payload["date"])
            for r in self.records
            if r.dataset == "shopee_ads"
        }
        start = self.config.start_date
        self.assertEqual(
            {f"{start + timedelta(days=i):%d/%m/%Y}" for i in range(3)}, dates
        )

    def test_validator_rejects_unknown_shopee_ad_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "world"
            config = resolve_config("story-demo", 360, root, days=3)
            generate_dataset(config)
            self.assertTrue(validate_dataset(root).valid)
            target = next((root / "shopee_ads").rglob("*.csv.gz"))
            with gzip.open(target, "rt", encoding="utf-8", newline="") as s:
                text = s.read()
            lines = text.splitlines()
            head, first = lines[0], lines[1].split(",")
            first[1] = "S-missing"
            lines[1] = ",".join(first)
            data = ("\r\n".join([head, *lines[1:]]) + "\r\n").encode()
            with gzip.GzipFile(target, "wb", mtime=0) as s:
                s.write(data)
            content = target.read_bytes()
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            relative = target.relative_to(root).as_posix()
            for item in manifest["files"]:
                if item["relative_path"] == relative:
                    item["byte_size"] = len(content)
                    item["sha256"] = hashlib.sha256(content).hexdigest()
            manifest_path.write_text(json.dumps(manifest))
            report = validate_dataset(root)
        self.assertFalse(report.valid)
        self.assertTrue(
            any("unknown platform campaign_id S-missing" in e
                for e in report.errors),
            report.errors[:5],
        )


class StoryChannelPersonaTests(unittest.TestCase):
    """Spend follows platform budgets; each platform has its own funnel persona."""

    @classmethod
    def setUpClass(cls) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cls.config = resolve_config("story-demo", 360, Path(tmp), days=30)
        _, cls.records = _generate_window(cls.config)
        totals: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        campaign_platform: dict[str, str] = {}
        for r in cls.records:
            if r.dataset != "ground_truth":
                continue
            campaign_platform[str(r.payload["campaign_id"])] = str(
                r.payload["platform"]
            )
            platform_totals = totals[str(r.payload["platform"])]
            for key in TRUTH_TOTALS:
                platform_totals[key] += int(r.payload[key])
        cls.totals = totals

        # Funnel stages reached per session, keyed by the session's own platform.
        session_platform: dict[str, str] = {}
        stages: dict[str, set[str]] = defaultdict(set)
        for r in cls.records:
            if r.dataset != "web_events":
                continue
            session_id = str(r.payload["session_id"])
            if r.payload["event_name"] == "page_view" and r.payload["campaign_id"]:
                session_platform.setdefault(
                    session_id, campaign_platform[str(r.payload["campaign_id"])]
                )
            stages[session_id].add(str(r.payload["event_name"]))
        funnel: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for session_id, platform in session_platform.items():
            for stage in stages[session_id]:
                funnel[platform][stage] += 1
        cls.funnel = funnel

    def _rate(self, platform: str, numerator: str, denominator: str) -> float:
        totals = self.totals[platform]
        return totals[numerator] / totals[denominator]

    def test_spend_follows_platform_shares(self) -> None:
        spend = {p: t["spend_minor"] for p, t in self.totals.items()}
        self.assertEqual(set(STORY_SPEND_SHARES), set(spend))
        total = sum(spend.values())
        for platform, share in STORY_SPEND_SHARES.items():
            with self.subTest(platform):
                self.assertAlmostEqual(
                    100 * spend[platform] / total, share, delta=3
                )

    def test_tiktok_buys_cheap_clicks_that_convert_less(self) -> None:
        self.assertGreater(
            self._rate("tiktok", "clicks", "impressions"),
            self._rate("google", "clicks", "impressions"),
        )
        self.assertLess(
            self._rate("tiktok", "orders", "sessions"),
            self._rate("google", "orders", "sessions"),
        )
        self.assertLess(
            self._rate("tiktok", "spend_minor", "impressions"),
            self._rate("google", "spend_minor", "impressions"),
        )

    def test_shopee_converts_best(self) -> None:
        rates = {p: self._rate(p, "orders", "sessions") for p in STORY_SPEND_SHARES}
        self.assertEqual("shopee", max(rates, key=rates.__getitem__), rates)

    def test_late_funnel_conditional_rates_are_capped(self) -> None:
        for platform in STORY_SPEND_SHARES:
            stages = self.funnel[platform]
            with self.subTest(platform):
                self.assertGreater(stages["add_to_cart"], 0)
                self.assertGreater(stages["begin_checkout"], 0)
                self.assertLessEqual(
                    stages["begin_checkout"] / stages["add_to_cart"], 0.9
                )
                self.assertLessEqual(
                    stages["purchase"] / stages["begin_checkout"], 0.9
                )

    def test_helpers_follow_personas(self) -> None:
        from marketpulse_mock.journeys import _click_threshold, _funnel_steps

        masters = generate_master_data(self.config)
        by_key = {(c.platform, c.objective): c for c in masters.campaigns}
        # Traffic's base cut-off is 90 per 1,000; CTR personas 0.8 and 1.6.
        self.assertEqual(72, _click_threshold(self.config, by_key["google", "traffic"]))
        self.assertEqual(
            144, _click_threshold(self.config, by_key["tiktok", "traffic"])
        )
        campaign = masters.campaigns[0]
        # View and cart cut-offs stay at 850 and 400 whatever the strength.
        self.assertEqual(
            (True, True, True, True), _funnel_steps(self.config, campaign, 0, 1.0)
        )
        self.assertEqual(
            (True, False, False, False),
            _funnel_steps(self.config, campaign, 849, 4.0),
        )
        self.assertEqual(
            (True, True, False, False),
            _funnel_steps(self.config, campaign, 399, 0.01),
        )
        # m = 1 reproduces the legacy 250 / 120 cut-offs.
        self.assertEqual(
            (True, True, True, False),
            _funnel_steps(self.config, campaign, 249, 1.0),
        )
        self.assertEqual(
            (True, True, False, False),
            _funnel_steps(self.config, campaign, 250, 1.0),
        )
        self.assertTrue(_funnel_steps(self.config, campaign, 119, 1.0)[3])
        self.assertFalse(_funnel_steps(self.config, campaign, 120, 1.0)[3])
        # Both conditional rates cap at 0.9: 400 * 0.9 = 360, 360 * 0.9 = 324.
        self.assertTrue(_funnel_steps(self.config, campaign, 359, 100.0)[2])
        self.assertFalse(_funnel_steps(self.config, campaign, 360, 100.0)[2])
        self.assertTrue(_funnel_steps(self.config, campaign, 323, 100.0)[3])
        self.assertFalse(_funnel_steps(self.config, campaign, 324, 100.0)[3])


def _generate_days(
    config: GenerationConfig, masters: MasterData, day_indices: range
) -> dict[int, list[SourceRecord]]:
    return {
        day_index: list(iter_day_records(config, masters, day_index))
        for day_index in day_indices
    }


def _session_starts(records: list[SourceRecord]) -> dict[str, SourceRecord]:
    """Return each session's first page view (bot probes share the session)."""

    starts: dict[str, SourceRecord] = {}
    for r in _page_views(records):
        starts.setdefault(str(r.payload["session_id"]), r)
    return starts


def _sessions_by_vn_date(records: list[SourceRecord]) -> Counter[date]:
    return Counter(
        r.event_time.astimezone(VIETNAM).date()
        for r in _session_starts(records).values()
    )


def _vendor_row(record: SourceRecord) -> tuple[tuple[str, str], tuple[int, ...]]:
    """Normalize one vendor report to ``((date, vendor id), totals)``."""

    p = record.payload
    if record.dataset == "google_ads":
        return (str(p["segments_date"]), str(p["campaign_id"])), (
            int(p["impressions"]), int(p["clicks"]),
            int(p["cost_micros"]) // 1_000_000, int(p["conversions"]),
            int(p["conversions_value"]),
        )
    if record.dataset == "meta_ads":
        return (str(p["date_start"]), str(p["campaign_id"])), (
            int(p["impressions"]), int(p["clicks"]), int(p["spend"]),
            int(p["actions"][0]["value"]), int(p["action_values"][0]["value"]),
        )
    if record.dataset == "shopee_ads":
        day, month, year = str(p["date"]).split("/")
        return (f"{year}-{month}-{day}", str(p["ad_id"])), (
            int(p["impression"]), int(p["clicks"]), int(p["expense"]),
            int(p["conversions"]), int(p["gmv"]),
        )
    return (str(p["stat_time_day"]), str(p["campaign_id"])), (
        int(p["impressions"]), int(p["clicks"]), int(p["spend"]),
        int(p["conversion"]), int(p["total_purchase_value"]),
    )


STORY_START = date(2025, 1, 1)
NOVEMBER_DAYS = range(304, 334)  # UTC 2025-11-01 .. 2025-11-30
TET_DAYS = range(403, 423)  # UTC 2026-02-08 .. 2026-02-27


class StoryCalendarRhythmTests(unittest.TestCase):
    """November 2025 of the full story: Vietnam-time hours, 11/11 and Shopee."""

    @classmethod
    def setUpClass(cls) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cls.config = resolve_config("story-demo", 360, Path(tmp))
        cls.masters = generate_master_data(cls.config)
        cls.days = _generate_days(cls.config, cls.masters, NOVEMBER_DAYS)
        cls.records = [r for i in NOVEMBER_DAYS for r in cls.days[i]]

    def test_day_indices_are_november(self) -> None:
        self.assertEqual(STORY_START, self.config.start_date)
        self.assertEqual(date(2025, 11, 1), STORY_START + timedelta(days=304))
        self.assertEqual(date(2025, 11, 30), STORY_START + timedelta(days=333))

    def test_daily_volume_follows_the_calendar(self) -> None:
        base = self.config.profile.impressions_per_day
        for day_index, records in self.days.items():
            day = STORY_START + timedelta(days=day_index)
            expected = round(
                base * sum(utc_hour_weights(day)) / sum(HOURLY_WEIGHTS)
            )
            impressions = sum(
                int(r.payload["impressions"])
                for r in records
                if r.dataset == "ground_truth"
            )
            self.assertEqual(expected, impressions, day)

    def test_evening_peak_dwarfs_the_small_hours(self) -> None:
        hours = Counter(
            r.event_time.astimezone(VIETNAM).hour
            for r in _session_starts(self.records).values()
        )
        evening = hours[20] + hours[21]
        night = hours[2] + hours[3]
        self.assertGreater(night, 0)
        self.assertGreaterEqual(evening, 4 * night, (evening, night))

    def test_double_day_lifts_its_vietnam_date(self) -> None:
        sessions = _sessions_by_vn_date(self.records)
        singles_day = date(2025, 11, 11)
        around = [
            sessions[date(2025, 11, d)] for d in range(4, 19) if d != 11
        ]
        self.assertGreaterEqual(
            sessions[singles_day], 1.5 * median(around),
            (sessions[singles_day], median(around)),
        )

    def test_shopee_spend_share_peaks_on_the_double_day(self) -> None:
        # Ground truth is per UTC day; UTC 11/11 holds VN 11/11 07:00-23:59,
        # the bulk of the double day's weight, while UTC 11/12 holds none of it.
        def share(day: date) -> float:
            spend: dict[str, int] = defaultdict(int)
            for r in self.days[(day - STORY_START).days]:
                if r.dataset == "ground_truth":
                    spend[str(r.payload["platform"])] += int(r.payload["spend_minor"])
            return spend["shopee"] / sum(spend.values())

        # Doubling an 18 % share gives about 30.5 %; noise between two
        # unboosted days of about 18 % stays far below a 1.4x margin.
        singles_day, next_day = share(date(2025, 11, 11)), share(date(2025, 11, 12))
        self.assertGreaterEqual(singles_day, 1.4 * next_day, (singles_day, next_day))

    def test_records_stay_ordered_inside_their_utc_day(self) -> None:
        for day_index, records in self.days.items():
            day = STORY_START + timedelta(days=day_index)
            last: dict[str, datetime] = {}
            for r in records:
                self.assertEqual(day, r.event_time.date(), (r.dataset, r.event_time))
                previous = last.get(r.dataset)
                if previous is not None:
                    self.assertLessEqual(previous, r.event_time, (day, r.dataset))
                last[r.dataset] = r.event_time

    def test_sessions_end_before_the_daily_reports(self) -> None:
        # Hour 23 draws only 23:00:00-23:57:59, so a funnel (<= 62 s) never
        # passes the 23:59:59 vendor reports or the UTC midnight.
        starts = [r.event_time.time() for r in _session_starts(self.records).values()]
        self.assertTrue(any(t.hour == 23 for t in starts))
        self.assertLessEqual(max(starts), datetime(2025, 1, 1, 23, 57, 59).time())

    def test_vendor_reports_equal_ground_truth_per_utc_day(self) -> None:
        vendor_id = {c.campaign_id: c.platform_campaign_id for c in self.masters.campaigns}
        for day_index, records in self.days.items():
            truth = {
                (str(r.payload["date"]), vendor_id[str(r.payload["campaign_id"])]): (
                    int(r.payload["impressions"]), int(r.payload["clicks"]),
                    int(r.payload["spend_minor"]), int(r.payload["orders"]),
                    int(r.payload["gross_revenue_minor"]),
                )
                for r in records
                if r.dataset == "ground_truth"
            }
            reports = dict(
                _vendor_row(r) for r in records if r.dataset.endswith("_ads")
            )
            self.assertEqual(16, len(truth), day_index)
            self.assertEqual(truth, reports, day_index)

    def test_anomalies_are_still_scheduled(self) -> None:
        replayable = [
            r for r in self.records
            if r.dataset in ("web_events", "orders", "email_events")
        ]
        event_ids = Counter(str(r.payload["event_id"]) for r in replayable)
        duplicates = sum(count - 1 for count in event_ids.values())
        # Every second late event is also a duplicate; count each event once.
        late = len(
            {str(r.payload["event_id"]) for r in replayable if r.payload.get("is_late")}
        )
        bots = sum(
            bool(r.payload["payload"].get("is_bot")) for r in _page_views(self.records)
        )
        refunds = sum(
            r.dataset == "orders" and r.payload["event_name"] == "order_refunded"
            for r in self.records
        )
        invalid = sum(r.dataset == "quality_cases" for r in self.records)
        for name, count in (
            ("duplicates", duplicates), ("late", late), ("bots", bots),
            ("refunds", refunds), ("invalid", invalid),
        ):
            with self.subTest(name):
                self.assertGreater(count, 0)
        # Rates follow the defaults (0.5 % duplicates, 1 % late) on any volume.
        self.assertAlmostEqual(duplicates / len(event_ids), 0.005, delta=0.001)
        self.assertAlmostEqual(late / len(event_ids), 0.01, delta=0.002)

    def test_session_and_event_ids_are_unique(self) -> None:
        session_days: dict[str, date] = {}
        for r in _page_views(self.records):
            session_id = str(r.payload["session_id"])
            day = session_days.setdefault(session_id, r.event_time.date())
            self.assertEqual(day, r.event_time.date(), session_id)
        payloads: dict[str, dict[str, object]] = {}
        for r in self.records:
            event_id = r.payload.get("event_id")
            if isinstance(event_id, str):
                first = payloads.setdefault(event_id, r.payload)
                self.assertEqual(first, r.payload, event_id)
        self.assertGreater(len(session_days), 5_000)

    def test_joiners_first_sessions_spread_over_vietnam_hours(self) -> None:
        window = {STORY_START + timedelta(days=i) for i in NOVEMBER_DAYS}
        joiners = {
            c.customer_id: c.created_at.date()
            for c in self.masters.customers
            if c.created_at.date() in window
        }
        first_hours: dict[str, int] = {}
        for r in _session_starts(self.records).values():
            customer_id = _customer_id(r)
            if customer_id in joiners and r.event_time.date() == joiners[customer_id]:
                first_hours.setdefault(
                    customer_id, r.event_time.astimezone(VIETNAM).hour
                )
        self.assertEqual(set(joiners), set(first_hours))
        self.assertGreaterEqual(len(set(first_hours.values())), 12, first_hours)

    def test_same_seed_gives_identical_records(self) -> None:
        def stream(records: list[SourceRecord]) -> list[tuple[str, str, str]]:
            return [
                (r.dataset, r.event_time.isoformat(), str(r.payload)) for r in records
            ]

        again = list(iter_day_records(self.config, self.masters, 314))
        self.assertEqual(stream(self.days[314]), stream(again))
        with tempfile.TemporaryDirectory() as tmp:
            other = resolve_config("story-demo", 361, Path(tmp))
        other_day = list(iter_day_records(other, generate_master_data(other), 314))
        self.assertNotEqual(stream(self.days[314]), stream(other_day))


class ShopeeDoubleDayStrengthTests(unittest.TestCase):
    """A Shopee session on a Vietnam double day converts at 1.5x its persona."""

    SINGLES_DAY = 314  # UTC 2025-11-11: VN 11/11 until 17:00 UTC, then VN 11/12

    @classmethod
    def setUpClass(cls) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cls.config = resolve_config("story-demo", 360, Path(tmp))
            cls.flat = resolve_config("fixture", 360, Path(tmp))
        masters = generate_master_data(cls.config)
        cls.shopee = next(c for c in masters.campaigns if c.platform == "shopee")
        cls.other = next(c for c in masters.campaigns if c.platform != "shopee")

    def _ordinal(self, double: bool) -> int:
        from marketpulse_mock.session_timing import _session_hour

        return next(
            ordinal
            for ordinal in range(1_000)
            if (_session_hour(self.config, self.SINGLES_DAY, ordinal) < 17) == double
        )

    def test_double_day_lifts_only_shopee_by_half(self) -> None:
        from marketpulse_mock.journeys import _persona, _purchase_strength

        double, plain = self._ordinal(True), self._ordinal(False)
        for campaign, lift in ((self.shopee, 1.5), (self.other, 1.0)):
            persona = _persona(self.config, campaign)[2]
            self.assertAlmostEqual(
                lift * persona,
                _purchase_strength(self.config, campaign, self.SINGLES_DAY, double),
                msg=campaign.platform,
            )
            self.assertAlmostEqual(
                persona,
                _purchase_strength(self.config, campaign, self.SINGLES_DAY, plain),
                msg=campaign.platform,
            )
        self.assertNotAlmostEqual(
            1.0, _persona(self.config, self.shopee)[2], msg="persona must be non-neutral"
        )

    def test_flat_strength_ignores_the_double_day(self) -> None:
        from marketpulse_mock.journeys import _purchase_strength

        for campaign in (self.shopee, self.other):
            for double in (True, False):
                self.assertEqual(
                    1.0,
                    _purchase_strength(
                        self.flat, campaign, self.SINGLES_DAY, self._ordinal(double)
                    ),
                )


class StoryTetDipTests(unittest.TestCase):
    """Tết 2026: eve through the fourth day dips below its surrounding weeks."""

    @classmethod
    def setUpClass(cls) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cls.config = resolve_config("story-demo", 360, Path(tmp))
        masters = generate_master_data(cls.config)
        days = _generate_days(cls.config, masters, TET_DAYS)
        cls.sessions = _sessions_by_vn_date([r for i in TET_DAYS for r in days[i]])

    def test_tet_eve_is_day_index_411(self) -> None:
        self.assertEqual(date(2026, 2, 16), STORY_START + timedelta(days=411))

    def test_tet_days_fall_below_their_surroundings(self) -> None:
        tet = [date(2026, 2, 16) + timedelta(days=n) for n in range(5)]
        around = [
            self.sessions[tet[0] - timedelta(days=n)] for n in range(1, 8)
        ] + [self.sessions[tet[-1] + timedelta(days=n)] for n in range(1, 8)]
        baseline = median(around)
        for day in tet:
            with self.subTest(day.isoformat()):
                self.assertLess(self.sessions[day], 0.7 * baseline)


NON_PAID_SOURCES = ("organic", "direct")


class StoryNonPaidTrafficTests(unittest.TestCase):
    """Organic and direct visits: a quarter of sessions, no campaign, no click."""

    @classmethod
    def setUpClass(cls) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cls.config = resolve_config("story-demo", 360, Path(tmp), days=30)
        cls.customers, cls.records = _generate_window(cls.config)
        cls.starts = _session_starts(cls.records)
        cls.sources = {
            session_id: _traffic_source(r) for session_id, r in cls.starts.items()
        }

    def _non_paid(self, record: SourceRecord) -> bool:
        return self.sources[str(record.payload["session_id"])] in NON_PAID_SOURCES

    def test_organic_and_direct_are_a_quarter_of_non_email_sessions(self) -> None:
        counts = Counter(self.sources.values())
        # Email-referred visits come on top of the paid and organic/direct mix.
        self.assertEqual({"paid", "organic", "direct", "email"}, set(counts))
        non_paid = counts["organic"] + counts["direct"]
        share = non_paid / (sum(counts.values()) - counts["email"])
        self.assertGreaterEqual(share, 0.22, counts)
        self.assertLessEqual(share, 0.28, counts)
        self.assertAlmostEqual(counts["organic"] / non_paid, 0.6, delta=0.03)

    def test_each_day_adds_a_third_of_its_paid_sessions(self) -> None:
        paid: Counter[date] = Counter()
        for r in self.records:
            if r.dataset == "ground_truth":
                paid[date.fromisoformat(str(r.payload["date"]))] += int(
                    r.payload["sessions"]
                )
        paid_starts = Counter(
            r.event_time.date()
            for r in self.starts.values()
            if _traffic_source(r) == "paid"
        )
        self.assertEqual(paid, paid_starts)
        non_paid = _non_paid_sessions_per_day(self.records)
        self.assertEqual(30, len(paid))
        for day, sessions in paid.items():
            self.assertEqual(round(sessions / 3), non_paid[day], day)

    def test_non_paid_sessions_carry_no_campaign_or_click(self) -> None:
        paid_clicks = {
            str(r.payload["click_id"])
            for r in self.starts.values()
            if _traffic_source(r) == "paid"
        }
        checked = 0
        for r in self.records:
            if r.dataset not in ("web_events", "orders"):
                continue
            session_source = self.sources[str(r.payload["session_id"])]
            self.assertEqual(session_source, _traffic_source(r), r.payload)
            if session_source == "paid":
                self.assertIsNotNone(r.payload["campaign_id"], r.payload)
                self.assertIsNotNone(r.payload["click_id"], r.payload)
                continue
            checked += 1
            if r.dataset == "orders":
                # An order may credit an earlier paid click, never its own;
                # the session's web events, purchase included, stay campaign-free.
                click_id = r.payload["click_id"]
                self.assertTrue(click_id is None or click_id in paid_clicks, r.payload)
                self.assertEqual(click_id is None, r.payload["campaign_id"] is None)
                continue
            for field in ("campaign_id", "platform_campaign_id", "click_id"):
                self.assertIsNone(r.payload[field], (field, r.payload))
            self.assertFalse(r.payload["payload"].get("is_bot"), r.payload)
            if r.payload["event_name"] == "page_view":
                self.assertEqual(
                    "https://shop.marketpulse.example.test/",
                    r.payload["payload"]["page_url"],
                )
        self.assertGreater(checked, 1_000)

    def test_non_paid_orders_credit_the_latest_paid_click_in_seven_days(self) -> None:
        _assert_attribution_matches_sessions(self, self.records)
        orders = [
            r
            for r in self.records
            if r.dataset == "orders"
            and r.payload["event_name"] == "order_created"
            and self._non_paid(r)
        ]
        click_days = {
            str(r.payload["click_id"]): r.event_time.date()
            for r in self.starts.values()
            if _traffic_source(r) == "paid"
        }
        attributed = [r for r in orders if r.payload["click_id"] is not None]
        unattributed = [r for r in orders if r.payload["click_id"] is None]
        anonymous = [r for r in unattributed if r.payload["customer_id"] is None]
        earlier_day = [
            r
            for r in attributed
            if click_days[str(r.payload["click_id"])] < r.event_time.date()
        ]
        for group in (attributed, unattributed, anonymous, earlier_day):
            self.assertGreater(len(group), 0, (len(attributed), len(unattributed)))
        vendor_id = {
            c.campaign_id: (c.platform, c.platform_campaign_id)
            for c in generate_master_data(self.config).campaigns
        }
        for r in attributed:
            payload = r.payload["payload"]
            platform, platform_campaign_id = vendor_id[str(payload["attributed_campaign_id"])]
            self.assertEqual(payload["attributed_campaign_id"], r.payload["campaign_id"])
            self.assertEqual(platform, payload["attributed_platform"])
            self.assertEqual(platform_campaign_id, r.payload["platform_campaign_id"])
        for r in unattributed:
            payload = r.payload["payload"]
            self.assertIsNone(payload["attributed_platform"])
            self.assertIsNone(payload["attributed_campaign_id"])
            self.assertIsNone(r.payload["campaign_id"])
            self.assertIsNone(r.payload["platform_campaign_id"])
        created = {str(r.payload["payload"]["order_id"]): r for r in orders}
        purchases = {
            str(r.payload["payload"]["order_id"]): r
            for r in self.records
            if r.dataset == "web_events" and r.payload["event_name"] == "purchase"
        }
        refunds = [
            r for r in self.records
            if r.dataset == "orders" and r.payload["event_name"] == "order_refunded"
            and self._non_paid(r)
        ]
        self.assertGreater(len(refunds), 0)
        for order_id, order in created.items():
            # Only the order carries the credited click; the web purchase
            # stays in the unattributed funnel with the rest of its session.
            purchase = purchases[order_id]
            for field in ("campaign_id", "platform_campaign_id", "click_id"):
                self.assertIsNone(purchase.payload[field], (field, purchase.payload))
            for field in ("customer_id", "session_id"):
                self.assertEqual(order.payload[field], purchase.payload[field])
            self.assertEqual(
                order.payload["payload"]["gross_amount_minor"],
                purchase.payload["payload"]["amount_minor"],
            )
        for refund in refunds:
            order = created[str(refund.payload["payload"]["order_id"])]
            for field in ("campaign_id", "platform_campaign_id", "click_id"):
                self.assertEqual(order.payload[field], refund.payload[field])
            for field in ("attributed_platform", "attributed_campaign_id"):
                self.assertEqual(
                    order.payload["payload"][field], refund.payload["payload"][field]
                )

    def test_campaign_funnels_never_purchase_more_than_checkout(self) -> None:
        # Gold build_funnel_daily buckets each web event by its own
        # campaign_id (bot sessions excluded); a non-paid purchase carrying
        # its credited campaign would land in a funnel without its checkout.
        bots = {
            str(r.payload["session_id"])
            for r in _page_views(self.records)
            if r.payload["payload"]["is_bot"]
        }
        funnels: dict[tuple[date, str], dict[str, set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        for r in self.records:
            if r.dataset != "web_events":
                continue
            session_id = str(r.payload["session_id"])
            if self._non_paid(r):
                self.assertIsNone(r.payload["campaign_id"], r.payload)
            if session_id in bots:
                continue
            campaign_id = str(r.payload["campaign_id"] or "__unattributed__")
            funnels[(r.event_time.date(), campaign_id)][
                str(r.payload["event_name"])
            ].add(session_id)
        purchases = 0
        for key, stages in funnels.items():
            purchases += len(stages["purchase"])
            self.assertLessEqual(
                len(stages["purchase"]), len(stages["begin_checkout"]), key
            )
        self.assertGreater(purchases, 1_000)

    def test_non_paid_orders_never_credit_a_bot_click(self) -> None:
        self.assertGreater(_assert_no_order_credits_a_bot_click(self, self.records), 0)

    def test_ground_truth_counts_paid_sessions_and_attributed_orders(self) -> None:
        truth: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0, 0, 0])
        for r in self.records:
            if r.dataset == "ground_truth":
                truth[(str(r.payload["date"]), str(r.payload["campaign_id"]))] = [
                    int(r.payload["clicks"]), int(r.payload["sessions"]),
                    int(r.payload["orders"]), int(r.payload["gross_revenue_minor"]),
                ]
        events: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0, 0, 0])
        for r in self.starts.values():
            if _traffic_source(r) == "paid":
                key = (r.event_time.date().isoformat(), str(r.payload["campaign_id"]))
                events[key][0] += 1
                events[key][1] += 1
        created = {
            str(r.payload["event_id"]): r
            for r in self.records
            if r.dataset == "orders" and r.payload["event_name"] == "order_created"
        }
        for r in created.values():
            campaign_id = r.payload["payload"]["attributed_campaign_id"]
            if campaign_id is not None:
                key = (r.event_time.date().isoformat(), str(campaign_id))
                events[key][2] += 1
                events[key][3] += int(r.payload["payload"]["gross_amount_minor"])
        self.assertEqual(
            {k: v for k, v in truth.items() if any(v)},
            {k: v for k, v in events.items() if any(v)},
        )

    def test_non_paid_visits_use_fresh_unique_ids(self) -> None:
        session_days: dict[str, set[date]] = defaultdict(set)
        for r in self.records:
            if r.dataset == "web_events":
                session_days[str(r.payload["session_id"])].add(r.event_time.date())
        self.assertEqual({1}, {len(days) for days in session_days.values()})
        devices: dict[str, set[str]] = defaultdict(set)
        for session_id, r in self.starts.items():
            devices[str(r.payload["anonymous_id"])].add(session_id)
        non_paid = [r for r in self.starts.values() if self._non_paid(r)]
        for r in non_paid:
            self.assertEqual(
                {str(r.payload["session_id"])}, devices[str(r.payload["anonymous_id"])]
            )
        identified = sum(r.payload["customer_id"] is not None for r in non_paid)
        self.assertAlmostEqual(identified / len(non_paid), 0.7, delta=0.03)
        event_ids = [
            str(r.payload["event_id"])
            for r in self.records
            if r.dataset in ("web_events", "orders")
        ]
        payloads: dict[str, dict[str, object]] = {}
        for r in self.records:
            if r.dataset in ("web_events", "orders"):
                first = payloads.setdefault(str(r.payload["event_id"]), r.payload)
                self.assertEqual(first, r.payload)
        # Duplicate anomalies repeat an event with its id and payload.
        self.assertGreater(len(event_ids), len(payloads))
        # Order ids are unique across sessions and days.
        created_by_order: dict[str, set[str]] = defaultdict(set)
        for r in self.records:
            if r.dataset == "orders" and r.payload["event_name"] == "order_created":
                created_by_order[str(r.payload["payload"]["order_id"])].add(
                    str(r.payload["event_id"])
                )
        self.assertEqual({1}, {len(ids) for ids in created_by_order.values()})

    def test_non_paid_visits_convert_at_four_fifths_strength(self) -> None:
        stages: dict[str, set[str]] = defaultdict(set)
        for r in self.records:
            if r.dataset == "web_events" and self._non_paid(r):
                stages[str(r.payload["session_id"])].add(str(r.payload["event_name"]))
        purchases = sum("purchase" in names for names in stages.values())
        # Story funnel at strength 0.8: purchase cut-off per 1,000 scores.
        cutoff = round(
            400 * min(0.9, 0.625 * math.sqrt(0.8)) * min(0.9, 0.48 * 0.8)
        )
        self.assertEqual(86, cutoff)
        self.assertGreater(len(stages), 2_000)
        self.assertAlmostEqual(purchases / len(stages), cutoff / 1_000, delta=0.015)

    def test_non_paid_visits_follow_the_vietnam_day(self) -> None:
        starts = [r for r in self.starts.values() if self._non_paid(r)]
        hours = Counter(r.event_time.astimezone(VIETNAM).hour for r in starts)
        self.assertGreater(hours[2] + hours[3], 0)
        self.assertGreaterEqual(hours[20] + hours[21], 4 * (hours[2] + hours[3]))
        self.assertLessEqual(
            max(r.event_time.time() for r in starts),
            datetime(2025, 1, 1, 23, 57, 59).time(),
        )

    def test_days_generate_independently(self) -> None:
        masters = generate_master_data(self.config)
        day = list(iter_day_records(self.config, masters, 12))
        start = self.config.start_date + timedelta(days=12)
        expected = [
            r for r in self.records
            if r.dataset != "crm" and r.event_time.date() == start
            or r.dataset == "crm" and r.payload["snapshot_date"] == start.isoformat()
        ]
        self.assertEqual(
            [(r.dataset, r.event_time, r.payload) for r in expected],
            [(r.dataset, r.event_time, r.payload) for r in day],
        )

    def test_story_window_with_non_paid_visits_validates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "story"
            generate_dataset(resolve_config("story-demo", 360, root, days=9))
            report = validate_dataset(root)
        self.assertTrue(report.valid, report.errors[:5])

    def test_flat_has_no_traffic_source_or_non_paid_visits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_config("fixture", 360, Path(tmp))
        _, records = _generate_window(config)
        web_and_orders = [r for r in records if r.dataset in ("web_events", "orders")]
        self.assertGreater(len(web_and_orders), 0)
        for r in web_and_orders:
            self.assertNotIn("traffic_source", r.payload["payload"])
            self.assertIsNotNone(r.payload["click_id"])


def _assert_no_order_credits_a_bot_click(
    test: unittest.TestCase, records: list[SourceRecord]
) -> int:
    """Check no non-paid order credits a bot session's click; return credited count.

    Gold ``_unique_touches`` skips bot sessions, so the generator's credit
    must skip them too.
    """

    starts = _session_starts(records)
    bot_clicks = {
        str(r.payload["click_id"])
        for r in _page_views(records)
        if r.payload["payload"]["is_bot"]
    }
    credited = 0
    for r in records:
        if r.dataset != "orders" or r.payload["event_name"] != "order_created":
            continue
        if _traffic_source(starts[str(r.payload["session_id"])]) == "paid":
            continue
        click_id = r.payload["click_id"]
        if click_id is None:
            continue
        credited += 1
        test.assertNotIn(click_id, bot_clicks, r.payload)
    return credited


class StoryBotTouchTests(unittest.TestCase):
    """Non-paid orders never credit a bot session's click."""

    @classmethod
    def setUpClass(cls) -> None:
        from dataclasses import replace

        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_config("story-demo", 360, Path(tmp), days=14)
        # Frequent bots make a bot click the latest touch of some customer.
        cls.config = replace(config, anomalies=replace(config.anomalies, bot_session=0.25))
        _, cls.records = _generate_window(cls.config)

    def test_non_paid_orders_skip_bot_clicks(self) -> None:
        bot_sessions = sum(
            1 for r in _session_starts(self.records).values() if r.payload["payload"]["is_bot"]
        )
        self.assertGreater(bot_sessions, 100)
        self.assertGreater(_assert_no_order_credits_a_bot_click(self, self.records), 0)

    def test_non_paid_orders_credit_the_latest_non_bot_click(self) -> None:
        _assert_attribution_matches_sessions(self, self.records)


class StoryDeviceConversionTests(unittest.TestCase):
    """Devices convert differently in the story world: desktop > tablet > mobile."""

    DAYS = 30

    @classmethod
    def setUpClass(cls) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cls.config = resolve_config("story-demo", 360, Path(tmp), days=cls.DAYS)
        _, cls.records = _generate_window(cls.config)

    def test_purchase_rate_orders_desktop_tablet_mobile(self) -> None:
        device = {
            str(r.payload["session_id"]): str(r.payload["payload"]["device_type"])
            for r in _page_views(self.records)
        }
        sessions = Counter(device.values())
        purchases = Counter(
            device[str(r.payload["session_id"])]
            for r in self.records
            if r.dataset == "web_events" and r.payload["event_name"] == "purchase"
        )
        rate = {d: purchases[d] / sessions[d] for d in sessions}
        self.assertEqual({"desktop", "mobile", "tablet"}, set(rate))
        self.assertGreater(rate["desktop"], rate["tablet"])
        self.assertGreater(rate["tablet"], rate["mobile"])

    def test_device_multiplies_strength_in_story_only(self) -> None:
        from marketpulse_mock.journeys import _device_strength

        story = self.config.scenario
        self.assertEqual({"mobile": 0.85, "desktop": 1.15, "tablet": 1.0}, dict(story.device_strength))
        self.assertAlmostEqual(1.6 * 1.15, _device_strength(self.config, "desktop", 1.6))
        self.assertAlmostEqual(1.6 * 0.85, _device_strength(self.config, "mobile", 1.6))
        self.assertAlmostEqual(1.6, _device_strength(self.config, "tablet", 1.6))
        with tempfile.TemporaryDirectory() as tmp:
            flat = resolve_config("fixture", 360, Path(tmp))
        for device in ("desktop", "mobile", "tablet"):
            self.assertEqual(1.6, _device_strength(flat, device, 1.6))


class FlatChannelHelperTests(unittest.TestCase):
    """The legacy ``flat`` scenario keeps round-robin, uniform-cost behaviour."""

    def test_flat_helpers_match_legacy_behaviour(self) -> None:
        from marketpulse_mock.journeys import (
            _click_threshold,
            _funnel_steps,
            _impression_cost,
            _pick_campaign,
        )
        from marketpulse_mock.ids import stable_int

        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_config("fixture", 360, Path(tmp))
        masters = generate_master_data(config)
        campaigns = masters.campaigns
        for ordinal in range(3 * len(campaigns)):
            campaign = _pick_campaign(config, masters, 1, ordinal)
            self.assertIs(campaigns[ordinal % len(campaigns)], campaign)
            self.assertEqual(
                100 + stable_int(360, "impression-cost", 1, ordinal, modulo=901),
                _impression_cost(config, campaign, 1, ordinal),
            )
        legacy = {"awareness": 45, "traffic": 90, "conversions": 70}
        for campaign in campaigns:
            self.assertEqual(
                legacy[campaign.objective], _click_threshold(config, campaign)
            )
        for score in range(1_000):
            self.assertEqual(
                (score < 850, score < 400, score < 250, score < 120),
                _funnel_steps(config, campaigns[0], score, 1.0),
            )


class StoryEmailTypeTests(unittest.TestCase):
    """Story emails carry their lifecycle type; rates match the targets."""

    TARGETS = {
        "welcome": (55.0, 12.0),
        "newsletter": (28.0, 2.5),
        "promo": (32.0, 4.5),
    }

    @classmethod
    def setUpClass(cls) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_config("story-demo", 360, Path(tmp), days=90)
        masters = generate_master_data(config)
        cls.records = []
        for day_index in range(config.profile.days):
            day_start = datetime.combine(
                config.start_date + timedelta(days=day_index),
                datetime.min.time(),
                tzinfo=timezone.utc,
            )
            cls.records.extend(_iter_email_events(config, masters, day_index, day_start))

    def test_every_story_email_event_carries_its_type(self) -> None:
        self.assertTrue(self.records)
        by_message: dict[str, set[object]] = defaultdict(set)
        for record in self.records:
            payload = record.payload["payload"]
            self.assertIn(payload["email_type"], self.TARGETS)
            self.assertRegex(
                payload["lifecycle_campaign_id"],
                rf"^LC-{payload['email_type']}-\d{{8}}$",
            )
            self.assertEqual(
                record.event_time.strftime("%Y%m%d"),
                payload["lifecycle_campaign_id"][-8:],
            )
            by_message[payload["message_id"]].add(payload["email_type"])
        self.assertTrue(all(len(types) == 1 for types in by_message.values()))
        self.assertEqual(set(self.TARGETS), {t for v in by_message.values() for t in v})

    def _assert_rates(
        self,
        email_type: str,
        delivered: int,
        opened: int,
        clicked: int,
        *,
        click_delta: float = 5.0,
        standard_errors: float | None = None,
    ) -> None:
        open_target, click_target = self.TARGETS[email_type]
        self.assertGreater(delivered, 0, email_type)
        msg = (
            f"{email_type}: delivered={delivered} "
            f"open={100 * opened / delivered:.2f} "
            f"click={100 * clicked / delivered:.2f}"
        )
        for observed, target, delta in (
            (100 * opened / delivered, open_target, 5.0),
            (100 * clicked / delivered, click_target, click_delta),
        ):
            self.assertAlmostEqual(target, observed, delta=delta, msg=msg)
            if standard_errors is not None:
                p = target / 100
                se = 100 * math.sqrt(p * (1 - p) / delivered)
                self.assertLessEqual(
                    abs(observed - target), standard_errors * se, msg
                )

    def test_newsletter_and_promo_rates_match_targets(self) -> None:
        delivered: Counter[str] = Counter()
        opened: Counter[str] = Counter()
        clicked: Counter[str] = Counter()
        for record in self.records:
            email_type = record.payload["payload"]["email_type"]
            name = record.payload["event_name"]
            delivered[email_type] += name == "email_delivered"
            opened[email_type] += name == "email_opened"
            clicked[email_type] += name == "email_clicked"
        for email_type in ("newsletter", "promo"):
            # n is ~12k / ~6k, so clicks must land within 1 point: a rate of
            # zero (or double) cannot hide inside the +-5 outer bound.
            self._assert_rates(
                email_type,
                delivered[email_type],
                opened[email_type],
                clicked[email_type],
                click_delta=1.0,
            )

    def test_welcome_rates_match_targets_over_the_full_window(self) -> None:
        # Welcome volume is ~50 a month, so a 90-day sample (n~160, one
        # standard error ~4 points) cannot judge +-5. The full story window
        # (n~1600) can; the schedule alone carries the outcomes.
        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_config("story-demo", 360, Path(tmp))
        schedule = EmailSchedule.build(config, generate_master_data(config))
        sends = [
            send
            for offset in range(config.profile.days)
            for send in schedule.sends_on(
                config.start_date + timedelta(days=offset)
            )
            if send.email_type == "welcome"
        ]
        self._assert_rates(
            "welcome",
            len(sends),
            sum(send.opened for send in sends),
            sum(send.clicked for send in sends),
            standard_errors=4.0,
        )


EMAIL_VISIT_WINDOW = timedelta(hours=2)
DOUBLE_DAYS_2025 = (0, 32, 61)  # UTC 2025-01-01, 02-02 and 03-03: promo days


def _email_clicks(records: list[SourceRecord]) -> dict[tuple[str, date], list[datetime]]:
    """Return each ``(customer, UTC day)``'s distinct ``email_clicked`` times."""

    seen: set[str] = set()
    clicks: dict[tuple[str, date], list[datetime]] = defaultdict(list)
    for r in records:
        if r.dataset != "email_events" or r.payload["event_name"] != "email_clicked":
            continue
        event_id = str(r.payload["event_id"])
        if event_id in seen:
            continue  # a duplicate anomaly repeats the click
        seen.add(event_id)
        clicks[(str(r.payload["customer_id"]), r.event_time.date())].append(r.event_time)
    return clicks


class StoryEmailReferredVisitTests(unittest.TestCase):
    """Every story email click sends its recipient back to the store within 2 h."""

    DAYS = 30

    @classmethod
    def setUpClass(cls) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cls.config = resolve_config("story-demo", 360, Path(tmp), days=cls.DAYS)
        cls.masters = generate_master_data(cls.config)
        cls.days = _generate_days(cls.config, cls.masters, range(cls.DAYS))
        cls.records = [r for i in range(cls.DAYS) for r in cls.days[i]]
        cls.starts = _session_starts(cls.records)
        cls.sources = {
            session_id: _traffic_source(r) for session_id, r in cls.starts.items()
        }
        cls.email_starts = [
            r for r in cls.starts.values() if _traffic_source(r) == "email"
        ]
        cls.customer_index = {
            c.customer_id: index for index, c in enumerate(cls.masters.customers)
        }

    def _session_records(self, source: str) -> list[SourceRecord]:
        return [
            r
            for r in self.records
            if r.dataset in ("web_events", "orders")
            and self.sources[str(r.payload["session_id"])] == source
        ]

    def test_each_email_click_starts_one_session_for_its_recipient(self) -> None:
        clicks = _email_clicks(self.records)
        sessions: Counter[tuple[str, date]] = Counter()
        for r in self.email_starts:
            customer_id = _customer_id(r)
            self.assertIsNotNone(customer_id, r.payload)
            key = (str(customer_id), r.event_time.date())
            sessions[key] += 1
            earlier = [
                click
                for click in clicks.get(key, ())
                if timedelta(0) <= r.event_time - click <= EMAIL_VISIT_WINDOW
            ]
            self.assertTrue(earlier, (key, r.event_time, clicks.get(key)))
        self.assertEqual(
            Counter({key: len(times) for key, times in clicks.items()}), sessions
        )
        self.assertGreater(len(self.email_starts), 200)

    def test_email_sessions_carry_no_campaign_or_click(self) -> None:
        paid_clicks = {
            str(r.payload["click_id"])
            for r in self.starts.values()
            if _traffic_source(r) == "paid"
        }
        records = self._session_records("email")
        self.assertGreater(len(records), 500)
        for r in records:
            self.assertEqual("email", _traffic_source(r), r.payload)
            self.assertIsNotNone(r.payload["customer_id"], r.payload)
            if r.dataset == "orders":
                # The order may credit an earlier paid click, never the email.
                click_id = r.payload["click_id"]
                self.assertTrue(click_id is None or click_id in paid_clicks, r.payload)
                continue
            for field in ("campaign_id", "platform_campaign_id", "click_id"):
                self.assertIsNone(r.payload[field], (field, r.payload))
            self.assertFalse(r.payload["payload"]["is_bot"], r.payload)
            if r.payload["event_name"] == "page_view":
                self.assertEqual(
                    "https://shop.marketpulse.example.test/",
                    r.payload["payload"]["page_url"],
                )
        quality_sessions = {
            str(r.payload["record"].get("session_id"))
            for r in self.records
            if r.dataset == "quality_cases"
        }
        self.assertFalse(quality_sessions & {str(r.payload["session_id"]) for r in records})

    def test_email_sessions_use_their_own_fresh_ids(self) -> None:
        first = first_email_ordinal(self.config)
        seed = self.config.seed
        email_message_sessions = {
            str(r.payload["session_id"])
            for r in self.records
            if r.dataset == "email_events"
        }
        for r in self.email_starts:
            day_index = (r.event_time.date() - self.config.start_date).days
            ordinal = first + self.customer_index[str(r.payload["customer_id"])]
            self.assertEqual(
                stable_id(seed, f"session-{day_index}", ordinal), r.payload["session_id"]
            )
            self.assertEqual(
                stable_id(seed, f"anonymous-{day_index}", ordinal),
                r.payload["anonymous_id"],
            )
            self.assertEqual(
                stable_id(seed, f"web_events-page_view-email-{day_index}", ordinal * 10),
                r.payload["event_id"],
            )
            self.assertNotIn(r.payload["session_id"], email_message_sessions)

        def ids(source: str, field: str) -> set[str]:
            return {str(r.payload[field]) for r in self._session_records(source)}

        for field in ("session_id", "anonymous_id", "event_id"):
            email = ids("email", field)
            self.assertTrue(email, field)
            for other in ("organic", "direct", "paid"):
                with self.subTest(field=field, other=other):
                    self.assertFalse(email & ids(other, field))
        devices: dict[str, set[str]] = defaultdict(set)
        session_days: dict[str, set[date]] = defaultdict(set)
        for r in self._session_records("email"):
            devices[str(r.payload["anonymous_id"])].add(str(r.payload["session_id"]))
            session_days[str(r.payload["session_id"])].add(r.event_time.date())
        self.assertEqual({1}, {len(sessions) for sessions in devices.values()})
        self.assertEqual({1}, {len(days) for days in session_days.values()})

    def test_email_ordinals_sit_above_every_non_paid_ordinal(self) -> None:
        from marketpulse_mock.journeys import _non_paid_count, _paid_day

        span = first_email_ordinal(self.config) - first_non_paid_ordinal(self.config)
        ceiling = impression_capacity(self.config) + self.config.profile.customers
        # A day's paid sessions never exceed impressions plus joiners.
        self.assertGreaterEqual(span, _non_paid_count(ceiling))
        for day_index in range(self.DAYS):
            paid = _paid_day(self.config, self.masters, day_index)
            self.assertLessEqual(_non_paid_count(len(paid.session_ordinals)), span)

    def test_email_visits_convert_at_six_fifths_strength(self) -> None:
        from marketpulse_mock.ids import stable_int
        from marketpulse_mock.journeys import _device_strength, _funnel_steps

        first = first_email_ordinal(self.config)
        purchased = {
            str(r.payload["session_id"])
            for r in self.records
            if r.dataset == "web_events" and r.payload["event_name"] == "purchase"
        }
        for r in self.email_starts:
            day_index = (r.event_time.date() - self.config.start_date).days
            ordinal = first + self.customer_index[str(r.payload["customer_id"])]
            score = stable_int(self.config.seed, "funnel", day_index, ordinal, modulo=1_000)
            strength = _device_strength(
                self.config, str(r.payload["payload"]["device_type"]), 1.2
            )
            expected = _funnel_steps(self.config, None, score, strength)[3]
            self.assertEqual(expected, str(r.payload["session_id"]) in purchased, r.payload)
        email_rate = sum(
            str(r.payload["session_id"]) in purchased for r in self.email_starts
        ) / len(self.email_starts)
        organic = [r for r in self.starts.values() if _traffic_source(r) == "organic"]
        organic_rate = sum(
            str(r.payload["session_id"]) in purchased for r in organic
        ) / len(organic)
        self.assertGreater(email_rate, organic_rate)

    def test_email_orders_follow_the_paid_click_policy(self) -> None:
        _assert_attribution_matches_sessions(self, self.records)
        orders = [
            r
            for r in self._session_records("email")
            if r.dataset == "orders" and r.payload["event_name"] == "order_created"
        ]
        self.assertGreater(len(orders), 50)
        for r in orders:
            payload = r.payload["payload"]
            self.assertEqual(
                payload["attributed_campaign_id"] is None, r.payload["click_id"] is None
            )
            self.assertEqual(
                payload["attributed_campaign_id"], r.payload["campaign_id"], r.payload
            )

    def test_email_sessions_stay_inside_their_utc_day(self) -> None:
        latest = max(r.event_time for r in self.email_starts)
        # Sends end by 03:00, opens follow within 8 h, clicks within 30 min
        # and visits within 2 h: every email visit starts before 14:00 UTC.
        self.assertLess(latest.time(), datetime(2025, 1, 1, 14).time())
        session_end = max(r.event_time for r in self._session_records("email"))
        self.assertLess(session_end.time(), datetime(2025, 1, 1, 23, 58).time())
        for day_index, records in self.days.items():
            day = self.config.start_date + timedelta(days=day_index)
            last: dict[str, datetime] = {}
            for r in records:
                self.assertEqual(day, r.event_time.date(), (r.dataset, r.event_time))
                previous = last.get(r.dataset)
                if previous is not None:
                    self.assertLessEqual(previous, r.event_time, (day, r.dataset))
                last[r.dataset] = r.event_time

    def test_an_email_visit_leaving_its_utc_day_raises(self) -> None:
        from unittest import mock

        from marketpulse_mock import session_timing

        self.assertTrue(session_timing._email_timeline(self.config, self.masters, 13))
        with mock.patch.object(session_timing, "EMAIL_VISIT_END", timedelta(hours=1)):
            with self.assertRaises(AssertionError):
                session_timing._email_timeline(self.config, self.masters, 13)

    def test_a_day_generated_alone_equals_its_sequential_run(self) -> None:
        day_index = 13  # 2025-01-14, a newsletter Tuesday
        alone = list(
            iter_day_records(self.config, generate_master_data(self.config), day_index)
        )
        self.assertTrue(
            any(
                _traffic_source(r) == "email"
                for r in alone
                if r.dataset == "web_events"
            )
        )
        self.assertEqual(
            [(r.dataset, r.event_time, r.payload) for r in self.days[day_index]],
            [(r.dataset, r.event_time, r.payload) for r in alone],
        )

    def test_replay_reader_keeps_event_time_order(self) -> None:
        from marketpulse_mock.replay_reader import iter_replay_records

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "story"
            generate_dataset(resolve_config("story-demo", 360, root, days=self.DAYS))
            previous: datetime | None = None
            email_visits = 0
            for record in iter_replay_records(root):
                if previous is not None:
                    self.assertLessEqual(previous, record.event_time, record.source_file)
                previous = record.event_time
                if record.dataset == "web_events":
                    value = json.loads(record.value)
                    email_visits += value["payload"].get("traffic_source") == "email"
        self.assertGreater(email_visits, 500)


class StoryPromoDayEmailShareTests(unittest.TestCase):
    """Email visits are a minority of traffic that rises on promo days.

    With the benchmark rates (promo 32 % open / 4.5 % click) a few thousand
    subscribers send about 50 visits on an early-2025 promo day against
    ~800 sessions, about 6 %; the share grows with the audience.
    """

    DAYS = 90

    @classmethod
    def setUpClass(cls) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_config("story-demo", 360, Path(tmp), days=cls.DAYS)
        masters = generate_master_data(config)
        cls.sources: dict[date, Counter[object]] = {}
        for day_index in range(cls.DAYS):
            records = list(iter_day_records(config, masters, day_index))
            cls.sources[config.start_date + timedelta(days=day_index)] = Counter(
                _traffic_source(r) for r in _session_starts(records).values()
            )
        cls.double_days = [
            config.start_date + timedelta(days=i) for i in DOUBLE_DAYS_2025
        ]

    @staticmethod
    def _share(sources: Counter[object]) -> float:
        return sources["email"] / sum(sources.values())

    def test_email_visits_are_3_to_35_percent_of_double_day_sessions(self) -> None:
        totals: Counter[object] = sum(
            (self.sources[day] for day in self.double_days), Counter()
        )
        share = self._share(totals)
        msg = f"double-day email share {share:.2%} ({dict(totals)})"
        self.assertGreaterEqual(share, 0.03, msg)
        self.assertLessEqual(share, 0.35, msg)
        for day in self.double_days:
            with self.subTest(day=day):
                self.assertGreater(self.sources[day]["email"], 0, self.sources[day])

    def test_promo_days_have_a_higher_email_share_than_plain_days(self) -> None:
        from marketpulse_mock.calendar import is_promo_day

        promo = [self._share(c) for d, c in self.sources.items() if is_promo_day(d)]
        plain = [self._share(c) for d, c in self.sources.items() if not is_promo_day(d)]
        self.assertTrue(promo and plain)
        promo_mean, plain_mean = sum(promo) / len(promo), sum(plain) / len(plain)
        self.assertGreater(
            promo_mean,
            plain_mean,
            f"promo mean {promo_mean:.2%} over {len(promo)} days, "
            f"plain mean {plain_mean:.2%} over {len(plain)} days",
        )


if __name__ == "__main__":
    unittest.main()
