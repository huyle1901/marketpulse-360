from __future__ import annotations

import time
import unittest
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from marketpulse_mock.calendar import VIETNAM, is_promo_day
from marketpulse_mock.config import resolve_config
from marketpulse_mock.email_schedule import EmailSchedule, EmailSend
from marketpulse_mock.journeys import iter_day_records
from marketpulse_mock.masters import generate_master_data


def _story(days: int | None = None):
    config = resolve_config("story-demo", 360, Path("unused"), days=days)
    return config, generate_master_data(config)


def _window(config) -> list[date]:
    return [
        config.start_date + timedelta(days=offset)
        for offset in range(config.profile.days)
    ]


class EmailScheduleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config, cls.masters = _story()
        cls.schedule = EmailSchedule.build(cls.config, cls.masters)
        cls.days = _window(cls.config)
        cls.by_customer: dict[str, list[tuple[date, EmailSend]]] = defaultdict(list)
        for day in cls.days:
            for send in cls.schedule.sends_on(day):
                cls.by_customer[send.customer.customer_id].append((day, send))

    def test_audience_grows_over_the_story(self) -> None:
        first = sum(len(self.schedule.sends_on(day)) for day in self.days[:30])
        last = sum(len(self.schedule.sends_on(day)) for day in self.days[-30:])
        self.assertGreater(first, 0)
        self.assertGreater(last, first)

    def test_each_in_window_consented_joiner_gets_one_welcome_on_join_day(self) -> None:
        start, end = self.days[0], self.days[-1]
        joiners = [
            customer
            for customer in self.masters.customers
            if customer.consent_marketing
            and start <= customer.created_at.date() <= end
        ]
        self.assertTrue(joiners)
        for customer in joiners:
            welcomes = [
                day
                for day, send in self.by_customer[customer.customer_id]
                if send.email_type == "welcome"
            ]
            self.assertEqual([customer.created_at.date()], welcomes, customer)

    def test_customers_who_joined_before_the_window_get_no_welcome(self) -> None:
        start = self.days[0]
        early = {
            customer.customer_id
            for customer in self.masters.customers
            if customer.created_at.date() < start
        }
        self.assertTrue(early)
        welcomed_early = [
            send
            for sends in self.by_customer.values()
            for _day, send in sends
            if send.email_type == "welcome" and send.customer.customer_id in early
        ]
        self.assertEqual([], welcomed_early)

    def test_no_send_before_joining_or_after_unsubscribing(self) -> None:
        unsubscribed = 0
        for customer_id, sends in self.by_customer.items():
            customer = sends[0][1].customer
            unsubscribed_on = self.schedule.unsubscribed_on(customer_id)
            for day, send in sends:
                self.assertGreaterEqual(day, customer.created_at.date())
                if unsubscribed_on is not None:
                    self.assertLessEqual(day, unsubscribed_on)
            flagged = [day for day, send in sends if send.unsubscribed]
            self.assertEqual(
                [] if unsubscribed_on is None else [unsubscribed_on], flagged
            )
            unsubscribed += unsubscribed_on is not None
        self.assertGreater(unsubscribed, 0)

    def test_a_welcome_unsubscriber_gets_no_newsletter_later(self) -> None:
        welcome_quitters = [
            sends
            for sends in self.by_customer.values()
            if any(s.email_type == "welcome" and s.unsubscribed for _d, s in sends)
        ]
        self.assertTrue(welcome_quitters)
        for sends in welcome_quitters:
            self.assertEqual(["welcome"], [s.email_type for _d, s in sends])

    def test_at_most_one_email_per_customer_per_day(self) -> None:
        for day in self.days:
            ids = Counter(s.customer.customer_id for s in self.schedule.sends_on(day))
            self.assertEqual([], [cid for cid, n in ids.items() if n > 1], day)

    def test_double_day_tuesday_sends_promos_and_welcomes_only(self) -> None:
        singles_day = date(2025, 11, 11)
        self.assertEqual(1, singles_day.weekday())
        types = Counter(s.email_type for s in self.schedule.sends_on(singles_day))
        self.assertGreater(types["promo"], 0)
        self.assertLessEqual(set(types), {"promo", "welcome"})

    def test_newsletters_on_vietnam_tuesdays_and_promos_on_sale_days(self) -> None:
        seen = Counter()
        for day in self.days:
            # Sends go out 08:00-10:00 Vietnam time, so the UTC day is the
            # Vietnam date.
            vn_date = datetime.combine(
                day, datetime.min.time(), tzinfo=timezone.utc
            ).replace(hour=1).astimezone(VIETNAM).date()
            for send in self.schedule.sends_on(day):
                seen[send.email_type] += 1
                if send.email_type == "newsletter":
                    self.assertEqual(1, vn_date.weekday(), day)
                    self.assertFalse(is_promo_day(vn_date), day)
                elif send.email_type == "promo":
                    self.assertTrue(is_promo_day(vn_date), day)
                else:
                    self.assertEqual(send.customer.created_at.date(), day)
        self.assertEqual({"welcome", "newsletter", "promo"}, set(seen))

    def test_non_consented_customers_never_appear(self) -> None:
        refused = {
            customer.customer_id
            for customer in self.masters.customers
            if not customer.consent_marketing
        }
        self.assertTrue(refused)
        self.assertEqual(set(), refused & set(self.by_customer))

    def test_a_click_is_always_an_open(self) -> None:
        clicks = 0
        for sends in self.by_customer.values():
            for _day, send in sends:
                if send.clicked:
                    clicks += 1
                    self.assertTrue(send.opened)
        self.assertGreater(clicks, 0)

    def test_a_shorter_window_is_a_prefix_of_the_full_schedule(self) -> None:
        config, masters = _story(days=60)
        short = EmailSchedule.build(config, masters)
        for day in _window(config):
            self.assertEqual(
                [(s.customer.customer_id, s.email_type) for s in short.sends_on(day)],
                [
                    (s.customer.customer_id, s.email_type)
                    for s in self.schedule.sends_on(day)
                ],
            )

    def test_build_is_cached_for_the_same_masters_only(self) -> None:
        self.assertIs(self.schedule, EmailSchedule.build(self.config, self.masters))
        other_masters = generate_master_data(self.config)
        self.assertIsNot(
            self.schedule, EmailSchedule.build(self.config, other_masters)
        )

    def test_a_year_schedule_builds_in_under_five_seconds(self) -> None:
        config, masters = _story(days=365)
        started = time.perf_counter()
        schedule = EmailSchedule.build(config, masters)
        elapsed = time.perf_counter() - started
        self.assertTrue(schedule.sends_on(config.start_date))
        self.assertLess(elapsed, 5.0)


class StoryEmailEventTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config, cls.masters = _story(days=30)
        cls.schedule = EmailSchedule.build(cls.config, cls.masters)
        cls.email_days: list[list] = []
        cls.crm: dict[tuple[str, str], bool] = {}
        for day_index in range(cls.config.profile.days):
            emails = []
            for record in iter_day_records(cls.config, cls.masters, day_index):
                if record.dataset == "email_events":
                    emails.append(record)
                elif record.dataset == "crm":
                    cls.crm[
                        (record.payload["snapshot_date"], record.payload["customer_id"])
                    ] = record.payload["consent_marketing"]
            cls.email_days.append(emails)

    def test_each_scheduled_send_is_emitted_once(self) -> None:
        for day_index, records in enumerate(self.email_days):
            day = self.config.start_date + timedelta(days=day_index)
            sent = {
                record.payload["event_id"]: record.payload["customer_id"]
                for record in records
                if record.payload["event_name"] == "email_sent"
            }
            self.assertEqual(
                sorted(s.customer.customer_id for s in self.schedule.sends_on(day)),
                sorted(sent.values()),
                day,
            )

    def test_day_email_records_are_sorted_and_inside_the_day(self) -> None:
        for day_index, records in enumerate(self.email_days):
            day_start = datetime.combine(
                self.config.start_date + timedelta(days=day_index),
                datetime.min.time(),
                tzinfo=timezone.utc,
            )
            times = [record.event_time for record in records]
            self.assertEqual(sorted(times), times, day_index)
            for event_time in times:
                self.assertGreaterEqual(event_time, day_start)
                self.assertLess(event_time, day_start + timedelta(days=1))

    def test_sends_go_out_between_eight_and_ten_vietnam_time(self) -> None:
        sends = [
            record
            for records in self.email_days
            for record in records
            if record.payload["event_name"] == "email_sent"
        ]
        self.assertTrue(sends)
        for record in sends:
            local = record.event_time.astimezone(VIETNAM)
            self.assertIn(local.hour, (8, 9), record.event_time)
            self.assertEqual(record.event_time.date(), local.date())

    def test_message_events_follow_the_send_in_order(self) -> None:
        messages: dict[str, dict[str, datetime]] = defaultdict(dict)
        for records in self.email_days:
            for record in records:
                messages[record.payload["payload"]["message_id"]][
                    record.payload["event_name"]
                ] = record.event_time
        opened = clicked = unsubscribed = 0
        for times in messages.values():
            sent = times["email_sent"]
            delivered = times["email_delivered"]
            self.assertLess(sent, delivered)
            self.assertLessEqual(delivered - sent, timedelta(minutes=1))
            if "email_opened" in times:
                opened += 1
                self.assertGreaterEqual(times["email_opened"], delivered)
                self.assertLessEqual(
                    times["email_opened"] - delivered, timedelta(hours=8)
                )
            if "email_clicked" in times:
                clicked += 1
                self.assertIn("email_opened", times)
                self.assertGreaterEqual(times["email_clicked"], times["email_opened"])
                self.assertLessEqual(
                    times["email_clicked"] - times["email_opened"],
                    timedelta(minutes=30),
                )
            if "email_unsubscribed" in times:
                unsubscribed += 1
                last = max(t for name, t in times.items() if name != "email_unsubscribed")
                self.assertGreater(times["email_unsubscribed"], last)
                self.assertLessEqual(
                    times["email_unsubscribed"] - last, timedelta(minutes=10)
                )
        self.assertGreater(opened, 0)
        self.assertGreater(clicked, 0)

    def test_crm_consent_turns_off_the_day_after_an_unsubscribe(self) -> None:
        start = self.config.start_date
        checked = 0
        for customer in self.masters.customers:
            unsubscribed_on = self.schedule.unsubscribed_on(customer.customer_id)
            for day_index in range(self.config.profile.days):
                day = start + timedelta(days=day_index)
                key = (day.isoformat(), customer.customer_id)
                if key not in self.crm:
                    continue
                expected = customer.consent_marketing and not (
                    unsubscribed_on is not None and unsubscribed_on < day
                )
                self.assertEqual(expected, self.crm[key], key)
                if unsubscribed_on is not None and day > unsubscribed_on:
                    checked += 1
        self.assertGreater(checked, 0)


if __name__ == "__main__":
    unittest.main()
