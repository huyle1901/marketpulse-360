#!/usr/bin/env python3
"""Compare a certified story-demo release with the generator files it came from.

Ungoverned measures (impressions, clicks, spend by platform and month) must equal the
generator's ground truth exactly. Governed measures (orders, gross, refunds, revenue by
city) are compared with an independent governed computation, and the raw-versus-governed
gap is reported with its reason. Money is integer minor units throughout.

Usage:
    PYTHONPATH=src .venv/bin/python scripts/story_demo_oracle.py \
        --root data/generated/story-demo-v2 --clickhouse localhost:48213 --through 2026-08-31
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
from collections import defaultdict
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field, fields
from datetime import date
from pathlib import Path
from typing import Any

Key = tuple[str, ...]
Measure = dict[Key, int]

GOVERNED = frozenset({
    "orders", "gross_minor", "refunds_minor", "city_gross_minor", "platform_orders", "platform_gross_minor",
    "sessions", "email_sends",
})
"""Measures the certified release governs (consent, bots, identity)."""

UNEXPLAINED = "unexplained raw−governed gap"


@dataclass(frozen=True)
class Totals:
    """Integer totals keyed by dimension tuples; an empty measure is "not available"."""

    impressions: Measure = field(default_factory=dict)
    clicks: Measure = field(default_factory=dict)
    spend_minor: Measure = field(default_factory=dict)
    orders: Measure = field(default_factory=dict)
    gross_minor: Measure = field(default_factory=dict)
    refunds_minor: Measure = field(default_factory=dict)
    city_gross_minor: Measure = field(default_factory=dict)
    platform_orders: Measure = field(default_factory=dict)
    platform_gross_minor: Measure = field(default_factory=dict)
    sessions: Measure = field(default_factory=dict)
    email_sends: Measure = field(default_factory=dict)
    distinct_orders: int = 0

    def __post_init__(self) -> None:
        for name in measure_names():
            for key, value in getattr(self, name).items():
                if type(value) is not int:
                    raise TypeError(f"{name} {key} must be an integer, got {value!r}")


@dataclass(frozen=True)
class Check:
    name: str
    expected: int
    actual: int
    equal: bool
    reason: str | None = None
    raw: int | None = None


def measure_names() -> tuple[str, ...]:
    return tuple(f.name for f in fields(Totals) if f.name != "distinct_orders")


def compare(
    truth: Totals,
    certified: Totals,
    *,
    governed: Totals | None = None,
    reasons: Mapping[str, str] | None = None,
) -> list[Check]:
    """Return one check per key of every measure both sides provide."""

    checks: list[Check] = []
    for name in measure_names():
        raw: Measure = getattr(truth, name)
        actual_values: Measure = getattr(certified, name)
        if not raw or not actual_values:
            continue
        is_governed = name in GOVERNED and governed is not None
        expected_values: Measure = getattr(governed, name) if is_governed else raw
        for key in sorted(set(raw) | set(expected_values) | set(actual_values)):
            expected = expected_values.get(key, 0)
            actual = actual_values.get(key, 0)
            reason = None
            raw_value = None
            if is_governed:
                raw_value = raw.get(key, 0)
                if raw_value != expected:
                    reason = (reasons or {}).get(name, UNEXPLAINED)
            checks.append(
                Check(" ".join((name, *key)), expected, actual, expected == actual, reason, raw_value)
            )
    return checks


def uncompared(truth: Totals, certified: Totals) -> tuple[str, ...]:
    """Measures the generator provides but the certified side does not serve."""

    return tuple(
        name for name in measure_names() if getattr(truth, name) and not getattr(certified, name)
    )


def markdown(checks: list[Check]) -> str:
    lines = ["| Check | Expected | Certified | Equal | Reason |", "| --- | ---: | ---: | --- | --- |"]
    for check in checks:
        reason = check.reason or ""
        if check.raw is not None and check.raw != check.expected:
            reason = f"raw {check.raw:,}: {reason}"
        lines.append(
            f"| {check.name} | {check.expected:,} | {check.actual:,} | "
            f"{'yes' if check.equal else 'NO'} | {reason} |"
        )
    return "\n".join(lines)


def _partitions(root: Path, dataset: str, through: date | None) -> Iterator[Path]:
    for directory in sorted((root / dataset).glob("date=*")):
        if through is None or date.fromisoformat(directory.name[5:]) <= through:
            yield from sorted(directory.glob("*.gz"))


def _jsonl(root: Path, dataset: str, through: date | None) -> Iterator[dict[str, Any]]:
    for path in _partitions(root, dataset, through):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def _unique_events(root: Path, dataset: str, through: date | None) -> Iterator[dict[str, Any]]:
    """Events once each: injected duplicates repeat an event id."""

    seen: set[str] = set()
    for event in _jsonl(root, dataset, through):
        if event["event_id"] not in seen:
            seen.add(event["event_id"])
            yield event


def _month(timestamp: str) -> str:
    return timestamp[:7]


def ground_truth_totals(root: Path, *, through: date | None = None) -> Totals:
    """Raw totals from the generator files, days ``<= through`` when given."""

    return _file_totals(root, through, consented_only=False)


def governed_totals(root: Path, *, through: date | None = None) -> Totals:
    """Totals under the release's governance, computed from the files alone.

    Silver drops events whose ``consent_analytics`` is false (CONSENT_DENIED); bot
    exclusion applies to web/app events only and late events stay eligible, so for
    commerce the governed orders are exactly the consented ones.
    """

    return _file_totals(root, through, consented_only=True)


def consent_denied_orders(root: Path, *, through: date | None = None) -> int:
    """Distinct orders whose order event withholds analytics consent."""

    denied: set[str] = set()
    for event in _unique_events(root, "orders", through):
        if event["event_name"] == "order_created" and event["consent_analytics"] is not True:
            denied.add(event["payload"]["order_id"])
    return len(denied)


def _file_totals(root: Path, through: date | None, *, consented_only: bool) -> Totals:
    impressions: Measure = defaultdict(int)
    clicks: Measure = defaultdict(int)
    spend: Measure = defaultdict(int)
    for row in _jsonl(root, "ground_truth", through):
        key = (row["platform"], _month(row["date"]))
        impressions[key] += int(row["impressions"])
        clicks[key] += int(row["clicks"])
        spend[key] += int(row["spend_minor"])

    with gzip.open(root / "master" / "customers.csv.gz", "rt", encoding="utf-8") as handle:
        city_of = {row["customer_id"]: row["city"] for row in csv.DictReader(handle)}

    orders: Measure = defaultdict(int)
    gross: Measure = defaultdict(int)
    refunds: Measure = defaultdict(int)
    city_gross: Measure = defaultdict(int)
    platform_orders: Measure = defaultdict(int)
    platform_gross: Measure = defaultdict(int)
    created: set[str] = set()
    refunded: set[str] = set()
    for event in _unique_events(root, "orders", through):
        if consented_only and event["consent_analytics"] is not True:
            continue
        payload = event["payload"]
        order_id = payload["order_id"]
        month = (_month(event["event_time"]),)
        if event["event_name"] == "order_created" and order_id not in created:
            created.add(order_id)
            amount = int(payload["gross_amount_minor"])
            orders[month] += 1
            gross[month] += amount
            customer = event.get("customer_id")
            city = city_of.get(customer, "unknown") if customer else "anonymous"
            city_gross[(city,)] += amount
            if platform := payload.get("attributed_platform"):
                platform_orders[(platform,)] += 1
                platform_gross[(platform,)] += amount
        elif event["event_name"] == "order_refunded" and order_id not in refunded:
            refunded.add(order_id)
            refunds[month] += int(payload["refund_amount_minor"])

    web = list(_unique_events(root, "web_events", through))
    # Silver drops every event of a bot session (web/app only) and consent-denied events.
    bot_sessions = {e["session_id"] for e in web if e["payload"].get("is_bot") is True}
    sessions: dict[str, set[str]] = defaultdict(set)
    for event in web:
        if event["event_name"] != "page_view":
            continue
        if consented_only and (event["consent_analytics"] is not True or event["session_id"] in bot_sessions):
            continue
        source = event["payload"].get("traffic_source") or "paid"
        sessions[source].add(event["session_id"])

    email_sends: Measure = defaultdict(int)
    for event in _unique_events(root, "email_events", through):
        if event["event_name"] == "email_sent" and (not consented_only or event["consent_analytics"] is True):
            email_type = event["payload"].get("email_type") or "untyped"
            email_sends[(email_type, _month(event["event_time"]))] += 1

    return Totals(
        impressions=dict(impressions),
        clicks=dict(clicks),
        spend_minor=dict(spend),
        orders=dict(orders),
        gross_minor=dict(gross),
        refunds_minor=dict(refunds),
        city_gross_minor=dict(city_gross),
        platform_orders=dict(platform_orders),
        platform_gross_minor=dict(platform_gross),
        sessions={(source,): len(ids) for source, ids in sessions.items()},
        email_sends=dict(email_sends),
        distinct_orders=len(created),
    )


def load_silver(path: Path) -> Totals:
    """Sessions by traffic source and email sends by type x month read from Silver.

    The JSON comes from ``scripts/story_demo_silver_mix.py`` (spark-submit in spark-lakehouse);
    these measures are not served by the release.
    """

    data = json.loads(path.read_text())
    return Totals(
        sessions={(source,): int(count) for source, count in data["sessions"].items()},
        email_sends={tuple(key.split(" ", 1)): int(count) for key, count in data["email_sends"].items()},
    )


def certified_totals(client: Any, database: str) -> Totals:
    """Totals served by one certified ClickHouse release database."""

    def rows(sql: str) -> list[tuple[Any, ...]]:
        return list(client.query(sql).result_rows)

    impressions: Measure = {}
    clicks: Measure = {}
    spend: Measure = {}
    for platform, month, imp, clk, spd in rows(
        f"SELECT platform, formatDateTime(event_date, '%Y-%m'), sum(impressions), sum(clicks), "
        f"sum(spend_minor) FROM {database}.campaign_performance_daily "
        "WHERE platform != '' GROUP BY 1, 2"
    ):
        if int(imp) or int(clk) or int(spd):
            impressions[(platform, month)] = int(imp)
            clicks[(platform, month)] = int(clk)
            spend[(platform, month)] = int(spd)

    orders: Measure = {}
    gross: Measure = {}
    refunds: Measure = {}
    for month, count, amount, refund in rows(
        f"SELECT formatDateTime(event_date, '%Y-%m'), sum(orders), sum(gross_revenue_minor), "
        f"sum(refund_amount_minor) FROM {database}.campaign_performance_daily GROUP BY 1"
    ):
        orders[(month,)] = int(count)
        gross[(month,)] = int(amount)
        refunds[(month,)] = int(refund)

    platform_orders: Measure = {}
    platform_gross: Measure = {}
    for platform, count, amount in rows(
        f"SELECT platform, sum(orders), sum(gross_revenue_minor) FROM {database}.campaign_performance_daily "
        "WHERE platform != '' GROUP BY 1"
    ):
        if int(count) or int(amount):
            platform_orders[(platform,)] = int(count)
            platform_gross[(platform,)] = int(amount)

    city_gross: Measure = {}
    for city, amount in rows(
        f"SELECT if(p.customer_id IS NULL OR p.customer_id = '', 'anonymous', "
        f"ifNull(nullIf(c.city, ''), 'unknown')), sum(p.gross_revenue_minor) "
        f"FROM {database}.customer_360 p LEFT JOIN "
        f"(SELECT customer_id, city FROM {database}.customers) c ON c.customer_id = p.customer_id "
        "GROUP BY 1"
    ):
        if int(amount):
            city_gross[(city,)] = int(amount)

    return Totals(
        impressions=impressions,
        clicks=clicks,
        spend_minor=spend,
        orders=orders,
        gross_minor=gross,
        refunds_minor=refunds,
        city_gross_minor=city_gross,
        platform_orders=platform_orders,
        platform_gross_minor=platform_gross,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--clickhouse", required=True, help="host:port of the ClickHouse HTTP port")
    parser.add_argument("--database", help="certified release database (default: the active publication)")
    parser.add_argument("--through", type=date.fromisoformat, required=True)
    parser.add_argument("--user", default="marketpulse")
    parser.add_argument("--password-file", type=Path)
    parser.add_argument("--silver", type=Path, help="JSON from story_demo_silver_mix.py to check unserved measures")
    args = parser.parse_args()

    import clickhouse_connect

    host, port = args.clickhouse.rsplit(":", 1)
    # The password comes from a file or the environment, never the command line.
    password = args.password_file.read_text().strip() if args.password_file else os.environ.get("CLICKHOUSE_PASSWORD", "")
    client = clickhouse_connect.get_client(host=host, port=int(port), username=args.user, password=password)
    if args.database:
        database = args.database
    else:
        from marketpulse_batch.publication import resolve_active

        database = resolve_active(client).database

    truth = ground_truth_totals(args.root, through=args.through)
    governed = governed_totals(args.root, through=args.through)
    certified = certified_totals(client, database)
    denied = consent_denied_orders(args.root, through=args.through)
    reason = f"CONSENT_DENIED: {denied:,} orders without analytics consent are not certified"
    reasons = dict.fromkeys(GOVERNED, reason)
    reasons["sessions"] = "consent-denied events and bot sessions (web/app) are not governed"
    reasons["email_sends"] = "consent-denied email events are not governed"
    checks = compare(truth, certified, governed=governed, reasons=reasons)
    if args.silver:
        checks += compare(truth, load_silver(args.silver), governed=governed, reasons=reasons)
    print(f"Release database: {database}\n")
    print(markdown(checks))
    missing = () if args.silver else uncompared(truth, certified)
    if missing:
        print("\nNot served by the release (checked elsewhere): " + ", ".join(missing))
    failed = [check for check in checks if not check.equal]
    print(f"\n{len(checks) - len(failed)}/{len(checks)} checks equal")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
