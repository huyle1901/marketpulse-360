import copy
import json
import os
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from marketpulse_app.streaming import deliver, parse_snapshot
from marketpulse_app.live_store import LiveStore

NOW = datetime(2026, 10, 13, tzinfo=timezone.utc)


def payload():
    return dict(schema_version=1, metric_key="a|2026-01-01", campaign_id="a", event_date="2026-01-01",
                sessions=2, page_view_sessions=2, view_item_sessions=1, add_to_cart_sessions=0,
                checkout_sessions=0, purchase_sessions=0, order_count=1, refund_count=0,
                gross_revenue_minor=9007199254740993, refund_amount_minor=0,
                net_revenue_minor=9007199254740993, currency="VND",
                max_event_time="2026-01-01T01:00:00Z", processed_at="2026-10-13T00:00:00Z")


def snapshot(value=None, offset=1, partition=0):
    return parse_snapshot(json.dumps(value or payload()).encode(), b"a|2026-01-01", partition, offset, NOW)


class StreamingBoundaryTests(unittest.TestCase):
    def test_strict_snapshot_contract(self):
        self.assertEqual(snapshot().payload["gross_revenue_minor"], 9007199254740993)
        for field, bad in (("sessions", True), ("order_count", -1), ("gross_revenue_minor", 2**63),
                           ("processed_at", "2026-01-01T00:00:00"), ("net_revenue_minor", 1),
                           ("currency", None), ("extra", "x"), ("max_event_time", "2026-01-02T00:00:00Z")):
            row = payload() | {field: bad}
            with self.subTest(field=field), self.assertRaises(ValueError):
                snapshot(row)
        row = payload()
        del row["sessions"]
        with self.assertRaises(ValueError):
            snapshot(row)
        with self.assertRaises(ValueError):
            parse_snapshot(json.dumps(payload()).encode(), b"wrong", 0, 1, NOW)
        zero = payload() | dict(currency=None, gross_revenue_minor=0, net_revenue_minor=0)
        self.assertIsNone(snapshot(zero).payload["currency"])

    def test_failed_write_does_not_commit(self):
        calls = []
        def fail(_snapshot):
            raise RuntimeError("unacknowledged")
        store = SimpleNamespace(write=fail)
        consumer = SimpleNamespace(commit=lambda **kwargs: calls.append(kwargs))
        message = SimpleNamespace(value=lambda: json.dumps(payload()).encode(), key=lambda: b"a|2026-01-01",
                                  partition=lambda: 0, offset=lambda: 1, error=lambda: None)
        with self.assertRaises(RuntimeError):
            deliver(message, store, consumer, NOW)
        self.assertEqual(calls, [])


@unittest.skipUnless(os.getenv("MP06_CLICKHOUSE_TEST"), "opt-in real ClickHouse")
class LiveProjectionIntegrationTests(unittest.TestCase):
    def setUp(self):
        import clickhouse_connect
        self.client = clickhouse_connect.get_client(host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "48123")), username=os.getenv("CLICKHOUSE_USER", "marketpulse"),
            password=os.environ["CLICKHOUSE_PASSWORD"])
        self.database = "mp06_live_test"
        self.client.command(f"DROP DATABASE IF EXISTS {self.database}")
        self.store = LiveStore(self.client, database=self.database)
        self.store.initialize("cluster/topic")

    def tearDown(self):
        self.client.command(f"DROP DATABASE {self.database}")
        self.client.close()

    def test_latest_offset_retry_conflicts_nullable_and_source_identity(self):
        self.assertEqual(self.store.source_identity(), "cluster/topic")
        self.store.write(snapshot(offset=2))
        self.store.write(snapshot(offset=2))
        zero = payload() | dict(currency=None, gross_revenue_minor=0, net_revenue_minor=0)
        self.store.write(snapshot(zero, offset=1))
        rows = self.store.latest()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["payload"]["gross_revenue_minor"], 9007199254740993)
        self.assertEqual(rows[0]["received_at"], NOW)
        with self.assertRaises(ValueError):
            self.store.write(snapshot(zero, offset=2))
        with self.assertRaises(ValueError):
            self.store.write(snapshot(offset=3, partition=1))
        with self.assertRaises(ValueError):
            self.store.initialize("different-cluster/topic")
        self.store.write(snapshot(zero, offset=3))
        self.assertIsNone(self.store.latest()[0]["payload"]["currency"])
