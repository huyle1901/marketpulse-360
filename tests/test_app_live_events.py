import json
import unittest
from datetime import datetime, timezone

from marketpulse_app.live_events import COLUMNS, LiveEventStore, TOPIC, parse_event

RECEIVED = datetime(2026, 10, 31, 9, tzinfo=timezone.utc)


def event(**overrides):
    row = {"schema_version": 1, "event_id": "e1", "event_name": "add_to_cart", "event_time": "2026-09-03T09:05:00Z",
           "ingested_at": "2026-09-03T09:05:01Z", "processed_at": "2026-10-31T09:00:00Z", "source": "mobile", "channel": "mobile",
           "customer_id": None, "anonymous_id": "a1", "identity_key": "a1", "session_id": "s1", "campaign_id": "c1", "campaign_key": "c1",
           "consent_analytics": True, "late_status": "on_time", "eligible_for_aggregates": True,
           "payload": {"product_id": "p1", "device_type": "mobile", "market": "VN", "is_bot": False},
           "lineage": {"topic": "marketing.raw.web-events.v1", "partition": 0, "offset": 9}}
    row.update(overrides)
    return json.dumps(row).encode()


def order(name="order_created", **payload):
    body = {"order_id": "o1", "items": [{"product_id": "p1", "quantity": 3, "unit_price_minor": 100}],
            "gross_amount_minor": 300, "refund_amount_minor": 0, "currency": "VND"}
    body.update(payload)
    return event(event_name=name, source="commerce", channel="commerce", session_id=None, payload=body)


class ParseTests(unittest.TestCase):
    def test_topic_is_flinks_clean_event_contract(self):
        self.assertEqual(TOPIC, "marketing.clean.customer-events.v1")

    def test_web_event_columns(self):
        row = parse_event(event(), 1, 42, RECEIVED)
        self.assertEqual(set(row), set(COLUMNS))
        self.assertEqual((row["partition"], row["offset"], row["event_name"], row["source"]), (1, 42, "add_to_cart", "mobile"))
        self.assertEqual((row["device_type"], row["product_id"], row["is_bot"]), ("mobile", "p1", False))
        self.assertEqual(row["event_time"], datetime(2026, 9, 3, 9, 5, tzinfo=timezone.utc))
        self.assertEqual((row["currency"], row["gross_amount_minor"], row["refund_amount_minor"], row["items"]), (None, 0, 0, []))
        self.assertEqual(row["received_at"], RECEIVED)

    def test_order_columns(self):
        row = parse_event(order(), 0, 1, RECEIVED)
        self.assertEqual((row["currency"], row["gross_amount_minor"], row["items"]), ("VND", 300, [("p1", 3)]))
        self.assertEqual((row["device_type"], row["is_bot"], row["session_id"]), ("unknown", False, None))
        refund = parse_event(order("order_refunded", refund_amount_minor=300), 0, 2, RECEIVED)
        self.assertEqual(refund["refund_amount_minor"], 300)

    def test_rejections(self):
        cases = [event(schema_version=2), event(event_name="purchase", source="commerce"), event(event_time="2026-09-03T09:05:00+07:00"),
                 order(gross_amount_minor=-1), order(currency=None), order(items=[{"product_id": "p1", "quantity": 0}]),
                 event(payload="x"), b"not json", event(consent_analytics="yes")]
        for value in cases:
            with self.subTest(value=value[:60]), self.assertRaises(ValueError):
                parse_event(value, 0, 1, RECEIVED)
        with self.assertRaises(ValueError):
            parse_event(event(), 0, 1, datetime(2026, 10, 31))


class FakeClient:
    def __init__(self, identities=()):
        self.commands, self.inserts, self.identities = [], [], list(identities)

    def command(self, sql):
        self.commands.append(sql)

    def query(self, sql, parameters=None):
        return type("R", (), {"result_rows": [(value,) for value in self.identities]})()

    def insert(self, table, rows, column_names=None, settings=None):
        self.inserts.append((table, rows, column_names))
        if table.endswith("event_source_identity"):
            self.identities.extend(row[0] for row in rows)


class StoreTests(unittest.TestCase):
    def test_initialize_creates_replacing_table_and_records_identity(self):
        client = FakeClient()
        LiveEventStore(client).initialize("cluster/topic")
        ddl = " ".join(client.commands)
        self.assertIn("CREATE TABLE IF NOT EXISTS marketpulse_live.events", ddl)
        self.assertIn("ReplacingMergeTree", ddl)
        self.assertIn("ORDER BY (partition,offset)", ddl)
        self.assertEqual(client.identities, ["cluster/topic"])

    def test_changed_identity_is_refused(self):
        with self.assertRaises(ValueError):
            LiveEventStore(FakeClient(["other/topic"])).initialize("cluster/topic")

    def test_batch_is_one_insert_and_empty_batch_inserts_nothing(self):
        client = FakeClient(["cluster/topic"])
        store = LiveEventStore(client)
        store.write_batch([])
        self.assertEqual(client.inserts, [])
        rows = [parse_event(event(), 0, 1, RECEIVED), parse_event(order(), 0, 2, RECEIVED)]
        store.write_batch(rows)
        table, values, names = client.inserts[0]
        self.assertEqual((table, len(values), tuple(names)), ("marketpulse_live.events", 2, COLUMNS))
        self.assertEqual(store.source_identity(), "cluster/topic")


if __name__ == "__main__":
    unittest.main()
