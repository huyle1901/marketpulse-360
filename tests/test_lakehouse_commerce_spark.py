"""Opt-in: the Spark commerce frame must equal the pure builder on the same events."""
import json
import os
import unittest
from datetime import UTC, date, datetime

from marketpulse_lakehouse.gold import build_commerce_daily


def _row(event_id, name, when, order, items, *, device=None, refund=0):
    instant = datetime.fromisoformat(when).replace(tzinfo=UTC)
    payload = {"order_id": order, "items": [{"product_id": p, "quantity": q, "unit_price_minor": u} for p, q, u in items]} if items else {"order_id": order, "device_type": device}
    gross = sum(q * u for _, q, u in items) if name == "order_created" else None
    return {"event_id": event_id, "event_name": name, "event_time": instant, "event_date": instant.date(), "order_id": order,
            "currency": "VND" if items else None, "gross_amount_minor": gross, "refund_amount_minor": refund if name == "order_refunded" else None,
            "payload_json": json.dumps(payload, separators=(",", ":"))}


EVENTS = [_row("w", "purchase", "2026-01-02T01:00:00", "o-1", None, device="mobile"),
          _row("c", "order_created", "2026-01-02T01:00:01", "o-1", [("p-1", 2, 300), ("p-2", 1, 400)]),
          _row("r", "order_refunded", "2026-01-05T09:00:00", "o-1", [("p-1", 2, 300), ("p-2", 1, 400)], refund=501),
          _row("c2", "order_created", "2026-01-03T01:00:00", "o-2", [("p-2", 5, 100)])]
PRODUCTS = [{"product_id": "p-1", "name": "Phone", "category": "electronics", "brand": "Nova"},
            {"product_id": "p-2", "name": "Lamp", "category": "home", "brand": "Lumen"}]


@unittest.skipUnless(os.getenv("MP_SPARK_TEST"), "opt-in local Spark")
class CommerceSparkTests(unittest.TestCase):
    def test_spark_commerce_frame_equals_pure_builder(self):
        from pyspark.sql import SparkSession
        from marketpulse_lakehouse.gold import _commerce_frame
        spark = SparkSession.builder.master("local[1]").config("spark.sql.session.timeZone", "UTC").getOrCreate()
        events = spark.createDataFrame(EVENTS, "event_id string, event_name string, event_time timestamp, event_date date, order_id string, currency string, gross_amount_minor long, refund_amount_minor long, payload_json string")
        products = spark.createDataFrame(PRODUCTS, "product_id string, name string, category string, brand string")
        actual = sorted((r.asDict() for r in _commerce_frame(events, products).collect()), key=lambda r: (r["event_date"], r["product_id"]))
        expected = sorted(build_commerce_daily(EVENTS, PRODUCTS), key=lambda r: (r["event_date"], r["product_id"]))
        self.assertEqual(actual, [dict(r) for r in expected])


if __name__ == "__main__":
    unittest.main()
