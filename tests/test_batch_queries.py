from __future__ import annotations

import unittest
from datetime import date

from marketpulse_batch.queries import QueryRequest, build_query
from marketpulse_batch.ledger import PublicationReservation
from marketpulse_batch.benchmark import nearest_rank


class QueryContractTests(unittest.TestCase):
    def release(self):
        return PublicationReservation(1, "a" * 64, "mp_release_" + "a" * 64 + "_1", date(2026, 1, 2), "b" * 64)

    def test_query_parameters_not_sql_interpolation(self) -> None:
        value = "customer'; DROP TABLE x --"
        request = QueryRequest("customer_drill", date(2026, 1, 1), date(2026, 1, 3), customer_id=value)
        sql, params = build_query(self.release(), request, single_currency="VND")
        self.assertNotIn(value, sql)
        self.assertEqual(params["customer_id"], value)
        self.assertIn("{customer_id:String}", sql)

    def test_request_pins_one_release(self) -> None:
        request = QueryRequest("campaign_overview", date(2026, 1, 1), date(2026, 1, 3))
        sql, params = build_query(self.release(), request)
        self.assertIn(self.release().database + ".campaign_performance_daily", sql)
        self.assertNotIn("publication_journal", sql)
        self.assertEqual(params["to_date"], date(2026, 1, 3))
        self.assertIn("event_date < {to_date:Date32}", sql)

    def test_currency_separation(self) -> None:
        request = QueryRequest("campaign_overview", date(2026, 1, 1), date(2026, 1, 3), currency="VND")
        sql, params = build_query(self.release(), request)
        self.assertIn("GROUP BY currency", sql)
        self.assertEqual(params["currency"], "VND")
        self.assertIn("toInt128(net_revenue_minor)", sql)

    def test_invalid_interval_rejected(self) -> None:
        for options in ({"to_date": date(2026, 1, 1)}, {"limit": 0}, {"currency": "bad';SQL"}, {"workload": "arbitrary_sql"}):
            fields = {"workload": "campaign_overview", "from_date": date(2026, 1, 1), "to_date": date(2026, 1, 3)} | options
            with self.subTest(options=options), self.assertRaises(ValueError):
                QueryRequest(**fields)

    def test_nearest_rank_p95(self) -> None:
        self.assertEqual(nearest_rank(list(range(1, 101)), 0.95), 95)
        self.assertEqual(nearest_rank([3], 0.95), 3)
        with self.assertRaises(ValueError):
            nearest_rank([], 0.95)
