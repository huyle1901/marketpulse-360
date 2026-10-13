from __future__ import annotations

import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from marketpulse_batch.benchmark import benchmark_queries
from marketpulse_batch.queries import QueryRequest
from marketpulse_batch.ledger import PublicationReservation


class BenchmarkTests(unittest.TestCase):
    def test_records_resources_and_counts_incorrect_answers_as_failures(self):
        release = PublicationReservation(1, "a" * 64, "mp_release_" + "a" * 64 + "_1", date(2026, 1, 2), "b" * 64)
        request = QueryRequest("daily_funnel", date(2026, 1, 1), date(2026, 1, 3))
        client = SimpleNamespace(query=lambda sql: SimpleNamespace(first_row=["26.8"]))
        # Only replace external query I/O; timing, answer comparison and reporting are real.
        with patch("marketpulse_batch.benchmark.run_query", side_effect=[[{"sessions": 2}], [{"sessions": 3}]]):
            report = benchmark_queries(client, release, [request], warmups=0, samples=2,
                                       expected={"daily_funnel": [{"sessions": 2}]})
        self.assertIn("resources", report)
        self.assertGreater(report["resources"]["visible_cpu_count"], 0)
        result = report["workloads"]["daily_funnel"]
        self.assertEqual(result["failures"], 1)
        self.assertEqual(result["successful_samples"], 1)
