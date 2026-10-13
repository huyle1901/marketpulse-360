from __future__ import annotations

import unittest
from datetime import date

from marketpulse_batch.queries import QueryRequest
from marketpulse_batch.oracles import fixture_oracle


class QueryOracleTests(unittest.TestCase):
    def test_campaign_oracle_financial_and_date_boundary(self) -> None:
        rows = [dict(event_date="2026-01-01", campaign_id="a", currency="VND", impressions=10, clicks=2,
                     spend_minor=3, gross_revenue_minor=9007199254740993, refund_amount_minor=1, net_revenue_minor=9007199254740992),
                dict(event_date="2026-01-03", campaign_id="b", currency="VND", impressions=999, clicks=99,
                     spend_minor=999, gross_revenue_minor=999, refund_amount_minor=0, net_revenue_minor=999)]
        request = QueryRequest("campaign_overview", date(2026, 1, 1), date(2026, 1, 3), currency="VND")
        self.assertEqual(fixture_oracle(request, iter(rows)), [{"currency": "VND", "campaigns": 1,
            "impressions": 10, "clicks": 2, "spend_minor": 3, "gross_revenue_minor": 9007199254740993,
            "refund_amount_minor": 1, "net_revenue_minor": 9007199254740992}])
