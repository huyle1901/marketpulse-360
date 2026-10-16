import json
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from decimal import Decimal

from marketpulse_app.contracts import AlertRule, CampaignSetting
from marketpulse_app.store import AppStore, VersionConflict


@unittest.skipUnless(os.getenv("MP06_POSTGRES_TEST"), "opt-in dedicated mp06_test PostgreSQL")
class AppStoreIntegrationTests(unittest.TestCase):
    def setUp(self):
        import psycopg
        self.dsn = os.environ["MP06_TEST_DSN"]
        with psycopg.connect(self.dsn) as connection:
            if connection.execute("SELECT current_database()").fetchone()[0] != "mp06_test":
                raise ValueError("test reset requires dedicated mp06_test database")
            connection.execute("DROP SCHEMA IF EXISTS marketpulse_app CASCADE")
        self.store = AppStore(self.dsn)
        self.store.initialize()

    def setting(self, amount=9007199254740993):
        return CampaignSetting("a", "VN", "VND", date(2026, 1, 1), date(2026, 1, 3), amount)

    def test_empty_startup_exact_money_and_stale_concurrent_edit(self):
        self.store.initialize()
        self.assertEqual(self.store.settings(), [])
        saved = self.store.save_setting(self.setting(), 0)
        self.assertEqual(saved.version, 1)
        self.assertEqual(saved.budget_minor, 9007199254740993)
        def edit(amount):
            try:
                return self.store.save_setting(self.setting(amount), 1).version
            except VersionConflict:
                return "conflict"
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(edit, [100, 200]))
        self.assertCountEqual(results, [2, "conflict"])
        self.assertEqual(AppStore(self.dsn).settings()[0].version, 2)

    def test_rule_version_and_immutable_idempotent_evidence(self):
        rule = AlertRule("r", "a", "refund_share", Decimal("0"))
        saved = self.store.save_rule(rule, 0)
        self.assertEqual(saved.version, 1)
        evidence = {"outcome": "triggered", "rule_id": "r", "operands": {"net": "9007199254740993"}}
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda _: self.store.record_evaluation("a"*64, evidence), range(2)))
        self.assertCountEqual(results, [True, False])
        self.store.save_rule(AlertRule("r", "a", "refund_share", Decimal("0.1")), 1)
        self.assertEqual(len(self.store.alerts()), 1)
        self.assertEqual(AppStore(self.dsn).alerts()[0]["evidence"]["operands"]["net"], "9007199254740993")
        with self.assertRaises(ValueError):
            self.store.record_evaluation("a"*64, evidence | {"outcome": "clear"})
