"""Transactional app metadata; never an analytical warehouse or source fixture."""
from __future__ import annotations

import json
import re
from collections.abc import Mapping
from decimal import Decimal
from importlib.resources import files
from typing import Any

from .contracts import AlertRule, CampaignSetting, integer

SETTING_COLUMNS = "campaign_id,market,currency,period_start,period_end,budget_minor,version"
RULE_COLUMNS = "rule_id,campaign_id,kind,threshold,enabled,version"


class VersionConflict(ValueError):
    """Another operator has edited this record since the caller read it."""


class AppStore:
    def __init__(self, dsn: str):
        if not dsn.strip():
            raise ValueError("app database must be configured")
        self._dsn = dsn

    def connect(self) -> Any:
        import psycopg
        return psycopg.connect(self._dsn, connect_timeout=5)

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.execute("SELECT pg_advisory_xact_lock(36006)")
            connection.execute(files("marketpulse_app").joinpath("sql/app.sql").read_text())

    def settings(self) -> list[CampaignSetting]:
        with self.connect() as connection:
            return [CampaignSetting(*row) for row in connection.execute(
                f"SELECT {SETTING_COLUMNS} FROM marketpulse_app.campaign_settings ORDER BY campaign_id").fetchall()]

    def save_setting(self, setting: CampaignSetting, expected_version: int) -> CampaignSetting:
        integer(expected_version)
        values = (setting.campaign_id, setting.market, setting.currency, setting.period_start,
                  setting.period_end, setting.budget_minor)
        with self.connect() as connection:
            if expected_version == 0:
                row = connection.execute(f"INSERT INTO marketpulse_app.campaign_settings ({SETTING_COLUMNS}) "
                    f"VALUES (%s,%s,%s,%s,%s,%s,1) ON CONFLICT DO NOTHING RETURNING {SETTING_COLUMNS}", values).fetchone()
            else:
                row = connection.execute("UPDATE marketpulse_app.campaign_settings SET market=%s,currency=%s,"
                    "period_start=%s,period_end=%s,budget_minor=%s,version=version+1,updated_at=now() "
                    f"WHERE campaign_id=%s AND version=%s RETURNING {SETTING_COLUMNS}",
                    (*values[1:], setting.campaign_id, expected_version)).fetchone()
            if row is None:
                raise VersionConflict("stale campaign setting version")
            return CampaignSetting(*row)

    def rules(self) -> list[AlertRule]:
        with self.connect() as connection:
            return [self._rule(row) for row in connection.execute(
                f"SELECT {RULE_COLUMNS} FROM marketpulse_app.alert_rules ORDER BY rule_id").fetchall()]

    @staticmethod
    def _rule(row: tuple) -> AlertRule:
        return AlertRule(row[0], row[1], row[2], Decimal(row[3]), row[4], row[5])

    def save_rule(self, rule: AlertRule, expected_version: int) -> AlertRule:
        integer(expected_version)
        values = (rule.rule_id, rule.campaign_id, rule.kind, str(rule.threshold), rule.enabled)
        with self.connect() as connection:
            if expected_version == 0:
                row = connection.execute(f"INSERT INTO marketpulse_app.alert_rules ({RULE_COLUMNS}) "
                    f"VALUES (%s,%s,%s,%s,%s,1) ON CONFLICT DO NOTHING RETURNING {RULE_COLUMNS}", values).fetchone()
            else:
                row = connection.execute("UPDATE marketpulse_app.alert_rules SET campaign_id=%s,kind=%s,"
                    "threshold=%s,enabled=%s,version=version+1,updated_at=now() "
                    f"WHERE rule_id=%s AND version=%s RETURNING {RULE_COLUMNS}",
                    (*values[1:], rule.rule_id, expected_version)).fetchone()
            if row is None:
                raise VersionConflict("stale alert rule version")
            return self._rule(row)

    def record_evaluation(self, key: str, evidence: Mapping[str, object]) -> bool:
        if re.fullmatch(r"[0-9a-f]{64}", key) is None or evidence.get("outcome") not in {"triggered", "clear", "insufficient_data"}:
            raise ValueError("invalid alert evaluation")
        payload = json.dumps(dict(evidence), sort_keys=True)
        with self.connect() as connection:
            inserted = connection.execute("INSERT INTO marketpulse_app.alert_evaluations(evaluation_key,evidence) "
                "VALUES(%s,%s::jsonb) ON CONFLICT DO NOTHING RETURNING evaluation_key", (key, payload)).fetchone()
            prior = connection.execute("SELECT evidence FROM marketpulse_app.alert_evaluations WHERE evaluation_key=%s", (key,)).fetchone()[0]
            if prior != dict(evidence):
                raise ValueError("immutable alert evaluation conflict")
            if inserted and evidence["outcome"] == "triggered":
                connection.execute("INSERT INTO marketpulse_app.alert_occurrences(evaluation_key) VALUES(%s)", (key,))
            return inserted is not None

    def alerts(self, limit: int = 100) -> list[dict[str, object]]:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("invalid alert limit")
        with self.connect() as connection:
            rows = connection.execute("SELECT e.evaluation_key,e.evidence,e.evaluated_at FROM marketpulse_app.alert_evaluations e "
                "JOIN marketpulse_app.alert_occurrences o USING(evaluation_key) "
                "ORDER BY e.evaluated_at DESC,e.evaluation_key LIMIT %s", (limit,)).fetchall()
            return [{"evaluation_key": row[0], "evidence": row[1], "evaluated_at": row[2].isoformat()} for row in rows]
