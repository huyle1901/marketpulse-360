"""Replay-safe append projection; no dependency on eventual background merges."""
from __future__ import annotations

import json
import re
from datetime import date
from typing import Any

from .streaming import COUNTERS, CampaignSnapshot, utc_timestamp


class LiveStore:
    def __init__(self, client: Any, database: str = "marketpulse_live"):
        if re.fullmatch(r"(?:marketpulse_live|mp06_live_test)", database) is None:
            raise ValueError("invalid live database")
        self.client, self.database = client, database

    def initialize(self, source_identity: str) -> None:
        if not source_identity or len(source_identity) > 512:
            raise ValueError("invalid source identity")
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.client.command(f"CREATE TABLE IF NOT EXISTS {self.database}.source_identity (identity String) ENGINE=MergeTree ORDER BY identity")
        rows = self.client.query(f"SELECT DISTINCT identity FROM {self.database}.source_identity").result_rows
        if rows and rows != [(source_identity,)]:
            raise ValueError("Kafka cluster/topic changed; explicit recovery required")
        if not rows:
            self.client.insert(f"{self.database}.source_identity", [(source_identity,)], column_names=["identity"])
        columns = ",".join(f"{name} Int64" for name in COUNTERS)
        self.client.command(f"CREATE TABLE IF NOT EXISTS {self.database}.campaign_snapshots ("
            "metric_key String,campaign_id String,event_date Date32,partition UInt32,offset UInt64,"
            "payload String,digest String,received_at DateTime64(6,'UTC'),max_event_time DateTime64(6,'UTC'),"
            f"processed_at DateTime64(6,'UTC'),currency Nullable(String),{columns}) "
            "ENGINE=MergeTree ORDER BY (metric_key,partition,offset)")

    def write(self, snapshot: CampaignSnapshot) -> None:
        params = {"partition": snapshot.partition, "offset": snapshot.offset, "key": snapshot.payload["metric_key"]}
        prior = self.client.query(f"SELECT DISTINCT digest,metric_key FROM {self.database}.campaign_snapshots "
            "WHERE partition={partition:UInt32} AND offset={offset:UInt64}", parameters=params).result_rows
        if prior:
            if prior != [(snapshot.digest, snapshot.payload["metric_key"])]:
                raise ValueError("conflicting Kafka source position")
            return  # Preserve first received_at even across uncertain acknowledgement.
        moved = self.client.query(f"SELECT count() FROM {self.database}.campaign_snapshots "
            "WHERE metric_key={key:String} AND partition!={partition:UInt32}", parameters=params).first_row[0]
        if moved:
            raise ValueError("metric key moved partitions")
        row = snapshot.payload
        names = ["metric_key", "campaign_id", "event_date", "partition", "offset", "payload", "digest",
                 "received_at", "max_event_time", "processed_at", "currency", *COUNTERS]
        values = [row["metric_key"], row["campaign_id"], date.fromisoformat(row["event_date"]), snapshot.partition,
                  snapshot.offset, snapshot.canonical, snapshot.digest, snapshot.received_at,
                  utc_timestamp(row["max_event_time"]), utc_timestamp(row["processed_at"]), row["currency"],
                  *(row[name] for name in COUNTERS)]
        self.client.insert(f"{self.database}.campaign_snapshots", [values], column_names=names, settings={"async_insert": 0})

    def latest(self) -> list[dict[str, object]]:
        for grouping, expression in (("partition,offset", "digest"), ("metric_key", "partition")):
            conflict = self.client.query(f"SELECT count() FROM (SELECT {grouping} FROM {self.database}.campaign_snapshots "
                f"GROUP BY {grouping} HAVING uniqExact({expression})>1)").first_row[0]
            if conflict:
                raise ValueError("live projection source conflict")
        result = self.client.query(f"SELECT metric_key,argMax(tuple(payload,received_at,partition,offset),offset) "
            f"FROM {self.database}.campaign_snapshots GROUP BY metric_key ORDER BY metric_key",
            query_tz="UTC", tz_mode="aware", settings={"use_query_cache": 0})
        return [{"payload": json.loads(value[0]), "received_at": value[1], "partition": value[2], "offset": value[3]}
                for _, value in result.result_rows]
