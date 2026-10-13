"""Validated runtime configuration whose representations never contain secrets."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote


@dataclass(frozen=True, slots=True)
class BatchSettings:
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str = field(repr=False)
    ledger_host: str
    ledger_port: int
    ledger_user: str
    ledger_database: str
    ledger_password: str = field(repr=False)
    minio_endpoint: str
    minio_user: str
    minio_password: str = field(repr=False)
    nessie_uri: str
    kafka_bootstrap_servers: str
    source_root: Path
    state_path: Path

    @property
    def ledger_dsn(self) -> str:
        return (f"postgresql://{quote(self.ledger_user, safe='')}:{quote(self.ledger_password, safe='')}"
                f"@{self.ledger_host}:{self.ledger_port}/{quote(self.ledger_database, safe='')}")


def load_settings(environ: Mapping[str, str]) -> BatchSettings:
    def secret(name: str) -> str:
        value = environ.get(name, "")
        if not value.strip():
            raise ValueError(f"{name} must be explicitly configured")
        return value

    def port(name: str, default: int) -> int:
        try:
            value = int(environ.get(name, str(default)))
            if not 1 <= value <= 65535:
                raise ValueError
            return value
        except ValueError as error:
            raise ValueError(f"invalid {name}") from error

    def identifier(name: str, default: str) -> str:
        value = environ.get(name, default)
        if re.fullmatch(r"[a-z][a-z0-9_]*", value) is None:
            raise ValueError(f"invalid {name}")
        return value

    return BatchSettings(
        clickhouse_host=environ.get("CLICKHOUSE_HOST", "clickhouse"),
        clickhouse_port=port("CLICKHOUSE_PORT", 8123),
        clickhouse_user=identifier("CLICKHOUSE_USER", "marketpulse"),
        clickhouse_password=secret("CLICKHOUSE_PASSWORD"),
        ledger_host=environ.get("DAGSTER_POSTGRES_HOST", "batch-postgres"),
        ledger_port=port("DAGSTER_POSTGRES_PORT", 5432),
        ledger_user=identifier("DAGSTER_POSTGRES_USER", "dagster"),
        ledger_database=identifier("DAGSTER_POSTGRES_DB", "dagster"),
        ledger_password=secret("DAGSTER_POSTGRES_PASSWORD"),
        minio_endpoint=environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        minio_user=environ.get("MINIO_ROOT_USER", "marketpulse"),
        minio_password=secret("MINIO_ROOT_PASSWORD"),
        nessie_uri=environ.get("NESSIE_URI", "http://nessie:19120/api/v2"),
        kafka_bootstrap_servers=environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        source_root=Path(environ.get("BATCH_SOURCE_ROOT", "/opt/marketpulse/data/mock/fixtures/seed-360")),
        state_path=Path(environ.get("BATCH_STATE_PATH", "/opt/marketpulse/batch-state")),
    )
