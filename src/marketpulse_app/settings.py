"""Local app runtime configuration, with independent operational credentials."""
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import quote


@dataclass(frozen=True, slots=True)
class AppSettings:
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str = field(repr=False)
    app_dsn: str = field(repr=False)
    kafka_bootstrap_servers: str = "kafka:29092"


def load_settings(environ: Mapping[str, str]) -> AppSettings:
    def secret(name: str) -> str:
        value = environ.get(name, "")
        if not value.strip():
            raise ValueError(f"{name} must be configured")
        return value
    port = int(environ.get("CLICKHOUSE_PORT", "8123"))
    if not 1 <= port <= 65535:
        raise ValueError("invalid ClickHouse port")
    user = quote(environ.get("APP_POSTGRES_USER", "marketpulse_app"), safe="")
    password = quote(secret("APP_POSTGRES_PASSWORD"), safe="")
    host = environ.get("APP_POSTGRES_HOST", "app-postgres")
    database = quote(environ.get("APP_POSTGRES_DB", "marketpulse_app"), safe="")
    return AppSettings(environ.get("CLICKHOUSE_HOST", "clickhouse"), port,
        environ.get("CLICKHOUSE_USER", "marketpulse"), secret("CLICKHOUSE_PASSWORD"),
        f"postgresql://{user}:{password}@{host}:5432/{database}", environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))


def clients(settings: AppSettings):
    import clickhouse_connect
    from .analytics import Analytics
    from .live_store import LiveStore
    from .store import AppStore
    client = clickhouse_connect.get_client(host=settings.clickhouse_host, port=settings.clickhouse_port,
        username=settings.clickhouse_user, password=settings.clickhouse_password, autogenerate_session_id=False,
        connect_timeout=5, send_receive_timeout=10)
    return Analytics(client, LiveStore(client)), AppStore(settings.app_dsn)
