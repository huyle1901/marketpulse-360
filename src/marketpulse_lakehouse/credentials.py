"""Read local credentials explicitly; never silently use a shared password."""

from __future__ import annotations

import os
from collections.abc import Mapping


def minio_password(env: Mapping[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    value = values.get("MINIO_ROOT_PASSWORD")
    if value is None or not value.strip():
        raise ValueError("MINIO_ROOT_PASSWORD must be set and non-empty; configure your local .env")
    return value
