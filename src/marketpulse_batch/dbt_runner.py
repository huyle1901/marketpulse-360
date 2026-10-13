"""Runtime credential injection and isolated dbt build artifact ownership."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

from .settings import BatchSettings


def run_dbt_build(database: str, *, settings: BatchSettings, artifact_root: Path,
                  project_dir: Path = Path("/opt/marketpulse/dbt/marketpulse_serving")) -> Path:
    if re.fullmatch(r"mp_release_[0-9a-f]{64}_[1-9][0-9]*", database) is None:
        raise ValueError("invalid owned dbt candidate database")
    artifact_root.mkdir(parents=True, exist_ok=False)
    env = os.environ | {"CLICKHOUSE_HOST": settings.clickhouse_host, "CLICKHOUSE_PORT": str(settings.clickhouse_port),
                        "CLICKHOUSE_USER": settings.clickhouse_user,
                        "DBT_ENV_SECRET_CLICKHOUSE_PASSWORD": settings.clickhouse_password,
                        "BATCH_CANDIDATE_DATABASE": database,
                        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    result = subprocess.run(["dbt", "build", "--project-dir", str(project_dir), "--profiles-dir", str(project_dir),
                             "--target-path", str(artifact_root / "target"), "--log-path", str(artifact_root / "logs")],
                            env=env, capture_output=True, text=True)
    output = result.stdout + result.stderr
    for secret in (settings.clickhouse_password, settings.ledger_password, settings.minio_password):
        output = output.replace(secret, "[REDACTED]")
    (artifact_root / "build.log").write_text(output)
    if result.returncode:
        raise ValueError("dbt build failed; inspect the owned redacted build.log")
    return artifact_root / "target"
