"""Local-only bootstrap for missing batch passwords; never emits their values."""

from __future__ import annotations

import argparse
import os
import secrets
import tempfile
from pathlib import Path

PASSWORD_KEYS = ("CLICKHOUSE_PASSWORD", "DAGSTER_POSTGRES_PASSWORD")


def initialize_batch_secrets(path: Path) -> tuple[str, ...]:
    path = Path(path)
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise ValueError("local environment must be a regular file")
    original = path.read_bytes() if path.exists() else b""
    lines = original.decode("utf-8").splitlines(keepends=True)
    positions: dict[str, int] = {}
    for index, line in enumerate(lines):
        key = line.split("=", 1)[0].strip()
        if key not in PASSWORD_KEYS:
            continue
        if key in positions:
            raise ValueError("duplicate batch password key")
        positions[key] = index
    changed = []
    for key in PASSWORD_KEYS:
        if key in positions:
            current = lines[positions[key]].split("=", 1)[1].strip()
            if current and current not in ("''", '""'):
                continue
            lines[positions[key]] = f"{key}={secrets.token_hex(24)}\n"
        else:
            if lines and not lines[-1].endswith("\n"):
                lines[-1] += "\n"
            lines.append(f"{key}={secrets.token_hex(24)}\n")
        changed.append(key)
    if not changed:
        if path.exists():
            path.chmod(0o600)
        return ()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".env-bootstrap-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write("".join(lines).encode())
            stream.flush()
            os.fsync(stream.fileno())
        # Refuse to overwrite a concurrently edited environment.
        if path.is_symlink() or (path.exists() and path.read_bytes() != original):
            raise ValueError("local environment changed during initialization")
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return tuple(changed)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, default=Path(".env"))
    args = parser.parse_args()
    changed = initialize_batch_secrets(args.path)
    print(f"Initialized {len(changed)} missing local batch password fields; values not displayed.")


if __name__ == "__main__":
    main()
