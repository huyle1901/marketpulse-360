"""Explicit local-only secret initialization; preserve all existing secrets."""
import argparse
import secrets
from pathlib import Path


def initialize(path: Path) -> None:
    content = path.read_text() if path.exists() else ""
    lines = content.splitlines()
    indices = [i for i, line in enumerate(lines) if line.startswith("APP_POSTGRES_PASSWORD=")]
    if any(lines[i].split("=", 1)[1].strip() for i in indices):
        return
    replacement = "APP_POSTGRES_PASSWORD=" + secrets.token_hex(24)
    if indices:
        for index in indices:
            lines[index] = replacement
    else:
        lines.append(replacement)
    path.write_text("\n".join(lines) + "\n")
    path.chmod(0o600)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=Path, default=Path(".env"))
    initialize(parser.parse_args().path)
