"""Compare consumed Kafka smoke payloads with deterministic fixture identities."""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

from .replay_reader import iter_replay_records


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 2:
        print("usage: smoke_verify FIXTURE CONSUMED_DIR", file=sys.stderr)
        return 2
    fixture, consumed_dir = map(Path, arguments)
    expected: dict[str, Counter[tuple[str, str | None]]] = defaultdict(Counter)
    for record in iter_replay_records(fixture, include_dlq=True):
        payload = json.loads(record.value)
        reason = payload.get("reason_code") if record.dataset == "quality_cases" else None
        expected[record.topic][(record.event_id, reason)] += 1

    errors: list[str] = []
    for topic, identities in expected.items():
        actual: Counter[tuple[str, str | None]] = Counter()
        path = consumed_dir / f"{topic}.jsonl"
        for line in path.read_text(encoding="utf-8").splitlines():
            payload = json.loads(line)
            event_id = str(payload.get("case_id") or payload.get("event_id"))
            actual[(event_id, payload.get("reason_code"))] += 1
        if actual != identities:
            errors.append(f"{topic}: expected identities {identities}, got {actual}")
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("PASS exact event IDs and DLQ reason codes match the fixture")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
