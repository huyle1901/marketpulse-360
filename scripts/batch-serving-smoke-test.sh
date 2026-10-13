#!/usr/bin/env bash
# Non-destructive recovery proof for an already bootstrapped, test-owned fixture stack.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${MP05_PROJECT:?Set MP05_PROJECT to the test-owned Compose project}"
case "$MP05_PROJECT" in marketpulse-mp05-*) ;; *) echo 'Refusing to restart a non-MP05 test project' >&2; exit 2;; esac
proof_root="${MP05_EVIDENCE_DIR:-.lakehouse/mp05-proof}"
mkdir -p "$proof_root"
compose=(docker compose -p "$MP05_PROJECT" --profile streaming --profile flink --profile lakehouse --profile batch --profile serving --profile tools --profile lakehouse-tools)
verify() {
  "${compose[@]}" run --rm --no-deps -v "$PWD/src:/opt/marketpulse/src:ro" batch-code \
    python3 -m marketpulse_batch.cli verify --from-date 2026-01-01 --to-date 2026-01-03 > "$1"
}
verify "$proof_root/before.json"
recovery_id="$("${MP05_PYTHON:-python3}" -c 'import uuid; print(uuid.uuid4().hex)')"
"${compose[@]}" run --rm --no-deps -e BATCH_TEST_RECOVERY=1 -e BATCH_RECOVERY_PHASE=pending -e "BATCH_RECOVERY_ID=$recovery_id" \
  -v "$PWD/src:/opt/marketpulse/src:ro" -v "$PWD/tests:/opt/marketpulse/tests:ro" batch-code \
  python3 -m unittest discover -s /opt/marketpulse/tests -p test_batch_proof.py > "$proof_root/recovery.log" 2>&1
"${compose[@]}" run --rm --no-deps -e "BATCH_RECOVERY_ID=$recovery_id" batch-code python3 -c '
import json, os
from pathlib import Path
path = Path(os.environ["BATCH_STATE_PATH"]) / "recovery" / (os.environ["BATCH_RECOVERY_ID"] + ".json")
assert json.loads(path.read_text())["acknowledged"] is False
'
"${compose[@]}" restart clickhouse batch-postgres batch-code dagster-webserver dagster-daemon
"${compose[@]}" up -d --wait --wait-timeout 180 clickhouse batch-postgres batch-code dagster-webserver dagster-daemon
"${compose[@]}" run --rm --no-deps -e BATCH_TEST_RECOVERY=1 -e BATCH_RECOVERY_PHASE=recover -e "BATCH_RECOVERY_ID=$recovery_id" \
  -v "$PWD/src:/opt/marketpulse/src:ro" -v "$PWD/tests:/opt/marketpulse/tests:ro" batch-code \
  python3 -m unittest discover -s /opt/marketpulse/tests -p test_batch_proof.py >> "$proof_root/recovery.log" 2>&1
verify "$proof_root/after.json"
PYTHONPATH=src "${MP05_PYTHON:-python3}" scripts/verify-batch-serving.py "$proof_root/before.json" "$proof_root/after.json"
docker info --format '{{json .MemTotal}} {{json .NCPU}}' > "$proof_root/docker-resources.txt"
echo "Evidence: $proof_root (retained; no volumes deleted)"
