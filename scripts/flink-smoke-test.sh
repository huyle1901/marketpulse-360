#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "${script_dir}/.." && pwd)"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-marketpulse-mp03-smoke-$$}"
export KAFKA_HOST_PORT="${KAFKA_HOST_PORT:-19092}"
export REGISTRY_HOST_PORT="${REGISTRY_HOST_PORT:-18081}"
export KAFKA_UI_HOST_PORT="${KAFKA_UI_HOST_PORT:-18080}"
export FLINK_HOST_PORT="${FLINK_HOST_PORT:-18082}"
export FLINK_CHECKPOINT_INTERVAL_MS="${FLINK_CHECKPOINT_INTERVAL_MS:-2000}"
export FLINK_IDLENESS_MS="${FLINK_IDLENESS_MS:-5000}"
compose=(
  docker compose -f "${project_root}/compose.yaml" -p "${COMPOSE_PROJECT_NAME}"
  --profile streaming --profile flink --profile tools
)
flink_url="http://localhost:${FLINK_HOST_PORT}"
fixture="${project_root}/data/mock/fixtures/seed-360"
if [[ -n "${SMOKE_EVIDENCE_DIR:-}" ]]; then
  evidence_dir="${SMOKE_EVIDENCE_DIR}"
  mkdir -p "${evidence_dir}"
  remove_evidence=0
else
  evidence_dir="$(mktemp -d)"
  remove_evidence=1
fi
chmod 777 "${evidence_dir}"
replay_pid=""
replay_container="${COMPOSE_PROJECT_NAME}-fixture-replay"

cleanup() {
  docker unpause "${replay_container}" >/dev/null 2>&1 || true
  if [[ -n "${replay_pid}" ]] && kill -0 "${replay_pid}" 2>/dev/null; then
    kill "${replay_pid}" 2>/dev/null || true
    wait "${replay_pid}" 2>/dev/null || true
  fi
  if [[ "${KEEP_STACK:-0}" != "1" ]]; then
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  else
    echo "Smoke stack retained: COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME}"
  fi
  if [[ "${remove_evidence}" == "1" && "${KEEP_EVIDENCE:-0}" != "1" ]]; then
    rm -rf "${evidence_dir}"
  else
    echo "Smoke evidence retained at ${evidence_dir}"
  fi
}
trap cleanup EXIT

job_id() {
  curl -fsS "${flink_url}/jobs/overview" | python3 -c '
import json, sys
jobs = json.load(sys.stdin).get("jobs", [])
match = next((job for job in jobs if job.get("name") == "marketpulse-stream-v1"), None)
if match is None:
    raise SystemExit(1)
print(match["jid"])
'
}

job_state() {
  local id="$1"
  curl -fsS "${flink_url}/jobs/${id}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])'
}

latest_checkpoint() {
  local id="$1"
  curl -fsS "${flink_url}/jobs/${id}/checkpoints" | python3 -c '
import json, sys
completed = json.load(sys.stdin).get("latest", {}).get("completed")
print(-1 if completed is None else completed["id"])
'
}

wait_for_running_job() {
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    local id
    if id="$(job_id 2>/dev/null)" && [[ "$(job_state "${id}" 2>/dev/null || true)" == "RUNNING" ]]; then
      printf '%s\n' "${id}"
      return 0
    fi
    sleep 2
  done
  echo "Flink job did not reach RUNNING" >&2
  return 1
}

wait_for_checkpoint_after() {
  local id="$1"
  local previous="$2"
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    local current
    current="$(latest_checkpoint "${id}" 2>/dev/null || echo -1)"
    if (( current > previous )); then
      printf '%s\n' "${current}"
      return 0
    fi
    sleep 1
  done
  echo "No completed checkpoint after ${previous}" >&2
  return 1
}

produce_line() {
  local topic="$1"
  local file="$2"
  "${compose[@]}" exec -T kafka \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:29092 \
    --topic "${topic}" <"${file}"
}

echo "Starting isolated MP-03 smoke stack ${COMPOSE_PROJECT_NAME}"
"${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
"${compose[@]}" up -d --build --wait \
  kafka schema-registry flink-jobmanager flink-taskmanager
"${compose[@]}" run --rm flink-job-submit

"${compose[@]}" run --rm replay kafka register-contracts \
  --registry-url http://schema-registry:8080 >/dev/null
registration_result="$("${compose[@]}" run --rm replay kafka register-contracts \
  --registry-url http://schema-registry:8080)"
grep -q '"unchanged": 10' <<<"${registration_result}" || {
  echo "Second contract registration was not idempotent: ${registration_result}" >&2
  exit 1
}

flink_job_id="$(wait_for_running_job)"
"${compose[@]}" run --rm --name "${replay_container}" replay kafka replay \
  --input /app/data/mock/fixtures/seed-360 \
  --bootstrap-servers kafka:29092 \
  --events-per-second 20 >"${evidence_dir}/first-replay.log" 2>&1 &
replay_pid=$!

replay_start_deadline=$((SECONDS + 90))
while ! grep -q 'pacing=events-per-second=20' "${evidence_dir}/first-replay.log" 2>/dev/null; do
  if (( SECONDS >= replay_start_deadline )) || ! kill -0 "${replay_pid}" 2>/dev/null; then
    echo "Replay container did not start" >&2
    cat "${evidence_dir}/first-replay.log" >&2 || true
    exit 1
  fi
  sleep 1
done
baseline_checkpoint="$(latest_checkpoint "${flink_job_id}")"

active_checkpoint="$(wait_for_checkpoint_after "${flink_job_id}" "${baseline_checkpoint}")"
kill -0 "${replay_pid}" 2>/dev/null || {
  echo "Replay ended before the recovery checkpoint was created" >&2
  exit 1
}
# Freeze the producer, drain its Kafka backlog, and cross two checkpoints. The
# second checkpoint is quiescent, so every transaction carrying prior records was
# committed by the preceding checkpoint before TaskManager recovery starts.
docker pause "${replay_container}" >/dev/null
drained_checkpoint="$(wait_for_checkpoint_after "${flink_job_id}" "${active_checkpoint}")"
checkpoint_before_restart="$(wait_for_checkpoint_after "${flink_job_id}" "${drained_checkpoint}")"
kill -0 "${replay_pid}" 2>/dev/null || {
  echo "Replay ended while its producer was paused" >&2
  exit 1
}

restart_started="$(date +%s)"
"${compose[@]}" restart flink-taskmanager
flink_job_id="$(wait_for_running_job)"
checkpoint_after_restart="$(wait_for_checkpoint_after "${flink_job_id}" "${checkpoint_before_restart}")"
restart_finished="$(date +%s)"
recovery_seconds=$((restart_finished - restart_started))
docker unpause "${replay_container}" >/dev/null

wait "${replay_pid}"
replay_pid=""
grep -q '"delivered": 219' "${evidence_dir}/first-replay.log" || {
  echo "First replay did not acknowledge all 219 raw records" >&2
  cat "${evidence_dir}/first-replay.log" >&2
  exit 1
}

"${compose[@]}" run --rm replay kafka replay \
  --input /app/data/mock/fixtures/seed-360 \
  --bootstrap-servers kafka:29092 \
  --no-sleep >"${evidence_dir}/second-replay.log"

PYTHONPATH="${project_root}/src" python3 -m marketpulse_mock.flink_smoke_verify \
  --fixture "${fixture}" \
  --output-dir "${evidence_dir}" \
  --write-probes >/dev/null

produce_line marketing.raw.web-events.v1 "${evidence_dir}/frontier.json"
sleep 7
produce_line marketing.raw.web-events.v1 "${evidence_dir}/late.json"
produce_line marketing.raw.web-events.v1 "${evidence_dir}/malformed.txt"
final_checkpoint="$(wait_for_checkpoint_after "${flink_job_id}" "${checkpoint_after_restart}")"

python3 - "${evidence_dir}/recovery.json" \
  "${checkpoint_before_restart}" "${checkpoint_after_restart}" \
  "${final_checkpoint}" "${recovery_seconds}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
path.write_text(json.dumps({
    "checkpoint_before_restart": int(sys.argv[2]),
    "checkpoint_after_restart": int(sys.argv[3]),
    "final_checkpoint": int(sys.argv[4]),
    "recovery_seconds": int(sys.argv[5]),
}, sort_keys=True) + "\n", encoding="utf-8")
PY

mkdir -p "${evidence_dir}/outputs"
chmod 777 "${evidence_dir}/outputs"
"${compose[@]}" run --rm \
  --entrypoint python \
  -v "${evidence_dir}:/evidence" \
  replay -m marketpulse_mock.flink_smoke_verify \
  --fixture /app/data/mock/fixtures/seed-360 \
  --input-dump /evidence/recovery.json \
  --output-dir /evidence/outputs \
  --consume-bootstrap kafka:29092 | tee "${evidence_dir}/verification.json"

echo "MP-03 Flink recovery smoke test passed (checkpoint ${checkpoint_before_restart} -> ${checkpoint_after_restart}, recovery ${recovery_seconds}s)."
