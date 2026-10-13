#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "${script_dir}/.." && pwd)"
export COMPOSE_PROJECT_NAME="marketpulse-mp04-smoke-$$"
export KAFKA_HOST_PORT="${KAFKA_HOST_PORT:-29092}"
export REGISTRY_HOST_PORT="${REGISTRY_HOST_PORT:-28081}"
export KAFKA_UI_HOST_PORT="${KAFKA_UI_HOST_PORT:-28080}"
export FLINK_HOST_PORT="${FLINK_HOST_PORT:-28082}"
export MINIO_API_HOST_PORT="${MINIO_API_HOST_PORT:-29000}"
export MINIO_CONSOLE_HOST_PORT="${MINIO_CONSOLE_HOST_PORT:-29001}"
export NESSIE_HOST_PORT="${NESSIE_HOST_PORT:-29120}"
export FLINK_CHECKPOINT_INTERVAL_MS="${FLINK_CHECKPOINT_INTERVAL_MS:-2000}"
export FLINK_IDLENESS_MS="${FLINK_IDLENESS_MS:-5000}"
compose=(
  docker compose -f "${project_root}/compose.yaml" -p "${COMPOSE_PROJECT_NAME}"
  --profile streaming --profile flink --profile tools
  --profile lakehouse --profile lakehouse-tools
)
fixture="/opt/marketpulse/data/mock/fixtures/seed-360"
if [[ -n "${SMOKE_EVIDENCE_DIR:-}" ]]; then
  mkdir -p "${SMOKE_EVIDENCE_DIR}"
  evidence_dir="$(mktemp -d "${SMOKE_EVIDENCE_DIR%/}/mp04-proof.XXXXXX")"
  remove_evidence=0
else
  evidence_dir="$(mktemp -d)"
  remove_evidence=1
fi

cleanup() {
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

topic_count() {
  local topic="$1"
  "${compose[@]}" run --rm --no-deps -T --entrypoint python replay -c '
import sys, tempfile
from pathlib import Path
from marketpulse_mock import flink_smoke_verify as verifier
verifier.OUTPUT_TOPICS = (sys.argv[1],)
with tempfile.TemporaryDirectory() as directory:
    counts = verifier.consume_outputs("kafka:29092", Path(directory))
print(counts[sys.argv[1]])
' "${topic}" | tail -n 1
}

wait_for_clean_events() {
  local expected_raw="$1"
  local deadline=$((SECONDS + 180))
  local baseline=-1
  local job vertex observed checkpoint
  while (( SECONDS < deadline )); do
    job="$(curl -fsS "http://localhost:${FLINK_HOST_PORT}/jobs/overview" |
      jq -r '.jobs[] | select(.name == "marketpulse-stream-v1") | .jid')"
    vertex="$(curl -fsS "http://localhost:${FLINK_HOST_PORT}/jobs/${job}" |
      jq -r '.vertices[] | select(.name | contains("raw-kafka-source")) | .id')"
    observed="$(curl -fsS \
      "http://localhost:${FLINK_HOST_PORT}/jobs/${job}/vertices/${vertex}/subtasks/metrics?get=Source__raw-kafka-source.numRecordsOut&agg=sum" |
      jq -r '(.[0].sum // 0) | tonumber | floor')"
    checkpoint="$(curl -fsS "http://localhost:${FLINK_HOST_PORT}/jobs/${job}/checkpoints" |
      jq -r '.latest.completed.id // -1')"
    if (( observed >= expected_raw )); then
      if (( baseline < 0 )); then baseline="${checkpoint}"; fi
      if (( checkpoint > baseline )); then
        [[ "$(topic_count marketing.clean.customer-events.v1)" == "217" ]]
        return 0
      fi
    fi
    sleep 2
  done
  echo "Flink did not process/checkpoint ${expected_raw} raw records" >&2
  return 1
}

run_lakehouse() {
  local label="$1"
  shift
  if "${compose[@]}" run --rm --no-deps spark-lakehouse "$@" \
    >"${evidence_dir}/${label}.stdout" 2>"${evidence_dir}/${label}.log"; then
    tail -n 1 "${evidence_dir}/${label}.stdout"
  else
    tail -n 20 "${evidence_dir}/${label}.stdout" >&2
    tail -n 20 "${evidence_dir}/${label}.log" >&2
    return 1
  fi
}

volume_write() {
  local path="$1"
  local value="$2"
  printf '%s\n' "${value}" | "${compose[@]}" run --rm --no-deps -T \
    --entrypoint /bin/sh spark-lakehouse \
    -c "tee /opt/marketpulse/evidence/${path} >/dev/null"
}

echo "Starting isolated MP-04 end-to-end stack ${COMPOSE_PROJECT_NAME}"
"${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
"${compose[@]}" build spark-lakehouse replay flink-jobmanager flink-taskmanager \
  flink-job-submit minio minio-init
"${compose[@]}" up -d --wait \
  kafka kafka-ui schema-registry flink-jobmanager flink-taskmanager \
  lakehouse-postgres minio minio-init nessie
"${compose[@]}" run --rm --no-deps flink-job-submit

"${compose[@]}" run --rm --no-deps replay kafka register-contracts \
  --registry-url http://schema-registry:8080 >/dev/null
"${compose[@]}" run --rm --no-deps replay kafka replay \
  --input /app/data/mock/fixtures/seed-360 \
  --bootstrap-servers kafka:29092 --no-sleep \
  >"${evidence_dir}/first-replay.log"
grep -q '"delivered": 219' "${evidence_dir}/first-replay.log"
wait_for_clean_events 219

# A second deterministic publication proves raw replay duplication cannot inflate clean Gold.
"${compose[@]}" run --rm --no-deps replay kafka replay \
  --input /app/data/mock/fixtures/seed-360 \
  --bootstrap-servers kafka:29092 --no-sleep \
  >"${evidence_dir}/second-replay.log"
grep -q '"delivered": 219' "${evidence_dir}/second-replay.log"
"${compose[@]}" run --rm --no-deps -T --entrypoint python replay -c '
from pathlib import Path
from confluent_kafka import Producer
from marketpulse_mock.replay_reader import iter_replay_records
record = next(iter_replay_records(Path("/app/data/mock/fixtures/seed-360")))
errors = []
producer = Producer({"bootstrap.servers": "kafka:29092", "acks": "all"})
producer.produce(record.topic, key=b"mp04-header-probe", value=record.value,
    headers=[("duplicate", b"one"), ("duplicate", None), ("binary", b"\xff")],
    on_delivery=lambda error, message: errors.append(str(error)) if error else None)
if producer.flush(30) or errors:
    raise SystemExit(f"audit probe delivery failed: {errors}")
'
wait_for_clean_events 439

capture_result="$(run_lakehouse capture capture-offsets \
  --run-id mp04-smoke --output /opt/marketpulse/evidence/offsets.json \
  --bootstrap-servers kafka:29092)"
jq -e '.manifest_sha256 | length == 64' <<<"${capture_result}" >/dev/null

# Release laptop memory before bounded Spark transformations.
"${compose[@]}" stop flink-job-submit flink-taskmanager flink-jobmanager

files_first="$(run_lakehouse files-first ingest-files \
  --run-id mp04-smoke --input "${fixture}")"
files_second="$(run_lakehouse files-second ingest-files \
  --run-id mp04-smoke-repeat --input "${fixture}")"
kafka_first="$(run_lakehouse kafka-first ingest-kafka \
  --run-id mp04-smoke --offset-manifest /opt/marketpulse/evidence/offsets.json \
  --bootstrap-servers kafka:29092)"
kafka_second="$(run_lakehouse kafka-second ingest-kafka \
  --run-id mp04-smoke-repeat --offset-manifest /opt/marketpulse/evidence/offsets.json \
  --bootstrap-servers kafka:29092)"

jq -e '.inserted_files == 12 and .inserted_rows == 380 and .uploaded_objects == 13' \
  <<<"${files_first}" >/dev/null
jq -e '.inserted_files == 0 and .inserted_rows == 0 and .noop_files == 12 and .noop_rows == 380' \
  <<<"${files_second}" >/dev/null
jq -e '.inserted_records > 0' <<<"${kafka_first}" >/dev/null
jq -e '.inserted_records == 0 and .noop_records == .observed_records' \
  <<<"${kafka_second}" >/dev/null
[[ "$(jq -r '.source_rows_snapshot_id' <<<"${files_first}")" == \
   "$(jq -r '.source_rows_snapshot_id' <<<"${files_second}")" ]]
[[ "$(jq -r '.snapshot_id' <<<"${kafka_first}")" == \
   "$(jq -r '.snapshot_id' <<<"${kafka_second}")" ]]

bronze_snapshots="$(jq -nc \
  --argjson kafka "$(jq '.snapshot_id' <<<"${kafka_first}")" \
  --argjson source_files "$(jq '.source_files_snapshot_id' <<<"${files_first}")" \
  --argjson files "$(jq '.source_rows_snapshot_id' <<<"${files_first}")" \
  '{"bronze.kafka_records":$kafka,"bronze.source_files":$source_files,"bronze.source_file_rows":$files}')"
volume_write bronze.json "${bronze_snapshots}"

# Build each governed layer twice. The second output is equal while the first remains
# as an actual earlier Iceberg snapshot for the Time travel proof.
silver_first="$(run_lakehouse silver-first build-silver \
  --run-id mp04-silver-1 --bronze-snapshots /opt/marketpulse/evidence/bronze.json)"
silver_baseline="$(jq -c '.snapshots | with_entries(.key = "silver." + .key)' \
  <<<"${silver_first}")"
volume_write silver-baseline.json "${silver_baseline}"
silver_second="$(run_lakehouse silver-second build-silver \
  --run-id mp04-silver-2 --bronze-snapshots /opt/marketpulse/evidence/bronze.json)"
silver_later="$(jq -c '.snapshots | with_entries(.key = "silver." + .key)' \
  <<<"${silver_second}")"
volume_write silver-later.json "${silver_later}"

gold_first="$(run_lakehouse gold-first build-gold \
  --run-id mp04-gold-1 --silver-snapshots /opt/marketpulse/evidence/silver-baseline.json)"
gold_baseline="$(jq -c '.snapshots | with_entries(.key = "gold." + .key)' \
  <<<"${gold_first}")"
volume_write gold-baseline.json "${gold_baseline}"
gold_second="$(run_lakehouse gold-second build-gold \
  --run-id mp04-gold-2 --silver-snapshots /opt/marketpulse/evidence/silver-later.json)"
gold_later="$(jq -c '.snapshots | with_entries(.key = "gold." + .key)' \
  <<<"${gold_second}")"
volume_write gold-later.json "${gold_later}"

run_lakehouse evidence write-evidence \
  --run-id mp04-smoke \
  --bronze-snapshots /opt/marketpulse/evidence/bronze.json \
  --silver-snapshots /opt/marketpulse/evidence/silver-baseline.json \
  --gold-snapshots /opt/marketpulse/evidence/gold-baseline.json \
  --later-silver-snapshots /opt/marketpulse/evidence/silver-later.json \
  --later-gold-snapshots /opt/marketpulse/evidence/gold-later.json \
  --offset-manifest /opt/marketpulse/evidence/offsets.json \
  --file-manifest "${fixture}/manifest.json" \
  --output /opt/marketpulse/evidence/lakehouse-evidence.json >/dev/null

# Catalog and objects must survive a restart before historical verification.
"${compose[@]}" restart lakehouse-postgres minio nessie
"${compose[@]}" up -d --wait lakehouse-postgres minio minio-init nessie
run_lakehouse verify verify \
  --evidence /opt/marketpulse/evidence/lakehouse-evidence.json >/dev/null
run_lakehouse backfill backfill-proof \
  --evidence /opt/marketpulse/evidence/lakehouse-evidence.json >/dev/null

"${compose[@]}" run --rm --no-deps -T --entrypoint /opt/spark/bin/spark-submit \
  -v "${project_root}/scripts/verify-lakehouse-fixture.py:/opt/marketpulse/verify-fixture.py:ro" \
  spark-lakehouse --master 'local[2]' --driver-memory 2g /opt/marketpulse/verify-fixture.py \
  --input "${fixture}" --evidence /opt/marketpulse/evidence/lakehouse-evidence.json \
  >"${evidence_dir}/fixture-oracle.json" 2>"${evidence_dir}/fixture-oracle.log"

"${compose[@]}" run --rm --no-deps -T --user "$(id -u):$(id -g)" --entrypoint /bin/sh \
  -v "${evidence_dir}:/export" spark-lakehouse \
  -c 'cp /opt/marketpulse/evidence/lakehouse-evidence.json /export/lakehouse-evidence.json'
evidence="${evidence_dir}/lakehouse-evidence.json"
jq -e '[.tables[] | .snapshot_id != .later_snapshot_id] | all' "${evidence}" >/dev/null
jq -e '.inputs.offset_manifest.size_bytes > 0 and .inputs.file_manifest.size_bytes > 0' \
  "${evidence}" >/dev/null
jq -e '.revenue.source == {gross:29718000,refund:4967000,net:24751000}' \
  "${evidence}" >/dev/null
jq -e '.revenue.governed == {gross:27160000,refund:4967000,net:22193000}' \
  "${evidence}" >/dev/null
jq -e '.revenue.attribution | all(. == {gross:27160000,refund:4967000,net:22193000})' \
  "${evidence}" >/dev/null
jq -e '.backfill.matches == true and (.backfill.namespace | startswith("rebuild_"))' \
  "${evidence}" >/dev/null

echo "MP-04 lakehouse smoke test passed with 10 verified and rebuilt tables."
echo "Evidence: ${evidence}"
echo "Kafka UI: http://localhost:${KAFKA_UI_HOST_PORT}"
echo "Flink dashboard: http://localhost:${FLINK_HOST_PORT} (stopped before Spark by design)"
echo "MinIO console: http://localhost:${MINIO_CONSOLE_HOST_PORT}"
echo "Nessie API: http://localhost:${NESSIE_HOST_PORT}/api/v2"
