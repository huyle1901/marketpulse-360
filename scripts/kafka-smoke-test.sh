#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "${script_dir}/.." && pwd)"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-marketpulse-mp02-smoke}"
compose=(docker compose -f "${project_root}/compose.yaml" -p "${COMPOSE_PROJECT_NAME}" --profile streaming)

cleanup() {
  if [[ "${KEEP_STACK:-0}" != "1" ]]; then
    "${compose[@]}" down --volumes --remove-orphans
  fi
}
trap cleanup EXIT

"${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
"${compose[@]}" up -d --build --wait
"${compose[@]}" run --rm topic-init >/dev/null
"${compose[@]}" run --rm replay kafka register-contracts \
  --registry-url http://schema-registry:8080 >/dev/null
"${compose[@]}" run --rm replay kafka register-contracts \
  --registry-url http://schema-registry:8080 >/dev/null
"${compose[@]}" run --rm replay kafka replay \
  --input /app/data/mock/fixtures/seed-360 \
  --bootstrap-servers kafka:29092 \
  --no-sleep \
  --include-dlq

consume_count() {
  local topic="$1"
  local output
  output="$("${compose[@]}" exec -T kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:29092 \
    --topic "${topic}" \
    --from-beginning \
    --timeout-ms 5000 2>/dev/null || true)"
  if [[ -z "${output}" ]]; then
    echo 0
  else
    printf '%s\n' "${output}" | awk 'NF { count += 1 } END { print count + 0 }'
  fi
}

assert_count() {
  local topic="$1"
  local expected="$2"
  local actual
  actual="$(consume_count "${topic}")"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "FAIL ${topic}: expected ${expected}, got ${actual}" >&2
    return 1
  fi
  echo "PASS ${topic}: ${actual} records"
}

assert_count marketing.raw.web-events.v1 199
assert_count marketing.raw.order-events.v1 14
assert_count marketing.raw.email-events.v1 6
assert_count marketing.dlq.validation.v1 4

echo "Kafka smoke test passed for 223 acknowledged fixture records."
