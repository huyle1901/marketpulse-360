#!/usr/bin/env bash
set -euo pipefail

jobmanager_url="${FLINK_JOBMANAGER_URL:-http://flink-jobmanager:8081}"
job_name="${FLINK_JOB_NAME:-marketpulse-stream-v1}"
job_jar="${FLINK_JOB_JAR:-/opt/flink/usrlib/marketpulse-flink-job.jar}"

overview="$(curl -fsS "${jobmanager_url}/jobs/overview")"
if [[ "${overview}" == *"\"name\":\"${job_name}\""* \
    && "${overview}" == *'"state":"RUNNING"'* ]]; then
  echo "Flink job ${job_name} is already RUNNING"
  exit 0
fi

submission="$(flink run -d \
  -m flink-jobmanager:8081 \
  -c io.marketpulse.flink.MarketPulseFlinkJob \
  "${job_jar}" \
  "$@")"
echo "${submission}"

job_id="$(printf '%s\n' "${submission}" | sed -n 's/.*JobID \([[:xdigit:]]\{32\}\).*/\1/p' | tail -n 1)"
if [[ -z "${job_id}" ]]; then
  echo "Flink submission did not return a job ID" >&2
  exit 1
fi
echo "Submitted ${job_name} as ${job_id}"
