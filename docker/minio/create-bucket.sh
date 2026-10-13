#!/usr/bin/env sh
set -eu

alias_name="marketpulse"
endpoint="${MINIO_ENDPOINT:-http://minio:9000}"
bucket="${MINIO_BUCKET:-marketpulse-lakehouse}"

until mc alias set "${alias_name}" "${endpoint}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"; do
  sleep 2
done

mc mb --ignore-existing "${alias_name}/${bucket}"
mc anonymous set none "${alias_name}/${bucket}"
