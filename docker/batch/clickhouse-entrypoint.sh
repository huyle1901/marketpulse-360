#!/usr/bin/env bash
set -euo pipefail
if [[ -z "${CLICKHOUSE_PASSWORD//[[:space:]]/}" ]]; then
  echo 'CLICKHOUSE_PASSWORD must be explicitly configured' >&2
  exit 1
fi
exec /entrypoint.sh "$@"
