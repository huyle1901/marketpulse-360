#!/usr/bin/env bash
set -euo pipefail

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-local[2]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-2g}" \
  --conf "spark.sql.session.timeZone=${SPARK_SQL_SESSION_TIMEZONE:-UTC}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-2}" \
  /opt/marketpulse/src/marketpulse_lakehouse/cli.py "$@"
