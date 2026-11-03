#!/usr/bin/env bash
# Source this file to operate the first (v1) story world, kept for comparison; the demo now uses v2.
export COMPOSE_PROJECT_NAME=marketpulse-story-demo
export WEB_HOST_PORT=43110
export API_HOST_PORT=48110
export CLICKHOUSE_HOST_PORT=48113
export DAGSTER_HOST_PORT=43120
export KAFKA_HOST_PORT=49193
export REGISTRY_HOST_PORT=48183
export KAFKA_UI_HOST_PORT=48184
export FLINK_HOST_PORT=48185
export MINIO_API_HOST_PORT=49110
export MINIO_CONSOLE_HOST_PORT=49111
export NESSIE_HOST_PORT=49221
export PROMETHEUS_HOST_PORT=49091
export GRAFANA_HOST_PORT=43002
export APP_PUBLIC_ORIGIN=http://localhost:${WEB_HOST_PORT}
export FLINK_TASKMANAGER_PROCESS_SIZE=3072m
export FLINK_TASKMANAGER_MEMORY_LIMIT=3584M
export BATCH_SOURCE_ROOT=/opt/marketpulse/data/generated/story-demo
export SPARK_DRIVER_MEMORY=4g
export SPARK_CONTAINER_MEMORY_LIMIT=6144M
export BATCH_SPARK_DRIVER_MEMORY=5g
export BATCH_SPARK_TIMEOUT_SECONDS=7200
export BATCH_CODE_MEMORY_LIMIT=7168M
export CLICKHOUSE_MEMORY_LIMIT=4096M
# The first world requires the default three ad reports; never inherit the v2 setting.
unset BATCH_REQUIRED_PLATFORMS
