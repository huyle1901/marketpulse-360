#!/usr/bin/env bash
# Source this file before operating the varied story demo (second project, v1 ports + 100).
# Values are set unconditionally so sourcing it after the v1 file never mixes the two projects.
export COMPOSE_PROJECT_NAME=marketpulse-story-demo-v2
export WEB_HOST_PORT=43210
export API_HOST_PORT=48210
export CLICKHOUSE_HOST_PORT=48213
export DAGSTER_HOST_PORT=43220
export KAFKA_HOST_PORT=49293
export REGISTRY_HOST_PORT=48283
export KAFKA_UI_HOST_PORT=48284
export FLINK_HOST_PORT=48285
export MINIO_API_HOST_PORT=49210
export MINIO_CONSOLE_HOST_PORT=49211
export NESSIE_HOST_PORT=49321
export PROMETHEUS_HOST_PORT=49191
export GRAFANA_HOST_PORT=43102
export APP_PUBLIC_ORIGIN=http://localhost:${WEB_HOST_PORT}
export FLINK_TASKMANAGER_PROCESS_SIZE=3072m
export FLINK_TASKMANAGER_MEMORY_LIMIT=3584M
export BATCH_SOURCE_ROOT=/opt/marketpulse/data/generated/story-demo-v2
export SPARK_DRIVER_MEMORY=4g
export SPARK_CONTAINER_MEMORY_LIMIT=6144M
export BATCH_SPARK_DRIVER_MEMORY=5g
export BATCH_SPARK_TIMEOUT_SECONDS=7200
export BATCH_CODE_MEMORY_LIMIT=7168M
export CLICKHOUSE_MEMORY_LIMIT=4096M
export BATCH_REQUIRED_PLATFORMS=google_ads,meta_ads,tiktok_ads,shopee_ads
