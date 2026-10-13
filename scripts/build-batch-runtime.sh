#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# The Spark base is a locally built image, not a published Docker Hub repository.
docker compose --profile lakehouse --profile lakehouse-tools build spark-lakehouse
docker compose --profile batch build batch-code
