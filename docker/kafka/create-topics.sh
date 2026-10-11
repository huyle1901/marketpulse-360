#!/usr/bin/env bash
set -euo pipefail

bootstrap_server="${BOOTSTRAP_SERVERS:-kafka:29092}"
topics='
marketing.raw.web-events.v1
marketing.raw.order-events.v1
marketing.raw.email-events.v1
marketing.dlq.validation.v1
marketing.clean.customer-events.v1
marketing.clean.sessions.v1
marketing.gold.funnel-metrics.v1
marketing.gold.revenue-metrics.v1
marketing.gold.campaign-metrics.v1
marketing.dlq.flink-validation.v1
'

for topic in ${topics}; do
  partitions=6
  retention_ms=604800000
  cleanup_policy=delete
  case "${topic}" in
    marketing.clean.sessions.v1)
      cleanup_policy=compact,delete
      ;;
    marketing.gold.*)
      partitions=3
      retention_ms=2592000000
      cleanup_policy=compact,delete
      ;;
  esac

  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${bootstrap_server}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config retention.ms="${retention_ms}" \
    --config cleanup.policy="${cleanup_policy}"

  description="$(/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${bootstrap_server}" \
    --describe \
    --topic "${topic}")"
  grep -q "PartitionCount: ${partitions}" <<<"${description}" || {
    echo "topic ${topic} does not have exactly ${partitions} partitions" >&2
    exit 1
  }
  configuration="$(/opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server "${bootstrap_server}" \
    --entity-type topics \
    --entity-name "${topic}" \
    --describe)"
  grep -q "retention.ms=${retention_ms}" <<<"${configuration}" || {
    echo "topic ${topic} has incompatible retention.ms" >&2
    exit 1
  }
  actual_cleanup="$(sed -n 's/.*cleanup.policy=\([^ ]*\).*/\1/p' <<<"${configuration}")"
  expected_cleanup="$(tr ',' '\n' <<<"${cleanup_policy}" | sort | paste -sd, -)"
  normalized_cleanup="$(tr ',' '\n' <<<"${actual_cleanup}" | sort | paste -sd, -)"
  [[ "${normalized_cleanup}" == "${expected_cleanup}" ]] || {
    echo "topic ${topic} has incompatible cleanup.policy" >&2
    exit 1
  }
done

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${bootstrap_server}" \
  --list
