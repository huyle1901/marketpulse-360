#!/usr/bin/env bash
set -euo pipefail

bootstrap_server="${BOOTSTRAP_SERVERS:-kafka:29092}"
topics='
marketing.raw.web-events.v1
marketing.raw.order-events.v1
marketing.raw.email-events.v1
marketing.dlq.validation.v1
'

for topic in ${topics}; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${bootstrap_server}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions 6 \
    --replication-factor 1 \
    --config retention.ms=604800000

  description="$(/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${bootstrap_server}" \
    --describe \
    --topic "${topic}")"
  grep -q "PartitionCount: 6" <<<"${description}" || {
    echo "topic ${topic} does not have exactly six partitions" >&2
    exit 1
  }
  configuration="$(/opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server "${bootstrap_server}" \
    --entity-type topics \
    --entity-name "${topic}" \
    --describe)"
  grep -q "retention.ms=604800000" <<<"${configuration}" || {
    echo "topic ${topic} has incompatible retention.ms" >&2
    exit 1
  }
done

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${bootstrap_server}" \
  --list
