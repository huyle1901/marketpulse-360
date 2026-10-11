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
    --replication-factor 1
done

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${bootstrap_server}" \
  --list
