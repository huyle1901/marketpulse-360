package io.marketpulse.flink.model;

import io.marketpulse.flink.serde.JsonSupport;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public record EnrichedEvent(
        MarketingEvent event,
        String lateStatus,
        boolean eligibleForAggregates,
        Instant processedAt) implements Serializable {
    public static final String TOPIC = "marketing.clean.customer-events.v1";
    private static final Set<String> LATE_STATUSES =
            Set.of("on_time", "source_marked_late", "watermark_late");

    public EnrichedEvent {
        if (!LATE_STATUSES.contains(lateStatus)) {
            throw new IllegalArgumentException("unsupported late status: " + lateStatus);
        }
    }

    public String metricKey() {
        return event.campaignKey() + "|" + eventDate();
    }

    public LocalDate eventDate() {
        return event.eventTime().atZone(ZoneOffset.UTC).toLocalDate();
    }

    public OutputRecord toOutputRecord() {
        RawKafkaRecord lineage = event.lineage();
        Map<String, Object> lineageJson = new LinkedHashMap<>();
        lineageJson.put("topic", lineage.topic());
        lineageJson.put("partition", lineage.partition());
        lineageJson.put("offset", lineage.offset());
        lineageJson.put("record_timestamp", lineage.timestamp());
        lineageJson.put("record_key", lineage.keyUtf8());

        Map<String, Object> value = new LinkedHashMap<>();
        value.put("schema_version", 1);
        value.put("event_id", event.eventId().toString());
        value.put("event_name", event.eventName());
        value.put("event_time", event.eventTime());
        value.put("ingested_at", event.ingestedAt());
        value.put("processed_at", processedAt);
        value.put("source", event.source());
        value.put("channel", event.channel());
        value.put("customer_id", event.customerId());
        value.put("anonymous_id", event.anonymousId());
        value.put("identity_key", event.identityKey());
        value.put("session_id", event.sessionId());
        value.put("campaign_id", event.campaignId());
        value.put("campaign_key", event.campaignKey());
        value.put("consent_analytics", event.consentAnalytics());
        value.put("late_status", lateStatus);
        value.put("eligible_for_aggregates", eligibleForAggregates);
        value.put("payload", event.payload());
        value.put("lineage", lineageJson);
        return new OutputRecord(
                TOPIC,
                event.identityKey(),
                JsonSupport.writeCanonical(value),
                Map.of(
                        "schema_version", "1".getBytes(StandardCharsets.UTF_8),
                        "event_id", event.eventId().toString().getBytes(StandardCharsets.UTF_8)));
    }
}
