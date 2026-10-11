package io.marketpulse.flink.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public record MarketingEvent(
        UUID eventId,
        String eventName,
        Instant eventTime,
        Instant ingestedAt,
        String source,
        int sourceSchemaVersion,
        String customerId,
        String anonymousId,
        String sessionId,
        String campaignId,
        String platformCampaignId,
        String clickId,
        boolean consentAnalytics,
        boolean sourceMarkedLate,
        JsonNode payload,
        RawKafkaRecord lineage) implements Serializable {

    public String identityKey() {
        String payloadIdentity = firstText(payload, "order_id", "message_id");
        return firstNonBlank(customerId, anonymousId, sessionId, payloadIdentity);
    }

    public String campaignKey() {
        return firstNonBlank(campaignId, text(payload, "lifecycle_campaign_id"));
    }

    public String channel() {
        return source;
    }

    private static String firstText(JsonNode value, String... fields) {
        for (String field : fields) {
            String candidate = text(value, field);
            if (candidate != null) {
                return candidate;
            }
        }
        return null;
    }

    private static String text(JsonNode value, String field) {
        JsonNode candidate = value == null ? null : value.get(field);
        return candidate != null && candidate.isTextual() ? candidate.textValue() : null;
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }
}
