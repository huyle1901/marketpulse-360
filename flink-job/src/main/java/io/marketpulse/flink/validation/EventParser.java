package io.marketpulse.flink.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.marketpulse.flink.model.InvalidRecord;
import io.marketpulse.flink.model.InvalidRecord.ReasonCode;
import io.marketpulse.flink.model.MarketingEvent;
import io.marketpulse.flink.model.RawKafkaRecord;
import io.marketpulse.flink.serde.JsonSupport;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public final class EventParser {
    private static final Set<String> REQUIRED = Set.of(
            "event_id", "event_name", "event_time", "ingested_at", "source",
            "schema_version", "customer_id", "anonymous_id", "session_id",
            "campaign_id", "platform_campaign_id", "click_id", "consent_analytics",
            "payload", "is_late");
    private static final Map<String, TopicRule> RULES = Map.of(
            "marketing.raw.web-events.v1",
            new TopicRule(Set.of("web", "mobile"), Set.of(
                    "page_view", "view_item", "add_to_cart", "begin_checkout", "purchase")),
            "marketing.raw.order-events.v1",
            new TopicRule(Set.of("commerce"), Set.of("order_created", "order_refunded")),
            "marketing.raw.email-events.v1",
            new TopicRule(Set.of("email"), Set.of(
                    "email_sent", "email_delivered", "email_opened", "email_clicked",
                    "email_unsubscribed")));

    public ParseResult parse(RawKafkaRecord raw, Instant detectedAt) {
        JsonNode parsed;
        try {
            parsed = JsonSupport.mapper().readTree(raw.value());
        } catch (Exception error) {
            return invalid(raw, ReasonCode.MALFORMED_JSON, detectedAt);
        }
        if (!(parsed instanceof ObjectNode object)) {
            return invalid(raw, ReasonCode.MALFORMED_JSON, detectedAt);
        }
        for (String field : REQUIRED) {
            if (!object.has(field)) {
                return invalid(raw, ReasonCode.MISSING_REQUIRED_FIELD, detectedAt);
            }
        }
        if (!hasCommonTypes(object)) {
            return invalid(raw, ReasonCode.INVALID_FIELD_TYPE, detectedAt);
        }
        if (object.get("schema_version").intValue() != 1) {
            return invalid(raw, ReasonCode.UNSUPPORTED_SCHEMA_VERSION, detectedAt);
        }

        UUID eventId;
        try {
            eventId = UUID.fromString(object.get("event_id").textValue());
        } catch (IllegalArgumentException error) {
            return invalid(raw, ReasonCode.INVALID_EVENT_ID, detectedAt);
        }

        Instant eventTime;
        Instant ingestedAt;
        try {
            eventTime = parseUtc(object.get("event_time").textValue());
            ingestedAt = parseUtc(object.get("ingested_at").textValue());
        } catch (DateTimeException error) {
            return invalid(raw, ReasonCode.INVALID_TIMESTAMP, detectedAt);
        }

        TopicRule rule = RULES.get(raw.topic());
        String source = object.get("source").textValue();
        if (rule == null || !rule.sources().contains(source)) {
            return invalid(raw, ReasonCode.TOPIC_SOURCE_MISMATCH, detectedAt);
        }
        String eventName = object.get("event_name").textValue();
        if (!rule.eventNames().contains(eventName)) {
            return invalid(raw, ReasonCode.INVALID_EVENT_NAME, detectedAt);
        }
        String customerId = nullableText(object.get("customer_id"));
        String anonymousId = nullableText(object.get("anonymous_id"));
        String sessionId = object.get("session_id").textValue();
        if (firstNonBlank(customerId, anonymousId, sessionId) == null) {
            return invalid(raw, ReasonCode.MISSING_IDENTITY, detectedAt);
        }
        if (!hasValidPayload(raw.topic(), object)) {
            return invalid(raw, ReasonCode.INVALID_PAYLOAD, detectedAt);
        }

        MarketingEvent event = new MarketingEvent(
                eventId,
                eventName,
                eventTime,
                ingestedAt,
                source,
                1,
                customerId,
                anonymousId,
                sessionId,
                nullableText(object.get("campaign_id")),
                nullableText(object.get("platform_campaign_id")),
                nullableText(object.get("click_id")),
                object.get("consent_analytics").booleanValue(),
                object.get("is_late").booleanValue(),
                object.get("payload").deepCopy(),
                raw);
        return new ParseResult(event, null);
    }

    private static boolean hasCommonTypes(ObjectNode object) {
        return textual(object, "event_id")
                && textual(object, "event_name")
                && textual(object, "event_time")
                && textual(object, "ingested_at")
                && textual(object, "source")
                && object.get("schema_version").isIntegralNumber()
                && nullableTextual(object, "customer_id")
                && nullableTextual(object, "anonymous_id")
                && textual(object, "session_id")
                && nullableTextual(object, "campaign_id")
                && nullableTextual(object, "platform_campaign_id")
                && nullableTextual(object, "click_id")
                && object.get("consent_analytics").isBoolean()
                && object.get("payload").isObject()
                && object.get("is_late").isBoolean();
    }

    private static boolean hasValidPayload(String topic, ObjectNode object) {
        JsonNode payload = object.get("payload");
        if (topic.endsWith("web-events.v1")) {
            return nonBlank(nullableText(object.get("campaign_id")));
        }
        if (topic.endsWith("order-events.v1")) {
            return nonBlank(nullableText(object.get("campaign_id")))
                    && payload.hasNonNull("order_id") && payload.get("order_id").isTextual()
                    && payload.has("items") && payload.get("items").isArray()
                    && integral(payload, "gross_amount_minor")
                    && integral(payload, "refund_amount_minor")
                    && payload.hasNonNull("currency") && payload.get("currency").isTextual()
                    && "VND".equals(payload.get("currency").textValue())
                    && payload.hasNonNull("attributed_platform")
                    && payload.get("attributed_platform").isTextual()
                    && payload.hasNonNull("attributed_campaign_id")
                    && payload.get("attributed_campaign_id").isTextual();
        }
        return nonBlank(nullableText(object.get("customer_id")))
                && payload.hasNonNull("message_id") && payload.get("message_id").isTextual()
                && payload.hasNonNull("lifecycle_campaign_id")
                && payload.get("lifecycle_campaign_id").isTextual()
                && payload.has("destination_url")
                && (payload.get("destination_url").isNull()
                    || payload.get("destination_url").isTextual());
    }

    private static boolean textual(ObjectNode object, String field) {
        return object.get(field).isTextual();
    }

    private static boolean nullableTextual(ObjectNode object, String field) {
        return object.get(field).isNull() || object.get(field).isTextual();
    }

    private static boolean integral(JsonNode object, String field) {
        return object.has(field) && object.get(field).isIntegralNumber();
    }

    private static Instant parseUtc(String value) {
        OffsetDateTime parsed = OffsetDateTime.parse(value);
        if (!ZoneOffset.UTC.equals(parsed.getOffset())) {
            throw new DateTimeException("timestamp must use UTC");
        }
        return parsed.toInstant();
    }

    private static String nullableText(JsonNode value) {
        return value == null || value.isNull() ? null : value.textValue();
    }

    private static boolean nonBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (nonBlank(value)) {
                return value;
            }
        }
        return null;
    }

    private static ParseResult invalid(
            RawKafkaRecord raw, ReasonCode reasonCode, Instant detectedAt) {
        return new ParseResult(null, InvalidRecord.from(raw, reasonCode, detectedAt));
    }

    private record TopicRule(Set<String> sources, Set<String> eventNames) {}

    public record ParseResult(MarketingEvent event, InvalidRecord invalid) {
        public ParseResult {
            if ((event == null) == (invalid == null)) {
                throw new IllegalArgumentException("parse result must contain exactly one value");
            }
        }

        public boolean valid() {
            return event != null;
        }
    }
}
