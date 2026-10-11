package io.marketpulse.flink.model;

import io.marketpulse.flink.serde.JsonSupport;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public record SessionSnapshot(
        String sessionId,
        String customerId,
        String anonymousId,
        String campaignId,
        Instant sessionStart,
        Instant sessionEnd,
        long eventCount,
        long pageViewCount,
        long viewItemCount,
        long addToCartCount,
        long checkoutCount,
        long purchaseCount,
        long grossRevenueMinor,
        String currency,
        long lateEventCount,
        String status,
        Instant processedAt) implements Serializable {
    public static final String TOPIC = "marketing.clean.sessions.v1";

    public OutputRecord toOutputRecord() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("schema_version", 1);
        value.put("session_id", sessionId);
        value.put("customer_id", customerId);
        value.put("anonymous_id", anonymousId);
        value.put("campaign_id", campaignId);
        value.put("session_start", sessionStart);
        value.put("session_end", sessionEnd);
        value.put("event_count", eventCount);
        value.put("page_view_count", pageViewCount);
        value.put("view_item_count", viewItemCount);
        value.put("add_to_cart_count", addToCartCount);
        value.put("checkout_count", checkoutCount);
        value.put("purchase_count", purchaseCount);
        value.put("gross_revenue_minor", grossRevenueMinor);
        value.put("currency", currency);
        value.put("late_event_count", lateEventCount);
        value.put("status", status);
        value.put("processed_at", processedAt);
        return output(TOPIC, sessionId, value);
    }

    private static OutputRecord output(String topic, String key, Map<String, Object> value) {
        return new OutputRecord(
                topic,
                key,
                JsonSupport.writeCanonical(value),
                Map.of("schema_version", "1".getBytes(StandardCharsets.UTF_8)));
    }
}
