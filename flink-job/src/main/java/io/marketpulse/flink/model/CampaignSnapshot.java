package io.marketpulse.flink.model;

import io.marketpulse.flink.serde.JsonSupport;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;

public record CampaignSnapshot(
        String metricKey,
        String campaignId,
        LocalDate eventDate,
        long sessions,
        long pageViewSessions,
        long viewItemSessions,
        long addToCartSessions,
        long checkoutSessions,
        long purchaseSessions,
        long orderCount,
        long refundCount,
        long grossRevenueMinor,
        long refundAmountMinor,
        long netRevenueMinor,
        String currency,
        Instant maxEventTime,
        Instant processedAt) implements Serializable {
    public static final String FUNNEL_TOPIC = "marketing.gold.funnel-metrics.v1";
    public static final String REVENUE_TOPIC = "marketing.gold.revenue-metrics.v1";
    public static final String CAMPAIGN_TOPIC = "marketing.gold.campaign-metrics.v1";

    public OutputRecord toFunnelRecord() {
        Map<String, Object> value = common();
        addFunnel(value);
        return output(FUNNEL_TOPIC, value);
    }

    public OutputRecord toRevenueRecord() {
        Map<String, Object> value = common();
        addRevenue(value);
        return output(REVENUE_TOPIC, value);
    }

    public OutputRecord toCampaignRecord() {
        Map<String, Object> value = common();
        addFunnel(value);
        addRevenue(value);
        return output(CAMPAIGN_TOPIC, value);
    }

    private Map<String, Object> common() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("schema_version", 1);
        value.put("metric_key", metricKey);
        value.put("campaign_id", campaignId);
        value.put("event_date", eventDate);
        return value;
    }

    private void addFunnel(Map<String, Object> value) {
        value.put("sessions", sessions);
        value.put("page_view_sessions", pageViewSessions);
        value.put("view_item_sessions", viewItemSessions);
        value.put("add_to_cart_sessions", addToCartSessions);
        value.put("checkout_sessions", checkoutSessions);
        value.put("purchase_sessions", purchaseSessions);
        addTimes(value);
    }

    private void addRevenue(Map<String, Object> value) {
        value.put("order_count", orderCount);
        value.put("refund_count", refundCount);
        value.put("gross_revenue_minor", grossRevenueMinor);
        value.put("refund_amount_minor", refundAmountMinor);
        value.put("net_revenue_minor", netRevenueMinor);
        value.put("currency", currency);
        addTimes(value);
    }

    private void addTimes(Map<String, Object> value) {
        value.put("max_event_time", maxEventTime);
        value.put("processed_at", processedAt);
    }

    private OutputRecord output(String topic, Map<String, Object> value) {
        return new OutputRecord(
                topic,
                metricKey,
                JsonSupport.writeCanonical(value),
                Map.of("schema_version", "1".getBytes(StandardCharsets.UTF_8)));
    }
}
