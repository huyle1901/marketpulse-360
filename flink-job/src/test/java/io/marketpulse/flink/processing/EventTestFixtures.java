package io.marketpulse.flink.processing;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.marketpulse.flink.model.EnrichedEvent;
import io.marketpulse.flink.model.MarketingEvent;
import io.marketpulse.flink.model.RawKafkaRecord;
import io.marketpulse.flink.serde.JsonSupport;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

final class EventTestFixtures {
    private EventTestFixtures() {}

    static MarketingEvent web(String id, String name, Instant time) {
        return web(id, name, time, "session-1", "campaign-1", false, payload());
    }

    static MarketingEvent web(
            String id,
            String name,
            Instant time,
            String sessionId,
            String campaignId,
            boolean sourceMarkedLate,
            ObjectNode payload) {
        return event(
                id,
                name,
                time,
                "web",
                "customer-1",
                "anonymous-1",
                sessionId,
                campaignId,
                sourceMarkedLate,
                payload);
    }

    static MarketingEvent order(
            String id,
            String name,
            Instant time,
            String sessionId,
            String campaignId,
            long gross,
            long refund) {
        ObjectNode payload = payload();
        payload.put("order_id", "order-" + id.substring(0, 8));
        payload.put("gross_amount_minor", gross);
        payload.put("refund_amount_minor", refund);
        payload.put("currency", "VND");
        return event(
                id,
                name,
                time,
                "commerce",
                "customer-1",
                "anonymous-1",
                sessionId,
                campaignId,
                false,
                payload);
    }

    static EnrichedEvent enriched(MarketingEvent event) {
        return new EnrichedEvent(event, "on_time", true, event.eventTime().plusSeconds(1));
    }

    static ObjectNode payload() {
        return JsonSupport.mapper().createObjectNode();
    }

    private static MarketingEvent event(
            String id,
            String name,
            Instant time,
            String source,
            String customerId,
            String anonymousId,
            String sessionId,
            String campaignId,
            boolean sourceMarkedLate,
            ObjectNode payload) {
        byte[] value = "{}".getBytes(StandardCharsets.UTF_8);
        RawKafkaRecord lineage = new RawKafkaRecord(
                "marketing.raw." + (source.equals("commerce") ? "order" : source) + "-events.v1",
                0,
                1L,
                time.toEpochMilli(),
                id.getBytes(StandardCharsets.UTF_8),
                Map.of(),
                value);
        return new MarketingEvent(
                UUID.fromString(id),
                name,
                time,
                time.plusSeconds(1),
                source,
                1,
                customerId,
                anonymousId,
                sessionId,
                campaignId,
                "platform-1",
                "click-1",
                true,
                sourceMarkedLate,
                payload,
                lineage);
    }
}
