package io.marketpulse.flink.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.marketpulse.flink.model.EnrichedEvent;
import io.marketpulse.flink.model.MarketingEvent;
import io.marketpulse.flink.model.SessionSnapshot;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class SessionizeFunctionTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-01-02T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void emitsOpenUpsertsAndClosesAtCurrentInactivityTimer() throws Exception {
        try (var harness = harness()) {
            MarketingEvent page = EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000001", "page_view", START);
            MarketingEvent item = EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000002", "view_item", START.plusSeconds(600));

            process(harness, EventTestFixtures.enriched(page));
            process(harness, EventTestFixtures.enriched(item));
            harness.processWatermark(new Watermark(START.plus(Duration.ofMinutes(30)).toEpochMilli()));
            assertEquals(List.of("open", "open"), snapshots(harness).stream().map(SessionSnapshot::status).toList());

            harness.processWatermark(new Watermark(START.plus(Duration.ofMinutes(40)).toEpochMilli()));
            List<SessionSnapshot> output = snapshots(harness);
            assertEquals(List.of("open", "open", "closed"), output.stream().map(SessionSnapshot::status).toList());
            assertEquals(2, output.get(2).eventCount());
            assertEquals(1, output.get(2).pageViewCount());
            assertEquals(1, output.get(2).viewItemCount());
            assertEquals(START, output.get(2).sessionStart());
            assertEquals(START.plusSeconds(600), output.get(2).sessionEnd());
        }
    }

    @Test
    void countsStagesRevenueAndLateEventsExactly() throws Exception {
        try (var harness = harness()) {
            process(harness, EventTestFixtures.enriched(EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000001", "page_view", START)));
            process(harness, EventTestFixtures.enriched(EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000002", "add_to_cart", START.plusSeconds(1))));
            ObjectNode purchasePayload = EventTestFixtures.payload();
            purchasePayload.put("amount_minor", 1_989_000L);
            purchasePayload.put("currency", "VND");
            MarketingEvent purchase = EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000003",
                    "purchase",
                    START.plusSeconds(2),
                    "session-1",
                    "campaign-1",
                    false,
                    purchasePayload);
            process(harness, new EnrichedEvent(purchase, "watermark_late", true, CLOCK.instant()));

            SessionSnapshot result = snapshots(harness).get(2);
            assertEquals(3, result.eventCount());
            assertEquals(1, result.addToCartCount());
            assertEquals(1, result.purchaseCount());
            assertEquals(1_989_000L, result.grossRevenueMinor());
            assertEquals("VND", result.currency());
            assertEquals(1, result.lateEventCount());
        }
    }

    @Test
    void firstNonNullIdentityAndCampaignWinByEventTimeThenEventId() throws Exception {
        try (var harness = harness()) {
            MarketingEvent later = EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000002", "page_view", START.plusSeconds(10));
            MarketingEvent earlier = copyWithIdentity(
                    EventTestFixtures.web(
                            "00000000-0000-0000-0000-000000000001", "view_item", START),
                    "customer-earlier",
                    "anonymous-earlier",
                    "campaign-earlier");
            process(harness, EventTestFixtures.enriched(later));
            process(harness, EventTestFixtures.enriched(earlier));

            SessionSnapshot result = snapshots(harness).get(1);
            assertEquals("customer-earlier", result.customerId());
            assertEquals("anonymous-earlier", result.anonymousId());
            assertEquals("campaign-earlier", result.campaignId());
        }
    }

    private static MarketingEvent copyWithIdentity(
            MarketingEvent event, String customerId, String anonymousId, String campaignId) {
        return new MarketingEvent(
                event.eventId(), event.eventName(), event.eventTime(), event.ingestedAt(), event.source(),
                event.sourceSchemaVersion(), customerId, anonymousId, event.sessionId(), campaignId,
                event.platformCampaignId(), event.clickId(), event.consentAnalytics(),
                event.sourceMarkedLate(), event.payload(), event.lineage());
    }

    private static void process(
            KeyedOneInputStreamOperatorTestHarness<String, EnrichedEvent, SessionSnapshot> harness,
            EnrichedEvent event) throws Exception {
        harness.processElement(new StreamRecord<>(event, event.event().eventTime().toEpochMilli()));
    }

    private static List<SessionSnapshot> snapshots(
            KeyedOneInputStreamOperatorTestHarness<String, EnrichedEvent, SessionSnapshot> harness) {
        return harness.extractOutputValues();
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, EnrichedEvent, SessionSnapshot> harness()
            throws Exception {
        var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new SessionizeFunction(Duration.ofMinutes(30), CLOCK)),
                event -> event.event().sessionId(),
                Types.STRING);
        harness.open();
        return harness;
    }
}
