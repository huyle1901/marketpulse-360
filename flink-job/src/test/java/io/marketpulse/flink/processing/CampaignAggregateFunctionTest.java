package io.marketpulse.flink.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.marketpulse.flink.model.CampaignSnapshot;
import io.marketpulse.flink.model.EnrichedEvent;
import io.marketpulse.flink.model.MarketingEvent;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class CampaignAggregateFunctionTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-01-02T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void countsDistinctSessionReachAndSuppressesRepeatedStage() throws Exception {
        try (var harness = harness()) {
            process(harness, EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000001", "page_view", START));
            process(harness, EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000002", "page_view", START.plusSeconds(1)));
            process(harness, EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000003",
                    "view_item",
                    START.plusSeconds(2),
                    "session-2",
                    "campaign-1",
                    false,
                    EventTestFixtures.payload()));

            CampaignSnapshot result = values(harness).get(2);
            assertEquals(2, result.sessions());
            assertEquals(1, result.pageViewSessions());
            assertEquals(1, result.viewItemSessions());
            assertEquals(0, result.orderCount());
            assertEquals(0L, result.netRevenueMinor());
        }
    }

    @Test
    void orderAndRefundArithmeticMatchesCommittedFixtureTotals() throws Exception {
        try (var harness = harness()) {
            process(harness, EventTestFixtures.order(
                    "00000000-0000-0000-0000-000000000001", "order_created", START,
                    "session-1", "campaign-1", 19_000_000L, 0L));
            process(harness, EventTestFixtures.order(
                    "00000000-0000-0000-0000-000000000002", "order_created", START.plusSeconds(1),
                    "session-2", "campaign-1", 10_718_000L, 0L));
            process(harness, EventTestFixtures.order(
                    "00000000-0000-0000-0000-000000000003", "order_refunded", START.plusSeconds(2),
                    "session-1", "campaign-1", 19_000_000L, 4_967_000L));

            CampaignSnapshot result = values(harness).get(2);
            assertEquals(2, result.orderCount());
            assertEquals(1, result.refundCount());
            assertEquals(29_718_000L, result.grossRevenueMinor());
            assertEquals(4_967_000L, result.refundAmountMinor());
            assertEquals(24_751_000L, result.netRevenueMinor());
            assertEquals("VND", result.currency());
            assertEquals(START.plusSeconds(2), result.maxEventTime());
        }
    }

    @Test
    void projectionRecordsShareTheSameMetricKeyAndState() throws Exception {
        try (var harness = harness()) {
            process(harness, EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000001", "purchase", START));
            CampaignSnapshot result = values(harness).get(0);
            assertEquals("campaign-1|2026-01-01", result.metricKey());
            assertEquals(result.metricKey(), result.toFunnelRecord().key());
            assertEquals(result.metricKey(), result.toRevenueRecord().key());
            assertEquals(result.metricKey(), result.toCampaignRecord().key());
        }
    }

    private static void process(
            KeyedOneInputStreamOperatorTestHarness<String, EnrichedEvent, CampaignSnapshot> harness,
            MarketingEvent event) throws Exception {
        EnrichedEvent enriched = EventTestFixtures.enriched(event);
        harness.processElement(new StreamRecord<>(enriched, event.eventTime().toEpochMilli()));
    }

    private static List<CampaignSnapshot> values(
            KeyedOneInputStreamOperatorTestHarness<String, EnrichedEvent, CampaignSnapshot> harness) {
        return harness.extractOutputValues();
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, EnrichedEvent, CampaignSnapshot> harness()
            throws Exception {
        var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new CampaignAggregateFunction(CLOCK)),
                EnrichedEvent::metricKey,
                Types.STRING);
        harness.open();
        return harness;
    }
}
