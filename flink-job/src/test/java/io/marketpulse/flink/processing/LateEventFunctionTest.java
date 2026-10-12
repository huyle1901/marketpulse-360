package io.marketpulse.flink.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.marketpulse.flink.model.EnrichedEvent;
import io.marketpulse.flink.model.MarketingEvent;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class LateEventFunctionTest {
    private static final Instant WATERMARK = Instant.parse("2026-01-01T01:00:00Z");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-01-02T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void watermarkLatenessOverridesSourceMarker() throws Exception {
        try (var harness = harness()) {
            harness.processWatermark(new Watermark(WATERMARK.toEpochMilli()));
            MarketingEvent event = EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000001",
                    "page_view",
                    WATERMARK.minusSeconds(1),
                    "session-1",
                    "campaign-1",
                    true,
                    EventTestFixtures.payload());
            harness.processElement(new StreamRecord<>(event));

            EnrichedEvent result = harness.extractOutputValues().get(0);
            assertEquals("watermark_late", result.lateStatus());
        }
    }

    @Test
    void correctionBoundaryIsStrictlyGreaterThanTenMinutes() throws Exception {
        try (var harness = harness()) {
            harness.processWatermark(new Watermark(WATERMARK.toEpochMilli()));
            MarketingEvent atBoundary = EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000001",
                    "page_view",
                    WATERMARK.minus(Duration.ofMinutes(10)));
            MarketingEvent beyondBoundary = EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000002",
                    "page_view",
                    WATERMARK.minus(Duration.ofMinutes(10)).minusMillis(1));
            harness.processElement(new StreamRecord<>(atBoundary));
            harness.processElement(new StreamRecord<>(beyondBoundary));

            List<EnrichedEvent> output = harness.extractOutputValues();
            assertTrue(output.get(0).eligibleForAggregates());
            assertFalse(output.get(1).eligibleForAggregates());
            assertEquals(CLOCK.instant(), output.get(0).processedAt());
        }
    }

    @Test
    void sourceMarkerIsVisibleWhenWatermarkHasNotPassedEvent() throws Exception {
        try (var harness = harness()) {
            MarketingEvent event = EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000001",
                    "page_view",
                    WATERMARK.plusSeconds(1),
                    "session-1",
                    "campaign-1",
                    true,
                    EventTestFixtures.payload());
            harness.processElement(new StreamRecord<>(event));
            EnrichedEvent result = harness.extractOutputValues().get(0);
            assertEquals("source_marked_late", result.lateStatus());
            assertTrue(result.eligibleForAggregates());
        }
    }

    @Test
    void eventIsAggregateEligibleBeforeFirstWatermarkWithoutLongUnderflow() throws Exception {
        try (var harness = harness()) {
            MarketingEvent event = EventTestFixtures.web(
                    "00000000-0000-0000-0000-000000000001",
                    "page_view",
                    Instant.parse("2026-01-01T00:00:00Z"));
            harness.processElement(new StreamRecord<>(event));

            EnrichedEvent result = harness.extractOutputValues().get(0);
            assertEquals("on_time", result.lateStatus());
            assertTrue(result.eligibleForAggregates());
        }
    }

    private static OneInputStreamOperatorTestHarness<MarketingEvent, EnrichedEvent> harness() throws Exception {
        var harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(new LateEventFunction(Duration.ofMinutes(10), CLOCK)));
        harness.open();
        return harness;
    }
}
