package io.marketpulse.flink.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.marketpulse.flink.model.MarketingEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class DeduplicateFunctionTest {
    private static final String ID_1 = "00000000-0000-0000-0000-000000000001";
    private static final String ID_2 = "00000000-0000-0000-0000-000000000002";

    @Test
    void dropsDuplicatesButPassesDistinctIds() throws Exception {
        try (var harness = harness(Duration.ofDays(7))) {
            MarketingEvent first = EventTestFixtures.web(ID_1, "page_view", Instant.parse("2026-01-01T00:00:00Z"));
            MarketingEvent second = EventTestFixtures.web(ID_2, "view_item", Instant.parse("2026-01-01T00:00:01Z"));

            harness.processElement(new StreamRecord<>(first));
            harness.processElement(new StreamRecord<>(first));
            harness.processElement(new StreamRecord<>(second));

            assertEquals(
                    List.of(first.eventId(), second.eventId()),
                    harness.extractOutputValues().stream().map(MarketingEvent::eventId).toList());
        }
    }

    @Test
    void checkpointRestoreStillDropsSeenId() throws Exception {
        MarketingEvent event = EventTestFixtures.web(ID_1, "page_view", Instant.parse("2026-01-01T00:00:00Z"));
        OperatorSubtaskState snapshot;
        try (var original = harness(Duration.ofDays(7))) {
            original.processElement(new StreamRecord<>(event));
            snapshot = original.snapshot(1L, 1L);
        }

        try (var restored = uninitializedHarness(Duration.ofDays(7))) {
            restored.initializeState(snapshot);
            restored.open();
            restored.processElement(new StreamRecord<>(event));
            assertEquals(List.of(), restored.extractOutputValues());
        }
    }

    @Test
    void processingTimeTtlAllowsIdAfterSevenDays() throws Exception {
        try (var harness = harness(Duration.ofDays(7))) {
            MarketingEvent event = EventTestFixtures.web(ID_1, "page_view", Instant.parse("2026-01-01T00:00:00Z"));
            harness.setStateTtlProcessingTime(1L);
            harness.processElement(new StreamRecord<>(event));
            harness.setStateTtlProcessingTime(Duration.ofDays(7).toMillis() + 2L);
            harness.processElement(new StreamRecord<>(event));
            assertEquals(
                    List.of(event.eventId(), event.eventId()),
                    harness.extractOutputValues().stream().map(MarketingEvent::eventId).toList());
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<UUID, MarketingEvent, MarketingEvent> harness(
            Duration ttl) throws Exception {
        var harness = uninitializedHarness(ttl);
        harness.open();
        return harness;
    }

    private static KeyedOneInputStreamOperatorTestHarness<UUID, MarketingEvent, MarketingEvent>
            uninitializedHarness(Duration ttl) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new DeduplicateFunction(ttl)),
                MarketingEvent::eventId,
                Types.GENERIC(UUID.class));
    }
}
