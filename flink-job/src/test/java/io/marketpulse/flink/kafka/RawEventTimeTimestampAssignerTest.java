package io.marketpulse.flink.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.marketpulse.flink.model.RawKafkaRecord;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class RawEventTimeTimestampAssignerTest {
    private final RawEventTimeTimestampAssigner assigner = new RawEventTimeTimestampAssigner();

    @Test
    void extractsPayloadEventTimeBeforeKafkaPartitionWatermarking() throws Exception {
        byte[] value;
        try (var stream = Objects.requireNonNull(
                getClass().getResourceAsStream("/events/web.json"))) {
            value = stream.readAllBytes();
        }
        RawKafkaRecord raw = new RawKafkaRecord(
                "marketing.raw.web-events.v1", 2, 1L, 999L, null, Map.of(), value);

        assertEquals(
                Instant.parse("2026-01-02T00:20:04Z").toEpochMilli(),
                assigner.extractTimestamp(raw, 999L));
    }

    @Test
    void malformedAndTombstoneRecordsCannotAdvanceWatermarks() {
        RawKafkaRecord malformed = new RawKafkaRecord(
                "topic", 2, 1L, 999L, null, Map.of(), "bad".getBytes(StandardCharsets.UTF_8));
        RawKafkaRecord tombstone = new RawKafkaRecord(
                "topic", 2, 2L, 999L, null, Map.of(), null);

        assertEquals(Long.MIN_VALUE, assigner.extractTimestamp(malformed, 999L));
        assertEquals(Long.MIN_VALUE, assigner.extractTimestamp(tombstone, 999L));
    }

    @Test
    void contractInvalidRecordWithFutureEventTimeCannotAdvanceWatermark() {
        byte[] value = ("""
                {"schema_version":1,"event_id":"00000000-0000-5000-8000-000000000001",
                 "event_time":"2030-01-01T00:00:00Z","source":"commerce",
                 "event_name":"order_created","payload":{}}
                """).getBytes(StandardCharsets.UTF_8);
        RawKafkaRecord wrongEnvelope = new RawKafkaRecord(
                "marketing.raw.web-events.v1", 2, 1L, 999L, null, Map.of(), value);

        assertEquals(Long.MIN_VALUE, assigner.extractTimestamp(wrongEnvelope, 999L));
    }
}
