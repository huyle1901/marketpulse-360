package io.marketpulse.flink.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.marketpulse.flink.model.OutputRecord;
import io.marketpulse.flink.model.RawKafkaRecord;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

class KafkaSerdeTest {
    @Test
    void sourceDeserializationRetainsMetadataHeadersAndRawBytes() throws Exception {
        byte[] key = "key-1".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[] {(byte) 0xff, 0x01};
        var headers = new RecordHeaders().add("schema-version", new byte[] {1});
        ConsumerRecord<byte[], byte[]> source = new ConsumerRecord<>(
                "marketing.raw.web-events.v1", 2, 41L, 1234L,
                TimestampType.CREATE_TIME, key.length, value.length, key, value, headers,
                Optional.of(3));
        CapturingCollector collector = new CapturingCollector();

        new RawKafkaDeserializationSchema().deserialize(source, collector);

        RawKafkaRecord result = collector.value;
        assertEquals(source.topic(), result.topic());
        assertEquals(2, result.partition());
        assertEquals(41L, result.offset());
        assertEquals(1234L, result.timestamp());
        assertArrayEquals(key, result.key());
        assertArrayEquals(value, result.value());
        assertArrayEquals(new byte[] {1}, result.headers().get("schema-version"));
    }

    @Test
    void sourceDeserializationPreservesNullableTombstoneAndHeader() throws Exception {
        var headers = new RecordHeaders().add("nullable", null);
        ConsumerRecord<byte[], byte[]> source = new ConsumerRecord<>(
                "marketing.raw.web-events.v1", 1, 9L, 1234L,
                TimestampType.CREATE_TIME, -1, -1, null, null, headers,
                Optional.empty());
        CapturingCollector collector = new CapturingCollector();

        new RawKafkaDeserializationSchema().deserialize(source, collector);

        assertNull(collector.value.key());
        assertNull(collector.value.value());
        assertNull(collector.value.headers().get("nullable"));
    }

    @Test
    void sinkSerializationRoutesDynamicTopicKeyValueAndHeaders() {
        byte[] json = "{\"schema_version\":1}".getBytes(StandardCharsets.UTF_8);
        OutputRecord output = new OutputRecord(
                "marketing.clean.customer-events.v1",
                "customer-1",
                json,
                Map.of("event_id", "id-1".getBytes(StandardCharsets.UTF_8)));

        ProducerRecord<byte[], byte[]> result =
                new OutputRecordSerializationSchema().serialize(output, null, 999L);

        assertEquals(output.topic(), result.topic());
        assertArrayEquals("customer-1".getBytes(StandardCharsets.UTF_8), result.key());
        assertArrayEquals(json, result.value());
        assertArrayEquals(
                "id-1".getBytes(StandardCharsets.UTF_8),
                result.headers().lastHeader("event_id").value());
        assertNull(result.timestamp(), "Kafka retention must use publish time, not event time");
    }

    @Test
    void sinkRejectsTopicsOutsideTheSixPublicOutputs() {
        OutputRecord output = new OutputRecord(
                "marketing.raw.web-events.v1", "key", new byte[] {1}, Map.of());
        var serializer = new OutputRecordSerializationSchema();
        assertThrows(IllegalArgumentException.class, () -> serializer.serialize(output, null, null));
    }

    private static final class CapturingCollector implements Collector<RawKafkaRecord> {
        private RawKafkaRecord value;

        @Override
        public void collect(RawKafkaRecord record) {
            value = record;
        }

        @Override
        public void close() {}
    }
}
