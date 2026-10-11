package io.marketpulse.flink.validation;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.marketpulse.flink.model.InvalidRecord;
import io.marketpulse.flink.model.MarketingEvent;
import io.marketpulse.flink.model.OutputRecord;
import io.marketpulse.flink.model.RawKafkaRecord;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class EventParserTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instant DETECTED_AT = Instant.parse("2026-10-11T00:00:00Z");
    private final EventParser parser = new EventParser();

    @Test
    void parsesAllSupportedTopicsAndPreservesLineage() throws Exception {
        var web = parser.parse(raw("marketing.raw.web-events.v1", "web.json"), DETECTED_AT);
        var order = parser.parse(raw("marketing.raw.order-events.v1", "order.json"), DETECTED_AT);
        var email = parser.parse(raw("marketing.raw.email-events.v1", "email.json"), DETECTED_AT);

        assertTrue(web.valid());
        assertTrue(order.valid());
        assertTrue(email.valid());
        MarketingEvent event = web.event();
        assertEquals(UUID.fromString("ed736c6c-3106-5224-be2a-c84648f29d65"), event.eventId());
        assertEquals(Instant.parse("2026-01-02T00:20:04Z"), event.eventTime());
        assertEquals("67a33a69-deaf-5c50-8e41-abab7127cd30", event.identityKey());
        assertEquals("web", event.channel());
        assertEquals("marketing.raw.web-events.v1", event.lineage().topic());
        assertEquals(2, event.lineage().partition());
        assertEquals(41L, event.lineage().offset());
        assertEquals("customer-key", event.lineage().keyUtf8());
        assertArrayEquals("v".getBytes(StandardCharsets.UTF_8), event.lineage().headers().get("h"));
        assertEquals("140d2ff6-e46c-53f0-a0e0-15bd325fe329", order.event().payload().get("order_id").asText());
        assertEquals("LC-0002", email.event().campaignKey());
    }

    @Test
    void assignsEveryValidationReasonToItsIntendedDefect() throws Exception {
        assertReason(rawBytes("marketing.raw.web-events.v1", "{bad".getBytes(StandardCharsets.UTF_8)), "MALFORMED_JSON");
        assertMutation("web.json", node -> node.put("schema_version", 2), "UNSUPPORTED_SCHEMA_VERSION");
        assertMutation("web.json", node -> node.put("source", "commerce"), "TOPIC_SOURCE_MISMATCH");
        assertMutation("web.json", node -> node.remove("event_name"), "MISSING_REQUIRED_FIELD");
        assertMutation("web.json", node -> node.put("consent_analytics", "true"), "INVALID_FIELD_TYPE");
        assertMutation("web.json", node -> node.put("event_id", "not-a-uuid"), "INVALID_EVENT_ID");
        assertMutation("web.json", node -> node.put("event_time", "2026-01-02T07:20:04+07:00"), "INVALID_TIMESTAMP");
        assertMutation("web.json", node -> node.put("event_name", "screen_exploded"), "INVALID_EVENT_NAME");
        assertMutation("web.json", node -> {
            node.put("customer_id", "");
            node.put("anonymous_id", "");
            node.put("session_id", "");
        }, "MISSING_IDENTITY");
        assertMutation("order.json", node -> ((ObjectNode) node.get("payload")).remove("order_id"), "INVALID_PAYLOAD", "marketing.raw.order-events.v1");
    }

    @Test
    void invalidRawBytesProduceDeterministicDlqOutputWithoutAssumingUtf8() {
        byte[] bytes = {(byte) 0xff, (byte) 0xfe};
        var first = parser.parse(rawBytes("marketing.raw.web-events.v1", bytes), DETECTED_AT).invalid();
        var second = parser.parse(rawBytes("marketing.raw.web-events.v1", bytes), DETECTED_AT.plusSeconds(5)).invalid();

        assertEquals(UUID.fromString("25fa15a5-40c1-5784-ae4a-223540878932"), first.errorId());
        assertEquals(first.errorId(), second.errorId());
        assertNull(first.failedValueUtf8());
        assertEquals(Base64.getEncoder().encodeToString(bytes), first.failedValueBase64());
        OutputRecord output = first.toOutputRecord();
        assertEquals("marketing.dlq.flink-validation.v1", output.topic());
        assertEquals(first.errorId().toString(), output.key());
        JsonNode value = assertDoesNotThrow(() -> MAPPER.readTree(output.value()));
        assertEquals("MALFORMED_JSON", value.get("reason_code").asText());
        assertEquals("marketpulse-flink", value.get("processor").asText());
        assertEquals("2026-10-11T00:00:00Z", value.get("detected_at").asText());
    }

    private void assertMutation(String resource, Consumer<ObjectNode> mutation, String reason) throws Exception {
        assertMutation(resource, mutation, reason, "marketing.raw.web-events.v1");
    }

    private void assertMutation(String resource, Consumer<ObjectNode> mutation, String reason, String topic) throws Exception {
        ObjectNode value = (ObjectNode) MAPPER.readTree(resource(resource));
        mutation.accept(value);
        assertReason(rawBytes(topic, MAPPER.writeValueAsBytes(value)), reason);
    }

    private void assertReason(RawKafkaRecord raw, String reason) {
        var result = parser.parse(raw, DETECTED_AT);
        assertFalse(result.valid());
        assertNull(result.event());
        assertEquals(InvalidRecord.ReasonCode.valueOf(reason), result.invalid().reasonCode());
    }

    private RawKafkaRecord raw(String topic, String resource) throws IOException {
        return rawBytes(topic, resource(resource));
    }

    private RawKafkaRecord rawBytes(String topic, byte[] bytes) {
        return new RawKafkaRecord(
                topic,
                2,
                41L,
                1_767_310_804_000L,
                "customer-key".getBytes(StandardCharsets.UTF_8),
                Map.of("h", "v".getBytes(StandardCharsets.UTF_8)),
                bytes);
    }

    private byte[] resource(String name) throws IOException {
        try (InputStream stream = getClass().getResourceAsStream("/events/" + name)) {
            assertNotNull(stream, name);
            return stream.readAllBytes();
        }
    }
}
