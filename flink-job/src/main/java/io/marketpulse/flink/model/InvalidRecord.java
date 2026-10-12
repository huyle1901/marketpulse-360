package io.marketpulse.flink.model;

import io.marketpulse.flink.serde.JsonSupport;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public record InvalidRecord(
        UUID errorId,
        Instant detectedAt,
        String sourceTopic,
        int sourcePartition,
        long sourceOffset,
        String sourceKey,
        ReasonCode reasonCode,
        byte[] failedValue,
        String failedValueUtf8) implements Serializable {

    private static final UUID ERROR_NAMESPACE =
            UUID.fromString("9a3d4d70-cd8b-4b65-9b50-f86265f41915");
    public static final String TOPIC = "marketing.dlq.flink-validation.v1";

    public enum ReasonCode {
        MALFORMED_JSON,
        UNSUPPORTED_SCHEMA_VERSION,
        TOPIC_SOURCE_MISMATCH,
        MISSING_REQUIRED_FIELD,
        INVALID_FIELD_TYPE,
        INVALID_EVENT_ID,
        INVALID_TIMESTAMP,
        INVALID_EVENT_NAME,
        MISSING_IDENTITY,
        INVALID_PAYLOAD
    }

    public InvalidRecord {
        failedValue = failedValue.clone();
    }

    public static InvalidRecord from(
            RawKafkaRecord raw, ReasonCode reasonCode, Instant detectedAt) {
        byte[] originalValue = raw.value();
        byte[] value = originalValue == null ? new byte[0] : originalValue;
        String digest = HexFormat.of().formatHex(hash("SHA-256", value));
        String name = "%s:%d:%d:%s".formatted(
                raw.topic(), raw.partition(), raw.offset(), digest);
        return new InvalidRecord(
                uuidV5(ERROR_NAMESPACE, name),
                detectedAt,
                raw.topic(),
                raw.partition(),
                raw.offset(),
                raw.keyUtf8(),
                reasonCode,
                value,
                originalValue == null ? null : decodeUtf8(value));
    }

    public String failedValueBase64() {
        return Base64.getEncoder().encodeToString(failedValue);
    }

    public OutputRecord toOutputRecord() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("schema_version", 1);
        body.put("error_id", errorId.toString());
        body.put("detected_at", detectedAt.toString());
        body.put("processor", "marketpulse-flink");
        body.put("source_topic", sourceTopic);
        body.put("source_partition", sourcePartition);
        body.put("source_offset", sourceOffset);
        body.put("source_key", sourceKey);
        body.put("reason_code", reasonCode.name());
        body.put("failed_value_base64", failedValueBase64());
        body.put("failed_value_utf8", failedValueUtf8);
        return new OutputRecord(
                TOPIC,
                errorId.toString(),
                JsonSupport.writeCanonical(body),
                Map.of(
                        "schema-version", "1".getBytes(StandardCharsets.UTF_8),
                        "reason-code", reasonCode.name().getBytes(StandardCharsets.UTF_8)));
    }

    private static UUID uuidV5(UUID namespace, String name) {
        ByteBuffer namespaceBytes = ByteBuffer.allocate(16)
                .putLong(namespace.getMostSignificantBits())
                .putLong(namespace.getLeastSignificantBits());
        MessageDigest sha1 = digest("SHA-1");
        sha1.update(namespaceBytes.array());
        byte[] bytes = sha1.digest(name.getBytes(StandardCharsets.UTF_8));
        bytes[6] = (byte) ((bytes[6] & 0x0f) | 0x50);
        bytes[8] = (byte) ((bytes[8] & 0x3f) | 0x80);
        ByteBuffer uuidBytes = ByteBuffer.wrap(bytes);
        return new UUID(uuidBytes.getLong(), uuidBytes.getLong());
    }

    private static byte[] hash(String algorithm, byte[] value) {
        return digest(algorithm).digest(value);
    }

    private static MessageDigest digest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(algorithm + " is not available", impossible);
        }
    }

    private static String decodeUtf8(byte[] value) {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(value)).toString();
        } catch (CharacterCodingException ignored) {
            return null;
        }
    }
}
