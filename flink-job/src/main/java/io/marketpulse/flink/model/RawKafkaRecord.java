package io.marketpulse.flink.model;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public record RawKafkaRecord(
        String topic,
        int partition,
        long offset,
        Long timestamp,
        byte[] key,
        Map<String, byte[]> headers,
        byte[] value) implements Serializable {

    public RawKafkaRecord {
        key = key == null ? null : key.clone();
        value = value.clone();
        Map<String, byte[]> copied = new LinkedHashMap<>();
        headers.forEach((name, bytes) -> copied.put(name, bytes.clone()));
        headers = Map.copyOf(copied);
    }

    @Override
    public byte[] key() {
        return key == null ? null : key.clone();
    }

    @Override
    public byte[] value() {
        return value.clone();
    }

    public String keyUtf8() {
        if (key == null) {
            return null;
        }
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(key)).toString();
        } catch (CharacterCodingException ignored) {
            return null;
        }
    }
}
