package io.marketpulse.flink.model;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public record OutputRecord(
        String topic,
        String key,
        byte[] value,
        Map<String, byte[]> headers) implements Serializable {

    public OutputRecord {
        value = value.clone();
        Map<String, byte[]> copied = new LinkedHashMap<>();
        headers.forEach((name, bytes) -> copied.put(name, bytes.clone()));
        headers = Map.copyOf(copied);
    }

    @Override
    public byte[] value() {
        return value.clone();
    }
}
