package io.marketpulse.flink.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class JsonSupport {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);

    private JsonSupport() {}

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static byte[] writeCanonical(Object value) {
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException("cannot encode canonical JSON", error);
        }
    }
}
