package io.marketpulse.flink.kafka;

import io.marketpulse.flink.model.RawKafkaRecord;
import io.marketpulse.flink.validation.EventParser;
import java.time.Instant;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

/** Extracts trusted payload event time before KafkaSource creates per-partition watermarks. */
public final class RawEventTimeTimestampAssigner
        implements SerializableTimestampAssigner<RawKafkaRecord> {
    private final EventParser parser = new EventParser();

    @Override
    public long extractTimestamp(RawKafkaRecord record, long recordTimestamp) {
        EventParser.ParseResult result = parser.parse(record, Instant.EPOCH);
        return result.valid() ? result.event().eventTime().toEpochMilli() : Long.MIN_VALUE;
    }
}
