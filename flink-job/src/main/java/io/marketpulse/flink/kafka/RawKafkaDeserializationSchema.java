package io.marketpulse.flink.kafka;

import io.marketpulse.flink.model.RawKafkaRecord;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

public final class RawKafkaDeserializationSchema
        implements KafkaRecordDeserializationSchema<RawKafkaRecord> {
    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record,
            Collector<RawKafkaRecord> output) throws IOException {
        Map<String, byte[]> headers = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), header.value());
        }
        output.collect(new RawKafkaRecord(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp() == ConsumerRecord.NO_TIMESTAMP ? null : record.timestamp(),
                record.key(),
                headers,
                record.value()));
    }

    @Override
    public TypeInformation<RawKafkaRecord> getProducedType() {
        return TypeInformation.of(new TypeHint<>() {});
    }
}
