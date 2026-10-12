package io.marketpulse.flink.kafka;

import io.marketpulse.flink.model.CampaignSnapshot;
import io.marketpulse.flink.model.EnrichedEvent;
import io.marketpulse.flink.model.InvalidRecord;
import io.marketpulse.flink.model.OutputRecord;
import io.marketpulse.flink.model.SessionSnapshot;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

public final class OutputRecordSerializationSchema
        implements KafkaRecordSerializationSchema<OutputRecord> {
    private static final Set<String> ALLOWED_TOPICS = Set.of(
            EnrichedEvent.TOPIC,
            SessionSnapshot.TOPIC,
            CampaignSnapshot.FUNNEL_TOPIC,
            CampaignSnapshot.REVENUE_TOPIC,
            CampaignSnapshot.CAMPAIGN_TOPIC,
            InvalidRecord.TOPIC);

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            OutputRecord record,
            KafkaSinkContext context,
            Long timestamp) {
        if (!ALLOWED_TOPICS.contains(record.topic())) {
            throw new IllegalArgumentException("unsupported output topic: " + record.topic());
        }
        RecordHeaders headers = new RecordHeaders();
        record.headers().forEach(headers::add);
        return new ProducerRecord<>(
                record.topic(),
                null,
                null,
                record.key().getBytes(StandardCharsets.UTF_8),
                record.value(),
                headers);
    }
}
