package io.marketpulse.flink;

import io.marketpulse.flink.config.JobConfig;
import io.marketpulse.flink.kafka.OutputRecordSerializationSchema;
import io.marketpulse.flink.kafka.RawKafkaDeserializationSchema;
import io.marketpulse.flink.model.CampaignSnapshot;
import io.marketpulse.flink.model.EnrichedEvent;
import io.marketpulse.flink.model.MarketingEvent;
import io.marketpulse.flink.model.OutputRecord;
import io.marketpulse.flink.model.RawKafkaRecord;
import io.marketpulse.flink.model.SessionSnapshot;
import io.marketpulse.flink.processing.CampaignAggregateFunction;
import io.marketpulse.flink.processing.DeduplicateFunction;
import io.marketpulse.flink.processing.LateEventFunction;
import io.marketpulse.flink.processing.SessionizeFunction;
import io.marketpulse.flink.validation.EventParser;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class MarketPulseFlinkJob {
    private static final List<String> SOURCE_TOPICS = List.of(
            "marketing.raw.web-events.v1",
            "marketing.raw.order-events.v1",
            "marketing.raw.email-events.v1");
    private static final OutputTag<OutputRecord> VALIDATION_DLQ =
            new OutputTag<>("validation-dlq", TypeInformation.of(new TypeHint<>() {}));

    private MarketPulseFlinkJob() {}

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        build(environment, config);
        environment.execute(config.jobName());
    }

    public static DataStream<OutputRecord> build(
            StreamExecutionEnvironment environment,
            JobConfig config) {
        configureEnvironment(environment, config);

        KafkaSource<RawKafkaRecord> source = KafkaSource.<RawKafkaRecord>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setGroupId(config.groupId())
                .setTopics(SOURCE_TOPICS)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new RawKafkaDeserializationSchema())
                .build();

        DataStream<RawKafkaRecord> raw = environment
                .fromSource(source, WatermarkStrategy.noWatermarks(), "raw-kafka-source")
                .uid("raw-kafka-source");

        SingleOutputStreamOperator<MarketingEvent> parsed = raw
                .process(new ParseAndValidateFunction(Clock.systemUTC()))
                .returns(TypeInformation.of(new TypeHint<>() {}))
                .name("parse-and-validate")
                .uid("parse-and-validate");

        DataStream<OutputRecord> validationDlq = parsed
                .getSideOutput(VALIDATION_DLQ)
                .map(value -> value)
                .returns(TypeInformation.of(new TypeHint<>() {}))
                .name("validation-dlq-projection")
                .uid("validation-dlq-projection");

        WatermarkStrategy<MarketingEvent> watermarks =
                WatermarkStrategy.<MarketingEvent>forBoundedOutOfOrderness(
                                config.watermarkOutOfOrderness())
                        .withTimestampAssigner((event, previous) -> event.eventTime().toEpochMilli())
                        .withIdleness(config.sourceIdleness());
        DataStream<MarketingEvent> timestamped = parsed
                .assignTimestampsAndWatermarks(watermarks)
                .name("assign-watermarks")
                .uid("assign-watermarks");

        DataStream<MarketingEvent> unique = timestamped
                .keyBy(MarketingEvent::eventId)
                .process(new DeduplicateFunction(config.dedupTtl()))
                .name("deduplicate-events")
                .uid("deduplicate-events");

        DataStream<EnrichedEvent> enriched = unique
                .process(new LateEventFunction(config.correctionHorizon(), Clock.systemUTC()))
                .name("classify-late-events")
                .uid("classify-late-events");

        DataStream<OutputRecord> clean = enriched
                .map(EnrichedEvent::toOutputRecord)
                .returns(TypeInformation.of(new TypeHint<>() {}))
                .name("clean-event-projection")
                .uid("clean-event-projection");

        DataStream<OutputRecord> sessions = enriched
                .keyBy(value -> value.event().sessionId())
                .process(new SessionizeFunction(config.sessionGap(), Clock.systemUTC()))
                .name("session-upserts")
                .uid("session-upserts")
                .map(SessionSnapshot::toOutputRecord)
                .returns(TypeInformation.of(new TypeHint<>() {}))
                .name("session-projection")
                .uid("session-projection");

        DataStream<CampaignSnapshot> campaignState = enriched
                .keyBy(EnrichedEvent::metricKey)
                .process(new CampaignAggregateFunction(Clock.systemUTC()))
                .name("campaign-state")
                .uid("campaign-state");
        DataStream<OutputRecord> funnel = campaignState
                .map(CampaignSnapshot::toFunnelRecord)
                .returns(TypeInformation.of(new TypeHint<>() {}))
                .name("funnel-projection")
                .uid("funnel-projection");
        DataStream<OutputRecord> revenue = campaignState
                .map(CampaignSnapshot::toRevenueRecord)
                .returns(TypeInformation.of(new TypeHint<>() {}))
                .name("revenue-projection")
                .uid("revenue-projection");
        DataStream<OutputRecord> campaign = campaignState
                .map(CampaignSnapshot::toCampaignRecord)
                .returns(TypeInformation.of(new TypeHint<>() {}))
                .name("campaign-projection")
                .uid("campaign-projection");

        DataStream<OutputRecord> outputs = clean
                .union(sessions, funnel, revenue, campaign, validationDlq);
        KafkaSink<OutputRecord> sink = KafkaSink.<OutputRecord>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(config.transactionPrefix())
                .setRecordSerializer(new OutputRecordSerializationSchema())
                .build();
        outputs.sinkTo(sink)
                .name("dynamic-kafka-sink")
                .uid("dynamic-kafka-sink");
        return outputs;
    }

    private static void configureEnvironment(
            StreamExecutionEnvironment environment,
            JobConfig config) {
        environment.setParallelism(config.parallelism());
        environment.enableCheckpointing(
                config.checkpointInterval().toMillis(), CheckpointingMode.EXACTLY_ONCE);
        environment.getCheckpointConfig().setCheckpointTimeout(Duration.ofMinutes(1).toMillis());
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        environment.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        Configuration runtime = new Configuration();
        runtime.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, config.checkpointUri());
        runtime.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        runtime.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        runtime.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
        environment.configure(runtime);
    }

    private static final class ParseAndValidateFunction
            extends ProcessFunction<RawKafkaRecord, MarketingEvent> {
        private final Clock clock;
        private final EventParser parser = new EventParser();

        private ParseAndValidateFunction(Clock clock) {
            this.clock = clock;
        }

        @Override
        public void processElement(
                RawKafkaRecord raw,
                Context context,
                Collector<MarketingEvent> output) {
            Instant detectedAt = clock.instant();
            EventParser.ParseResult result = parser.parse(raw, detectedAt);
            if (result.valid()) {
                output.collect(result.event());
            } else {
                context.output(VALIDATION_DLQ, result.invalid().toOutputRecord());
            }
        }
    }
}
