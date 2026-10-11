package io.marketpulse.flink.processing;

import io.marketpulse.flink.model.MarketingEvent;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class DeduplicateFunction
        extends KeyedProcessFunction<UUID, MarketingEvent, MarketingEvent> {
    private final Duration ttl;
    private transient ValueState<Boolean> seen;
    private transient Counter accepted;
    private transient Counter duplicates;

    public DeduplicateFunction(Duration ttl) {
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("ttl must be positive");
        }
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("seen-event-id", Boolean.class);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(ttl)
                .updateTtlOnCreateAndWrite()
                .neverReturnExpired()
                .useProcessingTime()
                .build());
        seen = getRuntimeContext().getState(descriptor);
        accepted = getRuntimeContext().getMetricGroup().counter("accepted");
        duplicates = getRuntimeContext().getMetricGroup().counter("duplicates");
    }

    @Override
    public void processElement(
            MarketingEvent event,
            Context context,
            Collector<MarketingEvent> output) throws Exception {
        if (seen.value() != null) {
            duplicates.inc();
            return;
        }
        seen.update(Boolean.TRUE);
        accepted.inc();
        output.collect(event);
    }
}
