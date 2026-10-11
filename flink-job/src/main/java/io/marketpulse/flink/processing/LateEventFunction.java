package io.marketpulse.flink.processing;

import io.marketpulse.flink.model.EnrichedEvent;
import io.marketpulse.flink.model.MarketingEvent;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public final class LateEventFunction extends ProcessFunction<MarketingEvent, EnrichedEvent> {
    private final Duration correctionHorizon;
    private final Clock clock;
    private transient Counter sourceMarkedLate;
    private transient Counter watermarkLate;
    private transient Counter aggregateIneligible;

    public LateEventFunction(Duration correctionHorizon, Clock clock) {
        this.correctionHorizon = Objects.requireNonNull(correctionHorizon, "correctionHorizon");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (correctionHorizon.isNegative()) {
            throw new IllegalArgumentException("correction horizon must not be negative");
        }
    }

    @Override
    public void open(OpenContext openContext) {
        sourceMarkedLate = getRuntimeContext().getMetricGroup().counter("source_marked_late");
        watermarkLate = getRuntimeContext().getMetricGroup().counter("watermark_late");
        aggregateIneligible = getRuntimeContext().getMetricGroup().counter("aggregate_ineligible");
    }

    @Override
    public void processElement(
            MarketingEvent event,
            Context context,
            Collector<EnrichedEvent> output) {
        long watermark = context.timerService().currentWatermark();
        boolean behindWatermark = event.eventTime().toEpochMilli() < watermark;
        String lateStatus;
        if (behindWatermark) {
            lateStatus = "watermark_late";
            watermarkLate.inc();
        } else if (event.sourceMarkedLate()) {
            lateStatus = "source_marked_late";
            sourceMarkedLate.inc();
        } else {
            lateStatus = "on_time";
        }

        boolean eligible = event.eventTime().toEpochMilli()
                >= watermark - correctionHorizon.toMillis();
        if (!eligible) {
            aggregateIneligible.inc();
        }
        output.collect(new EnrichedEvent(event, lateStatus, eligible, clock.instant()));
    }
}
