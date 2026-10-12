package io.marketpulse.flink.processing;

import io.marketpulse.flink.model.EnrichedEvent;
import io.marketpulse.flink.model.MarketingEvent;
import io.marketpulse.flink.model.SessionSnapshot;
import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class SessionizeFunction
        extends KeyedProcessFunction<String, EnrichedEvent, SessionSnapshot> {
    private final Duration inactivityGap;
    private final Clock clock;
    private transient ValueState<SessionState> state;

    public SessionizeFunction(Duration inactivityGap, Clock clock) {
        this.inactivityGap = Objects.requireNonNull(inactivityGap, "inactivityGap");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (inactivityGap.isZero() || inactivityGap.isNegative()) {
            throw new IllegalArgumentException("inactivity gap must be positive");
        }
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        state = getRuntimeContext().getState(
                new ValueStateDescriptor<>("web-session", SessionState.class));
    }

    @Override
    public void processElement(
            EnrichedEvent enriched,
            Context context,
            Collector<SessionSnapshot> output) throws Exception {
        MarketingEvent event = enriched.event();
        if (!enriched.eligibleForAggregates()
                || !(event.source().equals("web") || event.source().equals("mobile"))) {
            return;
        }

        SessionState current = state.value();
        if (current == null) {
            current = new SessionState(event.sessionId());
        }
        long oldCloseTimer = current.closeTimer;
        current.add(enriched);
        long newCloseTimer = current.sessionEnd.toEpochMilli() + inactivityGap.toMillis();
        if (oldCloseTimer != 0L && oldCloseTimer != newCloseTimer) {
            context.timerService().deleteEventTimeTimer(oldCloseTimer);
        }
        if (oldCloseTimer != newCloseTimer) {
            context.timerService().registerEventTimeTimer(newCloseTimer);
        }
        current.closeTimer = newCloseTimer;
        state.update(current);
        output.collect(current.snapshot("open", clock.instant()));
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext context,
            Collector<SessionSnapshot> output) throws Exception {
        SessionState current = state.value();
        if (current != null && current.closeTimer == timestamp) {
            output.collect(current.snapshot("closed", clock.instant()));
        }
    }

    public static final class SessionState implements Serializable {
        public String sessionId;
        public String customerId;
        public String anonymousId;
        public String campaignId;
        public Instant customerTime;
        public Instant anonymousTime;
        public Instant campaignTime;
        public UUID customerEventId;
        public UUID anonymousEventId;
        public UUID campaignEventId;
        public Instant sessionStart;
        public Instant sessionEnd;
        public long eventCount;
        public long pageViewCount;
        public long viewItemCount;
        public long addToCartCount;
        public long checkoutCount;
        public long purchaseCount;
        public long grossRevenueMinor;
        public String currency;
        public long lateEventCount;
        public long closeTimer;

        public SessionState() {}

        SessionState(String sessionId) {
            this.sessionId = sessionId;
        }

        void add(EnrichedEvent enriched) {
            MarketingEvent event = enriched.event();
            eventCount++;
            sessionStart = min(sessionStart, event.eventTime());
            sessionEnd = max(sessionEnd, event.eventTime());
            if (!enriched.lateStatus().equals("on_time")) {
                lateEventCount++;
            }
            if (shouldReplace(customerId, event.customerId(), customerTime, customerEventId, event)) {
                customerId = event.customerId();
                customerTime = event.eventTime();
                customerEventId = event.eventId();
            }
            if (shouldReplace(anonymousId, event.anonymousId(), anonymousTime, anonymousEventId, event)) {
                anonymousId = event.anonymousId();
                anonymousTime = event.eventTime();
                anonymousEventId = event.eventId();
            }
            if (shouldReplace(campaignId, event.campaignId(), campaignTime, campaignEventId, event)) {
                campaignId = event.campaignId();
                campaignTime = event.eventTime();
                campaignEventId = event.eventId();
            }
            switch (event.eventName()) {
                case "page_view" -> pageViewCount++;
                case "view_item" -> viewItemCount++;
                case "add_to_cart" -> addToCartCount++;
                case "begin_checkout" -> checkoutCount++;
                case "purchase" -> {
                    purchaseCount++;
                    grossRevenueMinor += event.payload().path("amount_minor").asLong(0L);
                    if (currency == null && event.payload().path("currency").isTextual()) {
                        currency = event.payload().path("currency").textValue();
                    }
                }
                default -> { }
            }
        }

        private static boolean shouldReplace(
                String current,
                String candidate,
                Instant currentTime,
                UUID currentEventId,
                MarketingEvent event) {
            if (candidate == null || candidate.isBlank()) {
                return false;
            }
            return current == null
                    || event.eventTime().isBefore(currentTime)
                    || (event.eventTime().equals(currentTime)
                            && event.eventId().compareTo(currentEventId) < 0);
        }

        SessionSnapshot snapshot(String status, Instant processedAt) {
            return new SessionSnapshot(
                    sessionId, customerId, anonymousId, campaignId, sessionStart, sessionEnd,
                    eventCount, pageViewCount, viewItemCount, addToCartCount, checkoutCount,
                    purchaseCount, grossRevenueMinor, currency, lateEventCount, status, processedAt);
        }

        private static Instant min(Instant left, Instant right) {
            return left == null || right.isBefore(left) ? right : left;
        }

        private static Instant max(Instant left, Instant right) {
            return left == null || right.isAfter(left) ? right : left;
        }
    }
}
