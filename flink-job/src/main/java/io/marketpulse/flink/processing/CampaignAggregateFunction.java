package io.marketpulse.flink.processing;

import io.marketpulse.flink.model.CampaignSnapshot;
import io.marketpulse.flink.model.EnrichedEvent;
import io.marketpulse.flink.model.MarketingEvent;
import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class CampaignAggregateFunction
        extends KeyedProcessFunction<String, EnrichedEvent, CampaignSnapshot> {
    private final Clock clock;
    private transient ValueState<CampaignState> state;
    private transient MapState<String, Boolean> reach;

    public CampaignAggregateFunction(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        state = getRuntimeContext().getState(
                new ValueStateDescriptor<>("campaign-metrics", CampaignState.class));
        reach = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("campaign-stage-reach", String.class, Boolean.class));
    }

    @Override
    public void processElement(
            EnrichedEvent enriched,
            Context context,
            Collector<CampaignSnapshot> output) throws Exception {
        MarketingEvent event = enriched.event();
        if (!enriched.eligibleForAggregates() || event.campaignKey() == null) {
            return;
        }

        CampaignState current = state.value();
        if (current == null) {
            current = new CampaignState(event.campaignKey(), enriched.eventDate());
        }
        current.maxEventTime = max(current.maxEventTime, event.eventTime());
        if (event.source().equals("web") || event.source().equals("mobile")) {
            addFunnel(event, current);
        } else if (event.source().equals("commerce")) {
            addRevenue(event, current);
        }
        state.update(current);
        output.collect(current.snapshot(clock.instant()));
    }

    private void addFunnel(MarketingEvent event, CampaignState current) throws Exception {
        if (mark("session|" + event.sessionId())) {
            current.sessions++;
        }
        String stage = switch (event.eventName()) {
            case "page_view" -> "page_view";
            case "view_item" -> "view_item";
            case "add_to_cart" -> "add_to_cart";
            case "begin_checkout" -> "checkout";
            case "purchase" -> "purchase";
            default -> null;
        };
        if (stage == null || !mark("stage|" + stage + "|" + event.sessionId())) {
            return;
        }
        switch (stage) {
            case "page_view" -> current.pageViewSessions++;
            case "view_item" -> current.viewItemSessions++;
            case "add_to_cart" -> current.addToCartSessions++;
            case "checkout" -> current.checkoutSessions++;
            case "purchase" -> current.purchaseSessions++;
            default -> throw new IllegalStateException("unknown stage " + stage);
        }
    }

    private void addRevenue(MarketingEvent event, CampaignState current) {
        if (event.eventName().equals("order_created")) {
            current.orderCount++;
            current.grossRevenueMinor += event.payload().path("gross_amount_minor").asLong(0L);
        } else if (event.eventName().equals("order_refunded")) {
            current.refundCount++;
            current.refundAmountMinor += event.payload().path("refund_amount_minor").asLong(0L);
        }
        if (current.currency == null && event.payload().path("currency").isTextual()) {
            current.currency = event.payload().path("currency").textValue();
        }
    }

    private boolean mark(String key) throws Exception {
        if (reach.contains(key)) {
            return false;
        }
        reach.put(key, Boolean.TRUE);
        return true;
    }

    private static Instant max(Instant left, Instant right) {
        return left == null || right.isAfter(left) ? right : left;
    }

    public static final class CampaignState implements Serializable {
        public String campaignId;
        public LocalDate eventDate;
        public long sessions;
        public long pageViewSessions;
        public long viewItemSessions;
        public long addToCartSessions;
        public long checkoutSessions;
        public long purchaseSessions;
        public long orderCount;
        public long refundCount;
        public long grossRevenueMinor;
        public long refundAmountMinor;
        public String currency;
        public Instant maxEventTime;

        public CampaignState() {}

        CampaignState(String campaignId, LocalDate eventDate) {
            this.campaignId = campaignId;
            this.eventDate = eventDate;
        }

        CampaignSnapshot snapshot(Instant processedAt) {
            return new CampaignSnapshot(
                    campaignId + "|" + eventDate,
                    campaignId,
                    eventDate,
                    sessions,
                    pageViewSessions,
                    viewItemSessions,
                    addToCartSessions,
                    checkoutSessions,
                    purchaseSessions,
                    orderCount,
                    refundCount,
                    grossRevenueMinor,
                    refundAmountMinor,
                    grossRevenueMinor - refundAmountMinor,
                    currency,
                    maxEventTime,
                    processedAt);
        }
    }
}
