package io.marketpulse.eval;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/** Benchmark-only contract. Production domain transformations remain MP-03. */
public final class Finance {
    public static final ObjectMapper JSON = new ObjectMapper();
    private Finance() {}
    public static class State implements Serializable {
        public long gross;
        public long refund;
        public long maxTime;
        public State() {}
    }
    public static boolean valid(JsonNode row) {
        return row.path("campaign").isIntegralNumber() && row.path("campaign").canConvertToInt()
            && row.path("campaign").intValue() >= 0 && row.path("campaign").intValue() < 64
            && row.path("event_id").isTextual() && !row.path("event_id").textValue().isEmpty()
            && row.path("event_ms").isIntegralNumber() && row.path("event_ms").canConvertToLong() && row.path("event_ms").longValue() >= 0
            && Set.of("order","refund").contains(row.path("kind").asText(""))
            && row.path("amount_minor").isIntegralNumber() && row.path("amount_minor").canConvertToLong()
            && row.path("amount_minor").longValue() >= 0 && row.path("amount_minor").longValue() <= 1000000000;
    }
    public static ObjectNode apply(JsonNode row, State state, boolean duplicate) {
        String classification = "invalid";
        if (valid(row)) {
            if (duplicate) classification = "duplicate";
            else {
                long time = row.path("event_ms").asLong();
                long watermark = state.maxTime - 300000;
                classification = time < watermark - 600000 ? "too_late" : time < watermark ? "late" : "accepted";
                state.maxTime = Math.max(state.maxTime,time);
                if (!classification.equals("too_late")) {
                    if (row.path("kind").asText().equals("order")) state.gross += row.path("amount_minor").asLong();
                    else state.refund += row.path("amount_minor").asLong();
                }
            }
        }
        ObjectNode out = JSON.createObjectNode();
        out.set("seq",row.path("seq"));out.set("campaign",row.path("campaign"));out.set("sent_ms",row.path("sent_ms"));
        out.put("classification",classification);out.put("gross_minor",state.gross);out.put("refund_minor",state.refund);out.put("net_minor",state.gross-state.refund);
        return out;
    }
}
