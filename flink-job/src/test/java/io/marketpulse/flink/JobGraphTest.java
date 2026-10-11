package io.marketpulse.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.marketpulse.flink.config.JobConfig;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class JobGraphTest {
    @Test
    void graphContainsStableNamedOperatorsAndOneDynamicSink() {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        JobConfig config = JobConfig.fromArgs(new String[] {"--parallelism", "1"});

        MarketPulseFlinkJob.build(environment, config);

        Set<String> names = environment.getStreamGraph().getStreamNodes().stream()
                .map(node -> node.getOperatorName())
                .collect(Collectors.toSet());
        assertTrue(names.stream().anyMatch(name -> name.endsWith("raw-kafka-source")), names::toString);
        assertTrue(names.containsAll(Set.of(
                "parse-and-validate",
                "validation-dlq-projection",
                "assign-watermarks",
                "deduplicate-events",
                "classify-late-events",
                "clean-event-projection",
                "session-upserts",
                "campaign-state",
                "funnel-projection",
                "revenue-projection",
                "campaign-projection")), names::toString);
        assertEquals(1L, names.stream().filter("dynamic-kafka-sink: Writer"::equals).count());
        assertEquals(1L, names.stream().filter("dynamic-kafka-sink: Committer"::equals).count());
    }

    @Test
    void graphEnablesRequiredCheckpointAndRecoverySettings() {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        JobConfig config = JobConfig.fromArgs(new String[] {"--parallelism", "1"});

        MarketPulseFlinkJob.build(environment, config);

        assertTrue(environment.getCheckpointConfig().isCheckpointingEnabled());
        assertEquals(5_000L, environment.getCheckpointConfig().getCheckpointInterval());
        assertEquals(60_000L, environment.getCheckpointConfig().getCheckpointTimeout());
        assertEquals(1, environment.getCheckpointConfig().getMaxConcurrentCheckpoints());
        assertEquals(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION,
                environment.getCheckpointConfig().getExternalizedCheckpointRetention());
        assertEquals(1, environment.getParallelism());
    }
}
