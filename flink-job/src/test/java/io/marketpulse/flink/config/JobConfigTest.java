package io.marketpulse.flink.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class JobConfigTest {
    @Test
    void usesStableProductionDefaults() {
        JobConfig config = JobConfig.fromArgs(new String[0]);
        assertEquals("kafka:29092", config.bootstrapServers());
        assertEquals("marketpulse-flink-v1", config.groupId());
        assertEquals("file:///opt/flink/checkpoints", config.checkpointUri());
        assertEquals(Duration.ofSeconds(5), config.checkpointInterval());
        assertEquals(Duration.ofMinutes(5), config.watermarkOutOfOrderness());
        assertEquals(Duration.ofMinutes(1), config.sourceIdleness());
        assertEquals(Duration.ofMinutes(10), config.correctionHorizon());
        assertEquals(Duration.ofMinutes(30), config.sessionGap());
        assertEquals(Duration.ofDays(7), config.dedupTtl());
        assertEquals(2, config.parallelism());
        assertEquals("marketpulse-flink-v1-", config.transactionPrefix());
        assertEquals("marketpulse-stream-v1", config.jobName());
    }

    @Test
    void parsesEveryRuntimeOverride() {
        JobConfig config = JobConfig.fromArgs(new String[] {
                "--bootstrap-servers", "localhost:9092",
                "--group-id", "group-x",
                "--checkpoint-uri", "file:///tmp/checkpoints",
                "--checkpoint-interval-ms", "2000",
                "--watermark-ms", "3000",
                "--idleness-ms", "4000",
                "--correction-horizon-ms", "5000",
                "--session-gap-ms", "6000",
                "--dedup-ttl-ms", "7000",
                "--parallelism", "1",
                "--transaction-prefix", "tx-x-",
                "--job-name", "job-x"
        });
        assertEquals("localhost:9092", config.bootstrapServers());
        assertEquals("group-x", config.groupId());
        assertEquals("file:///tmp/checkpoints", config.checkpointUri());
        assertEquals(Duration.ofMillis(2000), config.checkpointInterval());
        assertEquals(Duration.ofMillis(3000), config.watermarkOutOfOrderness());
        assertEquals(Duration.ofMillis(4000), config.sourceIdleness());
        assertEquals(Duration.ofMillis(5000), config.correctionHorizon());
        assertEquals(Duration.ofMillis(6000), config.sessionGap());
        assertEquals(Duration.ofMillis(7000), config.dedupTtl());
        assertEquals(1, config.parallelism());
        assertEquals("tx-x-", config.transactionPrefix());
        assertEquals("job-x", config.jobName());
    }

    @Test
    void rejectsNonPositiveDurationsAndParallelism() {
        assertThrows(IllegalArgumentException.class,
                () -> JobConfig.fromArgs(new String[] {"--checkpoint-interval-ms", "0"}));
        assertThrows(IllegalArgumentException.class,
                () -> JobConfig.fromArgs(new String[] {"--parallelism", "-1"}));
    }
}
