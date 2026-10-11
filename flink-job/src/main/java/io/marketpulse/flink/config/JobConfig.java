package io.marketpulse.flink.config;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import org.apache.flink.util.ParameterTool;

public record JobConfig(
        String bootstrapServers,
        String groupId,
        String checkpointUri,
        Duration checkpointInterval,
        Duration watermarkOutOfOrderness,
        Duration sourceIdleness,
        Duration correctionHorizon,
        Duration sessionGap,
        Duration dedupTtl,
        int parallelism,
        String transactionPrefix,
        String jobName) implements Serializable {

    public JobConfig {
        requireText(bootstrapServers, "bootstrap servers");
        requireText(groupId, "group id");
        requireText(checkpointUri, "checkpoint URI");
        positive(checkpointInterval, "checkpoint interval");
        positive(watermarkOutOfOrderness, "watermark");
        positive(sourceIdleness, "source idleness");
        positive(correctionHorizon, "correction horizon");
        positive(sessionGap, "session gap");
        positive(dedupTtl, "dedup TTL");
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be positive");
        }
        requireText(transactionPrefix, "transaction prefix");
        requireText(jobName, "job name");
    }

    public static JobConfig fromArgs(String[] args) {
        ParameterTool values = ParameterTool.fromArgs(args);
        return new JobConfig(
                values.get("bootstrap-servers", "kafka:29092"),
                values.get("group-id", "marketpulse-flink-v1"),
                values.get("checkpoint-uri", "file:///opt/flink/checkpoints"),
                duration(values, "checkpoint-interval-ms", 5_000L),
                duration(values, "watermark-ms", 300_000L),
                duration(values, "idleness-ms", 60_000L),
                duration(values, "correction-horizon-ms", 600_000L),
                duration(values, "session-gap-ms", 1_800_000L),
                duration(values, "dedup-ttl-ms", 604_800_000L),
                values.getInt("parallelism", 2),
                values.get("transaction-prefix", "marketpulse-flink-v1-"),
                values.get("job-name", "marketpulse-stream-v1"));
    }

    private static Duration duration(ParameterTool values, String key, long defaultMillis) {
        return Duration.ofMillis(values.getLong(key, defaultMillis));
    }

    private static void positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
