package dev.faro.flink;

import java.util.List;

/**
 * Per-pipeline configuration supplied to {@link FaroSink} at construction time.
 *
 * <p>Specifies the pipeline identity, the features computed at the sink, and the
 * capture interval. Infrastructure concerns (where to send events) belong in
 * {@link CaptureEventSink} implementations, not here.
 *
 * <p>Construct via {@link Builder}. {@code pipelineId} and at least one feature are required.
 */
public final class FaroConfig {

    private final String pipelineId;
    private final List<String> featureNames;
    private final long flushIntervalMs;

    private FaroConfig(String pipelineId, List<String> featureNames, long flushIntervalMs) {
        this.pipelineId = pipelineId;
        this.featureNames = List.copyOf(featureNames);
        this.flushIntervalMs = flushIntervalMs;
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public List<String> getFeatureNames() {
        return featureNames;
    }

    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private static final long DEFAULT_FLUSH_INTERVAL_MS = 30_000L;

        private String pipelineId;
        private List<String> featureNames;
        private long flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;

        private Builder() {}

        public Builder pipelineId(String v) {
            this.pipelineId = v;
            return this;
        }

        /**
         * Feature names computed at this sink. For a multi-feature sink, list all features
         * in the order they will be reported. At least one name is required.
         */
        public Builder features(List<String> v) {
            this.featureNames = v;
            return this;
        }

        public Builder features(String... v) {
            return features(List.of(v));
        }

        /**
         * Capture flush interval in milliseconds. Defaults to 30 000 ms.
         * Cardinality fields in emitted events cover exactly this interval.
         */
        public Builder flushIntervalMs(long v) {
            this.flushIntervalMs = v;
            return this;
        }

        /**
         * @throws IllegalStateException if {@code pipelineId} is null/empty or no features are specified.
         */
        public FaroConfig build() {
            if (pipelineId == null || pipelineId.isEmpty()) {
                throw new IllegalStateException("FaroConfig: pipelineId must not be null or empty");
            }
            if (featureNames == null || featureNames.isEmpty()) {
                throw new IllegalStateException("FaroConfig: at least one feature name is required");
            }
            if (flushIntervalMs <= 0) {
                throw new IllegalStateException("FaroConfig: flushIntervalMs must be positive");
            }
            return new FaroConfig(pipelineId, featureNames, flushIntervalMs);
        }
    }
}
