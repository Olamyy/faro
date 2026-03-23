package dev.faro.flink;

import java.io.Serializable;
import java.util.List;

/**
 * Per-pipeline configuration for Faro instrumentation.
 *
 * <p>Infrastructure concerns (where to send events) belong in {@link CaptureEventSink}
 * implementations. {@code pipelineId} and at least one feature name are required.
 */
public final class FaroConfig implements Serializable {

    private final String pipelineId;
    private final List<String> featureNames;

    private FaroConfig(String pipelineId, List<String> featureNames) {
        this.pipelineId = pipelineId;
        this.featureNames = List.copyOf(featureNames);
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public List<String> getFeatureNames() {
        return featureNames;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String pipelineId;
        private List<String> featureNames;

        private Builder() {}

        public Builder pipelineId(String v) {
            this.pipelineId = v;
            return this;
        }

        public Builder features(List<String> v) {
            this.featureNames = v;
            return this;
        }

        public Builder features(String... v) {
            return features(List.of(v));
        }

        public FaroConfig build() {
            if (pipelineId == null || pipelineId.isEmpty()) {
                throw new IllegalStateException("FaroConfig: pipelineId must not be null or empty");
            }
            if (featureNames == null || featureNames.isEmpty()) {
                throw new IllegalStateException("FaroConfig: at least one feature name is required");
            }
            return new FaroConfig(pipelineId, featureNames);
        }
    }
}
