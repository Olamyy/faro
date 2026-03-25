package dev.faro.flink;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Per-operator configuration for Faro instrumentation.
 *
 * <p>Use {@link #builder()} to construct. Two distinct registration methods exist:
 * <ul>
 *   <li>{@code .features(String...)} — registers features in AGGREGATE mode only. No entity
 *       capture occurs; one event per feature per flush interval is emitted.</li>
 *   <li>{@code .feature(String, FaroFeatureConfig)} — registers a feature in ENTITY mode.
 *       One event per entity per feature per flush interval is emitted, subject to
 *       {@link FaroFeatureConfig#getSampleRate()} and classification-based suppression.</li>
 * </ul>
 *
 * <p>{@code pipelineId} is not part of this config — it is held by the {@link Faro} instance
 * and injected at emit time.
 *
 * <p>The internal map value is nullable: {@code null} means AGGREGATE mode for that feature;
 * a non-null {@link FaroFeatureConfig} means ENTITY mode.
 *
 * @param <IN> the record type flowing through the instrumented operator
 */
public final class FaroConfig<IN> implements Serializable {

    private final Map<String, FaroFeatureConfig<IN>> features;

    private FaroConfig(Map<String, FaroFeatureConfig<IN>> features) {
        this.features = Collections.unmodifiableMap(features);
    }

    public Map<String, FaroFeatureConfig<IN>> getFeatures() {
        return features;
    }

    public static <IN> Builder<IN> builder() {
        return new Builder<>();
    }

    public static final class Builder<IN> {

        private final Map<String, FaroFeatureConfig<IN>> features = new LinkedHashMap<>();

        private Builder() {}

        public Builder<IN> features(String... names) {
            for (String name : names) {
                features.put(name, null);
            }
            return this;
        }

        public Builder<IN> feature(String name, FaroFeatureConfig<IN> config) {
            features.put(name, config);
            return this;
        }

        public FaroConfig<IN> build() {
            if (features.isEmpty()) {
                throw new IllegalStateException("FaroConfig: at least one feature is required");
            }
            return new FaroConfig<>(new LinkedHashMap<>(features));
        }
    }
}
