package dev.faro.flink;

import dev.faro.core.CaptureEvent.FeatureValueType;
import dev.faro.core.DataClassification;
import dev.faro.core.SerializableFunction;

import java.io.Serializable;

/**
 * Per-feature configuration for ENTITY mode capture.
 *
 * <p>Presence of this config on a feature activates ENTITY mode for that feature.
 * Absence means the feature is captured in AGGREGATE mode only.
 *
 * <p>{@code classification} controls suppression at capture time:
 * {@code PERSONAL} and {@code SENSITIVE} features degrade to AGGREGATE — neither
 * extractor is called and no entity identifier is emitted.
 *
 * <p>{@code sampleRate} is applied before either extractor is called. At 1.0 all
 * entities are captured; at 0.0 no entity events are emitted. This is the primary
 * cost control for high-cardinality pipelines.
 *
 * @param <IN> the record type flowing through the instrumented operator
 */
public final class FaroFeatureConfig<IN> implements Serializable {

    private final SerializableFunction<IN, String> entityKey;
    private final SerializableFunction<IN, Object> featureValue;
    private final FeatureValueType valueType;
    private final DataClassification classification;
    private final double sampleRate;

    private FaroFeatureConfig(
            SerializableFunction<IN, String> entityKey,
            SerializableFunction<IN, Object> featureValue,
            FeatureValueType valueType,
            DataClassification classification,
            double sampleRate) {
        this.entityKey = entityKey;
        this.featureValue = featureValue;
        this.valueType = valueType;
        this.classification = classification;
        this.sampleRate = sampleRate;
    }

    public SerializableFunction<IN, String> getEntityKey() {
        return entityKey;
    }

    public SerializableFunction<IN, Object> getFeatureValue() {
        return featureValue;
    }

    public FeatureValueType getValueType() {
        return valueType;
    }

    public DataClassification getClassification() {
        return classification;
    }

    public double getSampleRate() {
        return sampleRate;
    }

    public boolean isEntityEmitted() {
        return classification == DataClassification.NON_PERSONAL
                || classification == DataClassification.PSEUDONYMOUS;
    }

    public static <IN> Builder<IN> builder() {
        return new Builder<>();
    }

    public static final class Builder<IN> {

        private SerializableFunction<IN, String> entityKey;
        private SerializableFunction<IN, Object> featureValue;
        private FeatureValueType valueType;
        private DataClassification classification;
        private double sampleRate = 1.0;

        private Builder() {}

        public Builder<IN> entityKey(SerializableFunction<IN, String> v) {
            this.entityKey = v;
            return this;
        }

        public Builder<IN> featureValue(SerializableFunction<IN, Object> v) {
            this.featureValue = v;
            return this;
        }

        public Builder<IN> valueType(FeatureValueType v) {
            this.valueType = v;
            return this;
        }

        public Builder<IN> classification(DataClassification v) {
            this.classification = v;
            return this;
        }

        public Builder<IN> sampleRate(double v) {
            this.sampleRate = v;
            return this;
        }

        public FaroFeatureConfig<IN> build() {
            if (entityKey == null) {
                throw new IllegalStateException("FaroFeatureConfig: entityKey is required");
            }
            if (featureValue == null) {
                throw new IllegalStateException("FaroFeatureConfig: featureValue is required");
            }
            if (valueType == null) {
                throw new IllegalStateException("FaroFeatureConfig: valueType is required");
            }
            if (classification == null) {
                throw new IllegalStateException("FaroFeatureConfig: classification is required");
            }
            if (sampleRate < 0.0 || sampleRate > 1.0) {
                throw new IllegalStateException("FaroFeatureConfig: sampleRate must be between 0.0 and 1.0");
            }
            return new FaroFeatureConfig<>(entityKey, featureValue, valueType, classification, sampleRate);
        }
    }
}
