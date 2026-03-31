package dev.faro.spark;

import dev.faro.core.CaptureEvent;
import dev.faro.core.CaptureEventSink;
import dev.faro.core.CaptureEventSinkFactory;
import dev.faro.core.DataClassification;
import dev.faro.core.FaroBase;
import dev.faro.core.FaroConfig;
import dev.faro.core.FaroFeatureConfig;
import org.apache.spark.sql.Dataset;

import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * Entry point for Faro instrumentation in Spark Structured Streaming pipelines.
 * Create one instance per pipeline:
 * <pre>{@code
 * SparkFaro faro = new SparkFaro("my-pipeline-id", sinkFactory);
 *
 * Dataset<Row> result = faro.trace("my-op", OperatorType.AGG, config, ds -> ds.groupBy(...).agg(...));
 * }</pre>
 *
 * <p><b>Important:</b> {@code operatorId} passed to {@link #trace} must be stable across
 * restarts — it is the lineage correlation key, equivalent to Flink's {@code .uid()}.
 */
public final class SparkFaro extends FaroBase {

    private final CaptureEventSink sink;

    public SparkFaro(String pipelineId, CaptureEventSinkFactory sinkFactory) {
        super(pipelineId, sinkFactory);
        this.sink = sinkFactory.create();
    }

    /**
     * Wraps a user-supplied Dataset transform with Faro AGGREGATE (and optionally ENTITY)
     * capture. Events are delivered to the configured sink automatically after each call.
     *
     * @param operatorId stable identifier for this operator; must be non-null and non-empty
     * @param operatorType logical operator type for lineage classification
     * @param config per-operator feature configuration
     * @param fn the user-supplied Dataset transform
     */
    public <T, U> Function<Dataset<T>, Dataset<U>> trace(
            String operatorId,
            CaptureEvent.OperatorType operatorType,
            FaroConfig<T> config,
            Function<Dataset<T>, Dataset<U>> fn) {
        if (operatorId == null || operatorId.isEmpty()) {
            throw new IllegalArgumentException(
                    "SparkFaro.trace on pipeline '" + getPipelineId()
                    + "': operatorId must not be null or empty");
        }
        return input -> {
            long inputCardinality = input.count();
            Dataset<U> output = fn.apply(input);
            long outputCardinality = output.count();

            String processingTime = Instant.now().toString();
            String traceId = newTraceId();
            String spanId = newSpanId();

            emitAggregateEvents(operatorId, operatorType, config, processingTime,
                    traceId, spanId, inputCardinality, outputCardinality);

            emitEntityEvents(operatorId, operatorType, config, input, processingTime, traceId);

            return output;
        };
    }

    public void close() {
        sink.close();
    }

    private <T> void emitAggregateEvents(
            String operatorId,
            CaptureEvent.OperatorType operatorType,
            FaroConfig<T> config,
            String processingTime,
            String traceId,
            String spanId,
            long inputCardinality,
            long outputCardinality) {
        for (String featureName : config.getFeatures().keySet()) {
            sink.emit(CaptureEvent.builder()
                    .pipelineId(getPipelineId())
                    .operatorId(operatorId)
                    .operatorType(operatorType)
                    .captureMode(CaptureEvent.CaptureMode.AGGREGATE)
                    .featureName(featureName)
                    .processingTime(processingTime)
                    .inputCardinality(inputCardinality)
                    .outputCardinality(outputCardinality)
                    .emitIntervalMs(0)
                    .traceId(traceId)
                    .spanId(spanId)
                    .captureDropSinceLast(sink.droppedSinceLastFlush())
                    .build());
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <T> void emitEntityEvents(
            String operatorId,
            CaptureEvent.OperatorType operatorType,
            FaroConfig<T> config,
            Dataset<T> input,
            String processingTime,
            String traceId) {
        Map<String, FaroFeatureConfig<T>> features =
                config.getFeatures();

        for (Map.Entry<String, FaroFeatureConfig<T>> entry : features.entrySet()) {
            FaroFeatureConfig<T> fc = entry.getValue();
            if (fc == null) continue;

            boolean suppress = fc.getClassification() == DataClassification.PERSONAL
                    || fc.getClassification() == DataClassification.SENSITIVE;

            List<T> rows = input.collectAsList();
            for (T row : rows) {
                if (fc.getSampleRate() < 1.0
                        && ThreadLocalRandom.current().nextDouble() >= fc.getSampleRate()) {
                    continue;
                }

                CaptureEvent.Builder builder = CaptureEvent.builder()
                        .pipelineId(getPipelineId())
                        .operatorId(operatorId)
                        .operatorType(operatorType)
                        .featureName(entry.getKey())
                        .processingTime(processingTime)
                        .inputCardinality(1)
                        .outputCardinality(1)
                        .emitIntervalMs(0)
                        .traceId(traceId)
                        .spanId(newSpanId())
                        .captureDropSinceLast(false);

                if (suppress) {
                    builder.captureMode(CaptureEvent.CaptureMode.AGGREGATE);
                } else {
                    String entityId = fc.getEntityKey().apply(row);
                    Object value = fc.getFeatureValue().apply(row);
                    builder.captureMode(CaptureEvent.CaptureMode.ENTITY)
                            .entityId(entityId)
                            .featureValueType(fc.getValueType())
                            .featureValue(valueToBytes(value, fc.getValueType()));
                }

                sink.emit(builder.build());
            }
        }
    }

    private static byte[] valueToBytes(Object value, CaptureEvent.FeatureValueType type) {
        if (value == null) return null;
        return switch (type) {
            case SCALAR_DOUBLE -> doubleToBytes(((Number) value).doubleValue());
            case SCALAR_LONG -> longToBytes(((Number) value).longValue());
            case SCALAR_STRING -> ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            case VECTOR_FLOAT, STRUCT -> value instanceof byte[] b ? b : null;
        };
    }

    private static byte[] doubleToBytes(double v) {
        return longToBytes(Double.doubleToLongBits(v));
    }

    private static byte[] longToBytes(long v) {
        byte[] b = new byte[8];
        for (int i = 7; i >= 0; i--) {
            b[i] = (byte) (v & 0xFF);
            v >>= 8;
        }
        return b;
    }

    private static String newTraceId() {
        byte[] bytes = new byte[16];
        ThreadLocalRandom.current().nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    private static String newSpanId() {
        byte[] bytes = new byte[8];
        ThreadLocalRandom.current().nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }
}
