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
 * Create one instance per pipeline:
 * <pre>{@code
 * FaroSpark faro = new FaroSpark("my-pipeline-id", sinkFactory);
 *
 * Dataset<Row> result = faro.trace("my-op", OperatorType.AGG, config, ds -> ds.groupBy(...).agg(...))
 *                           .apply(inputDataset);
 * }</pre>
 *
 * <p>For Structured Streaming, call {@link #withStreamingContext} once before use and invoke
 * {@link #trace} inside a {@code foreachBatch} handler:
 * <pre>{@code
 * FaroSpark faro = new FaroSpark("my-pipeline-id", sinkFactory)
 *     .withStreamingContext("eventTime", null, listener);
 *
 * inputStream.writeStream()
 *     .foreachBatch((batchDf, batchId) -> {
 *         Dataset<Row> out = faro.trace("my-op", FILTER, config, ds -> ds.filter(...)).apply(batchDf);
 *         out.write().format("delta").save(outputPath);
 *     })
 *     .start();
 * }</pre>
 *
 * <p><b>Important:</b> {@code operatorId} passed to {@link #trace} must be stable across
 * restarts — it is the lineage correlation key, equivalent to Flink's {@code .uid()}.
 */
public final class FaroSpark extends FaroBase {

    private final CaptureEventSink sink;
    private final java.util.concurrent.atomic.AtomicLong lastInvokeMs =
            new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis());

    private volatile String eventTimeColumn;
    private volatile String windowColumn;
    private volatile FaroStreamingListener streamingListener;

    public FaroSpark(String pipelineId, CaptureEventSinkFactory sinkFactory) {
        super(pipelineId, sinkFactory);
        this.sink = sinkFactory.create();
    }

    /**
     * @param eventTimeColumn name of the event-time column in the Dataset (epoch-ms long or
     *                        TimestampType), or {@code null} to skip event-time capture
     * @param windowColumn    name of the Spark window struct column (e.g. {@code "window"}),
     *                        or {@code null} if the operator is not windowed
     * @param listener        the registered {@link FaroStreamingListener}, or {@code null}
     *                        to omit watermark from capture events
     */
    public FaroSpark withStreamingContext(
            String eventTimeColumn,
            String windowColumn,
            FaroStreamingListener listener) {
        this.eventTimeColumn = eventTimeColumn;
        this.windowColumn = windowColumn;
        this.streamingListener = listener;
        return this;
    }

    public <T, U> Function<Dataset<T>, Dataset<U>> trace(
            String operatorId,
            CaptureEvent.OperatorType operatorType,
            FaroConfig<T> config,
            Function<Dataset<T>, Dataset<U>> fn) {
        if (operatorId == null || operatorId.isEmpty()) {
            throw new IllegalArgumentException(
                    "FaroSpark.trace on pipeline '" + getPipelineId()
                    + "': operatorId must not be null or empty");
        }
        return input -> {
            long startMs = System.currentTimeMillis();
            long intervalMs = startMs - lastInvokeMs.getAndSet(startMs);

            long inputCardinality = input.count();
            Dataset<U> output = fn.apply(input);
            long outputCardinality = output.count();

            String processingTime = Instant.ofEpochMilli(startMs).toString();
            String traceId = newTraceId();
            String spanId = newSpanId();

            String watermark = streamingListener != null ? streamingListener.currentWatermark() : null;
            String eventTime = null;
            String eventTimeMin = null;
            String windowStart = null;
            String windowEnd = null;
            long emitIntervalMs = 0;

            if (streamingListener != null) {
                emitIntervalMs = intervalMs;

                if (eventTimeColumn != null) {
                    org.apache.spark.sql.Row etRow = input.agg(
                            org.apache.spark.sql.functions.max(eventTimeColumn).as("mx"),
                            org.apache.spark.sql.functions.min(eventTimeColumn).as("mn")
                    ).first();
                    eventTime = epochMsOrTimestampToIso(etRow, 0);
                    eventTimeMin = epochMsOrTimestampToIso(etRow, 1);
                }

                if (windowColumn != null) {
                    org.apache.spark.sql.Row wRow = input
                            .select(windowColumn + ".start", windowColumn + ".end")
                            .first();
                    if (wRow != null && !wRow.isNullAt(0) && !wRow.isNullAt(1)) {
                        java.sql.Timestamp ws = wRow.getTimestamp(0);
                        java.sql.Timestamp we = wRow.getTimestamp(1);
                        windowStart = Instant.ofEpochMilli(ws.getTime()).toString();
                        windowEnd = Instant.ofEpochMilli(we.getTime()).toString();
                        emitIntervalMs = we.getTime() - ws.getTime();
                    }
                }
            }

            emitAggregateEvents(operatorId, operatorType, config, processingTime,
                    traceId, spanId, inputCardinality, outputCardinality,
                    emitIntervalMs, watermark, eventTime, eventTimeMin, windowStart, windowEnd);

            if (streamingListener != null) {
                emitEntityEventsMapPartitions(operatorId, operatorType, config, input,
                        processingTime, traceId, watermark, eventTime);
            } else {
                emitEntityEvents(operatorId, operatorType, config, input, processingTime, traceId);
            }

            if (sink instanceof DeltaCaptureEventSink deltaSink) {
                deltaSink.flush();
            }

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
            long outputCardinality,
            long emitIntervalMs,
            String watermark,
            String eventTime,
            String eventTimeMin,
            String windowStart,
            String windowEnd) {
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
                    .emitIntervalMs(emitIntervalMs)
                    .watermark(watermark)
                    .eventTime(eventTime)
                    .eventTimeMin(eventTimeMin)
                    .windowStart(windowStart)
                    .windowEnd(windowEnd)
                    .traceId(traceId)
                    .spanId(spanId)
                    .captureDropSinceLast(sink.droppedSinceLastFlush())
                    .build());
        }
    }

    private <T> void emitEntityEvents(
            String operatorId,
            CaptureEvent.OperatorType operatorType,
            FaroConfig<T> config,
            Dataset<T> input,
            String processingTime,
            String traceId) {
        Map<String, FaroFeatureConfig<T>> features = config.getFeatures();

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

    private <T> void emitEntityEventsMapPartitions(
            String operatorId,
            CaptureEvent.OperatorType operatorType,
            FaroConfig<T> config,
            Dataset<T> input,
            String processingTime,
            String traceId,
            String watermark,
            String eventTime) {
        Map<String, FaroFeatureConfig<T>> features = config.getFeatures();

        for (Map.Entry<String, FaroFeatureConfig<T>> entry : features.entrySet()) {
            FaroFeatureConfig<T> fc = entry.getValue();
            if (fc == null) continue;

            boolean suppress = fc.getClassification() == DataClassification.PERSONAL
                    || fc.getClassification() == DataClassification.SENSITIVE;
            if (suppress) continue;

            double sampleRate = fc.getSampleRate();
            java.util.function.Function<T, String> entityKey = fc.getEntityKey();
            java.util.function.Function<T, Object> featureValue = fc.getFeatureValue();
            CaptureEvent.FeatureValueType valueType = fc.getValueType();

            Dataset<scala.Tuple2<String, byte[]>> extracted = input.mapPartitions(
                    (org.apache.spark.api.java.function.MapPartitionsFunction<T, scala.Tuple2<String, byte[]>>) iter -> {
                        List<scala.Tuple2<String, byte[]>> out = new java.util.ArrayList<>();
                        while (iter.hasNext()) {
                            T row = iter.next();
                            if (sampleRate < 1.0
                                    && ThreadLocalRandom.current().nextDouble() >= sampleRate) {
                                continue;
                            }
                            String id = entityKey.apply(row);
                            Object val = featureValue.apply(row);
                            out.add(scala.Tuple2.apply(id, valueToBytes(val, valueType)));
                        }
                        return out.iterator();
                    },
                    org.apache.spark.sql.Encoders.tuple(
                            org.apache.spark.sql.Encoders.STRING(),
                            org.apache.spark.sql.Encoders.BINARY()));

            for (scala.Tuple2<String, byte[]> pair : extracted.collectAsList()) {
                sink.emit(CaptureEvent.builder()
                        .pipelineId(getPipelineId())
                        .operatorId(operatorId)
                        .operatorType(operatorType)
                        .captureMode(CaptureEvent.CaptureMode.ENTITY)
                        .featureName(entry.getKey())
                        .processingTime(processingTime)
                        .inputCardinality(1)
                        .outputCardinality(1)
                        .emitIntervalMs(0)
                        .watermark(watermark)
                        .eventTime(eventTime)
                        .traceId(traceId)
                        .spanId(newSpanId())
                        .entityId(pair._1())
                        .featureValueType(valueType)
                        .featureValue(pair._2())
                        .captureDropSinceLast(false)
                        .build());
            }
        }
    }

    private static String epochMsOrTimestampToIso(org.apache.spark.sql.Row row, int index) {
        if (row == null || row.isNullAt(index)) return null;
        Object val = row.get(index);
        if (val instanceof java.sql.Timestamp ts) {
            return Instant.ofEpochMilli(ts.getTime()).toString();
        }
        if (val instanceof Long l) {
            return Instant.ofEpochMilli(l).toString();
        }
        return null;
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
