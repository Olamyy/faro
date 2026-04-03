package dev.faro.spark;

import dev.faro.core.CaptureEvent;
import dev.faro.core.CaptureEventSink;
import dev.faro.core.CaptureEventSinkFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Events are buffered in memory and written in a single Delta transaction on each
 * {@link #flush()} call. In batch mode, {@link #close()} triggers the flush. In Structured
 * Streaming, call {@link #flush()} at the end of each {@code foreachBatch} invocation.
 *
 * <p>The {@code SparkSession} must be configured with the Delta extensions:
 * <pre>{@code
 * SparkSession.builder()
 *     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
 *     .config("spark.sql.catalog.spark_catalog",
 *             "org.apache.spark.sql.delta.catalog.DeltaCatalog")
 *     ...
 * }</pre>
 */
public final class DeltaCaptureEventSink implements CaptureEventSink {

    private static final StructType SCHEMA = new StructType()
            .add("schemaVersion",      DataTypes.StringType,  true)
            .add("pipelineId",         DataTypes.StringType,  true)
            .add("operatorId",         DataTypes.StringType,  true)
            .add("operatorType",       DataTypes.StringType,  true)
            .add("featureName",        DataTypes.StringType,  true)
            .add("captureMode",        DataTypes.StringType,  true)
            .add("eventTime",          DataTypes.StringType,  true)
            .add("eventTimeMin",       DataTypes.StringType,  true)
            .add("processingTime",     DataTypes.StringType,  true)
            .add("watermark",          DataTypes.StringType,  true)
            .add("windowStart",        DataTypes.StringType,  true)
            .add("windowEnd",          DataTypes.StringType,  true)
            .add("lateEventCount",     DataTypes.LongType,    true)
            .add("lateTrackingMode",   DataTypes.StringType,  true)
            .add("inputCardinality",   DataTypes.LongType,    false)
            .add("outputCardinality",  DataTypes.LongType,    false)
            .add("emitIntervalMs",     DataTypes.LongType,    false)
            .add("timerFiredCount",    DataTypes.LongType,    true)
            .add("asyncPendingCount",  DataTypes.LongType,    true)
            .add("patternMatchCount",  DataTypes.LongType,    true)
            .add("joinInputSide",      DataTypes.StringType,  true)
            .add("joinLowerBoundMs",   DataTypes.LongType,    true)
            .add("joinUpperBoundMs",   DataTypes.LongType,    true)
            .add("joinMatchRate",      DataTypes.DoubleType,  true)
            .add("valueCount",         DataTypes.LongType,    true)
            .add("valueMin",           DataTypes.DoubleType,  true)
            .add("valueMax",           DataTypes.DoubleType,  true)
            .add("valueMean",          DataTypes.DoubleType,  true)
            .add("valueP50",           DataTypes.DoubleType,  true)
            .add("valueP95",           DataTypes.DoubleType,  true)
            .add("nullCount",          DataTypes.LongType,    true)
            .add("entityId",           DataTypes.StringType,  true)
            .add("featureValue",       DataTypes.BinaryType,  true)
            .add("featureValueType",   DataTypes.StringType,  true)
            .add("upstreamSource",     DataTypes.StringType,  true)
            .add("upstreamSystem",     DataTypes.StringType,  true)
            .add("traceId",            DataTypes.StringType,  true)
            .add("spanId",             DataTypes.StringType,  true)
            .add("parentSpanId",       DataTypes.StringType,  true)
            .add("captureDropSinceLast", DataTypes.BooleanType, false);

    private final SparkSession spark;
    private final String tablePath;
    private final List<CaptureEvent> buffer = new ArrayList<>();

    public DeltaCaptureEventSink(SparkSession spark, String tablePath) {
        this.spark = spark;
        this.tablePath = tablePath;
    }

    public static CaptureEventSinkFactory factory(SparkSession spark, String tablePath) {
        return () -> new DeltaCaptureEventSink(spark, tablePath);
    }

    @Override
    public void emit(CaptureEvent event) {
        buffer.add(event);
    }

    public void flush() {
        if (buffer.isEmpty()) return;
        List<Row> rows = new ArrayList<>(buffer.size());
        for (CaptureEvent e : buffer) {
            rows.add(toRow(e));
        }
        spark.createDataFrame(rows, SCHEMA)
                .write()
                .format("delta")
                .mode("append")
                .save(tablePath);
        buffer.clear();
    }

    @Override
    public void close() {
        flush();
    }

    private static Row toRow(CaptureEvent e) {
        return RowFactory.create(
                e.getSchemaVersion(),
                e.getPipelineId(),
                e.getOperatorId(),
                enumName(e.getOperatorType()),
                e.getFeatureName(),
                enumName(e.getCaptureMode()),
                e.getEventTime(),
                e.getEventTimeMin(),
                e.getProcessingTime(),
                e.getWatermark(),
                e.getWindowStart(),
                e.getWindowEnd(),
                e.getLateEventCount(),
                enumName(e.getLateTrackingMode()),
                e.getInputCardinality(),
                e.getOutputCardinality(),
                e.getEmitIntervalMs(),
                e.getTimerFiredCount(),
                e.getAsyncPendingCount(),
                e.getPatternMatchCount(),
                enumName(e.getJoinInputSide()),
                e.getJoinLowerBoundMs(),
                e.getJoinUpperBoundMs(),
                e.getJoinMatchRate(),
                e.getValueCount(),
                e.getValueMin(),
                e.getValueMax(),
                e.getValueMean(),
                e.getValueP50(),
                e.getValueP95(),
                e.getNullCount(),
                e.getEntityId(),
                e.getFeatureValue(),
                enumName(e.getFeatureValueType()),
                e.getUpstreamSource(),
                e.getUpstreamSystem(),
                e.getTraceId(),
                e.getSpanId(),
                e.getParentSpanId(),
                e.isCaptureDropSinceLast()
        );
    }

    private static String enumName(Enum<?> e) {
        return e == null ? null : e.name();
    }
}
