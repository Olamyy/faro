package dev.faro.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Faro capture event — the unit of observability emitted by every instrumented operator.
 *
 * <p>Immutable. Construct via {@link Builder}. Required fields are enforced at build time.
 * The Avro schema is loaded from {@code faro-event-v1.avsc} in the classpath resources.
 *
 * <p>Wire format: Avro on {@code faro.capture.events}. JSON available via {@link #toJson()}.
 * Schema version: 1.0.0 (frozen 2026-03-21).
 */
public final class CaptureEvent {

    public enum OperatorType {
        SOURCE, FILTER, MAP, JOIN, WINDOW, AGG, BROADCAST, CEP, SINK
    }

    public enum CaptureMode {
        AGGREGATE, ENTITY
    }

    public enum LateTrackingMode {
        SIDE_OUTPUT, UNKNOWN
    }

    public enum JoinInputSide {
        LEFT, RIGHT, OUTPUT
    }

    public enum FeatureValueType {
        SCALAR_DOUBLE, SCALAR_LONG, SCALAR_STRING, VECTOR_FLOAT, STRUCT
    }

    private final String schemaVersion;
    private final String pipelineId;
    private final String operatorId;
    private final OperatorType operatorType;
    private final String featureName;
    private final CaptureMode captureMode;
    private final String eventTime;
    private final String eventTimeMin;
    private final String processingTime;
    private final String watermark;
    private final String windowStart;
    private final String windowEnd;
    private final Long lateEventCount;
    private final LateTrackingMode lateTrackingMode;
    private final long inputCardinality;
    private final long outputCardinality;
    private final long emitIntervalMs;
    private final Long timerFiredCount;
    private final Long asyncPendingCount;
    private final Long patternMatchCount;
    private final JoinInputSide joinInputSide;
    private final Long joinLowerBoundMs;
    private final Long joinUpperBoundMs;
    private final Double joinMatchRate;
    private final Long valueCount;
    private final Double valueMin;
    private final Double valueMax;
    private final Double valueMean;
    private final Double valueP50;
    private final Double valueP95;
    private final Long nullCount;
    private final String entityId;
    private final byte[] featureValue;
    private final FeatureValueType featureValueType;
    private final String upstreamSource;
    private final String upstreamSystem;
    private final String traceId;
    private final String spanId;
    private final String parentSpanId;
    private final boolean captureDropSinceLast;

    @JsonCreator
    private CaptureEvent(
            @JsonProperty("schema_version") String schemaVersion,
            @JsonProperty("pipeline_id") String pipelineId,
            @JsonProperty("operator_id") String operatorId,
            @JsonProperty("operator_type") OperatorType operatorType,
            @JsonProperty("feature_name") String featureName,
            @JsonProperty("capture_mode") CaptureMode captureMode,
            @JsonProperty("event_time") String eventTime,
            @JsonProperty("event_time_min") String eventTimeMin,
            @JsonProperty("processing_time") String processingTime,
            @JsonProperty("watermark") String watermark,
            @JsonProperty("window_start") String windowStart,
            @JsonProperty("window_end") String windowEnd,
            @JsonProperty("late_event_count") Long lateEventCount,
            @JsonProperty("late_tracking_mode") LateTrackingMode lateTrackingMode,
            @JsonProperty("input_cardinality") long inputCardinality,
            @JsonProperty("output_cardinality") long outputCardinality,
            @JsonProperty("emit_interval_ms") long emitIntervalMs,
            @JsonProperty("timer_fired_count") Long timerFiredCount,
            @JsonProperty("async_pending_count") Long asyncPendingCount,
            @JsonProperty("pattern_match_count") Long patternMatchCount,
            @JsonProperty("join_input_side") JoinInputSide joinInputSide,
            @JsonProperty("join_lower_bound_ms") Long joinLowerBoundMs,
            @JsonProperty("join_upper_bound_ms") Long joinUpperBoundMs,
            @JsonProperty("join_match_rate") Double joinMatchRate,
            @JsonProperty("value_count") Long valueCount,
            @JsonProperty("value_min") Double valueMin,
            @JsonProperty("value_max") Double valueMax,
            @JsonProperty("value_mean") Double valueMean,
            @JsonProperty("value_p50") Double valueP50,
            @JsonProperty("value_p95") Double valueP95,
            @JsonProperty("null_count") Long nullCount,
            @JsonProperty("entity_id") String entityId,
            @JsonProperty("feature_value") byte[] featureValue,
            @JsonProperty("feature_value_type") FeatureValueType featureValueType,
            @JsonProperty("upstream_source") String upstreamSource,
            @JsonProperty("upstream_system") String upstreamSystem,
            @JsonProperty("trace_id") String traceId,
            @JsonProperty("span_id") String spanId,
            @JsonProperty("parent_span_id") String parentSpanId,
            @JsonProperty("capture_drop_since_last") boolean captureDropSinceLast) {
        this.schemaVersion = schemaVersion;
        this.pipelineId = pipelineId;
        this.operatorId = operatorId;
        this.operatorType = operatorType;
        this.featureName = featureName;
        this.captureMode = captureMode;
        this.eventTime = eventTime;
        this.eventTimeMin = eventTimeMin;
        this.processingTime = processingTime;
        this.watermark = watermark;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.lateEventCount = lateEventCount;
        this.lateTrackingMode = lateTrackingMode;
        this.inputCardinality = inputCardinality;
        this.outputCardinality = outputCardinality;
        this.emitIntervalMs = emitIntervalMs;
        this.timerFiredCount = timerFiredCount;
        this.asyncPendingCount = asyncPendingCount;
        this.patternMatchCount = patternMatchCount;
        this.joinInputSide = joinInputSide;
        this.joinLowerBoundMs = joinLowerBoundMs;
        this.joinUpperBoundMs = joinUpperBoundMs;
        this.joinMatchRate = joinMatchRate;
        this.valueCount = valueCount;
        this.valueMin = valueMin;
        this.valueMax = valueMax;
        this.valueMean = valueMean;
        this.valueP50 = valueP50;
        this.valueP95 = valueP95;
        this.nullCount = nullCount;
        this.entityId = entityId;
        this.featureValue = featureValue;
        this.featureValueType = featureValueType;
        this.upstreamSource = upstreamSource;
        this.upstreamSystem = upstreamSystem;
        this.traceId = traceId;
        this.spanId = spanId;
        this.parentSpanId = parentSpanId;
        this.captureDropSinceLast = captureDropSinceLast;
    }

    @JsonProperty("schema_version")   public String getSchemaVersion()       { return schemaVersion; }
    @JsonProperty("pipeline_id")      public String getPipelineId()           { return pipelineId; }
    @JsonProperty("operator_id")      public String getOperatorId()           { return operatorId; }
    @JsonProperty("operator_type")    public OperatorType getOperatorType()   { return operatorType; }
    @JsonProperty("feature_name")     public String getFeatureName()          { return featureName; }
    @JsonProperty("capture_mode")     public CaptureMode getCaptureMode()     { return captureMode; }
    @JsonProperty("event_time")       public String getEventTime()            { return eventTime; }
    @JsonProperty("event_time_min")   public String getEventTimeMin()         { return eventTimeMin; }
    @JsonProperty("processing_time")  public String getProcessingTime()       { return processingTime; }
    @JsonProperty("watermark")        public String getWatermark()            { return watermark; }
    @JsonProperty("window_start")     public String getWindowStart()          { return windowStart; }
    @JsonProperty("window_end")       public String getWindowEnd()            { return windowEnd; }
    @JsonProperty("late_event_count") public Long getLateEventCount()         { return lateEventCount; }
    @JsonProperty("late_tracking_mode") public LateTrackingMode getLateTrackingMode() { return lateTrackingMode; }
    @JsonProperty("input_cardinality")  public long getInputCardinality()     { return inputCardinality; }
    @JsonProperty("output_cardinality") public long getOutputCardinality()    { return outputCardinality; }
    @JsonProperty("emit_interval_ms")   public long getEmitIntervalMs()       { return emitIntervalMs; }
    @JsonProperty("timer_fired_count")  public Long getTimerFiredCount()      { return timerFiredCount; }
    @JsonProperty("async_pending_count") public Long getAsyncPendingCount()   { return asyncPendingCount; }
    @JsonProperty("pattern_match_count") public Long getPatternMatchCount()   { return patternMatchCount; }
    @JsonProperty("join_input_side")    public JoinInputSide getJoinInputSide() { return joinInputSide; }
    @JsonProperty("join_lower_bound_ms") public Long getJoinLowerBoundMs()    { return joinLowerBoundMs; }
    @JsonProperty("join_upper_bound_ms") public Long getJoinUpperBoundMs()    { return joinUpperBoundMs; }
    @JsonProperty("join_match_rate")    public Double getJoinMatchRate()      { return joinMatchRate; }
    @JsonProperty("value_count")        public Long getValueCount()           { return valueCount; }
    @JsonProperty("value_min")          public Double getValueMin()           { return valueMin; }
    @JsonProperty("value_max")          public Double getValueMax()           { return valueMax; }
    @JsonProperty("value_mean")         public Double getValueMean()          { return valueMean; }
    @JsonProperty("value_p50")          public Double getValueP50()           { return valueP50; }
    @JsonProperty("value_p95")          public Double getValueP95()           { return valueP95; }
    @JsonProperty("null_count")         public Long getNullCount()            { return nullCount; }
    @JsonProperty("entity_id")          public String getEntityId()           { return entityId; }
    @JsonProperty("feature_value")      public byte[] getFeatureValue()       { return featureValue; }
    @JsonProperty("feature_value_type") public FeatureValueType getFeatureValueType() { return featureValueType; }
    @JsonProperty("upstream_source")    public String getUpstreamSource()     { return upstreamSource; }
    @JsonProperty("upstream_system")    public String getUpstreamSystem()     { return upstreamSystem; }
    @JsonProperty("trace_id")           public String getTraceId()            { return traceId; }
    @JsonProperty("span_id")            public String getSpanId()             { return spanId; }
    @JsonProperty("parent_span_id")     public String getParentSpanId()       { return parentSpanId; }
    @JsonProperty("capture_drop_since_last") public boolean isCaptureDropSinceLast() { return captureDropSinceLast; }

    private static final ObjectMapper JSON = new ObjectMapper()
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    public String toJson() {
        try {
            return JSON.writeValueAsString(this);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Deserialise a JSON string produced by {@link #toJson()}. */
    public static CaptureEvent fromJson(String json) {
        try {
            return JSON.readValue(json, CaptureEvent.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final Schema AVRO_SCHEMA = loadAvroSchema();

    private static Schema loadAvroSchema() {
        try (InputStream in = CaptureEvent.class.getClassLoader()
                .getResourceAsStream("faro-event-v1.avsc")) {
            if (in == null) {
                throw new IllegalStateException(
                        "faro-event-v1.avsc not found on classpath. " +
                        "Ensure faro-core resources are on the classpath.");
            }
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Return the parsed Avro schema (loaded once at class init). */
    public static Schema avroSchema() {
        return AVRO_SCHEMA;
    }

    /**
     * Convert this event to an Avro {@link GenericRecord} using the v1 schema.
     *
     * <p>The returned record is suitable for use with an Avro {@code DatumWriter}
     * or the Kafka Avro serialiser.
     */
    public GenericRecord toAvroRecord() {
        GenericRecord record = new GenericData.Record(AVRO_SCHEMA);

        record.put("schema_version", schemaVersion);
        record.put("pipeline_id", pipelineId);
        record.put("operator_id", operatorId);
        record.put("operator_type", toAvroEnum("OperatorType", operatorType.name()));
        record.put("feature_name", featureName);
        record.put("capture_mode", toAvroEnum("CaptureMode", captureMode.name()));
        record.put("event_time", eventTime);
        record.put("event_time_min", eventTimeMin);
        record.put("processing_time", processingTime);
        record.put("watermark", watermark);
        record.put("window_start", windowStart);
        record.put("window_end", windowEnd);
        record.put("late_event_count", lateEventCount);
        record.put("late_tracking_mode",
                lateTrackingMode == null ? null
                        : toAvroEnum("LateTrackingMode", lateTrackingMode.name()));
        record.put("input_cardinality", inputCardinality);
        record.put("output_cardinality", outputCardinality);
        record.put("emit_interval_ms", emitIntervalMs);
        record.put("timer_fired_count", timerFiredCount);
        record.put("async_pending_count", asyncPendingCount);
        record.put("pattern_match_count", patternMatchCount);
        record.put("join_input_side",
                joinInputSide == null ? null
                        : toAvroEnum("JoinInputSide", joinInputSide.name()));
        record.put("join_lower_bound_ms", joinLowerBoundMs);
        record.put("join_upper_bound_ms", joinUpperBoundMs);
        record.put("join_match_rate", joinMatchRate);
        record.put("value_count", valueCount);
        record.put("value_min", valueMin);
        record.put("value_max", valueMax);
        record.put("value_mean", valueMean);
        record.put("value_p50", valueP50);
        record.put("value_p95", valueP95);
        record.put("null_count", nullCount);
        record.put("entity_id", entityId);
        record.put("feature_value", featureValue == null ? null : ByteBuffer.wrap(featureValue));
        record.put("feature_value_type",
                featureValueType == null ? null
                        : toAvroEnum("FeatureValueType", featureValueType.name()));
        record.put("upstream_source", upstreamSource);
        record.put("upstream_system", upstreamSystem);
        record.put("trace_id", traceId);
        record.put("span_id", spanId);
        record.put("parent_span_id", parentSpanId);
        record.put("capture_drop_since_last", captureDropSinceLast);

        return record;
    }

    /**
     * Deserialise a {@link GenericRecord} produced by Avro v1 into a {@code CaptureEvent}.
     *
     * <p>String fields are extracted via {@code toString()} to handle Avro {@code Utf8}.
     */
    public static CaptureEvent fromAvroRecord(GenericRecord r) {
        return new Builder()
                .pipelineId(str(r, "pipeline_id"))
                .operatorId(str(r, "operator_id"))
                .operatorType(OperatorType.valueOf(r.get("operator_type").toString()))
                .captureMode(CaptureMode.valueOf(r.get("capture_mode").toString()))
                .processingTime(str(r, "processing_time"))
                .inputCardinality((Long) r.get("input_cardinality"))
                .outputCardinality((Long) r.get("output_cardinality"))
                .emitIntervalMs((Long) r.get("emit_interval_ms"))
                .traceId(str(r, "trace_id"))
                .spanId(str(r, "span_id"))
                .captureDropSinceLast((Boolean) r.get("capture_drop_since_last"))
                .featureName(str(r, "feature_name"))
                .eventTime(str(r, "event_time"))
                .eventTimeMin(str(r, "event_time_min"))
                .watermark(str(r, "watermark"))
                .windowStart(str(r, "window_start"))
                .windowEnd(str(r, "window_end"))
                .lateEventCount((Long) r.get("late_event_count"))
                .lateTrackingMode(enumOrNull(r, "late_tracking_mode", LateTrackingMode.class))
                .timerFiredCount((Long) r.get("timer_fired_count"))
                .asyncPendingCount((Long) r.get("async_pending_count"))
                .patternMatchCount((Long) r.get("pattern_match_count"))
                .joinInputSide(enumOrNull(r, "join_input_side", JoinInputSide.class))
                .joinLowerBoundMs((Long) r.get("join_lower_bound_ms"))
                .joinUpperBoundMs((Long) r.get("join_upper_bound_ms"))
                .joinMatchRate((Double) r.get("join_match_rate"))
                .valueCount((Long) r.get("value_count"))
                .valueMin((Double) r.get("value_min"))
                .valueMax((Double) r.get("value_max"))
                .valueMean((Double) r.get("value_mean"))
                .valueP50((Double) r.get("value_p50"))
                .valueP95((Double) r.get("value_p95"))
                .nullCount((Long) r.get("null_count"))
                .entityId(str(r, "entity_id"))
                .featureValue(featureValueBytes(r))
                .featureValueType(enumOrNull(r, "feature_value_type", FeatureValueType.class))
                .upstreamSource(str(r, "upstream_source"))
                .upstreamSystem(str(r, "upstream_system"))
                .parentSpanId(str(r, "parent_span_id"))
                .build();
    }

    private static GenericData.EnumSymbol toAvroEnum(String enumName, String symbol) {
        Schema enumSchema = AVRO_SCHEMA.getField(enumName) != null
                ? AVRO_SCHEMA.getField(enumName).schema()
                : findEnumSchema(enumName);
        return new GenericData.EnumSymbol(enumSchema, symbol);
    }

    private static Schema findEnumSchema(String enumName) {
        for (Schema.Field field : AVRO_SCHEMA.getFields()) {
            Schema s = unwrapUnion(field.schema());
            if (s.getType() == Schema.Type.ENUM && s.getName().equals(enumName)) {
                return s;
            }
        }
        throw new IllegalArgumentException("Enum schema not found: " + enumName);
    }

    private static Schema unwrapUnion(Schema s) {
        if (s.getType() != Schema.Type.UNION) return s;
        for (Schema t : s.getTypes()) {
            if (t.getType() != Schema.Type.NULL) return t;
        }
        return s;
    }

    private static String str(GenericRecord r, String field) {
        Object v = r.get(field);
        return v == null ? null : v.toString();
    }

    private static <E extends Enum<E>> E enumOrNull(GenericRecord r, String field, Class<E> type) {
        Object v = r.get(field);
        return v == null ? null : Enum.valueOf(type, v.toString());
    }

    private static byte[] featureValueBytes(GenericRecord r) {
        Object v = r.get("feature_value");
        if (v == null) return null;
        if (v instanceof ByteBuffer buf) {
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return bytes;
        }
        return (byte[]) v;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String pipelineId;
        private String operatorId;
        private OperatorType operatorType;
        private CaptureMode captureMode;
        private String processingTime;
        private long inputCardinality;
        private long outputCardinality;
        private long emitIntervalMs;
        private String traceId;
        private String spanId;
        private boolean captureDropSinceLast;
        private String featureName;
        private String eventTime;
        private String eventTimeMin;
        private String watermark;
        private String windowStart;
        private String windowEnd;
        private Long lateEventCount;
        private LateTrackingMode lateTrackingMode;
        private Long timerFiredCount;
        private Long asyncPendingCount;
        private Long patternMatchCount;
        private JoinInputSide joinInputSide;
        private Long joinLowerBoundMs;
        private Long joinUpperBoundMs;
        private Double joinMatchRate;
        private Long valueCount;
        private Double valueMin;
        private Double valueMax;
        private Double valueMean;
        private Double valueP50;
        private Double valueP95;
        private Long nullCount;
        private String entityId;
        private byte[] featureValue;
        private FeatureValueType featureValueType;
        private String upstreamSource;
        private String upstreamSystem;
        private String parentSpanId;

        private Builder() {}

        public Builder pipelineId(String v)           { this.pipelineId = v; return this; }
        public Builder operatorId(String v)           { this.operatorId = v; return this; }
        public Builder operatorType(OperatorType v)   { this.operatorType = v; return this; }
        public Builder captureMode(CaptureMode v)     { this.captureMode = v; return this; }
        public Builder processingTime(String v)       { this.processingTime = v; return this; }
        public Builder inputCardinality(long v)       { this.inputCardinality = v; return this; }
        public Builder outputCardinality(long v)      { this.outputCardinality = v; return this; }
        public Builder emitIntervalMs(long v)         { this.emitIntervalMs = v; return this; }
        public Builder traceId(String v)              { this.traceId = v; return this; }
        public Builder spanId(String v)               { this.spanId = v; return this; }
        public Builder captureDropSinceLast(boolean v){ this.captureDropSinceLast = v; return this; }
        public Builder featureName(String v)          { this.featureName = v; return this; }
        public Builder eventTime(String v)            { this.eventTime = v; return this; }
        public Builder eventTimeMin(String v)         { this.eventTimeMin = v; return this; }
        public Builder watermark(String v)            { this.watermark = v; return this; }
        public Builder windowStart(String v)          { this.windowStart = v; return this; }
        public Builder windowEnd(String v)            { this.windowEnd = v; return this; }
        public Builder lateEventCount(Long v)         { this.lateEventCount = v; return this; }
        public Builder lateTrackingMode(LateTrackingMode v) { this.lateTrackingMode = v; return this; }
        public Builder timerFiredCount(Long v)        { this.timerFiredCount = v; return this; }
        public Builder asyncPendingCount(Long v)      { this.asyncPendingCount = v; return this; }
        public Builder patternMatchCount(Long v)      { this.patternMatchCount = v; return this; }
        public Builder joinInputSide(JoinInputSide v) { this.joinInputSide = v; return this; }
        public Builder joinLowerBoundMs(Long v)       { this.joinLowerBoundMs = v; return this; }
        public Builder joinUpperBoundMs(Long v)       { this.joinUpperBoundMs = v; return this; }
        public Builder joinMatchRate(Double v)        { this.joinMatchRate = v; return this; }
        public Builder valueCount(Long v)             { this.valueCount = v; return this; }
        public Builder valueMin(Double v)             { this.valueMin = v; return this; }
        public Builder valueMax(Double v)             { this.valueMax = v; return this; }
        public Builder valueMean(Double v)            { this.valueMean = v; return this; }
        public Builder valueP50(Double v)             { this.valueP50 = v; return this; }
        public Builder valueP95(Double v)             { this.valueP95 = v; return this; }
        public Builder nullCount(Long v)              { this.nullCount = v; return this; }
        public Builder entityId(String v)             { this.entityId = v; return this; }
        public Builder featureValue(byte[] v)         { this.featureValue = v; return this; }
        public Builder featureValueType(FeatureValueType v) { this.featureValueType = v; return this; }
        public Builder upstreamSource(String v)       { this.upstreamSource = v; return this; }
        public Builder upstreamSystem(String v)       { this.upstreamSystem = v; return this; }
        public Builder parentSpanId(String v)         { this.parentSpanId = v; return this; }

        /**
         * @throws IllegalStateException if any required field is null or empty.
         */
        public CaptureEvent build() {
            requireNonEmpty(pipelineId,    "pipeline_id");
            requireNonEmpty(operatorId,    "operator_id");
            requireNonNull(operatorType,   "operator_type");
            requireNonNull(captureMode,    "capture_mode");
            requireNonEmpty(processingTime,"processing_time");
            requireNonEmpty(traceId,       "trace_id");
            requireNonEmpty(spanId,        "span_id");

            return new CaptureEvent(
                    "1.0.0",
                    pipelineId, operatorId, operatorType, featureName, captureMode,
                    eventTime, eventTimeMin, processingTime, watermark,
                    windowStart, windowEnd, lateEventCount, lateTrackingMode,
                    inputCardinality, outputCardinality, emitIntervalMs,
                    timerFiredCount, asyncPendingCount, patternMatchCount,
                    joinInputSide, joinLowerBoundMs, joinUpperBoundMs, joinMatchRate,
                    valueCount, valueMin, valueMax, valueMean, valueP50, valueP95, nullCount,
                    entityId, featureValue, featureValueType,
                    upstreamSource, upstreamSystem,
                    traceId, spanId, parentSpanId,
                    captureDropSinceLast);
        }

        private static void requireNonNull(Object value, String field) {
            if (value == null) {
                throw new IllegalStateException("Required field '" + field + "' must not be null");
            }
        }

        private static void requireNonEmpty(String value, String field) {
            if (value == null || value.isEmpty()) {
                throw new IllegalStateException("Required field '" + field + "' must not be null or empty");
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CaptureEvent e)) return false;
        return inputCardinality == e.inputCardinality
                && outputCardinality == e.outputCardinality
                && emitIntervalMs == e.emitIntervalMs
                && captureDropSinceLast == e.captureDropSinceLast
                && Objects.equals(schemaVersion, e.schemaVersion)
                && Objects.equals(pipelineId, e.pipelineId)
                && Objects.equals(operatorId, e.operatorId)
                && operatorType == e.operatorType
                && Objects.equals(featureName, e.featureName)
                && captureMode == e.captureMode
                && Objects.equals(eventTime, e.eventTime)
                && Objects.equals(eventTimeMin, e.eventTimeMin)
                && Objects.equals(processingTime, e.processingTime)
                && Objects.equals(watermark, e.watermark)
                && Objects.equals(windowStart, e.windowStart)
                && Objects.equals(windowEnd, e.windowEnd)
                && Objects.equals(lateEventCount, e.lateEventCount)
                && lateTrackingMode == e.lateTrackingMode
                && Objects.equals(timerFiredCount, e.timerFiredCount)
                && Objects.equals(asyncPendingCount, e.asyncPendingCount)
                && Objects.equals(patternMatchCount, e.patternMatchCount)
                && joinInputSide == e.joinInputSide
                && Objects.equals(joinLowerBoundMs, e.joinLowerBoundMs)
                && Objects.equals(joinUpperBoundMs, e.joinUpperBoundMs)
                && Objects.equals(joinMatchRate, e.joinMatchRate)
                && Objects.equals(valueCount, e.valueCount)
                && Objects.equals(valueMin, e.valueMin)
                && Objects.equals(valueMax, e.valueMax)
                && Objects.equals(valueMean, e.valueMean)
                && Objects.equals(valueP50, e.valueP50)
                && Objects.equals(valueP95, e.valueP95)
                && Objects.equals(nullCount, e.nullCount)
                && Objects.equals(entityId, e.entityId)
                && Objects.equals(featureValueType, e.featureValueType)
                && Objects.equals(upstreamSource, e.upstreamSource)
                && Objects.equals(upstreamSystem, e.upstreamSystem)
                && Objects.equals(traceId, e.traceId)
                && Objects.equals(spanId, e.spanId)
                && Objects.equals(parentSpanId, e.parentSpanId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaVersion, pipelineId, operatorId, operatorType,
                captureMode, processingTime, traceId, spanId);
    }

    @Override
    public String toString() {
        return "CaptureEvent{pipeline=" + pipelineId
                + ", operator=" + operatorId
                + ", type=" + operatorType
                + ", processing_time=" + processingTime
                + ", span=" + spanId + "}";
    }
}
