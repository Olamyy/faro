package dev.faro.core;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CaptureEventTest {

    private static CaptureEvent.Builder minimalBuilder() {
        return CaptureEvent.builder()
                .pipelineId("purchase-features-v3")
                .operatorId("sink.purchase-features")
                .operatorType(CaptureEvent.OperatorType.SINK)
                .captureMode(CaptureEvent.CaptureMode.AGGREGATE)
                .processingTime("2026-03-21T12:00:00Z")
                .inputCardinality(1200)
                .outputCardinality(1200)
                .emitIntervalMs(30_000)
                .traceId("0af7651916cd43dd8448eb211c80319c")
                .spanId("b7ad6b7169203331")
                .captureDropSinceLast(false);
    }

    @Test
    void builder_rejectsNullPipelineId() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                minimalBuilder().pipelineId(null).build());
        assertTrue(ex.getMessage().contains("pipeline_id"));
    }

    @Test
    void builder_rejectsEmptyOperatorId() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                minimalBuilder().operatorId("").build());
        assertTrue(ex.getMessage().contains("operator_id"));
    }

    @Test
    void builder_rejectsNullOperatorType() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                minimalBuilder().operatorType(null).build());
        assertTrue(ex.getMessage().contains("operator_type"));
    }

    @Test
    void builder_rejectsNullCaptureMode() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                minimalBuilder().captureMode(null).build());
        assertTrue(ex.getMessage().contains("capture_mode"));
    }

    @Test
    void builder_rejectsNullProcessingTime() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                minimalBuilder().processingTime(null).build());
        assertTrue(ex.getMessage().contains("processing_time"));
    }

    @Test
    void builder_rejectsNullTraceId() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                minimalBuilder().traceId(null).build());
        assertTrue(ex.getMessage().contains("trace_id"));
    }

    @Test
    void builder_rejectsNullSpanId() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                minimalBuilder().spanId(null).build());
        assertTrue(ex.getMessage().contains("span_id"));
    }

    @Test
    void build_alwaysSetsSchemaVersion() {
        CaptureEvent e = minimalBuilder().build();
        assertEquals("1.0.0", e.getSchemaVersion());
    }

    @Test
    void optionalFields_areNullByDefault() {
        CaptureEvent e = minimalBuilder().build();
        assertNull(e.getFeatureName());
        assertNull(e.getEventTime());
        assertNull(e.getEventTimeMin());
        assertNull(e.getWatermark());
        assertNull(e.getWindowStart());
        assertNull(e.getWindowEnd());
        assertNull(e.getLateEventCount());
        assertNull(e.getLateTrackingMode());
        assertNull(e.getTimerFiredCount());
        assertNull(e.getAsyncPendingCount());
        assertNull(e.getPatternMatchCount());
        assertNull(e.getJoinInputSide());
        assertNull(e.getJoinLowerBoundMs());
        assertNull(e.getJoinUpperBoundMs());
        assertNull(e.getJoinMatchRate());
        assertNull(e.getValueCount());
        assertNull(e.getValueMin());
        assertNull(e.getValueMax());
        assertNull(e.getValueMean());
        assertNull(e.getValueP50());
        assertNull(e.getValueP95());
        assertNull(e.getNullCount());
        assertNull(e.getEntityId());
        assertNull(e.getFeatureValue());
        assertNull(e.getFeatureValueType());
        assertNull(e.getUpstreamSource());
        assertNull(e.getUpstreamSystem());
        assertNull(e.getParentSpanId());
    }

    @Test
    void jsonRoundTrip_minimalEvent() {
        CaptureEvent original = minimalBuilder().build();
        String json = original.toJson();
        CaptureEvent restored = CaptureEvent.fromJson(json);

        assertEquals(original.getPipelineId(), restored.getPipelineId());
        assertEquals(original.getOperatorId(), restored.getOperatorId());
        assertEquals(original.getOperatorType(), restored.getOperatorType());
        assertEquals(original.getCaptureMode(), restored.getCaptureMode());
        assertEquals(original.getProcessingTime(), restored.getProcessingTime());
        assertEquals(original.getInputCardinality(), restored.getInputCardinality());
        assertEquals(original.getOutputCardinality(), restored.getOutputCardinality());
        assertEquals(original.getEmitIntervalMs(), restored.getEmitIntervalMs());
        assertEquals(original.getTraceId(), restored.getTraceId());
        assertEquals(original.getSpanId(), restored.getSpanId());
        assertEquals(original.isCaptureDropSinceLast(), restored.isCaptureDropSinceLast());
        assertEquals(original.getSchemaVersion(), restored.getSchemaVersion());
    }

    @Test
    void jsonRoundTrip_fullEvent() {
        CaptureEvent original = minimalBuilder()
                .featureName("avg_purchase_7d")
                .eventTime("2026-03-21T11:59:59Z")
                .eventTimeMin("2026-03-21T11:30:00Z")
                .watermark("2026-03-21T11:59:50Z")
                .valueCount(1200L)
                .valueMin(0.5)
                .valueMax(999.99)
                .valueMean(42.3)
                .valueP50(38.1)
                .valueP95(189.0)
                .nullCount(3L)
                .captureDropSinceLast(false)
                .build();

        CaptureEvent restored = CaptureEvent.fromJson(original.toJson());

        assertEquals(original.getFeatureName(), restored.getFeatureName());
        assertEquals(original.getEventTime(), restored.getEventTime());
        assertEquals(original.getEventTimeMin(), restored.getEventTimeMin());
        assertEquals(original.getWatermark(), restored.getWatermark());
        assertEquals(original.getValueCount(), restored.getValueCount());
        assertEquals(original.getValueMin(), restored.getValueMin());
        assertEquals(original.getValueMax(), restored.getValueMax());
        assertEquals(original.getValueMean(), restored.getValueMean());
        assertEquals(original.getValueP50(), restored.getValueP50());
        assertEquals(original.getValueP95(), restored.getValueP95());
        assertEquals(original.getNullCount(), restored.getNullCount());
    }

    @Test
    void jsonRoundTrip_nullOptionalFieldsRoundTripAsNull() {
        CaptureEvent original = minimalBuilder().build();
        CaptureEvent restored = CaptureEvent.fromJson(original.toJson());
        assertNull(restored.getFeatureName());
        assertNull(restored.getWatermark());
        assertNull(restored.getParentSpanId());
    }

    @Test
    void avroRoundTrip_minimalEvent() {
        CaptureEvent original = minimalBuilder().build();
        GenericRecord record = original.toAvroRecord();
        CaptureEvent restored = CaptureEvent.fromAvroRecord(record);

        assertEquals(original.getPipelineId(), restored.getPipelineId());
        assertEquals(original.getOperatorId(), restored.getOperatorId());
        assertEquals(original.getOperatorType(), restored.getOperatorType());
        assertEquals(original.getCaptureMode(), restored.getCaptureMode());
        assertEquals(original.getProcessingTime(), restored.getProcessingTime());
        assertEquals(original.getInputCardinality(), restored.getInputCardinality());
        assertEquals(original.getOutputCardinality(), restored.getOutputCardinality());
        assertEquals(original.getEmitIntervalMs(), restored.getEmitIntervalMs());
        assertEquals(original.getTraceId(), restored.getTraceId());
        assertEquals(original.getSpanId(), restored.getSpanId());
        assertEquals(original.isCaptureDropSinceLast(), restored.isCaptureDropSinceLast());
    }

    @Test
    void avroRoundTrip_windowEvent() {
        CaptureEvent original = CaptureEvent.builder()
                .pipelineId("purchase-features-v3")
                .operatorId("window.hourly-agg")
                .operatorType(CaptureEvent.OperatorType.WINDOW)
                .captureMode(CaptureEvent.CaptureMode.AGGREGATE)
                .processingTime("2026-03-21T12:00:00Z")
                .inputCardinality(3600)
                .outputCardinality(1)
                .emitIntervalMs(3_600_000)
                .traceId("0af7651916cd43dd8448eb211c80319c")
                .spanId("a1b2c3d4e5f67890")
                .captureDropSinceLast(false)
                .windowStart("2026-03-21T11:00:00Z")
                .windowEnd("2026-03-21T12:00:00Z")
                .lateTrackingMode(CaptureEvent.LateTrackingMode.SIDE_OUTPUT)
                .lateEventCount(2L)
                .valueCount(3600L)
                .valueMean(55.7)
                .build();

        CaptureEvent restored = CaptureEvent.fromAvroRecord(original.toAvroRecord());

        assertEquals(original.getWindowStart(), restored.getWindowStart());
        assertEquals(original.getWindowEnd(), restored.getWindowEnd());
        assertEquals(original.getLateTrackingMode(), restored.getLateTrackingMode());
        assertEquals(original.getLateEventCount(), restored.getLateEventCount());
    }

    @Test
    void avroRoundTrip_nullOptionalFieldsRoundTripAsNull() {
        CaptureEvent original = minimalBuilder().build();
        CaptureEvent restored = CaptureEvent.fromAvroRecord(original.toAvroRecord());
        assertNull(restored.getFeatureName());
        assertNull(restored.getWatermark());
        assertNull(restored.getParentSpanId());
        assertNull(restored.getLateEventCount());
        assertNull(restored.getJoinInputSide());
    }

    @Test
    void avroRoundTrip_joinEvent() {
        CaptureEvent original = CaptureEvent.builder()
                .pipelineId("purchase-features-v3")
                .operatorId("join.purchase-user")
                .operatorType(CaptureEvent.OperatorType.JOIN)
                .captureMode(CaptureEvent.CaptureMode.AGGREGATE)
                .processingTime("2026-03-21T12:00:00Z")
                .inputCardinality(500)
                .outputCardinality(480)
                .emitIntervalMs(30_000)
                .traceId("0af7651916cd43dd8448eb211c80319c")
                .spanId("f1e2d3c4b5a67890")
                .captureDropSinceLast(false)
                .joinInputSide(CaptureEvent.JoinInputSide.OUTPUT)
                .joinLowerBoundMs(-60_000L)
                .joinUpperBoundMs(0L)
                .joinMatchRate(0.96)
                .build();

        CaptureEvent restored = CaptureEvent.fromAvroRecord(original.toAvroRecord());

        assertEquals(original.getJoinInputSide(), restored.getJoinInputSide());
        assertEquals(original.getJoinLowerBoundMs(), restored.getJoinLowerBoundMs());
        assertEquals(original.getJoinUpperBoundMs(), restored.getJoinUpperBoundMs());
        assertEquals(original.getJoinMatchRate(), restored.getJoinMatchRate());
    }

    @Test
    void avroSchema_loadsFromClasspath() {
        org.apache.avro.Schema schema = CaptureEvent.avroSchema();
        assertNotNull(schema);
        assertEquals("CaptureEvent", schema.getName());
        assertEquals("dev.faro.schema", schema.getNamespace());
        assertNotNull(schema.getField("pipeline_id"));
        assertNotNull(schema.getField("operator_type"));
        assertNotNull(schema.getField("capture_drop_since_last"));
    }

    @Test
    void captureDropSinceLast_defaultsFalse() {
        CaptureEvent e = minimalBuilder().captureDropSinceLast(false).build();
        assertFalse(e.isCaptureDropSinceLast());
    }

    @Test
    void captureDropSinceLast_canBeSetTrue() {
        CaptureEvent e = minimalBuilder().captureDropSinceLast(true).build();
        assertTrue(e.isCaptureDropSinceLast());
    }

    @Test
    void captureDropSinceLast_roundTripsViaJson() {
        CaptureEvent original = minimalBuilder().captureDropSinceLast(true).build();
        CaptureEvent restored = CaptureEvent.fromJson(original.toJson());
        assertTrue(restored.isCaptureDropSinceLast());
    }
}
