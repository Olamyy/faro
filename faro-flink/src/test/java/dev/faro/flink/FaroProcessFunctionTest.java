package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FaroProcessFunctionTest {

    private static final String PIPELINE_ID = "test-pipeline";
    private static final String OPERATOR_UID = "process.test-operator";

    private CapturingCaptureEventSink captured;
    private StreamingRuntimeContext runtimeContext;

    @BeforeEach
    void setUp() {
        captured = new CapturingCaptureEventSink();
        runtimeContext = mock(StreamingRuntimeContext.class);
        when(runtimeContext.getOperatorUniqueID()).thenReturn(OPERATOR_UID);
    }

    private FaroProcessFunction<String, String> fnWithFeatures(String... features) throws Exception {
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features(features)
                .build();
        FaroProcessFunction<String, String> fn = new FaroProcessFunction<>(
                CaptureEvent.OperatorType.MAP, config, new PassThroughFn(), captured);
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());
        return fn;
    }

    @Test
    void open_throwsWhenNoUid() {
        when(runtimeContext.getOperatorUniqueID()).thenReturn("");
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();
        FaroProcessFunction<String, String> fn = new FaroProcessFunction<>(
                CaptureEvent.OperatorType.MAP, config, new PassThroughFn(), captured);
        fn.setRuntimeContext(runtimeContext);
        assertThrows(IllegalStateException.class, () -> fn.open(new Configuration()));
    }

    @Test
    void construction_throwsOnInvalidOperatorType() {
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();
        assertThrows(IllegalArgumentException.class, () ->
                new FaroProcessFunction<>(CaptureEvent.OperatorType.SINK, config, new PassThroughFn(), captured));
        assertThrows(IllegalArgumentException.class, () ->
                new FaroProcessFunction<>(CaptureEvent.OperatorType.WINDOW, config, new PassThroughFn(), captured));
        assertThrows(IllegalArgumentException.class, () ->
                new FaroProcessFunction<>(CaptureEvent.OperatorType.SOURCE, config, new PassThroughFn(), captured));
    }

    @Test
    void flush_emitsOneEventPerFeature() throws Exception {
        FaroProcessFunction<String, String> fn = fnWithFeatures("avg_purchase_7d", "max_purchase_7d");
        fn.processElement("r1", mockCtx(null, Long.MIN_VALUE), noopCollector());
        fn.flush();

        assertEquals(2, captured.events.size());
        assertEquals("avg_purchase_7d", captured.events.get(0).getFeatureName());
        assertEquals("max_purchase_7d", captured.events.get(1).getFeatureName());
    }

    @Test
    void flush_cardinalityReflectsInterval() throws Exception {
        FaroProcessFunction<String, String> fn = fnWithFeatures("feature-a");
        fn.processElement("r1", mockCtx(null, Long.MIN_VALUE), noopCollector());
        fn.processElement("r2", mockCtx(null, Long.MIN_VALUE), noopCollector());
        fn.processElement("r3", mockCtx(null, Long.MIN_VALUE), noopCollector());
        fn.flush();

        assertEquals(3L, captured.events.get(0).getInputCardinality());
        assertEquals(3L, captured.events.get(0).getOutputCardinality());
    }

    @Test
    void flush_cardinalityResetsAfterEachInterval() throws Exception {
        FaroProcessFunction<String, String> fn = fnWithFeatures("feature-a");
        fn.processElement("r1", mockCtx(null, Long.MIN_VALUE), noopCollector());
        fn.processElement("r2", mockCtx(null, Long.MIN_VALUE), noopCollector());
        fn.flush();
        captured.events.clear();

        fn.processElement("r3", mockCtx(null, Long.MIN_VALUE), noopCollector());
        fn.flush();

        assertEquals(1L, captured.events.get(0).getInputCardinality());
    }

    @Test
    void flush_outputCardinalityReflectsFailures() throws Exception {
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();
        FaroProcessFunction<String, String> fn = new FaroProcessFunction<>(
                CaptureEvent.OperatorType.MAP, config, new FailingAfterNFn(2), captured);
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());

        fn.processElement("r1", mockCtx(null, Long.MIN_VALUE), noopCollector());
        fn.processElement("r2", mockCtx(null, Long.MIN_VALUE), noopCollector());
        assertThrows(Exception.class,
                () -> fn.processElement("r3", mockCtx(null, Long.MIN_VALUE), noopCollector()));
        fn.flush();

        assertEquals(3L, captured.events.get(0).getInputCardinality());
        assertEquals(2L, captured.events.get(0).getOutputCardinality());
    }

    @Test
    void flush_traceIdIsStableAcrossIntervals() throws Exception {
        FaroProcessFunction<String, String> fn = fnWithFeatures("feature-a");
        fn.flush();
        String firstTraceId = captured.events.get(0).getTraceId();
        captured.events.clear();

        fn.flush();
        assertEquals(firstTraceId, captured.events.get(0).getTraceId());
    }

    @Test
    void close_flushesPartialInterval() throws Exception {
        FaroProcessFunction<String, String> fn = fnWithFeatures("feature-a");
        fn.processElement("r1", mockCtx(null, Long.MIN_VALUE), noopCollector());
        fn.close();

        assertFalse(captured.events.isEmpty());
        assertEquals(1L, captured.events.get(0).getInputCardinality());
    }

    @Test
    void flush_watermarkIsNullWhenMinValue() throws Exception {
        FaroProcessFunction<String, String> fn = fnWithFeatures("feature-a");
        fn.processElement("r1", mockCtx(null, Long.MIN_VALUE), noopCollector());
        fn.flush();

        assertNull(captured.events.get(0).getWatermark());
    }

    @Test
    void flush_watermarkIsIso8601WhenAssigned() throws Exception {
        FaroProcessFunction<String, String> fn = fnWithFeatures("feature-a");
        long watermarkMs = Instant.parse("2026-03-21T12:00:00Z").toEpochMilli();
        fn.processElement("r1", mockCtx(null, watermarkMs), noopCollector());
        fn.flush();

        assertEquals("2026-03-21T12:00:00Z", captured.events.get(0).getWatermark());
    }

    @Test
    void flush_eventTimeIsNullWhenNoTimestamp() throws Exception {
        FaroProcessFunction<String, String> fn = fnWithFeatures("feature-a");
        fn.processElement("r1", mockCtx(null, Long.MIN_VALUE), noopCollector());
        fn.flush();

        assertNull(captured.events.get(0).getEventTime());
        assertNull(captured.events.get(0).getEventTimeMin());
    }

    @Test
    void flush_eventTimeReflectsMaxTimestampInInterval() throws Exception {
        FaroProcessFunction<String, String> fn = fnWithFeatures("feature-a");
        long ts1 = Instant.parse("2026-03-21T10:00:00Z").toEpochMilli();
        long ts2 = Instant.parse("2026-03-21T12:00:00Z").toEpochMilli();
        fn.processElement("r1", mockCtx(ts1, Long.MIN_VALUE), noopCollector());
        fn.processElement("r2", mockCtx(ts2, Long.MIN_VALUE), noopCollector());
        fn.flush();

        assertEquals("2026-03-21T12:00:00Z", captured.events.get(0).getEventTime());
    }

    @Test
    void flush_eventTimeMinReflectsMinTimestampInInterval() throws Exception {
        FaroProcessFunction<String, String> fn = fnWithFeatures("feature-a");
        long ts1 = Instant.parse("2026-03-21T10:00:00Z").toEpochMilli();
        long ts2 = Instant.parse("2026-03-21T12:00:00Z").toEpochMilli();
        fn.processElement("r1", mockCtx(ts1, Long.MIN_VALUE), noopCollector());
        fn.processElement("r2", mockCtx(ts2, Long.MIN_VALUE), noopCollector());
        fn.flush();

        assertEquals("2026-03-21T10:00:00Z", captured.events.get(0).getEventTimeMin());
    }

    @Test
    void flush_timerFiredCountIsNull() throws Exception {
        FaroProcessFunction<String, String> fn = fnWithFeatures("feature-a");
        fn.flush();

        assertNull(captured.events.get(0).getTimerFiredCount());
    }

    @Test
    void processElement_delegatesElement() throws Exception {
        CountingFn delegate = new CountingFn();
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();
        FaroProcessFunction<String, String> fn = new FaroProcessFunction<>(
                CaptureEvent.OperatorType.MAP, config, delegate, captured);
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());

        fn.processElement("r1", mockCtx(null, Long.MIN_VALUE), noopCollector());
        fn.processElement("r2", mockCtx(null, Long.MIN_VALUE), noopCollector());

        assertEquals(2, delegate.processElementCallCount);
    }

    private static ProcessFunction<String, String>.Context mockCtx(Long timestamp, long watermark) {
        @SuppressWarnings("unchecked")
        ProcessFunction<String, String>.Context ctx =
                (ProcessFunction<String, String>.Context) mock(ProcessFunction.Context.class);
        when(ctx.timestamp()).thenReturn(timestamp != null ? timestamp : Long.MIN_VALUE);
        TimerService timerService = mock(TimerService.class);
        when(timerService.currentWatermark()).thenReturn(watermark);
        when(ctx.timerService()).thenReturn(timerService);
        return ctx;
    }

    @SuppressWarnings("unchecked")
    private static Collector<String> noopCollector() {
        return mock(Collector.class);
    }

    private static final class PassThroughFn extends ProcessFunction<String, String> {
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            out.collect(value);
        }
    }

    private static final class CountingFn extends ProcessFunction<String, String> {
        int processElementCallCount = 0;

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            processElementCallCount++;
        }
    }

    private static final class FailingAfterNFn extends ProcessFunction<String, String> {
        private final int succeedCount;
        private int invocations = 0;

        FailingAfterNFn(int succeedCount) {
            this.succeedCount = succeedCount;
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            invocations++;
            if (invocations > succeedCount) {
                throw new RuntimeException("simulated processing failure");
            }
            out.collect(value);
        }
    }
}
