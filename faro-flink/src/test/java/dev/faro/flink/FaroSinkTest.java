package dev.faro.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FaroSinkTest {

    private static final String PIPELINE_ID = "test-pipeline";
    private static final String OPERATOR_UID = "sink.test-operator";

    private CapturingCaptureEventSink captured;
    private StreamingRuntimeContext runtimeContext;

    @BeforeEach
    void setUp() {
        captured = new CapturingCaptureEventSink();
        runtimeContext = mock(StreamingRuntimeContext.class);
        when(runtimeContext.getOperatorUniqueID()).thenReturn(OPERATOR_UID);
    }

    private FaroSink<String> sinkWithFeatures(String... features) throws Exception {
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features(features)
                .build();
        FaroSink<String> sink = new FaroSink<>(new NoopSink<>(), config, captured);
        sink.setRuntimeContext(runtimeContext);
        sink.open(new Configuration());
        return sink;
    }

    @Test
    void open_throwsWhenNoUid() {
        when(runtimeContext.getOperatorUniqueID()).thenReturn("");
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();
        FaroSink<String> sink = new FaroSink<>(new NoopSink<>(), config, captured);
        sink.setRuntimeContext(runtimeContext);
        assertThrows(IllegalStateException.class, () -> sink.open(new Configuration()));
    }

    @Test
    void flush_emitsOneEventPerFeature() throws Exception {
        FaroSink<String> sink = sinkWithFeatures("avg_purchase_7d", "max_purchase_7d");
        sink.invoke("record1", null);
        sink.flush();

        assertEquals(2, captured.events.size());
        assertEquals("avg_purchase_7d", captured.events.get(0).getFeatureName());
        assertEquals("max_purchase_7d", captured.events.get(1).getFeatureName());
    }

    @Test
    void flush_cardinalityReflectsInterval() throws Exception {
        FaroSink<String> sink = sinkWithFeatures("feature-a");
        sink.invoke("r1", null);
        sink.invoke("r2", null);
        sink.invoke("r3", null);
        sink.flush();

        assertEquals(3L, captured.events.get(0).getInputCardinality());
        assertEquals(3L, captured.events.get(0).getOutputCardinality());
    }

    @Test
    void flush_cardinalityResetsAfterEachInterval() throws Exception {
        FaroSink<String> sink = sinkWithFeatures("feature-a");
        sink.invoke("r1", null);
        sink.invoke("r2", null);
        sink.flush();
        captured.events.clear();

        sink.invoke("r3", null);
        sink.flush();

        assertEquals(1L, captured.events.get(0).getInputCardinality());
    }

    @Test
    void flush_outputCardinalityReflectsFailures() throws Exception {
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();
        FaroSink<String> sink = new FaroSink<>(new FailingAfterNSink<>(2), config, captured);
        sink.setRuntimeContext(runtimeContext);
        sink.open(new Configuration());

        sink.invoke("r1", null);
        sink.invoke("r2", null);
        assertThrows(Exception.class, () -> sink.invoke("r3", null));
        sink.flush();

        assertEquals(3L, captured.events.get(0).getInputCardinality());
        assertEquals(2L, captured.events.get(0).getOutputCardinality());
    }

    @Test
    void flush_traceIdIsStableAcrossIntervals() throws Exception {
        FaroSink<String> sink = sinkWithFeatures("feature-a");
        sink.flush();
        String firstTraceId = captured.events.get(0).getTraceId();
        captured.events.clear();

        sink.flush();
        assertEquals(firstTraceId, captured.events.get(0).getTraceId());
    }

    @Test
    void close_flushesPartialInterval() throws Exception {
        FaroSink<String> sink = sinkWithFeatures("feature-a");
        sink.invoke("r1", null);
        sink.close();

        assertFalse(captured.events.isEmpty());
        assertEquals(1L, captured.events.get(0).getInputCardinality());
    }

    @Test
    void flush_watermarkIsNullWhenMinValue() throws Exception {
        FaroSink<String> sink = sinkWithFeatures("feature-a");
        SinkFunction.Context ctx = mock(SinkFunction.Context.class);
        when(ctx.currentWatermark()).thenReturn(Long.MIN_VALUE);
        sink.invoke("r1", ctx);
        sink.flush();

        assertNull(captured.events.get(0).getWatermark());
    }

    @Test
    void flush_watermarkIsIso8601WhenAssigned() throws Exception {
        FaroSink<String> sink = sinkWithFeatures("feature-a");
        long watermarkMs = Instant.parse("2026-03-21T12:00:00Z").toEpochMilli();
        SinkFunction.Context ctx = mock(SinkFunction.Context.class);
        when(ctx.currentWatermark()).thenReturn(watermarkMs);
        sink.invoke("r1", ctx);
        sink.flush();

        assertEquals("2026-03-21T12:00:00Z", captured.events.get(0).getWatermark());
    }

    private static final class NoopSink<T> implements SinkFunction<T> {
        @Override
        public void invoke(T value, Context context) {}
    }

    private static final class FailingAfterNSink<T> implements SinkFunction<T> {
        private final int succeedCount;
        private int invocations = 0;

        FailingAfterNSink(int succeedCount) {
            this.succeedCount = succeedCount;
        }

        @Override
        public void invoke(T value, Context context) {
            invocations++;
            if (invocations > succeedCount) {
                throw new RuntimeException("simulated write failure");
            }
        }
    }
}
