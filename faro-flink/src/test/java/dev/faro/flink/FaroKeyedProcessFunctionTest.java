package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FaroKeyedProcessFunctionTest {

    private static final String PIPELINE_ID = "test-pipeline";
    private static final String OPERATOR_UID = "keyed.test-operator";

    private CapturingCaptureEventSink captured;
    private StreamingRuntimeContext runtimeContext;

    @BeforeEach
    void setUp() {
        captured = new CapturingCaptureEventSink();
        runtimeContext = mock(StreamingRuntimeContext.class);
        when(runtimeContext.getOperatorUniqueID()).thenReturn(OPERATOR_UID);
    }

    private FaroKeyedProcessFunction<String, String, String> fnWithFeatures() throws Exception {
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();
        FaroKeyedProcessFunction<String, String, String> fn = new FaroKeyedProcessFunction<>(
                CaptureEvent.OperatorType.AGG, config, new PassThroughKeyedFn(), captured);
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());
        return fn;
    }

    @Test
    void flush_timerFiredCountIsZeroWhenNoTimersFired() throws Exception {
        FaroKeyedProcessFunction<String, String, String> fn = fnWithFeatures();
        fn.flush();

        assertEquals(0L, captured.events.get(0).getTimerFiredCount());
    }

    @Test
    void flush_timerFiredCountReflectsOnTimerCalls() throws Exception {
        FaroKeyedProcessFunction<String, String, String> fn = fnWithFeatures();
        fn.onTimer(1000L, mockOnTimerCtx(), noopCollector());
        fn.onTimer(2000L, mockOnTimerCtx(), noopCollector());
        fn.onTimer(3000L, mockOnTimerCtx(), noopCollector());
        fn.flush();

        assertEquals(3L, captured.events.get(0).getTimerFiredCount());
    }

    @Test
    void flush_timerFiredCountResetsAfterEachInterval() throws Exception {
        FaroKeyedProcessFunction<String, String, String> fn = fnWithFeatures();
        fn.onTimer(1000L, mockOnTimerCtx(), noopCollector());
        fn.onTimer(2000L, mockOnTimerCtx(), noopCollector());
        fn.flush();
        captured.events.clear();

        fn.onTimer(3000L, mockOnTimerCtx(), noopCollector());
        fn.flush();

        assertEquals(1L, captured.events.get(0).getTimerFiredCount());
    }

    @Test
    void onTimer_delegatesAndCounts() throws Exception {
        CountingKeyedFn delegate = new CountingKeyedFn();
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();
        FaroKeyedProcessFunction<String, String, String> fn = new FaroKeyedProcessFunction<>(
                CaptureEvent.OperatorType.AGG, config, delegate, captured);
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());

        fn.onTimer(1000L, mockOnTimerCtx(), noopCollector());
        fn.onTimer(2000L, mockOnTimerCtx(), noopCollector());

        assertEquals(2, delegate.onTimerCallCount);
        fn.flush();
        assertEquals(2L, captured.events.get(0).getTimerFiredCount());
    }

    private static KeyedProcessFunction<String, String, String>.OnTimerContext mockOnTimerCtx() {
        @SuppressWarnings("unchecked")
        KeyedProcessFunction<String, String, String>.OnTimerContext ctx =
                (KeyedProcessFunction<String, String, String>.OnTimerContext)
                        mock(KeyedProcessFunction.OnTimerContext.class);
        return ctx;
    }

    @SuppressWarnings("unchecked")
    private static Collector<String> noopCollector() {
        return mock(Collector.class);
    }

    private static final class PassThroughKeyedFn extends KeyedProcessFunction<String, String, String> {
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            out.collect(value);
        }
    }

    private static final class CountingKeyedFn extends KeyedProcessFunction<String, String, String> {
        int onTimerCallCount = 0;

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
            onTimerCallCount++;
        }
    }
}
