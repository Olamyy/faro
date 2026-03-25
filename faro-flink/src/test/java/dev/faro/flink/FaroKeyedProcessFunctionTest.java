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

    @Test
    void onTimer_delegatesAndCounts() throws Exception {
        CountingKeyedFn delegate = new CountingKeyedFn();
        FaroConfig<String> config = FaroConfig.<String>builder()
                .features("feature-a")
                .build();
        FaroKeyedProcessFunction<String, String, String> fn = new FaroKeyedProcessFunction<>(
                CaptureEvent.OperatorType.AGG, PIPELINE_ID, config, delegate, captured);
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());

        fn.onTimer(1000L, mockOnTimerCtx(), noopCollector());
        fn.onTimer(2000L, mockOnTimerCtx(), noopCollector());

        fn.flush();
        assertEquals(2L, captured.events.get(0).getTimerFiredCount());
        assertEquals(2, delegate.onTimerCallCount);
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
