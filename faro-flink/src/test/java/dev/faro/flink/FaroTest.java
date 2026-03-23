package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FaroTest {

    private static final String PIPELINE_ID = "test-pipeline";
    private static final String OPERATOR_UID = "faro.test-operator";

    private CapturingCaptureEventSink captured;
    private StreamingRuntimeContext runtimeContext;
    private Faro faro;

    @BeforeEach
    void setUp() {
        captured = new CapturingCaptureEventSink();
        runtimeContext = mock(StreamingRuntimeContext.class);
        when(runtimeContext.getOperatorUniqueID()).thenReturn(OPERATOR_UID);
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();
        faro = new Faro(config, captured);
    }

    @Test
    void trace_wiresPipelineIdFromInstanceConfig() throws Exception {
        FaroProcessFunction<String, String> fn = faro.trace(
                CaptureEvent.OperatorType.MAP, new NoopProcessFn());
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());
        fn.processElement("r1", mockProcessCtx(), mock(Collector.class));
        fn.flush();

        assertEquals(PIPELINE_ID, captured.events.get(0).getPipelineId());
    }

    @Test
    void keyedTrace_wiresPipelineIdFromInstanceConfig() throws Exception {
        FaroKeyedProcessFunction<String, String, String> fn = faro.keyedTrace(
                CaptureEvent.OperatorType.AGG, new NoopKeyedProcessFn());
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());
        fn.processElement("r1", mockKeyedProcessCtx(), mock(Collector.class));
        fn.flush();

        assertEquals(PIPELINE_ID, captured.events.get(0).getPipelineId());
    }

    @Test
    void windowTrace_wiresPipelineIdFromInstanceConfig() throws Exception {
        FaroProcessWindowFunction<String, String, String, TimeWindow> fn =
                faro.windowTrace(new NoopWindowFn());
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());

        fn.process("key", mockWindowCtx(new TimeWindow(1000L, 2000L)),
                List.of("r1"), mock(Collector.class));

        assertEquals(PIPELINE_ID, captured.events.get(0).getPipelineId());
    }

    @Test
    void windowTrace_withLateDataTag_wiresTagToWindowFunction() throws Exception {
        OutputTag<String> lateTag = new OutputTag<>("late"){};
        FaroProcessWindowFunction<String, String, String, TimeWindow> fn =
                faro.windowTrace(new NoopWindowFn(), lateTag);
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());

        fn.process("key", mockWindowCtx(new TimeWindow(1000L, 2000L)),
                List.of("r1"), mock(Collector.class));

        assertEquals(CaptureEvent.LateTrackingMode.SIDE_OUTPUT,
                captured.events.get(0).getLateTrackingMode());
    }

    @SuppressWarnings("unchecked")
    private static ProcessFunction<String, String>.Context mockProcessCtx() {
        ProcessFunction<String, String>.Context ctx =
                (ProcessFunction<String, String>.Context) mock(ProcessFunction.Context.class);
        when(ctx.timestamp()).thenReturn(Long.MIN_VALUE);
        org.apache.flink.streaming.api.TimerService ts =
                mock(org.apache.flink.streaming.api.TimerService.class);
        when(ts.currentWatermark()).thenReturn(Long.MIN_VALUE);
        when(ctx.timerService()).thenReturn(ts);
        return ctx;
    }

    @SuppressWarnings("unchecked")
    private static KeyedProcessFunction<String, String, String>.Context mockKeyedProcessCtx() {
        KeyedProcessFunction<String, String, String>.Context ctx =
                (KeyedProcessFunction<String, String, String>.Context)
                        mock(KeyedProcessFunction.Context.class);
        when(ctx.timestamp()).thenReturn(Long.MIN_VALUE);
        org.apache.flink.streaming.api.TimerService ts =
                mock(org.apache.flink.streaming.api.TimerService.class);
        when(ts.currentWatermark()).thenReturn(Long.MIN_VALUE);
        when(ctx.timerService()).thenReturn(ts);
        return ctx;
    }

    @SuppressWarnings("unchecked")
    private static ProcessWindowFunction<String, String, String, TimeWindow>.Context mockWindowCtx(
            TimeWindow window) {
        ProcessWindowFunction<String, String, String, TimeWindow>.Context ctx =
                (ProcessWindowFunction<String, String, String, TimeWindow>.Context)
                        mock(ProcessWindowFunction.Context.class);
        when(ctx.window()).thenReturn(window);
        when(ctx.currentProcessingTime()).thenReturn(1000L);
        when(ctx.windowState()).thenReturn(mock(org.apache.flink.api.common.state.KeyedStateStore.class));
        when(ctx.globalState()).thenReturn(mock(org.apache.flink.api.common.state.KeyedStateStore.class));
        return ctx;
    }

    private static final class NoopProcessFn extends ProcessFunction<String, String> {
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {}
    }

    private static final class NoopKeyedProcessFn
            extends KeyedProcessFunction<String, String, String> {
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {}
    }

    private static final class NoopWindowFn
            extends ProcessWindowFunction<String, String, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx,
                Iterable<String> elements, Collector<String> out) {}
    }
}
