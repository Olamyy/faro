package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import dev.faro.core.FaroConfig;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FaroProcessWindowFunctionTest {

    private static final String PIPELINE_ID = "test-pipeline";
    private static final String OPERATOR_UID = "window.test-operator";
    private static final long PROCESSING_TIME_MS = Instant.parse("2026-03-21T12:00:00Z").toEpochMilli();

    private CapturingCaptureEventSink captured;
    private StreamingRuntimeContext runtimeContext;

    @BeforeEach
    void setUp() {
        captured = new CapturingCaptureEventSink();
        runtimeContext = mock(StreamingRuntimeContext.class);
        when(runtimeContext.getOperatorUniqueID()).thenReturn(OPERATOR_UID);
    }

    private FaroProcessWindowFunction<String, String, String, TimeWindow> fnWithFeatures(
            String... features) throws Exception {
        FaroConfig<String> config = FaroConfig.<String>builder()
                .features(features)
                .build();
        FaroProcessWindowFunction<String, String, String, TimeWindow> fn =
                new FaroProcessWindowFunction<>(PIPELINE_ID, config, new PassThroughWindowFn(), captured, null);
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());
        return fn;
    }

    private ProcessWindowFunction<String, String, String, TimeWindow>.Context mockCtx(
            TimeWindow window) {
        @SuppressWarnings("unchecked")
        ProcessWindowFunction<String, String, String, TimeWindow>.Context ctx =
                (ProcessWindowFunction<String, String, String, TimeWindow>.Context)
                        mock(ProcessWindowFunction.Context.class);
        when(ctx.window()).thenReturn(window);
        when(ctx.currentProcessingTime()).thenReturn(PROCESSING_TIME_MS);
        when(ctx.currentWatermark()).thenReturn(Long.MIN_VALUE);
        when(ctx.windowState()).thenReturn(mock(KeyedStateStore.class));
        when(ctx.globalState()).thenReturn(mock(KeyedStateStore.class));
        return ctx;
    }

    @SuppressWarnings("unchecked")
    private static Collector<String> noopCollector() {
        return mock(Collector.class);
    }

    @Test
    void open_throwsWhenNoUid() {
        when(runtimeContext.getOperatorUniqueID()).thenReturn("");
        FaroConfig<String> config = FaroConfig.<String>builder()
                .features("feature-a")
                .build();
        FaroProcessWindowFunction<String, String, String, TimeWindow> fn =
                new FaroProcessWindowFunction<>(PIPELINE_ID, config, new PassThroughWindowFn(), captured, null);
        fn.setRuntimeContext(runtimeContext);
        assertThrows(IllegalStateException.class, () -> fn.open(new Configuration()));
    }

    @Test
    void process_windowBoundsArePopulated() throws Exception {
        FaroProcessWindowFunction<String, String, String, TimeWindow> fn =
                fnWithFeatures("feature-a");
        long start = Instant.parse("2026-03-21T10:00:00Z").toEpochMilli();
        long end = Instant.parse("2026-03-21T11:00:00Z").toEpochMilli();
        fn.process("key", mockCtx(new TimeWindow(start, end)), List.of("r1"), noopCollector());

        assertEquals("2026-03-21T10:00:00Z", captured.events.get(0).getWindowStart());
        assertEquals("2026-03-21T11:00:00Z", captured.events.get(0).getWindowEnd());
    }

    @Test
    void process_lateEventCountTrackedViaSideOutput() throws Exception {
        OutputTag<String> lateTag = new OutputTag<>("late-data"){};
        FaroConfig<String> config = FaroConfig.<String>builder()
                .features("feature-a")
                .build();
        FaroProcessWindowFunction<String, String, String, TimeWindow> fn =
                new FaroProcessWindowFunction<>(
                        PIPELINE_ID, config, new SideOutputWindowFn(lateTag, 2), captured, lateTag);
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());

        fn.process("key", mockCtx(new TimeWindow(1000L, 2000L)), List.of("r1"), noopCollector());

        assertEquals(2L, captured.events.get(0).getLateEventCount());
        assertEquals(CaptureEvent.LateTrackingMode.SIDE_OUTPUT,
                captured.events.get(0).getLateTrackingMode());
    }

    private static final class PassThroughWindowFn
            extends ProcessWindowFunction<String, String, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx,
                Iterable<String> elements, Collector<String> out) {
            for (String e : elements) {
                out.collect(e);
            }
        }
    }

    private static final class SideOutputWindowFn
            extends ProcessWindowFunction<String, String, String, TimeWindow> {
        private final OutputTag<String> lateTag;
        private final int sideOutputCount;

        SideOutputWindowFn(OutputTag<String> lateTag, int sideOutputCount) {
            this.lateTag = lateTag;
            this.sideOutputCount = sideOutputCount;
        }

        @Override
        public void process(String key, Context ctx,
                Iterable<String> elements, Collector<String> out) {
            for (int i = 0; i < sideOutputCount; i++) {
                ctx.output(lateTag, "late-" + i);
            }
        }
    }

}
