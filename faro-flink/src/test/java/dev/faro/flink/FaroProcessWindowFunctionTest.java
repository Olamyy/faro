package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
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
    void process_windowBoundsAreNullForNonTimeWindow() throws Exception {
        FaroConfig<String> config = FaroConfig.<String>builder()
                .features("feature-a")
                .build();
        FaroProcessWindowFunction<String, String, String, CustomWindow> fn =
                new FaroProcessWindowFunction<>(
                        PIPELINE_ID, config, new CustomWindowFn(), captured, null);
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());

        @SuppressWarnings("unchecked")
        ProcessWindowFunction<String, String, String, CustomWindow>.Context ctx =
                (ProcessWindowFunction<String, String, String, CustomWindow>.Context)
                        mock(ProcessWindowFunction.Context.class);
        when(ctx.window()).thenReturn(new CustomWindow());
        when(ctx.currentProcessingTime()).thenReturn(PROCESSING_TIME_MS);
        when(ctx.currentWatermark()).thenReturn(Long.MIN_VALUE);
        when(ctx.windowState()).thenReturn(mock(KeyedStateStore.class));
        when(ctx.globalState()).thenReturn(mock(KeyedStateStore.class));

        fn.process("key", ctx, List.of("r1"), noopCollector());

        assertNull(captured.events.get(0).getWindowStart());
        assertNull(captured.events.get(0).getWindowEnd());
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

    @Test
    void close_closesDelegateSink() throws Exception {
        TrackingCloseFn delegate = new TrackingCloseFn();
        FaroConfig<String> config = FaroConfig.<String>builder()
                .features("feature-a")
                .build();
        TrackingCaptureEventSink sink = new TrackingCaptureEventSink();
        FaroProcessWindowFunction<String, String, String, TimeWindow> fn =
                new FaroProcessWindowFunction<>(PIPELINE_ID, config, delegate, sink, null);
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());
        fn.close();

        assertTrue(sink.closed);
        assertTrue(delegate.closed);
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

    private static final class TrackingCloseFn
            extends ProcessWindowFunction<String, String, String, TimeWindow>
            implements org.apache.flink.api.common.functions.RichFunction {
        boolean closed = false;

        @Override
        public void process(String key, Context ctx,
                Iterable<String> elements, Collector<String> out) {}

        @Override
        public void open(Configuration parameters) {}

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public void setRuntimeContext(org.apache.flink.api.common.functions.RuntimeContext ctx) {}

        @Override
        public org.apache.flink.api.common.functions.RuntimeContext getRuntimeContext() {
            return null;
        }

        @Override
        public org.apache.flink.api.common.functions.IterationRuntimeContext getIterationRuntimeContext() {
            return null;
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

    private static final class CustomWindow extends Window {
        @Override
        public long maxTimestamp() {
            return Long.MAX_VALUE;
        }
    }

    private static final class CustomWindowFn
            extends ProcessWindowFunction<String, String, String, CustomWindow> {
        @Override
        public void process(String key, Context ctx,
                Iterable<String> elements, Collector<String> out) {
            for (String e : elements) {
                out.collect(e);
            }
        }
    }

    private static final class TrackingCaptureEventSink implements CaptureEventSink, CaptureEventSinkFactory {
        boolean closed = false;

        @Override
        public CaptureEventSink create() {
            return this;
        }

        @Override
        public void emit(CaptureEvent event) {}

        @Override
        public void close() {
            closed = true;
        }
    }
}
