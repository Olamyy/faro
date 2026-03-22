package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Faro observability decorator for Flink {@link ProcessWindowFunction}.
 *
 * <p>Emits one {@link CaptureEvent} per feature name on every window trigger. Window bounds,
 * input and output cardinality, late-event count (when a side-output tag is supplied), and
 * processing time are captured at the moment of window close.
 *
 * <p>A stable operator UID must be set via {@code operator.uid("...")}. {@link #open} throws
 * {@link IllegalStateException} if the UID is absent or empty.
 *
 * <p>Watermark is always {@code null}: {@code ProcessWindowFunction.Context} does not expose
 * the current watermark. Window bounds serve as the temporal anchor.
 *
 * @param <IN>  input element type
 * @param <OUT> output element type
 * @param <KEY> key type
 * @param <W>   window type
 */
public final class FaroProcessWindowFunction<IN, OUT, KEY, W extends Window>
        extends ProcessWindowFunction<IN, OUT, KEY, W> {

    private final FaroConfig config;
    private final ProcessWindowFunction<IN, OUT, KEY, W> delegate;
    private final CaptureEventSink captureEventSink;
    private final OutputTag<IN> lateDataTag;

    private transient String operatorId;
    private transient String traceId;

    FaroProcessWindowFunction(
            FaroConfig config,
            ProcessWindowFunction<IN, OUT, KEY, W> delegate,
            CaptureEventSink captureEventSink,
            OutputTag<IN> lateDataTag) {
        this.config = config;
        this.delegate = delegate;
        this.captureEventSink = captureEventSink;
        this.lateDataTag = lateDataTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StreamingRuntimeContext rtc = (StreamingRuntimeContext) getRuntimeContext();
        String uid = rtc.getOperatorUniqueID();
        if (uid == null || uid.isEmpty()) {
            throw new IllegalStateException(
                    "Faro window function on pipeline '" + config.getPipelineId()
                    + "' has no stable operator UID. "
                    + "Call .uid(\"your-stable-id\") on the operator in your pipeline definition. "
                    + "Without a stable UID, lineage correlation will break across restarts.");
        }
        this.operatorId = uid;
        this.traceId = newTraceId();

        if (delegate != null) {
            delegate.setRuntimeContext(getRuntimeContext());
            delegate.open(parameters);
        }
    }

    @Override
    public void process(KEY key, Context ctx, Iterable<IN> elements, Collector<OUT> out)
            throws Exception {
        List<IN> list = new ArrayList<>();
        for (IN element : elements) {
            list.add(element);
        }
        long inputCardinality = list.size();

        CountingCollector<OUT> countingCollector = new CountingCollector<>(out);
        CountingContext countingContext = new CountingContext(ctx);

        Exception delegateException = null;
        try {
            delegate.process(key, countingContext, list, countingCollector);
        } catch (Exception e) {
            delegateException = e;
        }

        long outputCardinality = countingCollector.count;
        Long lateEventCount = lateDataTag != null ? countingContext.lateCount : null;
        CaptureEvent.LateTrackingMode lateTrackingMode =
                lateDataTag != null ? CaptureEvent.LateTrackingMode.SIDE_OUTPUT : null;

        String windowStart = null;
        String windowEnd = null;
        long emitIntervalMs = 0;
        if (ctx.window() instanceof TimeWindow tw) {
            windowStart = Instant.ofEpochMilli(tw.getStart()).toString();
            windowEnd = Instant.ofEpochMilli(tw.getEnd()).toString();
            emitIntervalMs = tw.getEnd() - tw.getStart();
        }

        String processingTime = Instant.ofEpochMilli(ctx.currentProcessingTime()).toString();
        String spanId = newSpanId();
        List<String> features = config.getFeatureNames();

        for (String featureName : features) {
            CaptureEvent event = CaptureEvent.builder()
                    .pipelineId(config.getPipelineId())
                    .operatorId(operatorId)
                    .operatorType(CaptureEvent.OperatorType.WINDOW)
                    .captureMode(CaptureEvent.CaptureMode.AGGREGATE)
                    .featureName(featureName)
                    .processingTime(processingTime)
                    .inputCardinality(inputCardinality)
                    .outputCardinality(outputCardinality)
                    .emitIntervalMs(emitIntervalMs)
                    .windowStart(windowStart)
                    .windowEnd(windowEnd)
                    .lateEventCount(lateEventCount)
                    .lateTrackingMode(lateTrackingMode)
                    .watermark(null)
                    .eventTime(null)
                    .eventTimeMin(null)
                    .traceId(traceId)
                    .spanId(spanId)
                    .captureDropSinceLast(false)
                    .build();

            captureEventSink.emit(event);
        }

        if (delegateException != null) {
            throw delegateException;
        }
    }

    @Override
    public void close() throws Exception {
        captureEventSink.close();
        if (delegate != null) {
            delegate.close();
        }
    }

    /**
     * Counts successful {@link #collect} calls, then delegates to the wrapped collector.
     */
    static final class CountingCollector<T> implements Collector<T> {
        private final Collector<T> inner;
        long count = 0;

        CountingCollector(Collector<T> inner) {
            this.inner = inner;
        }

        @Override
        public void collect(T record) {
            count++;
            inner.collect(record);
        }

        @Override
        public void close() {
            inner.close();
        }
    }

    /**
     * Wraps the real {@link Context} and intercepts {@link #output} calls for the configured
     * late-data side-output tag to count late events.
     */
    final class CountingContext extends ProcessWindowFunction<IN, OUT, KEY, W>.Context {

        private final Context inner;
        long lateCount = 0;

        CountingContext(Context inner) {
            FaroProcessWindowFunction.this.super();
            this.inner = inner;
        }

        @Override
        public W window() {
            return inner.window();
        }

        @Override
        public long currentProcessingTime() {
            return inner.currentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return inner.currentWatermark();
        }

        @Override
        public KeyedStateStore windowState() {
            return inner.windowState();
        }

        @Override
        public KeyedStateStore globalState() {
            return inner.globalState();
        }

        @Override
        public <X> void output(OutputTag<X> tag, X value) {
            if (tag.equals(lateDataTag)) {
                lateCount++;
            }
            inner.output(tag, value);
        }
    }

    private static String newTraceId() {
        byte[] bytes = new byte[16];
        ThreadLocalRandom.current().nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    private static String newSpanId() {
        byte[] bytes = new byte[8];
        ThreadLocalRandom.current().nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }
}
