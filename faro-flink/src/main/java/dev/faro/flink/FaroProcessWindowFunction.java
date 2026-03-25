package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import dev.faro.core.DataClassification;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Faro observability decorator for Flink {@link ProcessWindowFunction}.
 *
 * <p>Emits one {@link CaptureEvent} per feature name on every window trigger. Window bounds,
 * input and output cardinality, late-event count (when a side-output tag is supplied), and
 * processing time are captured at window close.
 *
 * <p>A stable operator UID must be set via {@code operator.uid("...")}. {@link #open} throws
 * {@link IllegalStateException} if the UID is absent or empty.
 *
 * <p>{@code watermark} is always {@code null}: {@code ProcessWindowFunction.Context} does not
 * expose the current watermark. Window bounds serve as the temporal anchor.
 */
public final class FaroProcessWindowFunction<IN, OUT, KEY, W extends Window>
        extends ProcessWindowFunction<IN, OUT, KEY, W> {

    private final String pipelineId;
    @SuppressWarnings("rawtypes")
    private final FaroConfig config;
    private final ProcessWindowFunction<IN, OUT, KEY, W> delegate;
    private final CaptureEventSinkFactory captureEventSinkFactory;
    private final OutputTag<IN> lateDataTag;

    private transient CaptureEventSink captureEventSink;
    private transient String operatorId;
    private transient String traceId;

    @SuppressWarnings("rawtypes")
    FaroProcessWindowFunction(
            String pipelineId,
            FaroConfig config,
            ProcessWindowFunction<IN, OUT, KEY, W> delegate,
            CaptureEventSinkFactory captureEventSinkFactory,
            OutputTag<IN> lateDataTag) {
        this.pipelineId = pipelineId;
        this.config = config;
        this.delegate = delegate;
        this.captureEventSinkFactory = captureEventSinkFactory;
        this.lateDataTag = lateDataTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StreamingRuntimeContext rtc = (StreamingRuntimeContext) getRuntimeContext();
        String uid = rtc.getOperatorUniqueID();
        if (uid == null || uid.isEmpty()) {
            throw new IllegalStateException(
                    "Faro window function on pipeline '" + pipelineId
                    + "' has no stable operator UID. "
                    + "Call .uid(\"your-stable-id\") on the operator in your pipeline definition. "
                    + "Without a stable UID, lineage correlation will break across restarts.");
        }
        this.captureEventSink = captureEventSinkFactory.create();
        this.operatorId = uid;
        this.traceId = FaroProcessFunctionBase.newTraceId();

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
        String eventTime = null;
        String eventTimeMin = null;
        long emitIntervalMs = 0;
        if (ctx.window() instanceof TimeWindow tw) {
            windowStart = Instant.ofEpochMilli(tw.getStart()).toString();
            windowEnd = Instant.ofEpochMilli(tw.getEnd()).toString();
            emitIntervalMs = tw.getEnd() - tw.getStart();
            eventTime = Instant.ofEpochMilli(tw.getEnd() - 1).toString();
            eventTimeMin = Instant.ofEpochMilli(tw.getStart()).toString();
        }

        long wmMs = countingContext.currentWatermark();
        String watermark = wmMs == Long.MIN_VALUE ? null : Instant.ofEpochMilli(wmMs).toString();

        String processingTime = Instant.ofEpochMilli(ctx.currentProcessingTime()).toString();
        String spanId = FaroProcessFunctionBase.newSpanId();
        boolean dropped = captureEventSink.droppedSinceLastFlush();

        for (String featureName : ((Map<String, ?>) config.getFeatures()).keySet()) {
            CaptureEvent event = CaptureEvent.builder()
                    .pipelineId(pipelineId)
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
                    .watermark(watermark)
                    .eventTime(eventTime)
                    .eventTimeMin(eventTimeMin)
                    .traceId(traceId)
                    .spanId(spanId)
                    .captureDropSinceLast(dropped)
                    .build();

            captureEventSink.emit(event);
        }

        emitEntityEvents(list, eventTime, watermark, processingTime);

        if (delegateException != null) {
            throw delegateException;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void emitEntityEvents(List<IN> elements, String eventTime, String watermark, String processingTime) {
        Map<String, FaroFeatureConfig> features = (Map<String, FaroFeatureConfig>) config.getFeatures();
        for (Map.Entry<String, FaroFeatureConfig> entry : features.entrySet()) {
            FaroFeatureConfig<IN> fc = entry.getValue();
            if (fc == null) continue;

            for (IN record : elements) {
                if (fc.getSampleRate() < 1.0
                        && ThreadLocalRandom.current().nextDouble() >= fc.getSampleRate()) {
                    continue;
                }

                boolean suppress = fc.getClassification() == DataClassification.PERSONAL
                        || fc.getClassification() == DataClassification.SENSITIVE;

                CaptureEvent.Builder builder = CaptureEvent.builder()
                        .pipelineId(pipelineId)
                        .operatorId(operatorId)
                        .operatorType(CaptureEvent.OperatorType.WINDOW)
                        .featureName(entry.getKey())
                        .processingTime(processingTime)
                        .inputCardinality(1)
                        .outputCardinality(1)
                        .emitIntervalMs(0)
                        .traceId(traceId)
                        .spanId(FaroProcessFunctionBase.newSpanId())
                        .watermark(watermark)
                        .eventTime(eventTime)
                        .captureDropSinceLast(false);

                if (suppress) {
                    builder.captureMode(CaptureEvent.CaptureMode.AGGREGATE);
                } else {
                    String entityId = fc.getEntityKey().apply(record);
                    Object value = fc.getFeatureValue().apply(record);
                    builder.captureMode(CaptureEvent.CaptureMode.ENTITY)
                            .entityId(entityId)
                            .featureValueType(fc.getValueType())
                            .featureValue(FaroProcessFunctionBase.valueToBytes(value, fc.getValueType()));
                }

                captureEventSink.emit(builder.build());
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (captureEventSink != null) {
            captureEventSink.close();
        }
        if (delegate != null) {
            delegate.close();
        }
    }

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

}
