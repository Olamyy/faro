package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Faro observability decorator for Flink {@link SinkFunction}.
 *
 * <p>Wraps an existing sink and emits a {@link CaptureEvent} per configured feature on each
 * flush interval. The delegate sink is invoked first on every record; capture is best-effort
 * and never interferes with pipeline execution.
 *
 * <p><b>Flink version:</b> targets Flink 1.18. {@link SinkFunction} was deprecated in 1.15
 * in favour of the {@code Sink} interface. This implementation covers pipelines on Flink
 * 1.13–1.18 that use the legacy sink API. Support for the new {@code Sink} interface is
 * deferred.
 *
 * <p><b>UID requirement:</b> the operator wrapping this sink must have a stable UID set via
 * {@code operator.uid("...")}. The UID is read at {@link #open} via
 * {@link StreamingRuntimeContext#getOperatorUniqueID()} (available since Flink 1.14). If no
 * stable UID is present, {@link #open} throws {@link IllegalStateException} — the job will
 * not start.
 *
 * <p><b>Watermark semantics:</b> the watermark field in emitted events reflects the most
 * recent watermark seen across all {@link #invoke} calls in the flush interval.
 * {@link Long#MIN_VALUE} (Flink's sentinel for "no watermark assigned") is emitted as
 * {@code null}. This is a known limitation of sink-only instrumentation: the captured
 * watermark is the most advanced watermark in the job at the time of the last record in
 * the interval, not a per-feature watermark.
 *
 * <p><b>Output cardinality semantics:</b> {@code output_cardinality} reflects the number of
 * {@link SinkFunction#invoke} calls that returned without throwing, not confirmed writes to the
 * external system. Sinks that swallow individual record errors will report full output
 * cardinality even when some writes failed silently. This is a known limitation of the
 * {@link SinkFunction} contract.
 *
 * <p><b>Multi-feature sinks:</b> one {@link CaptureEvent} is emitted per feature per flush
 * interval, all sharing the same {@code operator_id} and {@code input_cardinality}. See the
 * schema spec for the consumer rule on summing cardinality across co-emitted events.
 *
 * <p><b>Flush on close:</b> a final flush is triggered in {@link #close} to capture the
 * partial interval before the operator shuts down.
 *
 * @param <IN> the input record type accepted by the delegate sink
 */
public final class FaroSink<IN> extends RichSinkFunction<IN> {

    private final SinkFunction<IN> delegate;
    private final FaroConfig config;
    private final CaptureEventSinkFactory captureEventSinkFactory;

    private transient CaptureEventSink captureEventSink;
    private transient String operatorId;
    private transient String traceId;
    private transient AtomicLong inputCounter;
    private transient AtomicLong outputCounter;
    private transient long intervalStartMs;
    private transient volatile long lastWatermark;

    public FaroSink(SinkFunction<IN> delegate, FaroConfig config, CaptureEventSinkFactory captureEventSinkFactory) {
        this.delegate = delegate;
        this.config = config;
        this.captureEventSinkFactory = captureEventSinkFactory;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String uid = ((StreamingRuntimeContext) getRuntimeContext()).getOperatorUniqueID();
        if (uid == null || uid.isEmpty()) {
            throw new IllegalStateException(
                    "FaroSink on pipeline '" + config.getPipelineId() + "' has no stable operator UID. " +
                    "Call .uid(\"your-stable-id\") on the operator in your pipeline definition. " +
                    "Without a stable UID, lineage correlation will break across restarts.");
        }

        this.captureEventSink = captureEventSinkFactory.create();
        this.operatorId = uid;
        this.traceId = newTraceId();
        this.inputCounter = new AtomicLong(0);
        this.outputCounter = new AtomicLong(0);
        this.intervalStartMs = System.currentTimeMillis();
        this.lastWatermark = Long.MIN_VALUE;

        if (delegate instanceof RichSinkFunction) {
            ((RichSinkFunction<IN>) delegate).setRuntimeContext(getRuntimeContext());
            ((RichSinkFunction<IN>) delegate).open(parameters);
        }
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        inputCounter.incrementAndGet();
        if (context != null) {
            lastWatermark = context.currentWatermark();
        }
        delegate.invoke(value, context);
        outputCounter.incrementAndGet();
    }

    @Override
    public void close() throws Exception {
        if (captureEventSink != null) {
            flush();
            captureEventSink.close();
        }
        if (delegate instanceof RichSinkFunction) {
            ((RichSinkFunction<IN>) delegate).close();
        }
    }

    void flush() {
        long now = System.currentTimeMillis();
        long input = inputCounter.getAndSet(0);
        long output = outputCounter.getAndSet(0);
        long intervalMs = now - intervalStartMs;
        intervalStartMs = now;

        String processingTime = Instant.ofEpochMilli(now).toString();
        String spanId = newSpanId();
        String watermark = lastWatermark == Long.MIN_VALUE
                ? null
                : Instant.ofEpochMilli(lastWatermark).toString();
        List<String> features = config.getFeatureNames();

        for (String featureName : features) {
            CaptureEvent event = CaptureEvent.builder()
                    .pipelineId(config.getPipelineId())
                    .operatorId(operatorId)
                    .operatorType(CaptureEvent.OperatorType.SINK)
                    .captureMode(CaptureEvent.CaptureMode.AGGREGATE)
                    .processingTime(processingTime)
                    .featureName(featureName)
                    .inputCardinality(input)
                    .outputCardinality(output)
                    .emitIntervalMs(intervalMs)
                    .traceId(traceId)
                    .spanId(spanId)
                    .watermark(watermark)
                    .captureDropSinceLast(false)
                    .build();

            captureEventSink.emit(event);
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
