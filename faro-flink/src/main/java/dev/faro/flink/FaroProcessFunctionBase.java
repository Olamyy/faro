package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import javax.annotation.Nonnull;
import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared capture state and flush logic for {@link FaroProcessFunction} and
 * {@link FaroKeyedProcessFunction}.
 *
 * <p>Valid operator types are {@code FILTER}, {@code MAP}, and {@code AGG}. Any other value
 * throws {@link IllegalArgumentException} at construction time.
 *
 * <p>A stable operator UID must be set via {@code operator.uid("...")}. {@link #open} throws
 * {@link IllegalStateException} if the UID is absent or empty.
 *
 * <p>{@code event_time} in emitted events reflects the maximum context timestamp seen within
 * the flush interval; {@code event_time_min} reflects the minimum. Both are {@code null} when
 * no records with a non-{@code Long.MIN_VALUE} timestamp arrived in the interval.
 */
class FaroProcessFunctionBase<IN, OUT> implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final Set<CaptureEvent.OperatorType> VALID_TYPES =
            Set.of(CaptureEvent.OperatorType.FILTER,
                   CaptureEvent.OperatorType.MAP,
                   CaptureEvent.OperatorType.AGG);

    final CaptureEvent.OperatorType operatorType;
    final FaroConfig config;
    final CaptureEventSinkFactory captureEventSinkFactory;

    transient RichFunction owner;
    transient CaptureEventSink captureEventSink;
    transient String operatorId;
    transient String traceId;
    transient IntervalCounters counters;
    transient volatile long lastWatermarkMs;

    /**
     * Optional timer counter wired by {@link FaroKeyedProcessFunction}. When non-null,
     * {@link #timerFiredCountSnapshot()} returns and resets this counter.
     */
    transient AtomicLong timerCounterRef;

    FaroProcessFunctionBase(
            CaptureEvent.OperatorType operatorType,
            FaroConfig config,
            CaptureEventSinkFactory captureEventSinkFactory,
            RichFunction owner) {
        if (!VALID_TYPES.contains(operatorType)) {
            throw new IllegalArgumentException(
                    "FaroProcessFunctionBase: operatorType must be FILTER, MAP, or AGG; got " + operatorType);
        }
        this.operatorType = operatorType;
        this.config = config;
        this.captureEventSinkFactory = captureEventSinkFactory;
        this.owner = owner;
    }

    void open(Configuration parameters, RichFunction ownerFn, Object delegate) throws Exception {
        this.owner = ownerFn;
        this.captureEventSink = captureEventSinkFactory.create();
        this.operatorId = getOperatorID();
        this.traceId = newTraceId();
        this.counters = new IntervalCounters();
        this.lastWatermarkMs = Long.MIN_VALUE;

        if (delegate instanceof RichFunction) {
            ((RichFunction) delegate).setRuntimeContext(getRuntimeContext());
            ((RichFunction) delegate).open(parameters);
        }
    }

    @Nonnull
    private String getOperatorID() {
        StreamingRuntimeContext rtc = (StreamingRuntimeContext) getRuntimeContext();
        String uid = rtc.getOperatorUniqueID();
        if (uid == null || uid.isEmpty()) {
            throw new IllegalStateException(
                    "Faro process function on pipeline '" + config.getPipelineId()
                    + "' has no stable operator UID. "
                    + "Call .uid(\"your-stable-id\") on the operator in your pipeline definition. "
                    + "Without a stable UID, lineage correlation will break across restarts.");
        }
        return uid;
    }

    RuntimeContext getRuntimeContext() {
        return owner.getRuntimeContext();
    }

    void processElement(Long timestamp, TimerService timerService, ThrowingRunnable delegateCall)
            throws Exception {
        counters.input.incrementAndGet();
        long ts = timestamp != null ? timestamp : Long.MIN_VALUE;
        long wm = timerService != null ? timerService.currentWatermark() : Long.MIN_VALUE;
        recordObserved(ts, wm);
        delegateCall.run();
        counters.output.incrementAndGet();
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }

    void recordObserved(long contextTimestamp, long contextWatermark) {
        lastWatermarkMs = contextWatermark;
        counters.recordTimestamp(contextTimestamp);
    }

    protected Long timerFiredCountSnapshot() {
        return timerCounterRef != null ? timerCounterRef.getAndSet(0) : null;
    }

    void flush() {
        Long timerFiredCount = timerFiredCountSnapshot();
        IntervalCounters.Snapshot s = counters.snapshot();
        if (s.input() == 0 && (timerFiredCount == null || timerFiredCount == 0)) return;

        String watermark = lastWatermarkMs == Long.MIN_VALUE
                ? null : Instant.ofEpochMilli(lastWatermarkMs).toString();

        emitCaptureEvents(captureEventSink, config, operatorId, operatorType, traceId,
                s.nowMs(), s.input(), s.output(), s.maxEventTimeMs(), s.minEventTimeMs(),
                s.intervalMs(), watermark, timerFiredCount);
    }

    static void emitCaptureEvents(
            CaptureEventSink sink,
            FaroConfig config,
            String operatorId,
            CaptureEvent.OperatorType operatorType,
            String traceId,
            long nowMs,
            long input,
            long output,
            long maxEventTimeMs,
            long minEventTimeMs,
            long intervalMs,
            String watermark,
            Long timerFiredCount) {
        String processingTime = Instant.ofEpochMilli(nowMs).toString();
        String spanId = newSpanId();
        String eventTime = maxEventTimeMs == Long.MIN_VALUE ? null : Instant.ofEpochMilli(maxEventTimeMs).toString();
        String eventTimeMin = minEventTimeMs == Long.MIN_VALUE ? null : Instant.ofEpochMilli(minEventTimeMs).toString();

        for (String featureName : config.getFeatureNames()) {
            sink.emit(CaptureEvent.builder()
                    .pipelineId(config.getPipelineId())
                    .operatorId(operatorId)
                    .operatorType(operatorType)
                    .captureMode(CaptureEvent.CaptureMode.AGGREGATE)
                    .processingTime(processingTime)
                    .featureName(featureName)
                    .inputCardinality(input)
                    .outputCardinality(output)
                    .emitIntervalMs(intervalMs)
                    .traceId(traceId)
                    .spanId(spanId)
                    .watermark(watermark)
                    .eventTime(eventTime)
                    .eventTimeMin(eventTimeMin)
                    .timerFiredCount(timerFiredCount)
                    .captureDropSinceLast(false)
                    .build());
        }
    }

    void close(Object delegate) throws Exception {
        if (captureEventSink != null) {
            flush();
            captureEventSink.close();
        }
        if (delegate instanceof RichFunction) {
            ((RichFunction) delegate).close();
        }
    }

    static String newTraceId() {
        byte[] bytes = new byte[16];
        ThreadLocalRandom.current().nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    static String newSpanId() {
        byte[] bytes = new byte[8];
        ThreadLocalRandom.current().nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }
}
