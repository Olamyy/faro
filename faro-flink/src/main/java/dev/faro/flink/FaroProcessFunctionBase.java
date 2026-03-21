package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared capture state and flush logic for Faro process function wrappers.
 *
 * <p>Concrete subclasses extend the appropriate Flink base class ({@link FaroProcessFunction}
 * for stateless operators, {@link FaroKeyedProcessFunction} for keyed operators) and call
 * {@link #recordObserved} from their {@code processElement} implementations.
 *
 * <p><b>Operator type constraint:</b> only {@code FILTER}, {@code MAP}, and {@code AGG} are
 * valid for mid-DAG instrumentation. Passing any other type throws
 * {@link IllegalArgumentException} at construction time.
 *
 * <p><b>UID requirement:</b> identical to {@link FaroSink} — a stable operator UID must be
 * set via {@code operator.uid("...")}. {@link #open} throws {@link IllegalStateException}
 * if the UID is absent or empty.
 *
 * <p><b>Event-time semantics:</b> {@code event_time} in emitted events reflects the maximum
 * context timestamp seen within the flush interval; {@code event_time_min} reflects the
 * minimum. Both are {@code null} when no records with a non-{@code Long.MIN_VALUE} timestamp
 * arrived in the interval.
 */
abstract class FaroProcessFunctionBase<IN, OUT> {

    private static final Set<CaptureEvent.OperatorType> VALID_TYPES =
            Set.of(CaptureEvent.OperatorType.FILTER,
                   CaptureEvent.OperatorType.MAP,
                   CaptureEvent.OperatorType.AGG);

    final CaptureEvent.OperatorType operatorType;
    final FaroConfig config;
    final CaptureEventSink captureEventSink;

    transient String operatorId;
    transient String traceId;
    transient AtomicLong inputCounter;
    transient AtomicLong outputCounter;
    transient AtomicLong eventTimeMaxMs;
    transient AtomicLong eventTimeMinMs;
    transient long intervalStartMs;
    transient volatile long lastWatermarkMs;

    FaroProcessFunctionBase(
            CaptureEvent.OperatorType operatorType,
            FaroConfig config,
            CaptureEventSink captureEventSink) {
        if (!VALID_TYPES.contains(operatorType)) {
            throw new IllegalArgumentException(
                    "FaroProcessFunctionBase: operatorType must be FILTER, MAP, or AGG; got " + operatorType);
        }
        this.operatorType = operatorType;
        this.config = config;
        this.captureEventSink = captureEventSink;
    }

    /**
     * Initialise transient capture state. Subclasses must call {@code super.open(parameters)}
     * before their own initialisation.
     *
     * @throws IllegalStateException if the operator has no stable UID
     */
    void open(Configuration parameters, RichFunction richDelegate) throws Exception {

        this.operatorId = getOperatorID();
        this.traceId = newTraceId();
        this.inputCounter = new AtomicLong(0);
        this.outputCounter = new AtomicLong(0);
        this.eventTimeMaxMs = new AtomicLong(Long.MIN_VALUE);
        this.eventTimeMinMs = new AtomicLong(Long.MIN_VALUE);
        this.intervalStartMs = System.currentTimeMillis();
        this.lastWatermarkMs = Long.MIN_VALUE;

        if (richDelegate != null) {
            richDelegate.setRuntimeContext(getRuntimeContext());
            richDelegate.open(parameters);
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

    /**
     * Returns the {@link StreamingRuntimeContext} for this operator. Subclasses must provide
     * their runtime context here; implemented via the concrete Flink base class's
     * {@code getRuntimeContext()}.
     */
    abstract org.apache.flink.api.common.functions.RuntimeContext getRuntimeContext();

    /**
     * Shared {@code processElement} body. Increments input counter, records timestamp and
     * watermark from {@code timerService}, invokes {@code delegateCall}, then increments
     * output counter. The {@code delegateCall} must throw if the delegate fails — the output
     * counter is not incremented on exception.
     *
     * @param timestamp    element event-time timestamp, or {@code null} if unset
     * @param timerService the operator's timer service (source of current watermark)
     * @param delegateCall the delegate {@code processElement} invocation
     */
    void processElement(Long timestamp, TimerService timerService, ThrowingRunnable delegateCall)
            throws Exception {
        inputCounter.incrementAndGet();
        long ts = timestamp != null ? timestamp : Long.MIN_VALUE;
        long wm = timerService != null ? timerService.currentWatermark() : Long.MIN_VALUE;
        recordObserved(ts, wm);
        delegateCall.run();
        outputCounter.incrementAndGet();
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * Records a single observed element. Called from {@link #processElement} after
     * {@link #inputCounter} has been incremented.
     *
     * @param contextTimestamp  the element's event-time timestamp ({@code ctx.timestamp()})
     * @param contextWatermark  the current watermark
     */
    void recordObserved(long contextTimestamp, long contextWatermark) {
        lastWatermarkMs = contextWatermark;

        if (contextTimestamp != Long.MIN_VALUE) {
            eventTimeMaxMs.getAndUpdate(prev -> prev == Long.MIN_VALUE
                    ? contextTimestamp
                    : Math.max(prev, contextTimestamp));
            eventTimeMinMs.getAndUpdate(prev -> prev == Long.MIN_VALUE
                    ? contextTimestamp
                    : Math.min(prev, contextTimestamp));
        }
    }

    /**
     * Returns the timer-fired count snapshot for this interval, or {@code null} if this
     * operator does not track timers. Overridden by {@link FaroKeyedProcessFunction}.
     */
    protected Long timerFiredCountSnapshot() {
        return null;
    }

    /**
     * Emits one {@link CaptureEvent} per configured feature and resets interval counters.
     * Package-private to allow test access.
     */
    void flush() {
        long now = System.currentTimeMillis();
        long input = inputCounter.getAndSet(0);
        long output = outputCounter.getAndSet(0);
        long maxMs = eventTimeMaxMs.getAndSet(Long.MIN_VALUE);
        long minMs = eventTimeMinMs.getAndSet(Long.MIN_VALUE);
        long intervalMs = now - intervalStartMs;
        intervalStartMs = now;

        String processingTime = Instant.ofEpochMilli(now).toString();
        String spanId = newSpanId();
        String watermark = lastWatermarkMs == Long.MIN_VALUE
                ? null
                : Instant.ofEpochMilli(lastWatermarkMs).toString();
        String eventTimeMax = maxMs == Long.MIN_VALUE ? null : Instant.ofEpochMilli(maxMs).toString();
        String eventTimeMin = minMs == Long.MIN_VALUE ? null : Instant.ofEpochMilli(minMs).toString();
        Long timerFiredCount = timerFiredCountSnapshot();

        List<String> features = config.getFeatureNames();
        for (String featureName : features) {
            CaptureEvent event = CaptureEvent.builder()
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
                    .eventTime(eventTimeMax)
                    .eventTimeMin(eventTimeMin)
                    .timerFiredCount(timerFiredCount)
                    .captureDropSinceLast(false)
                    .build();

            captureEventSink.emit(event);
        }
    }

    void close(RichFunction richDelegate) throws Exception {
        flush();
        captureEventSink.close();
        if (richDelegate != null) {
            richDelegate.close();
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
