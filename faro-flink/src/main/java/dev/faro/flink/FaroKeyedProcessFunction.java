package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Faro observability decorator for Flink {@link KeyedProcessFunction}.
 *
 * <p>Wraps a delegate {@link KeyedProcessFunction} and emits {@link CaptureEvent} instances on
 * each flush interval. The delegate is invoked first on every element and timer callback;
 * capture is best-effort and never interferes with pipeline execution.
 *
 * <p>In addition to the metrics captured by {@link FaroProcessFunction}, this class tracks the
 * number of timer callbacks ({@code timer_fired_count}) fired since the last flush interval.
 * The count is reset on each flush.
 *
 * <p>Operator type must be {@code FILTER}, {@code MAP}, or {@code AGG}. See
 * {@link FaroProcessFunctionBase} for UID and watermark semantics.
 *
 * @param <KEY> key type
 * @param <IN>  input type
 * @param <OUT> output type
 */
public final class FaroKeyedProcessFunction<KEY, IN, OUT> extends KeyedProcessFunction<KEY, IN, OUT> {

    private final KeyedProcessFunction<KEY, IN, OUT> delegate;
    private final FaroProcessFunctionBase<IN, OUT> base;

    private transient AtomicLong timerCounter;

    FaroKeyedProcessFunction(
            CaptureEvent.OperatorType operatorType,
            FaroConfig config,
            KeyedProcessFunction<KEY, IN, OUT> delegate,
            CaptureEventSink captureEventSink) {
        this.delegate = delegate;
        this.base = new FaroProcessFunctionBase<>(operatorType, config, captureEventSink) {
            @Override
            RuntimeContext getRuntimeContext() {
                return FaroKeyedProcessFunction.this.getRuntimeContext();
            }

            @Override
            protected Long timerFiredCountSnapshot() {
                return timerCounter.getAndSet(0);
            }
        };
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        timerCounter = new AtomicLong(0);
        base.open(parameters, delegate);
    }

    @Override
    public void processElement(IN value, Context ctx, Collector<OUT> out) throws Exception {
        base.processElement(ctx.timestamp(), ctx.timerService(), () -> delegate.processElement(value, ctx, out));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        timerCounter.incrementAndGet();
        delegate.onTimer(timestamp, ctx, out);
    }

    @Override
    public void close() throws Exception {
        base.close(delegate);
    }

    void flush() {
        base.flush();
    }
}
