package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Faro observability decorator for Flink {@link KeyedProcessFunction}.
 *
 * <p>Extends {@link FaroProcessFunction} behaviour with {@code timer_fired_count}: the number
 * of {@link #onTimer} callbacks fired since the last flush, reset on each flush.
 *
 * <p>See {@link FaroProcessFunctionBase} for operator type constraints, UID requirements,
 * and watermark semantics.
 */
public final class FaroKeyedProcessFunction<KEY, IN, OUT> extends KeyedProcessFunction<KEY, IN, OUT> {

    private final KeyedProcessFunction<KEY, IN, OUT> delegate;
    private final FaroProcessFunctionBase base;

    private transient AtomicLong timerCounter;

    FaroKeyedProcessFunction(
            CaptureEvent.OperatorType operatorType,
            String pipelineId,
            FaroConfig<IN> config,
            KeyedProcessFunction<KEY, IN, OUT> delegate,
            CaptureEventSinkFactory captureEventSinkFactory) {
        this.delegate = delegate;
        this.base = new FaroProcessFunctionBase(operatorType, pipelineId, config, captureEventSinkFactory, this);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        timerCounter = new AtomicLong(0);
        base.timerCounterRef = timerCounter;
        base.open(parameters, this, delegate);
    }

    @Override
    public void processElement(IN value, Context ctx, Collector<OUT> out) throws Exception {
        base.processElement(value, ctx.timestamp(), ctx.timerService(), () -> delegate.processElement(value, ctx, out));
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
