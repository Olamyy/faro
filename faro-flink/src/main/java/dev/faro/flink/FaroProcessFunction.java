package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Faro observability decorator for Flink {@link ProcessFunction}.
 *
 * <p>Wraps a delegate {@link ProcessFunction} and emits {@link CaptureEvent} instances on each
 * flush interval. The delegate is invoked first on every element; capture is best-effort and
 * never interferes with pipeline execution.
 *
 * <p>Operator type must be {@code FILTER}, {@code MAP}, or {@code AGG}. See
 * {@link FaroProcessFunctionBase} for UID and watermark semantics.
 *
 * @param <IN>  input type
 * @param <OUT> output type
 */
public final class FaroProcessFunction<IN, OUT> extends ProcessFunction<IN, OUT> {

    private final ProcessFunction<IN, OUT> delegate;
    private final FaroProcessFunctionBase<IN, OUT> base;

    FaroProcessFunction(
            CaptureEvent.OperatorType operatorType,
            FaroConfig config,
            ProcessFunction<IN, OUT> delegate,
            CaptureEventSink captureEventSink) {
        this.delegate = delegate;
        this.base = new FaroProcessFunctionBase<>(operatorType, config, captureEventSink) {
            @Override
            RuntimeContext getRuntimeContext() {
                return FaroProcessFunction.this.getRuntimeContext();
            }
        };
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        base.open(parameters, (RichFunction) delegate);
    }

    @Override
    public void processElement(IN value, Context ctx, Collector<OUT> out) throws Exception {
        base.processElement(ctx.timestamp(), ctx.timerService(), () -> delegate.processElement(value, ctx, out));
    }

    @Override
    public void close() throws Exception {
        base.close((RichFunction) delegate);
    }

    void flush() {
        base.flush();
    }
}
