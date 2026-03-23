package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Faro observability decorator for Flink {@link ProcessFunction}.
 *
 * <p>See {@link FaroProcessFunctionBase} for operator type constraints, UID requirements,
 * and watermark semantics.
 */
public final class FaroProcessFunction<IN, OUT> extends ProcessFunction<IN, OUT> {

    private final ProcessFunction<IN, OUT> delegate;
    private final FaroProcessFunctionBase<IN, OUT> base;

    FaroProcessFunction(
            CaptureEvent.OperatorType operatorType,
            FaroConfig config,
            ProcessFunction<IN, OUT> delegate,
            CaptureEventSinkFactory captureEventSinkFactory) {
        this.delegate = delegate;
        this.base = new FaroProcessFunctionBase<>(operatorType, config, captureEventSinkFactory, this);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        base.open(parameters, this, delegate);
    }

    @Override
    public void processElement(IN value, Context ctx, Collector<OUT> out) throws Exception {
        base.processElement(ctx.timestamp(), ctx.timerService(), () -> delegate.processElement(value, ctx, out));
    }

    @Override
    public void close() throws Exception {
        base.close(delegate);
    }

    void flush() {
        base.flush();
    }
}
