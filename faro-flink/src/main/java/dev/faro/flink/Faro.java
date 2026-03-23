package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

/**
 * Entry point for Faro instrumentation.
 *
 * <p>Create one instance per pipeline, then call the {@code trace()} methods inline:
 * <pre>{@code
 * Faro faro = new Faro(config, sink);
 *
 * stream.process(faro.trace(OperatorType.MAP, myFn))
 *       .uid("my-op");
 *
 * keyedStream.process(faro.keyedTrace(OperatorType.AGG, myKeyedFn))
 *            .uid("my-keyed-op");
 *
 * windowedStream.process(faro.windowTrace(myWindowFn))
 *               .uid("my-window-op");
 *
 * windowedStream.process(faro.windowTrace(myWindowFn, lateDataTag))
 *               .uid("my-window-op");
 * }</pre>
 */
public final class Faro {

    private final FaroConfig config;
    private final CaptureEventSinkFactory sinkFactory;

    public Faro(FaroConfig config, CaptureEventSinkFactory sinkFactory) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(sinkFactory, "sinkFactory must not be null");
        this.config = config;
        this.sinkFactory = sinkFactory;
    }

    public <IN, OUT> FaroProcessFunction<IN, OUT> trace(
            CaptureEvent.OperatorType type,
            ProcessFunction<IN, OUT> delegate) {
        return new FaroProcessFunction<>(type, config, delegate, sinkFactory);
    }

    public <KEY, IN, OUT> FaroKeyedProcessFunction<KEY, IN, OUT> keyedTrace(
            CaptureEvent.OperatorType type,
            KeyedProcessFunction<KEY, IN, OUT> delegate) {
        return new FaroKeyedProcessFunction<>(type, config, delegate, sinkFactory);
    }

    public <IN, OUT, KEY, W extends Window> FaroProcessWindowFunction<IN, OUT, KEY, W> windowTrace(
            ProcessWindowFunction<IN, OUT, KEY, W> delegate) {
        return new FaroProcessWindowFunction<>(config, delegate, sinkFactory, null);
    }

    public <IN, OUT, KEY, W extends Window> FaroProcessWindowFunction<IN, OUT, KEY, W> windowTrace(
            ProcessWindowFunction<IN, OUT, KEY, W> delegate,
            OutputTag<IN> lateDataTag) {
        return new FaroProcessWindowFunction<>(config, delegate, sinkFactory, lateDataTag);
    }

}
