package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;

/**
 * Static factory for Faro instrumentation wrappers.
 *
 * <p>Usage:
 * <pre>{@code
 * stream.process(Faro.process(CaptureEvent.OperatorType.MAP, config, myFn, sink))
 *       .uid("my-op");
 *
 * keyedStream.process(Faro.keyedProcess(CaptureEvent.OperatorType.AGG, config, myKeyedFn, sink))
 *            .uid("my-keyed-op");
 * }</pre>
 */
public final class Faro {

    private Faro() {}

    public static <IN, OUT> FaroProcessFunction<IN, OUT> process(
            CaptureEvent.OperatorType type,
            FaroConfig config,
            ProcessFunction<IN, OUT> delegate,
            CaptureEventSink sink) {
        return new FaroProcessFunction<>(type, config, delegate, sink);
    }

    public static <KEY, IN, OUT> FaroKeyedProcessFunction<KEY, IN, OUT> keyedProcess(
            CaptureEvent.OperatorType type,
            FaroConfig config,
            KeyedProcessFunction<KEY, IN, OUT> delegate,
            CaptureEventSink sink) {
        return new FaroKeyedProcessFunction<>(type, config, delegate, sink);
    }
}
