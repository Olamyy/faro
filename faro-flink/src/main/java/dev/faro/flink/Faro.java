package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import dev.faro.core.CaptureEventSinkFactory;
import dev.faro.core.FaroBase;
import dev.faro.core.FaroConfig;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.OutputTag;

/**
 * Entry point for Faro instrumentation in Flink pipelines. Create one instance per pipeline:
 * <pre>{@code
 * Faro faro = new Faro("my-pipeline-id", sink);
 *
 * stream.process(faro.trace(OperatorType.MAP, myFn, config)).uid("my-op");
 * keyedStream.process(faro.keyedTrace(OperatorType.AGG, myKeyedFn, config)).uid("my-keyed-op");
 * windowedStream.process(faro.windowTrace(myWindowFn, config)).uid("my-window-op");
 * }</pre>
 *
 * <p><b>Important:</b> {@code .uid()} must be called on the {@code DataStream} returned by
 * {@code .process()}, not on the stream before it. Wrong placement sets the UID on the upstream
 * operator; the adapter's UID validation in {@code open()} will catch it at runtime.
 */
public final class Faro extends FaroBase {

    public Faro(String pipelineId, CaptureEventSinkFactory sinkFactory) {
        super(pipelineId, sinkFactory);
    }

    public <IN, OUT> FaroProcessFunction<IN, OUT> trace(
            CaptureEvent.OperatorType type,
            ProcessFunction<IN, OUT> delegate,
            FaroConfig<IN> config) {
        return new FaroProcessFunction<>(type, getPipelineId(), config, delegate, getSinkFactory());
    }

    public <KEY, IN, OUT> FaroKeyedProcessFunction<KEY, IN, OUT> keyedTrace(
            CaptureEvent.OperatorType type,
            KeyedProcessFunction<KEY, IN, OUT> delegate,
            FaroConfig<IN> config) {
        return new FaroKeyedProcessFunction<>(type, getPipelineId(), config, delegate, getSinkFactory());
    }

    public <IN, OUT, KEY, W extends Window> FaroProcessWindowFunction<IN, OUT, KEY, W> windowTrace(
            ProcessWindowFunction<IN, OUT, KEY, W> delegate,
            FaroConfig<IN> config) {
        return new FaroProcessWindowFunction<>(getPipelineId(), config, delegate, getSinkFactory(), null);
    }

    public <IN, OUT, KEY, W extends Window> FaroProcessWindowFunction<IN, OUT, KEY, W> windowTrace(
            ProcessWindowFunction<IN, OUT, KEY, W> delegate,
            FaroConfig<IN> config,
            OutputTag<IN> lateDataTag) {
        return new FaroProcessWindowFunction<>(getPipelineId(), config, delegate, getSinkFactory(), lateDataTag);
    }
}
