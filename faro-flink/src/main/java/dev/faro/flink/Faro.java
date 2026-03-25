package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

/**
 * Entry point for Faro instrumentation. Create one instance per pipeline:
 * <pre>{@code
 * Faro faro = new Faro("my-pipeline-id", sink);
 *
 * stream.process(faro.trace(OperatorType.MAP, myFn, config)).uid("my-op");
 * keyedStream.process(faro.keyedTrace(OperatorType.AGG, myKeyedFn, config)).uid("my-keyed-op");
 * windowedStream.process(faro.windowTrace(myWindowFn)).uid("my-window-op");
 * }</pre>
 *
 * <p>{@code pipelineId} is stable across restarts and shared by all operators in the pipeline.
 * It is injected into every emitted {@link CaptureEvent} by this instance.
 *
 * <p><b>Important:</b> {@code .uid()} must be called on the {@code DataStream} returned by
 * {@code .process()}, not on the stream before it. Wrong placement sets the UID on the upstream
 * operator; the adapter's UID validation in {@code open()} will catch it at runtime.
 */
public final class Faro {

    private final String pipelineId;
    private final CaptureEventSinkFactory sinkFactory;

    public Faro(String pipelineId, CaptureEventSinkFactory sinkFactory) {
        if (pipelineId == null || pipelineId.isEmpty()) {
            throw new IllegalArgumentException("Faro: pipelineId must not be null or empty");
        }
        Objects.requireNonNull(sinkFactory, "sinkFactory must not be null");
        this.pipelineId = pipelineId;
        this.sinkFactory = sinkFactory;
    }

    public <IN, OUT> FaroProcessFunction<IN, OUT> trace(
            CaptureEvent.OperatorType type,
            ProcessFunction<IN, OUT> delegate,
            FaroConfig<IN> config) {
        return new FaroProcessFunction<>(type, pipelineId, config, delegate, sinkFactory);
    }

    public <KEY, IN, OUT> FaroKeyedProcessFunction<KEY, IN, OUT> keyedTrace(
            CaptureEvent.OperatorType type,
            KeyedProcessFunction<KEY, IN, OUT> delegate,
            FaroConfig<IN> config) {
        return new FaroKeyedProcessFunction<>(type, pipelineId, config, delegate, sinkFactory);
    }

    public <IN, OUT, KEY, W extends Window> FaroProcessWindowFunction<IN, OUT, KEY, W> windowTrace(
            ProcessWindowFunction<IN, OUT, KEY, W> delegate,
            FaroConfig<IN> config) {
        return new FaroProcessWindowFunction<>(pipelineId, config, delegate, sinkFactory, null);
    }

    public <IN, OUT, KEY, W extends Window> FaroProcessWindowFunction<IN, OUT, KEY, W> windowTrace(
            ProcessWindowFunction<IN, OUT, KEY, W> delegate,
            FaroConfig<IN> config,
            OutputTag<IN> lateDataTag) {
        return new FaroProcessWindowFunction<>(pipelineId, config, delegate, sinkFactory, lateDataTag);
    }
}
