package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.time.Instant;

/**
 * Faro observability decorator for Flink {@link Sink}.
 *
 * <p>Wraps a delegate sink and emits one {@link CaptureEvent} per configured feature on each
 * flush. {@code operatorId} must be stable across restarts — it is the lineage correlation key.
 * {@code output_cardinality} counts {@link Writer#write} calls that did not throw, not confirmed
 * external writes. Watermark is captured via {@link Writer#writeWatermark} — Flink calls this
 * directly as watermark elements pass through the operator, bypassing {@code SinkWriter.Context}
 * which always returns {@link Long#MIN_VALUE}.
 */
public final class FaroSink<IN> implements Sink<IN> {

    private final Sink<IN> delegate;
    private final FaroConfig config;
    private final CaptureEventSinkFactory captureEventSinkFactory;
    private final String operatorId;

    public FaroSink(
            Sink<IN> delegate,
            FaroConfig config,
            CaptureEventSinkFactory captureEventSinkFactory,
            String operatorId) {
        if (operatorId == null || operatorId.isEmpty()) {
            throw new IllegalArgumentException(
                    "FaroSink on pipeline '" + config.getPipelineId() + "' requires a non-empty operatorId. "
                    + "Pass a stable, unique string that identifies this sink across restarts.");
        }
        this.delegate = delegate;
        this.config = config;
        this.captureEventSinkFactory = captureEventSinkFactory;
        this.operatorId = operatorId;
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        return new Writer(delegate.createWriter(context));
    }

    final class Writer implements SinkWriter<IN> {

        private final SinkWriter<IN> delegateWriter;
        private final CaptureEventSink captureEventSink;
        private final String traceId;
        private final IntervalCounters counters = new IntervalCounters();
        private volatile long lastRealWatermarkMs = Long.MIN_VALUE;

        Writer(SinkWriter<IN> delegateWriter) {
            this.delegateWriter = delegateWriter;
            this.captureEventSink = captureEventSinkFactory.create();
            this.traceId = FaroProcessFunctionBase.newTraceId();
        }

        @Override
        public void write(IN element, Context context) throws IOException, InterruptedException {
            counters.input.incrementAndGet();
            counters.recordTimestamp(context.timestamp() != null ? context.timestamp() : Long.MIN_VALUE);
            delegateWriter.write(element, context);
            counters.output.incrementAndGet();
        }

        @Override
        public void writeWatermark(org.apache.flink.api.common.eventtime.Watermark watermark)
                throws IOException, InterruptedException {
            long ts = watermark.getTimestamp();
            if (ts != Long.MIN_VALUE && ts != Long.MAX_VALUE) {
                lastRealWatermarkMs = ts;
            }
            delegateWriter.writeWatermark(watermark);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            IntervalCounters.Snapshot s = counters.snapshot();
            if (s.input() > 0) {
                String watermark = lastRealWatermarkMs == Long.MIN_VALUE
                        ? null : Instant.ofEpochMilli(lastRealWatermarkMs).toString();
                FaroProcessFunctionBase.emitCaptureEvents(captureEventSink, config, operatorId,
                        CaptureEvent.OperatorType.SINK, traceId, s.nowMs(), s.input(), s.output(),
                        s.maxEventTimeMs(), s.minEventTimeMs(), s.intervalMs(), watermark, null);
            }
            delegateWriter.flush(endOfInput);
        }

        @Override
        public void close() throws Exception {
            flush(true);
            captureEventSink.close();
            delegateWriter.close();
        }
    }

}
