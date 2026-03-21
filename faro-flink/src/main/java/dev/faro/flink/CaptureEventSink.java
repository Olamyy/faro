package dev.faro.flink;

import dev.faro.core.CaptureEvent;

/**
 * Contract for emitting {@link CaptureEvent} instances out of the Flink operator thread.
 *
 * <p>Implementations must be non-blocking and must not throw. {@link FaroSink} calls
 * {@link #emit} on the flush timer thread — any blocking or exception propagation will
 * stall the flush interval and delay pipeline processing. Implementations own their own
 * buffering, backpressure, and error handling.
 *
 * <p>{@link #close()} is called once when the operator shuts down. Implementations should
 * flush any buffered events and release resources. {@link #close()} may block.
 */
public interface CaptureEventSink {

    void emit(CaptureEvent event);

    void close();
}
