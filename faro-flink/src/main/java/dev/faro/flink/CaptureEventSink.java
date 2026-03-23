package dev.faro.flink;

import dev.faro.core.CaptureEvent;

public interface CaptureEventSink {

    /**
     * Emit a capture event. Must return immediately and must not throw.
     */
    void emit(CaptureEvent event);

    void close();

    /**
     * Returns {@code true} if at least one event was dropped since the last call to this method,
     * then resets the flag to {@code false}.
     */
    default boolean droppedSinceLastFlush() {
        return false;
    }
}
