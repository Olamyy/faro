package dev.faro.core;

public interface CaptureEventSink {

    /**
     * Emit a capture event. Must return immediately and must not throw.
     */
    void emit(CaptureEvent event);

    void close();

    default boolean droppedSinceLastFlush() {
        return false;
    }
}
