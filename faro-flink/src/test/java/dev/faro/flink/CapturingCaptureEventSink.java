package dev.faro.flink;

import dev.faro.core.CaptureEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Test double that captures emitted events and acts as its own factory.
 *
 * <p>{@link #create()} returns {@code this}, so the test holds the same instance that
 * receives events after {@code open()} is called. This assumes {@code open()} is called
 * at most once per test — correct for unit tests but not for multi-subtask scenarios.
 */
final class CapturingCaptureEventSink implements CaptureEventSink, CaptureEventSinkFactory {
    final List<CaptureEvent> events = new ArrayList<>();

    @Override
    public CaptureEventSink create() {
        return this;
    }

    @Override
    public void emit(CaptureEvent event) {
        events.add(event);
    }

    @Override
    public void close() {}
}
