package dev.faro.spark;

import dev.faro.core.CaptureEvent;
import dev.faro.core.CaptureEventSink;
import dev.faro.core.CaptureEventSinkFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Test double that captures emitted events and acts as its own factory.
 *
 * <p>{@link #create()} returns {@code this}, so the test holds the same instance that
 * receives events after construction. This assumes {@code create()} is called
 * at most once per test — correct for unit tests.
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
