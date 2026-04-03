package dev.faro.spark;

import dev.faro.core.CaptureEvent;
import dev.faro.core.CaptureEventSink;
import dev.faro.core.CaptureEventSinkFactory;

import java.util.ArrayList;
import java.util.List;

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
