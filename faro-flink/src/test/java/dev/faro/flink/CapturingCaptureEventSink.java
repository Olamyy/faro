package dev.faro.flink;

import dev.faro.core.CaptureEvent;

import java.util.ArrayList;
import java.util.List;

final class CapturingCaptureEventSink implements CaptureEventSink {
    final List<CaptureEvent> events = new ArrayList<>();

    @Override
    public void emit(CaptureEvent event) {
        events.add(event);
    }

    @Override
    public void close() {}
}
