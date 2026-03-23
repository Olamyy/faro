package dev.faro.flink;

import dev.faro.core.CaptureEvent;

import java.io.Serial;

/**
 * {@link CaptureEventSink} that prints each event as JSON to stdout.
 *
 * <p>Intended for local development and smoke-testing. Not suitable for production.
 */
public final class StdoutCaptureEventSink implements CaptureEventSink, CaptureEventSinkFactory {

    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public CaptureEventSink create() {
        return this;
    }

    @Override
    public void emit(CaptureEvent event) {
        System.out.println(event.toJson());
    }

    @Override
    public void close() {}
}
