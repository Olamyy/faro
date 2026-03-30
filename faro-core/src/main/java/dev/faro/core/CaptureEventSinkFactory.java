package dev.faro.core;

import java.io.Serializable;

/**
 * Serializable factory for {@link CaptureEventSink}.
 *
 * <p>Distributed runtimes serialize operator instances and ship them to remote nodes;
 * {@link #create()} is called on the remote node to construct the live sink.
 */
@FunctionalInterface
public interface CaptureEventSinkFactory extends Serializable {

    CaptureEventSink create();
}
