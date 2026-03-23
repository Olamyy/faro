package dev.faro.flink;

import java.io.Serializable;

/**
 * Serializable factory for {@link CaptureEventSink}.
 *
 * <p>Flink serializes operator instances and ships them to task managers. This factory is
 * serialized instead of the sink itself; {@link #create()} is called once inside {@code open()}
 * on the task manager to construct the live sink.
 *
 * <p>Lambda factories are serializable only if they capture no non-serializable references —
 * violations fail at job submission, not at test time.
 */
@FunctionalInterface
public interface CaptureEventSinkFactory extends Serializable {

    CaptureEventSink create();
}
