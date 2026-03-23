package dev.faro.flink;

import java.io.Serializable;

/**
 * Serializable factory for {@link CaptureEventSink}.
 *
 * <p>Flink serializes operator instances and ships them to task managers. {@link CaptureEventSink}
 * implementations backed by network resources (Kafka producers, OTLP exporters) cannot be
 * serialized. This factory is serialized instead; {@link #create()} is called once inside
 * {@code open()} on the task manager to construct the live sink.
 *
 * <p>Implementations must be serializable. Lambda factories are serializable if they do not
 * capture non-serializable references from an enclosing scope — serialization will fail at
 * job submission, not at test time, if that contract is violated.
 */
@FunctionalInterface
public interface CaptureEventSinkFactory extends Serializable {

    /**
     * Create a new {@link CaptureEventSink}. Called once per operator instance inside
     * {@code open()}.
     */
    CaptureEventSink create();
}
