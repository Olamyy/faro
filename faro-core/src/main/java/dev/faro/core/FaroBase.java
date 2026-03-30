package dev.faro.core;

import java.io.Serializable;
import java.util.Objects;

/**
 * Engine-agnostic base for Faro pipeline instrumentation.
 */
public abstract class FaroBase implements Serializable {

    private final String pipelineId;
    private final CaptureEventSinkFactory sinkFactory;

    protected FaroBase(String pipelineId, CaptureEventSinkFactory sinkFactory) {
        if (pipelineId == null || pipelineId.isEmpty()) {
            throw new IllegalArgumentException("Faro: pipelineId must not be null or empty");
        }
        Objects.requireNonNull(sinkFactory, "sinkFactory must not be null");
        this.pipelineId = pipelineId;
        this.sinkFactory = sinkFactory;
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public CaptureEventSinkFactory getSinkFactory() {
        return sinkFactory;
    }
}
