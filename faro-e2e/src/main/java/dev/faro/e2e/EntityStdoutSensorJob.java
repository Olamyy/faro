package dev.faro.e2e;

import dev.faro.flink.StdoutCaptureEventSink;

public final class EntityStdoutSensorJob {

    public static void main(String[] args) throws Exception {
        EntitySensorPipeline.execute(new StdoutCaptureEventSink(), "sensor-pipeline-entity-stdout");
    }
}
