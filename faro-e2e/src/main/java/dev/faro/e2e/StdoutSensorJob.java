package dev.faro.e2e;

import dev.faro.core.StdoutCaptureEventSink;

public final class StdoutSensorJob {

    public static void main(String[] args) throws Exception {
        SensorPipeline.execute(new StdoutCaptureEventSink(), "sensor-pipeline-stdout");
    }
}
