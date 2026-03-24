package dev.faro.e2e;

import dev.faro.flink.HttpCaptureEventSink;

/**
 * Set {@code FARO_API_URL} to override the default ingest endpoint.
 */
public class FaroSensorJob {

    public static void main(String[] args) throws Exception {
        String faroApiUrl = System.getenv().getOrDefault("FARO_API_URL", "http://faro-api:9000/ingest");
        SensorPipeline.execute(HttpCaptureEventSink.factory(faroApiUrl), "sensor-pipeline-faro", "sensor-pipeline-faro");
    }
}
