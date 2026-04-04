package dev.faro.e2e;

import dev.faro.core.HttpCaptureEventSink;

public class FaroSensorJob {

    public static void main(String[] args) throws Exception {
        String faroApiUrl = System.getenv().getOrDefault("FARO_API_URL", "http://faro-api:9000/ingest");
        ScenarioMode scenario = ScenarioMode.fromEnv();
        SensorPipeline.execute(HttpCaptureEventSink.factory(faroApiUrl), "sensor-pipeline-faro", "sensor-pipeline-faro", scenario, CaptureMode.ENTITY);
    }
}
