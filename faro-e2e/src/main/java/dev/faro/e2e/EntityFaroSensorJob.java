package dev.faro.e2e;

import dev.faro.flink.HttpCaptureEventSink;

public final class EntityFaroSensorJob {

    public static void main(String[] args) throws Exception {
        String faroApiUrl = System.getenv().getOrDefault("FARO_API_URL", "http://faro-api:9000/ingest");
        EntitySensorPipeline.execute(HttpCaptureEventSink.factory(faroApiUrl), "sensor-pipeline-entity-faro", "sensor-pipeline-entity-faro");
    }
}
