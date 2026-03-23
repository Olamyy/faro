package dev.faro.e2e;

import dev.faro.flink.HttpCaptureEventSink;

public final class HttpSensorJob {

    public static void main(String[] args) throws Exception {
        String webhookUrl = System.getenv().getOrDefault("WEBHOOK_URL", "http://webhook-echo:8080");
        SensorPipeline.execute(HttpCaptureEventSink.factory(webhookUrl), "sensor-pipeline-http");
    }
}
