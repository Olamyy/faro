package dev.faro.e2e;

import dev.faro.flink.KafkaCaptureEventSink;
import dev.faro.flink.OtelCaptureEventSink;

public final class OtelSensorJob {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "redpanda:29092");
        String otlpEndpoint = System.getenv().getOrDefault("OTLP_ENDPOINT", "http://tempo:4318/v1/traces");
        SensorPipeline.execute(
                OtelCaptureEventSink.factory(KafkaCaptureEventSink.factory(bootstrapServers), otlpEndpoint),
                "sensor-pipeline-otel");
    }
}
