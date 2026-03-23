package dev.faro.e2e;

import dev.faro.flink.KafkaCaptureEventSink;

public final class KafkaSensorJob {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "redpanda:29092");
        SensorPipeline.execute(KafkaCaptureEventSink.factory(bootstrapServers), "sensor-pipeline-kafka");
    }
}
