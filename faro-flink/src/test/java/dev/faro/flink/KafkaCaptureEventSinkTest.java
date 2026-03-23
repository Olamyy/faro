package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfig$;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class KafkaCaptureEventSinkTest {

    private io.github.embeddedkafka.EmbeddedKafkaConfig kafkaConfig;

    @BeforeEach
    void startKafka() {
        kafkaConfig = EmbeddedKafkaConfig$.MODULE$.defaultConfig();
        EmbeddedKafka.start(kafkaConfig);
    }

    @AfterEach
    void stopKafka() {
        EmbeddedKafka.stop();
    }

    private String bootstrapServers() {
        return "localhost:" + kafkaConfig.kafkaPort();
    }

    private KafkaConsumer<String, String> newConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers());
        props.setProperty("group.id", "test-consumer-" + UUID.randomUUID());
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("enable.auto.commit", "false");
        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }

    private ConsumerRecords<String, String> pollUntilNonEmpty(
            KafkaConsumer<String, String> consumer) {
        ConsumerRecords<String, String> records = ConsumerRecords.empty();
        long deadline = System.currentTimeMillis() + 10_000;
        while (records.isEmpty() && System.currentTimeMillis() < deadline) {
            records = consumer.poll(Duration.ofMillis(200));
        }
        return records;
    }

    private CaptureEvent sampleEvent() {
        return CaptureEvent.builder()
                .pipelineId("test-pipeline")
                .operatorId("op-1")
                .operatorType(CaptureEvent.OperatorType.SINK)
                .captureMode(CaptureEvent.CaptureMode.AGGREGATE)
                .processingTime(Instant.now().toString())
                .featureName("feature-a")
                .inputCardinality(5)
                .outputCardinality(5)
                .emitIntervalMs(30_000)
                .traceId("aabbccddeeff00112233445566778899")
                .spanId("aabbccdd11223344")
                .captureDropSinceLast(false)
                .build();
    }

    @Test
    void emit_messageArrivesOnTopic() {
        CaptureEventSink sink = KafkaCaptureEventSink.factory(bootstrapServers()).create();
        sink.emit(sampleEvent());
        sink.close();

        try (KafkaConsumer<String, String> consumer = newConsumer()) {
            consumer.subscribe(java.util.List.of(KafkaCaptureEventSink.DEFAULT_TOPIC));
            ConsumerRecords<String, String> records = pollUntilNonEmpty(consumer);
            assertFalse(records.isEmpty());
            assertTrue(records.iterator().next().value().contains("\"pipeline_id\":\"test-pipeline\""));
        }
    }

    @Test
    void emit_messageKeyIsPipelineId() {
        CaptureEventSink sink = KafkaCaptureEventSink.factory(bootstrapServers()).create();
        sink.emit(sampleEvent());
        sink.close();

        try (KafkaConsumer<String, String> consumer = newConsumer()) {
            consumer.subscribe(java.util.List.of(KafkaCaptureEventSink.DEFAULT_TOPIC));
            ConsumerRecords<String, String> records = pollUntilNonEmpty(consumer);
            assertFalse(records.isEmpty());
            assertEquals("test-pipeline", records.iterator().next().key());
        }
    }

    @Test
    void emit_customTopicIsUsed() {
        String customTopic = "custom.capture.topic";
        Properties extra = new Properties();
        extra.setProperty("faro.topic", customTopic);
        CaptureEventSink sink = KafkaCaptureEventSink.factory(bootstrapServers(), extra).create();
        sink.emit(sampleEvent());
        sink.close();

        try (KafkaConsumer<String, String> consumer = newConsumer()) {
            consumer.subscribe(java.util.List.of(customTopic));
            ConsumerRecords<String, String> records = pollUntilNonEmpty(consumer);
            assertFalse(records.isEmpty());
        }
    }

    @Test
    void factory_disabledWhenBrokerUnreachable() {
        CaptureEventSink sink = KafkaCaptureEventSink.factory("localhost:19999").create();
        assertDoesNotThrow(() -> sink.emit(sampleEvent()));
        assertDoesNotThrow(sink::close);
    }
}
