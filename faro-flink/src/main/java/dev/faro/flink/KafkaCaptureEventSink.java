package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * {@link CaptureEventSink} that produces each event as a JSON string to a Kafka topic.
 *
 * <p>Messages are sent fire-and-forget: the producer is configured with {@code acks=1} and
 * delivery failures are logged but never propagated to the operator thread.
 *
 * <p>The message key is {@code pipeline_id}, which co-locates all events from the same pipeline
 * on the same partition and preserves intra-pipeline ordering.
 *
 * <p>If the producer cannot be initialised (e.g. broker unreachable at startup), the sink enters
 * a disabled state: {@link #emit} becomes a no-op and the pipeline continues unaffected.
 */
public final class KafkaCaptureEventSink implements CaptureEventSink {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCaptureEventSink.class);

    static final String DEFAULT_TOPIC = "faro.capture.events";

    private final String topic;
    private final KafkaProducer<String, String> producer;
    private final boolean disabled;

    KafkaCaptureEventSink(Properties kafkaProperties, String topic) {
        this.topic = topic;
        KafkaProducer<String, String> p = null;
        boolean fail = false;
        try {
            p = new KafkaProducer<>(kafkaProperties, new StringSerializer(), new StringSerializer());
        } catch (Exception e) {
            LOG.warn("Faro: failed to create Kafka producer — capture disabled for this operator", e);
            fail = true;
        }
        this.producer = p;
        this.disabled = fail;
    }

    @Override
    public void emit(CaptureEvent event) {
        if (disabled) return;
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, event.getPipelineId(), event.toJson());
        producer.send(record, (metadata, ex) -> {
            if (ex != null) {
                LOG.warn("Faro: failed to send capture event to Kafka topic '{}': {}", topic, ex.getMessage());
            }
        });
    }

    @Override
    public void close() {
        if (!disabled) {
            producer.flush();
            producer.close();
        }
    }

    /**
     * Returns a serializable factory for {@code KafkaCaptureEventSink}.
     *
     * <p>{@code bootstrapServers} is the only required property; all other Kafka producer
     * settings use the defaults below unless overridden via {@code extraProperties}:
     * <ul>
     *   <li>{@code acks=1} — leader acknowledgement only; balances durability and latency
     *   <li>{@code retries=3}
     *   <li>{@code linger.ms=5} — small batching window to reduce request overhead
     * </ul>
     */
    public static CaptureEventSinkFactory factory(String bootstrapServers, Properties extraProperties) {
        Properties props = defaultProperties(bootstrapServers);
        if (extraProperties != null) {
            props.putAll(extraProperties);
        }
        String topic = props.getProperty("faro.topic", DEFAULT_TOPIC);
        props.remove("faro.topic");
        return () -> new KafkaCaptureEventSink(props, topic);
    }

    public static CaptureEventSinkFactory factory(String bootstrapServers) {
        return factory(bootstrapServers, null);
    }

    private static Properties defaultProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("acks", "1");
        props.setProperty("retries", "3");
        props.setProperty("linger.ms", "5");
        return props;
    }
}
