package dev.faro.spark;

import dev.faro.core.CaptureEvent;
import dev.faro.core.DataClassification;
import dev.faro.core.FaroConfig;
import dev.faro.core.FaroFeatureConfig;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class FaroSparkTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("FaroSparkTest")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    private Dataset<String> ds(String... rows) {
        return spark.createDataset(List.of(rows), Encoders.STRING());
    }

    @Test
    void trace_nullOperatorIdThrows() {
        CapturingCaptureEventSink captured = new CapturingCaptureEventSink();
        FaroSpark faro = new FaroSpark("p1", captured);
        FaroConfig<String> config = FaroConfig.<String>builder().features("f").build();
        assertThrows(IllegalArgumentException.class,
                () -> faro.trace(null, CaptureEvent.OperatorType.MAP, config, Function.identity()));
    }

    @Test
    void trace_emptyOperatorIdThrows() {
        CapturingCaptureEventSink captured = new CapturingCaptureEventSink();
        FaroSpark faro = new FaroSpark("p1", captured);
        FaroConfig<String> config = FaroConfig.<String>builder().features("f").build();
        assertThrows(IllegalArgumentException.class,
                () -> faro.trace("", CaptureEvent.OperatorType.MAP, config, Function.identity()));
    }

    @Test
    void trace_aggregateEventHasCorrectFields() {
        CapturingCaptureEventSink captured = new CapturingCaptureEventSink();
        FaroSpark faro = new FaroSpark("my-pipeline", captured);
        FaroConfig<String> config = FaroConfig.<String>builder().features("feature-a").build();

        faro.trace("op1", CaptureEvent.OperatorType.AGG, config, Function.identity())
                .apply(ds("r1", "r2"));

        CaptureEvent event = captured.events.get(0);
        assertEquals("my-pipeline", event.getPipelineId());
        assertEquals("op1", event.getOperatorId());
        assertEquals("feature-a", event.getFeatureName());
        assertEquals(CaptureEvent.CaptureMode.AGGREGATE, event.getCaptureMode());
        assertEquals(CaptureEvent.OperatorType.AGG, event.getOperatorType());
        assertEquals(2L, event.getInputCardinality());
        assertEquals(2L, event.getOutputCardinality());
    }

    @Test
    void trace_cardinalitiesReflectTransformResult() {
        CapturingCaptureEventSink captured = new CapturingCaptureEventSink();
        FaroSpark faro = new FaroSpark("p1", captured);
        FaroConfig<String> config = FaroConfig.<String>builder().features("f").build();

        faro.trace("op1", CaptureEvent.OperatorType.FILTER, config,
                ds -> ds.filter((FilterFunction<String>) s -> s.startsWith("a")))
                .apply(ds("a1", "b1", "a2"));

        CaptureEvent event = captured.events.get(0);
        assertEquals(3L, event.getInputCardinality());
        assertEquals(2L, event.getOutputCardinality());
    }

    @Test
    void trace_personalClassificationDegradesToAggregate() {
        CapturingCaptureEventSink captured = new CapturingCaptureEventSink();
        FaroSpark faro = new FaroSpark("p1", captured);
        FaroFeatureConfig<String> fc = FaroFeatureConfig.<String>builder()
                .entityKey(s -> s)
                .featureValue(String::length)
                .valueType(CaptureEvent.FeatureValueType.SCALAR_LONG)
                .classification(DataClassification.PERSONAL)
                .build();
        FaroConfig<String> config = FaroConfig.<String>builder().feature("f", fc).build();

        faro.trace("op1", CaptureEvent.OperatorType.MAP, config, Function.identity())
                .apply(ds("alice", "bob"));

        assertTrue(captured.events.stream()
                .allMatch(e -> e.getCaptureMode() == CaptureEvent.CaptureMode.AGGREGATE
                        && e.getEntityId() == null
                        && e.getFeatureValue() == null));
    }

    @Test
    void trace_entityEventsEmittedPerRow() {
        CapturingCaptureEventSink captured = new CapturingCaptureEventSink();
        FaroSpark faro = new FaroSpark("p1", captured);
        FaroFeatureConfig<String> fc = FaroFeatureConfig.<String>builder()
                .entityKey(s -> s)
                .featureValue(s -> (long) s.length())
                .valueType(CaptureEvent.FeatureValueType.SCALAR_LONG)
                .classification(DataClassification.NON_PERSONAL)
                .build();
        FaroConfig<String> config = FaroConfig.<String>builder().feature("score", fc).build();

        faro.trace("op1", CaptureEvent.OperatorType.MAP, config, Function.identity())
                .apply(ds("alice", "bob"));

        long entityEvents = captured.events.stream()
                .filter(e -> e.getCaptureMode() == CaptureEvent.CaptureMode.ENTITY)
                .count();
        assertEquals(2L, entityEvents);

        CaptureEvent first = captured.events.stream()
                .filter(e -> e.getCaptureMode() == CaptureEvent.CaptureMode.ENTITY)
                .findFirst().orElseThrow();
        assertNotNull(first.getEntityId());
        assertNotNull(first.getFeatureValue());
        assertEquals(CaptureEvent.FeatureValueType.SCALAR_LONG, first.getFeatureValueType());
        assertEquals("p1", first.getPipelineId());
        assertEquals("score", first.getFeatureName());
    }

    @Test
    void trace_sampleRateZeroProducesNoEntityEvents() {
        CapturingCaptureEventSink captured = new CapturingCaptureEventSink();
        FaroSpark faro = new FaroSpark("p1", captured);
        FaroFeatureConfig<String> fc = FaroFeatureConfig.<String>builder()
                .entityKey(s -> s)
                .featureValue(String::length)
                .valueType(CaptureEvent.FeatureValueType.SCALAR_LONG)
                .classification(DataClassification.NON_PERSONAL)
                .sampleRate(0.0)
                .build();
        FaroConfig<String> config = FaroConfig.<String>builder().feature("f", fc).build();

        faro.trace("op1", CaptureEvent.OperatorType.MAP, config, Function.identity())
                .apply(ds("a", "b", "c", "d", "e"));

        assertTrue(captured.events.stream()
                .noneMatch(e -> e.getCaptureMode() == CaptureEvent.CaptureMode.ENTITY));
    }
}
