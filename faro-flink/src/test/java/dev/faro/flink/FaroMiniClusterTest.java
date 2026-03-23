package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.Serial;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class FaroMiniClusterTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setConfiguration(miniClusterConfig())
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(4)
                    .build());

    private static Configuration miniClusterConfig() {
        Configuration cfg = new Configuration();
        cfg.set(RestOptions.BIND_PORT, "0");
        return cfg;
    }

    static final ConcurrentLinkedQueue<CaptureEvent> CAPTURED = new ConcurrentLinkedQueue<>();

    static final CaptureEventSinkFactory COLLECTING_FACTORY = new CollectingFactory();

    private static final String PIPELINE_ID = "e2e-test-pipeline";

    @BeforeEach
    void clearCaptured() {
        CAPTURED.clear();
    }

    @Test
    void statelessPipeline_captureEventsEmittedFromBothOperators() throws Exception {
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();
        Faro faro = new Faro(config, COLLECTING_FACTORY);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements("r1", "r2", "r3")
                .process(faro.trace(CaptureEvent.OperatorType.MAP, new PassThroughFn()))
                .returns(String.class)
                .uid("map.passthrough")
                .sinkTo(new FaroSink<>(new NoopSink<>(), config, COLLECTING_FACTORY, "sink.noop"))
                .uid("sink.noop");

        env.execute("stateless-pipeline");

        List<CaptureEvent> events = new ArrayList<>(CAPTURED);
        assertFalse(events.isEmpty());
        assertTrue(events.stream().allMatch(e -> PIPELINE_ID.equals(e.getPipelineId())));
        assertTrue(events.stream().anyMatch(e -> e.getOperatorType() == CaptureEvent.OperatorType.MAP));
        assertTrue(events.stream().anyMatch(e -> e.getOperatorType() == CaptureEvent.OperatorType.SINK));
    }

    @Test
    void statelessPipeline_inputCardinalityReflectsRecordCount() throws Exception {
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements("r1", "r2", "r3")
                .sinkTo(new FaroSink<>(new NoopSink<>(), config, COLLECTING_FACTORY, "sink.cardinality"))
                .uid("sink.cardinality");

        env.execute("cardinality-pipeline");

        long totalInput = CAPTURED.stream().mapToLong(CaptureEvent::getInputCardinality).sum();
        assertEquals(3L, totalInput);
    }

    @Test
    void keyedPipeline_captureEventsEmittedWithCorrectPipelineId() throws Exception {
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();
        Faro faro = new Faro(config, COLLECTING_FACTORY);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements("a", "b", "a", "c")
                .keyBy(s -> s)
                .process(faro.keyedTrace(CaptureEvent.OperatorType.AGG, new PassThroughKeyedFn()))
                .returns(String.class)
                .uid("keyed.agg")
                .sinkTo(new NoopSink<>());

        env.execute("keyed-pipeline");

        List<CaptureEvent> events = new ArrayList<>(CAPTURED);
        assertFalse(events.isEmpty());
        assertTrue(events.stream().allMatch(e -> PIPELINE_ID.equals(e.getPipelineId())));
        assertTrue(events.stream().allMatch(e -> e.getOperatorType() == CaptureEvent.OperatorType.AGG));
    }

    @Test
    void windowedPipeline_windowBoundsAndCardinalityPopulated() throws Exception {
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();
        Faro faro = new Faro(config, COLLECTING_FACTORY);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromCollection(
                        List.of(
                                new TimestampedEvent("key", 1000L),
                                new TimestampedEvent("key", 2000L),
                                new TimestampedEvent("key", 6000L),
                                new TimestampedEvent("key", 20000L)),
                        TypeInformation.of(TimestampedEvent.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TimestampedEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((e, ts) -> e.timestamp))
                .keyBy(e -> e.key)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(faro.windowTrace(new PassThroughWindowFn()))
                .returns(TypeInformation.of(TimestampedEvent.class))
                .uid("window.tumbling")
                .sinkTo(new NoopSink<>());

        env.execute("windowed-pipeline");

        List<CaptureEvent> events = new ArrayList<>(CAPTURED);
        assertFalse(events.isEmpty(), "expected capture events from window triggers");
        assertTrue(events.stream().allMatch(e -> e.getWindowStart() != null));
        assertTrue(events.stream().allMatch(e -> e.getWindowEnd() != null));

        List<Long> cardinalities = events.stream()
                .map(CaptureEvent::getInputCardinality)
                .sorted()
                .toList();
        assertTrue(cardinalities.contains(2L), "expected a window with 2 elements");
        assertTrue(cardinalities.contains(1L), "expected a window with 1 element");
        assertTrue(cardinalities.stream().allMatch(c -> c > 0), "all windows should have at least 1 element");
    }

    static final class CollectingFactory implements CaptureEventSinkFactory {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public CaptureEventSink create() {
            return new CollectingSink();
        }
    }

    static final class CollectingSink implements CaptureEventSink {
        @Override
        public void emit(CaptureEvent event) {
            CAPTURED.add(event);
        }

        @Override
        public void close() {}
    }

    static final class TimestampedEvent {
        public String key;
        public long timestamp;

        TimestampedEvent(String key, long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }
    }

    private static final class PassThroughFn extends ProcessFunction<String, String> {
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            out.collect(value);
        }
    }

    private static final class PassThroughKeyedFn
            extends KeyedProcessFunction<String, String, String> {
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            out.collect(value);
        }
    }

    private static final class PassThroughWindowFn
            extends ProcessWindowFunction<TimestampedEvent, TimestampedEvent, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx,
                Iterable<TimestampedEvent> elements, Collector<TimestampedEvent> out) {
            for (TimestampedEvent e : elements) {
                out.collect(e);
            }
        }
    }

    private static final class NoopSink<T> implements Sink<T> {
        @Override
        public SinkWriter<T> createWriter(InitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(T element, Context ctx) {
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }
    }
}
