package dev.faro.flink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FaroSinkTest {

    private static final String PIPELINE_ID = "test-pipeline";
    private static final String OPERATOR_ID = "sink.test-operator";

    private CapturingCaptureEventSink captured;

    @BeforeEach
    void setUp() {
        captured = new CapturingCaptureEventSink();
    }

    private SinkWriter<String> writerWithFeatures(String... features) throws IOException {
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features(features)
                .build();
        FaroSink<String> sink = new FaroSink<>(new NoopSink<>(), config, captured, OPERATOR_ID);
        return sink.createWriter(mock(Sink.InitContext.class));
    }

    private static SinkWriter.Context ctx(Long timestamp) {
        SinkWriter.Context ctx = mock(SinkWriter.Context.class);
        when(ctx.timestamp()).thenReturn(timestamp);
        return ctx;
    }

    @Test
    void construction_throwsOnEmptyOperatorId() {
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();
        assertThrows(IllegalArgumentException.class,
                () -> new FaroSink<>(new NoopSink<>(), config, captured, ""));
    }

    @Test
    void flush_emitsOneEventPerFeature() throws Exception {
        try (SinkWriter<String> writer = writerWithFeatures("avg_purchase_7d", "max_purchase_7d")) {
            writer.write("record1", ctx(null));
            writer.flush(false);

            assertEquals(2, captured.events.size());
            assertEquals("avg_purchase_7d", captured.events.get(0).getFeatureName());
            assertEquals("max_purchase_7d", captured.events.get(1).getFeatureName());
        }
    }

    @Test
    void flush_cardinalityReflectsInterval() throws Exception {
        try (SinkWriter<String> writer = writerWithFeatures("feature-a")) {
            writer.write("r1", ctx(null));
            writer.write("r2", ctx(null));
            writer.write("r3", ctx(null));
            writer.flush(false);

            assertEquals(3L, captured.events.get(0).getInputCardinality());
            assertEquals(3L, captured.events.get(0).getOutputCardinality());
        }
    }

    @Test
    void flush_cardinalityResetsAfterEachInterval() throws Exception {
        try (SinkWriter<String> writer = writerWithFeatures("feature-a")) {
            writer.write("r1", ctx(null));
            writer.write("r2", ctx(null));
            writer.flush(false);
            captured.events.clear();

            writer.write("r3", ctx(null));
            writer.flush(false);

            assertEquals(1L, captured.events.get(0).getInputCardinality());
        }
    }

    @Test
    void flush_outputCardinalityReflectsFailures() throws Exception {
        FaroConfig config = FaroConfig.builder()
                .pipelineId(PIPELINE_ID)
                .features("feature-a")
                .build();
        FaroSink<String> sink = new FaroSink<>(new FailingAfterNSink<>(2), config, captured, OPERATOR_ID);
        try (SinkWriter<String> writer = sink.createWriter(mock(Sink.InitContext.class))) {
            writer.write("r1", ctx(null));
            writer.write("r2", ctx(null));
            assertThrows(Exception.class, () -> writer.write("r3", ctx(null)));
            writer.flush(false);

            assertEquals(3L, captured.events.get(0).getInputCardinality());
            assertEquals(2L, captured.events.get(0).getOutputCardinality());
        }
    }

    @Test
    void close_flushesPartialInterval() throws Exception {
        try (SinkWriter<String> writer = writerWithFeatures("feature-a")) {
            writer.write("r1", ctx(null));
        }

        assertFalse(captured.events.isEmpty());
        assertEquals(1L, captured.events.get(0).getInputCardinality());
    }

    @Test
    void flush_watermarkIsNullForMaxValueSentinel() throws Exception {
        try (SinkWriter<String> writer = writerWithFeatures("feature-a")) {
            writer.writeWatermark(new org.apache.flink.api.common.eventtime.Watermark(Long.MAX_VALUE));
            writer.write("r1", ctx(null));
            writer.flush(false);

            assertNull(captured.events.get(0).getWatermark());
        }
    }

    @Test
    void flush_watermarkReflectsLastWriteWatermark() throws Exception {
        try (SinkWriter<String> writer = writerWithFeatures("feature-a")) {
            long wmMs = Instant.parse("2026-03-21T12:00:00Z").toEpochMilli();
            writer.writeWatermark(new org.apache.flink.api.common.eventtime.Watermark(wmMs));
            writer.write("r1", ctx(null));
            writer.flush(false);

            assertEquals("2026-03-21T12:00:00Z", captured.events.get(0).getWatermark());
        }
    }

    @Test
    void flush_eventTimeIsNullWhenNoTimestamp() throws Exception {
        try (SinkWriter<String> writer = writerWithFeatures("feature-a")) {
            writer.write("r1", ctx(null));
            writer.flush(false);

            assertNull(captured.events.get(0).getEventTime());
            assertNull(captured.events.get(0).getEventTimeMin());
        }
    }

    @Test
    void flush_eventTimeReflectsMaxAndMinTimestampInInterval() throws Exception {
        try (SinkWriter<String> writer = writerWithFeatures("feature-a")) {
            long t1 = Instant.parse("2026-03-21T10:00:00Z").toEpochMilli();
            long t2 = Instant.parse("2026-03-21T11:00:00Z").toEpochMilli();
            writer.write("r1", ctx(t1));
            writer.write("r2", ctx(t2));
            writer.flush(false);

            assertEquals("2026-03-21T11:00:00Z", captured.events.get(0).getEventTime());
            assertEquals("2026-03-21T10:00:00Z", captured.events.get(0).getEventTimeMin());
        }
    }

    private static final class NoopSink<T> implements Sink<T> {
        @Override
        public SinkWriter<T> createWriter(InitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(T element, Context context) {
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

    private record FailingAfterNSink<T>(int succeedCount) implements Sink<T> {

        @Override
            public SinkWriter<T> createWriter(InitContext context) {
                return new SinkWriter<>() {
                    private int invocations = 0;

                    @Override
                    public void write(T element, Context context) {
                        invocations++;
                        if (invocations > succeedCount) {
                            throw new RuntimeException("simulated write failure");
                        }
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
