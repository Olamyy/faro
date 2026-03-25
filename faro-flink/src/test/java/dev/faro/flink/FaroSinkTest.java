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
        FaroConfig<String> config = FaroConfig.<String>builder()
                .features(features)
                .build();
        FaroSink<String> sink = new FaroSink<>(new NoopSink<>(), PIPELINE_ID, config, captured, OPERATOR_ID);
        return sink.createWriter(mock(Sink.InitContext.class));
    }

    private static SinkWriter.Context ctx(Long timestamp) {
        SinkWriter.Context ctx = mock(SinkWriter.Context.class);
        when(ctx.timestamp()).thenReturn(timestamp);
        return ctx;
    }

    @Test
    void construction_throwsOnEmptyOperatorId() {
        FaroConfig<String> config = FaroConfig.<String>builder()
                .features("feature-a")
                .build();
        assertThrows(IllegalArgumentException.class,
                () -> new FaroSink<>(new NoopSink<>(), PIPELINE_ID, config, captured, ""));
    }

    @Test
    void flush_outputCardinalityReflectsFailures() throws Exception {
        FaroConfig<String> config = FaroConfig.<String>builder()
                .features("feature-a")
                .build();
        FaroSink<String> sink = new FaroSink<>(new FailingAfterNSink<>(2), PIPELINE_ID, config, captured, OPERATOR_ID);
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
