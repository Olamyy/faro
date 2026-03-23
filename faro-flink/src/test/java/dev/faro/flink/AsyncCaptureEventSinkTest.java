package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class AsyncCaptureEventSinkTest {

    private static CaptureEvent sampleEvent(String featureName) {
        return CaptureEvent.builder()
                .pipelineId("test-pipeline")
                .operatorId("op-1")
                .operatorType(CaptureEvent.OperatorType.MAP)
                .captureMode(CaptureEvent.CaptureMode.AGGREGATE)
                .processingTime(Instant.now().toString())
                .featureName(featureName)
                .inputCardinality(1)
                .outputCardinality(1)
                .emitIntervalMs(30_000)
                .traceId("aabbccddeeff00112233445566778899")
                .spanId("aabbccdd11223344")
                .captureDropSinceLast(false)
                .build();
    }

    @Test
    void emit_eventReachesDelegate() {
        CapturingCaptureEventSink inner = new CapturingCaptureEventSink();
        AsyncCaptureEventSink sink = new AsyncCaptureEventSink(inner, 16);

        sink.emit(sampleEvent("feature-a"));
        sink.close();

        assertEquals(1, inner.events.size());
        assertEquals("feature-a", inner.events.get(0).getFeatureName());
    }

    @Test
    void emit_multipleEventsAreDrainedInOrder() {
        CapturingCaptureEventSink inner = new CapturingCaptureEventSink();
        AsyncCaptureEventSink sink = new AsyncCaptureEventSink(inner, 16);

        sink.emit(sampleEvent("f1"));
        sink.emit(sampleEvent("f2"));
        sink.emit(sampleEvent("f3"));
        sink.close();

        assertEquals(3, inner.events.size());
        assertEquals("f1", inner.events.get(0).getFeatureName());
        assertEquals("f2", inner.events.get(1).getFeatureName());
        assertEquals("f3", inner.events.get(2).getFeatureName());
    }

    @Test
    void emit_dropsOldestOnOverflow() throws Exception {
        CountDownLatch drainBlocker = new CountDownLatch(1);
        LatchedSink inner = new LatchedSink(drainBlocker);
        AsyncCaptureEventSink sink = new AsyncCaptureEventSink(inner, 3);

        sink.emit(sampleEvent("f1"));
        Thread.sleep(50);

        sink.emit(sampleEvent("f2"));
        sink.emit(sampleEvent("f3"));
        sink.emit(sampleEvent("f4"));
        sink.emit(sampleEvent("f5"));

        drainBlocker.countDown();
        sink.close();

        List<String> names = inner.received.stream()
                .map(CaptureEvent::getFeatureName)
                .toList();
        assertFalse(names.contains("f2"), "f2 should have been dropped on overflow");
        assertTrue(names.contains("f3"));
        assertTrue(names.contains("f4"));
        assertTrue(names.contains("f5"));
    }

    @Test
    void droppedSinceLastFlush_trueWhenOverflowOccurred() throws Exception {
        CountDownLatch blocker = new CountDownLatch(1);
        LatchedSink inner = new LatchedSink(blocker);
        AsyncCaptureEventSink sink = new AsyncCaptureEventSink(inner, 2);

        sink.emit(sampleEvent("f1"));
        Thread.sleep(50);

        sink.emit(sampleEvent("f2"));
        sink.emit(sampleEvent("f3"));
        sink.emit(sampleEvent("f4"));

        assertTrue(sink.droppedSinceLastFlush());

        blocker.countDown();
        sink.close();
    }

    @Test
    void droppedSinceLastFlush_resetsAfterRead() throws Exception {
        CountDownLatch blocker = new CountDownLatch(1);
        LatchedSink inner = new LatchedSink(blocker);
        AsyncCaptureEventSink sink = new AsyncCaptureEventSink(inner, 2);

        sink.emit(sampleEvent("f1"));
        Thread.sleep(50);

        sink.emit(sampleEvent("f2"));
        sink.emit(sampleEvent("f3"));
        sink.emit(sampleEvent("f4"));

        assertTrue(sink.droppedSinceLastFlush());
        assertFalse(sink.droppedSinceLastFlush());

        blocker.countDown();
        sink.close();
    }

    @Test
    void close_drainsRemainingEventsBeforeShutdown() {
        CapturingCaptureEventSink inner = new CapturingCaptureEventSink();
        AsyncCaptureEventSink sink = new AsyncCaptureEventSink(inner, 64);

        for (int i = 0; i < 10; i++) {
            sink.emit(sampleEvent("f" + i));
        }
        sink.close();

        assertEquals(10, inner.events.size());
    }

    @Test
    void factory_lambdaIsSerializable() throws Exception {
        CapturingCaptureEventSink inner = new CapturingCaptureEventSink();
        CaptureEventSinkFactory factory = () -> new AsyncCaptureEventSink(inner, 16);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(factory);
        }
        byte[] bytes = baos.toByteArray();
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            CaptureEventSinkFactory deserialized = (CaptureEventSinkFactory) ois.readObject();
            assertNotNull(deserialized);
            CaptureEventSink created = deserialized.create();
            assertNotNull(created);
            created.close();
        }
    }

    @Test
    void emit_afterClose_isNoOp() {
        CapturingCaptureEventSink inner = new CapturingCaptureEventSink();
        AsyncCaptureEventSink sink = new AsyncCaptureEventSink(inner, 16);
        sink.close();

        assertDoesNotThrow(() -> sink.emit(sampleEvent("f1")));
        assertEquals(0, inner.events.size());
    }

    /**
     * A {@link CaptureEventSink} that blocks on {@code emit()} until a latch is released.
     * Used to pause the drain thread so the queue can be filled to capacity.
     */
    private static final class LatchedSink implements CaptureEventSink {
        final List<CaptureEvent> received = new ArrayList<>();
        private final CountDownLatch blocker;
        private final AtomicBoolean blocked = new AtomicBoolean(false);

        LatchedSink(CountDownLatch blocker) {
            this.blocker = blocker;
        }

        @Override
        public void emit(CaptureEvent event) {
            if (!blocked.getAndSet(true)) {
                try {
                    blocker.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            received.add(event);
        }

        @Override
        public void close() {}
    }
}
