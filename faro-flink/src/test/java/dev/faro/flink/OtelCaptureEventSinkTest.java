package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OtelCaptureEventSinkTest {

    private static CaptureEvent sampleEvent() {
        return CaptureEvent.builder()
                .pipelineId("p1")
                .operatorId("op1")
                .operatorType(CaptureEvent.OperatorType.SINK)
                .featureName("temperature")
                .captureMode(CaptureEvent.CaptureMode.AGGREGATE)
                .processingTime(Instant.now().toString())
                .traceId("trace-abc")
                .spanId("span-xyz")
                .inputCardinality(10)
                .outputCardinality(10)
                .emitIntervalMs(30_000)
                .captureDropSinceLast(false)
                .build();
    }

    @Test
    void emit_forwardsToDelegate() {
        CapturingCaptureEventSink inner = new CapturingCaptureEventSink();
        OtelCaptureEventSink sink = new OtelCaptureEventSink(inner, "http://localhost:14317");

        sink.emit(sampleEvent());
        sink.close();

        List<CaptureEvent> captured = inner.events;
        assertEquals(1, captured.size());
        assertEquals("p1", captured.get(0).getPipelineId());
    }

}

