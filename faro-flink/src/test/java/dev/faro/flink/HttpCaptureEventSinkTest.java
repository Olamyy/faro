package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class HttpCaptureEventSinkTest {

    private MockWebServer server;

    @BeforeEach
    void startServer() throws IOException {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void stopServer() throws IOException {
        server.shutdown();
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
    void emit_postsJsonToUrl() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));

        CaptureEventSink sink = HttpCaptureEventSink.factory(server.url("/events").toString()).create();
        sink.emit(sampleEvent());
        sink.close();

        RecordedRequest request = server.takeRequest();
        assertEquals("POST", request.getMethod());
        assertEquals("application/json", Objects.requireNonNull(request.getHeader("Content-Type")).split(";")[0].trim());
        assertTrue(request.getBody().readUtf8().contains("\"pipeline_id\":\"test-pipeline\""));
    }

    @Test
    void emit_nonSuccessResponseIsLoggedNotThrown() {
        server.enqueue(new MockResponse().setResponseCode(500));

        CaptureEventSink sink = HttpCaptureEventSink.factory(server.url("/events").toString()).create();
        assertDoesNotThrow(() -> sink.emit(sampleEvent()));
        sink.close();
    }

    @Test
    void emit_connectionFailureIsLoggedNotThrown() {
        CaptureEventSink sink = HttpCaptureEventSink.factory(
                "http://localhost:19999/events",
                Duration.ofMillis(200),
                Duration.ofMillis(200)).create();
        assertDoesNotThrow(() -> sink.emit(sampleEvent()));
        sink.close();
    }

}
