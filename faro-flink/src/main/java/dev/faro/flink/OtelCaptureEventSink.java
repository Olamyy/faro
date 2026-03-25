package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * {@link CaptureEventSink} decorator that emits each capture event as an OTEL span via OTLP/HTTP,
 * then forwards to an inner delegate sink.
 *
 * <p>Each span carries all non-null {@link CaptureEvent} fields as {@code faro.*} span attributes.
 * {@code parent_span_id} is not set by the adapter — it is resolved by the indexing job.
 * Export failures are logged and never propagated; SDK init failure falls back to delegate-only mode.
 */
public final class OtelCaptureEventSink implements CaptureEventSink {

    private static final Logger LOG = LoggerFactory.getLogger(OtelCaptureEventSink.class);
    private static final String INSTRUMENTATION_SCOPE = "dev.faro";

    private final CaptureEventSink delegate;
    private final OpenTelemetrySdk sdk;
    private final Tracer tracer;

    OtelCaptureEventSink(CaptureEventSink delegate, String otlpEndpoint) {
        this.delegate = delegate;
        OpenTelemetrySdk initialised = null;
        Tracer t = null;
        try {
            OtlpHttpSpanExporter exporter = OtlpHttpSpanExporter.builder()
                    .setEndpoint(otlpEndpoint)
                    .setTimeout(Duration.ofSeconds(5))
                    .build();
            SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                    .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
                    .build();
            initialised = OpenTelemetrySdk.builder()
                    .setTracerProvider(tracerProvider)
                    .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                    .build();
            t = initialised.getTracer(INSTRUMENTATION_SCOPE);
        } catch (Exception e) {
            LOG.warn("Faro: failed to initialise OTEL SDK, span emission disabled: {}", e.getMessage());
        }
        this.sdk = initialised;
        this.tracer = t;
    }

    @Override
    public void emit(CaptureEvent event) {
        if (tracer != null) {
            emitSpan(event);
        }
        delegate.emit(event);
    }

    @Override
    public boolean droppedSinceLastFlush() {
        return delegate.droppedSinceLastFlush();
    }

    @Override
    public void close() {
        if (sdk != null) {
            sdk.getSdkTracerProvider().forceFlush().join(5, TimeUnit.SECONDS);
            sdk.close();
        }
        delegate.close();
    }

    private void emitSpan(CaptureEvent event) {
        try {
            AttributesBuilder attrs = Attributes.builder();
            attrs.put("faro.schema_version", event.getSchemaVersion());
            attrs.put("faro.pipeline_id", event.getPipelineId());
            attrs.put("faro.operator_id", event.getOperatorId());
            attrs.put("faro.operator_type", event.getOperatorType().name());
            attrs.put("faro.feature_name", event.getFeatureName());
            attrs.put("faro.capture_mode", event.getCaptureMode().name());
            attrs.put("faro.trace_id", event.getTraceId());
            attrs.put("faro.span_id", event.getSpanId());
            attrs.put("faro.capture_drop_since_last", event.isCaptureDropSinceLast());
            attrs.put("faro.input_cardinality", event.getInputCardinality());
            attrs.put("faro.output_cardinality", event.getOutputCardinality());
            attrs.put("faro.emit_interval_ms", event.getEmitIntervalMs());
            putIfNonNull(attrs, "faro.watermark", event.getWatermark());
            putIfNonNull(attrs, "faro.event_time", event.getEventTime());
            putIfNonNull(attrs, "faro.event_time_min", event.getEventTimeMin());
            putIfNonNull(attrs, "faro.window_start", event.getWindowStart());
            putIfNonNull(attrs, "faro.window_end", event.getWindowEnd());
            putIfNonNull(attrs, "faro.late_event_count", event.getLateEventCount());
            putIfNonNull(attrs, "faro.timer_fired_count", event.getTimerFiredCount());
            putIfNonNull(attrs, "faro.entity_id", event.getEntityId());
            putIfNonNull(attrs, "faro.upstream_source", event.getUpstreamSource());
            putIfNonNull(attrs, "faro.upstream_system", event.getUpstreamSystem());
            putIfNonNull(attrs, "faro.parent_span_id", event.getParentSpanId());

            Instant processingTime = Instant.parse(event.getProcessingTime());
            Instant spanEnd = processingTime.plusMillis(event.getEmitIntervalMs() > 0 ? event.getEmitIntervalMs() : 1);

            SpanBuilder spanBuilder = tracer
                    .spanBuilder(event.getOperatorId() + "/" + event.getFeatureName())
                    .setSpanKind(SpanKind.INTERNAL)
                    .setStartTimestamp(processingTime.toEpochMilli(), TimeUnit.MILLISECONDS)
                    .setAllAttributes(attrs.build());

            Span span = spanBuilder.startSpan();
            span.end(spanEnd.toEpochMilli(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.warn("Faro: failed to emit OTEL span: {}", e.getMessage());
        }
    }

    private static void putIfNonNull(AttributesBuilder attrs, String key, String value) {
        if (value != null) attrs.put(key, value);
    }

    private static void putIfNonNull(AttributesBuilder attrs, String key, Long value) {
        if (value != null) attrs.put(key, value);
    }

    /**
     * Exports to {@code otlpEndpoint} (e.g. {@code http://tempo:4318/v1/traces}).
     */
    public static CaptureEventSinkFactory factory(CaptureEventSinkFactory inner, String otlpEndpoint) {
        return () -> new OtelCaptureEventSink(inner.create(), otlpEndpoint);
    }

    public static CaptureEventSinkFactory factory(CaptureEventSinkFactory inner) {
        return factory(inner, "http://localhost:4318/v1/traces");
    }
}
