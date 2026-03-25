package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import dev.faro.core.DataClassification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FaroEntityModeTest {

    private static final String PIPELINE_ID = "entity-test-pipeline";
    private static final String OPERATOR_UID = "entity.test-operator";

    private CapturingCaptureEventSink captured;
    private StreamingRuntimeContext runtimeContext;

    @BeforeEach
    void setUp() {
        captured = new CapturingCaptureEventSink();
        runtimeContext = mock(StreamingRuntimeContext.class);
        when(runtimeContext.getOperatorUniqueID()).thenReturn(OPERATOR_UID);
    }

    private FaroProcessFunction<TestRecord, TestRecord> buildFn(FaroConfig<TestRecord> config)
            throws Exception {
        FaroProcessFunction<TestRecord, TestRecord> fn = new FaroProcessFunction<>(
                CaptureEvent.OperatorType.AGG, PIPELINE_ID, config, new PassThroughFn(), captured);
        fn.setRuntimeContext(runtimeContext);
        fn.open(new Configuration());
        return fn;
    }

    private static ProcessFunction<TestRecord, TestRecord>.Context mockCtx() {
        @SuppressWarnings("unchecked")
        ProcessFunction<TestRecord, TestRecord>.Context ctx =
                (ProcessFunction<TestRecord, TestRecord>.Context) mock(ProcessFunction.Context.class);
        when(ctx.timestamp()).thenReturn(null);
        TimerService ts = mock(TimerService.class);
        when(ts.currentWatermark()).thenReturn(Long.MIN_VALUE);
        when(ctx.timerService()).thenReturn(ts);
        return ctx;
    }

    @Test
    void nonPersonal_emitsEntityIdAndFeatureValue() throws Exception {
        FaroConfig<TestRecord> config = FaroConfig.<TestRecord>builder()
                .feature("score", FaroFeatureConfig.<TestRecord>builder()
                        .entityKey(r -> r.userId)
                        .featureValue(r -> r.score)
                        .valueType(CaptureEvent.FeatureValueType.SCALAR_DOUBLE)
                        .classification(DataClassification.NON_PERSONAL)
                        .build())
                .build();

        FaroProcessFunction<TestRecord, TestRecord> fn = buildFn(config);
        fn.processElement(new TestRecord("user-1", 42.0), mockCtx(), mock(Collector.class));

        List<CaptureEvent> entityEvents = captured.events.stream()
                .filter(e -> e.getCaptureMode() == CaptureEvent.CaptureMode.ENTITY)
                .toList();

        assertEquals(1, entityEvents.size());
        assertEquals("user-1", entityEvents.get(0).getEntityId());
        assertNotNull(entityEvents.get(0).getFeatureValue());
        assertEquals(CaptureEvent.FeatureValueType.SCALAR_DOUBLE, entityEvents.get(0).getFeatureValueType());
        assertEquals(CaptureEvent.CaptureMode.ENTITY, entityEvents.get(0).getCaptureMode());
    }

    @Test
    void pseudonymous_emitsEntityIdAndFeatureValue() throws Exception {
        FaroConfig<TestRecord> config = FaroConfig.<TestRecord>builder()
                .feature("score", FaroFeatureConfig.<TestRecord>builder()
                        .entityKey(r -> r.userId)
                        .featureValue(r -> r.score)
                        .valueType(CaptureEvent.FeatureValueType.SCALAR_DOUBLE)
                        .classification(DataClassification.PSEUDONYMOUS)
                        .build())
                .build();

        FaroProcessFunction<TestRecord, TestRecord> fn = buildFn(config);
        fn.processElement(new TestRecord("user-2", 7.5), mockCtx(), mock(Collector.class));

        List<CaptureEvent> entityEvents = captured.events.stream()
                .filter(e -> e.getCaptureMode() == CaptureEvent.CaptureMode.ENTITY)
                .toList();

        assertEquals(1, entityEvents.size());
        assertEquals("user-2", entityEvents.get(0).getEntityId());
        assertNotNull(entityEvents.get(0).getFeatureValue());
    }

    @Test
    void personal_suppressesEntityIdAndFeatureValue() throws Exception {
        FaroConfig<TestRecord> config = FaroConfig.<TestRecord>builder()
                .feature("score", FaroFeatureConfig.<TestRecord>builder()
                        .entityKey(r -> r.userId)
                        .featureValue(r -> r.score)
                        .valueType(CaptureEvent.FeatureValueType.SCALAR_DOUBLE)
                        .classification(DataClassification.PERSONAL)
                        .build())
                .build();

        FaroProcessFunction<TestRecord, TestRecord> fn = buildFn(config);
        fn.processElement(new TestRecord("user-3", 99.0), mockCtx(), mock(Collector.class));

        List<CaptureEvent> entityEvents = captured.events.stream()
                .filter(e -> e.getCaptureMode() == CaptureEvent.CaptureMode.ENTITY)
                .toList();
        List<CaptureEvent> aggregateEvents = captured.events.stream()
                .filter(e -> e.getCaptureMode() == CaptureEvent.CaptureMode.AGGREGATE)
                .toList();

        assertEquals(0, entityEvents.size());
        assertFalse(aggregateEvents.isEmpty());
        assertNull(aggregateEvents.stream()
                .filter(e -> "score".equals(e.getFeatureName()))
                .findFirst()
                .map(CaptureEvent::getEntityId)
                .orElse(null));
    }

    @Test
    void sensitive_suppressesEntityIdAndFeatureValue() throws Exception {
        FaroConfig<TestRecord> config = FaroConfig.<TestRecord>builder()
                .feature("score", FaroFeatureConfig.<TestRecord>builder()
                        .entityKey(r -> r.userId)
                        .featureValue(r -> r.score)
                        .valueType(CaptureEvent.FeatureValueType.SCALAR_DOUBLE)
                        .classification(DataClassification.SENSITIVE)
                        .build())
                .build();

        FaroProcessFunction<TestRecord, TestRecord> fn = buildFn(config);
        fn.processElement(new TestRecord("user-4", 1.0), mockCtx(), mock(Collector.class));

        assertTrue(captured.events.stream()
                .noneMatch(e -> e.getCaptureMode() == CaptureEvent.CaptureMode.ENTITY));
    }

    @Test
    void sampleRateZero_noEntityEventsEmitted() throws Exception {
        FaroConfig<TestRecord> config = FaroConfig.<TestRecord>builder()
                .feature("score", FaroFeatureConfig.<TestRecord>builder()
                        .entityKey(r -> r.userId)
                        .featureValue(r -> r.score)
                        .valueType(CaptureEvent.FeatureValueType.SCALAR_DOUBLE)
                        .classification(DataClassification.NON_PERSONAL)
                        .sampleRate(0.0)
                        .build())
                .build();

        FaroProcessFunction<TestRecord, TestRecord> fn = buildFn(config);
        for (int i = 0; i < 100; i++) {
            fn.processElement(new TestRecord("user-" + i, (double) i), mockCtx(), mock(Collector.class));
        }

        assertTrue(captured.events.stream()
                .noneMatch(e -> e.getCaptureMode() == CaptureEvent.CaptureMode.ENTITY));
    }

    @Test
    void multiFeaturesOnSameOperator_differentEntityKeys() throws Exception {
        FaroConfig<TestRecord> config = FaroConfig.<TestRecord>builder()
                .feature("user_score", FaroFeatureConfig.<TestRecord>builder()
                        .entityKey(r -> r.userId)
                        .featureValue(r -> r.score)
                        .valueType(CaptureEvent.FeatureValueType.SCALAR_DOUBLE)
                        .classification(DataClassification.NON_PERSONAL)
                        .build())
                .feature("item_rank", FaroFeatureConfig.<TestRecord>builder()
                        .entityKey(r -> r.itemId)
                        .featureValue(r -> r.score)
                        .valueType(CaptureEvent.FeatureValueType.SCALAR_DOUBLE)
                        .classification(DataClassification.NON_PERSONAL)
                        .build())
                .build();

        FaroProcessFunction<TestRecord, TestRecord> fn = buildFn(config);
        fn.processElement(new TestRecord("user-5", "item-99", 3.14), mockCtx(), mock(Collector.class));

        List<CaptureEvent> entityEvents = captured.events.stream()
                .filter(e -> e.getCaptureMode() == CaptureEvent.CaptureMode.ENTITY)
                .toList();

        assertEquals(2, entityEvents.size());

        CaptureEvent userEvent = entityEvents.stream()
                .filter(e -> "user_score".equals(e.getFeatureName()))
                .findFirst().orElseThrow();
        CaptureEvent itemEvent = entityEvents.stream()
                .filter(e -> "item_rank".equals(e.getFeatureName()))
                .findFirst().orElseThrow();

        assertEquals("user-5", userEvent.getEntityId());
        assertEquals("item-99", itemEvent.getEntityId());
    }

    @Test
    void aggregateAndEntityFeaturesOnSameOperator() throws Exception {
        FaroConfig<TestRecord> config = FaroConfig.<TestRecord>builder()
                .features("aggregate_count")
                .feature("entity_score", FaroFeatureConfig.<TestRecord>builder()
                        .entityKey(r -> r.userId)
                        .featureValue(r -> r.score)
                        .valueType(CaptureEvent.FeatureValueType.SCALAR_DOUBLE)
                        .classification(DataClassification.NON_PERSONAL)
                        .build())
                .build();

        FaroProcessFunction<TestRecord, TestRecord> fn = buildFn(config);
        fn.processElement(new TestRecord("user-6", 5.0), mockCtx(), mock(Collector.class));
        fn.flush();

        long entityEventCount = captured.events.stream()
                .filter(e -> e.getCaptureMode() == CaptureEvent.CaptureMode.ENTITY).count();
        long aggregateEventCount = captured.events.stream()
                .filter(e -> e.getCaptureMode() == CaptureEvent.CaptureMode.AGGREGATE).count();

        assertEquals(1, entityEventCount);
        assertTrue(aggregateEventCount >= 1);
    }

    private static final class TestRecord {
        final String userId;
        final String itemId;
        final Double score;

        TestRecord(String userId, Double score) {
            this(userId, null, score);
        }

        TestRecord(String userId, String itemId, Double score) {
            this.userId = userId;
            this.itemId = itemId;
            this.score = score;
        }
    }

    private static final class PassThroughFn extends ProcessFunction<TestRecord, TestRecord> {
        @Override
        public void processElement(TestRecord value, Context ctx, Collector<TestRecord> out) {
            out.collect(value);
        }
    }
}
