package dev.faro.e2e;

import dev.faro.core.AsyncCaptureEventSink;
import dev.faro.core.CaptureEvent;
import dev.faro.core.CaptureEventSinkFactory;
import dev.faro.core.DataClassification;
import dev.faro.flink.FaroFlink;
import dev.faro.core.FaroConfig;
import dev.faro.core.FaroFeatureConfig;
import dev.faro.flink.FaroSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.Random;

final class SensorPipeline {

    private static final String[] DEVICES = {"device-A", "device-B", "device-C", "device-D"};
    private static final long WINDOW_SIZE_MS = 10_000L;
    private static final double RECORDS_PER_SECOND = 4_000.0;
    /** Records per device per window — kept fixed so the event-time layout is stable across phases. */
    private static final long RECORDS_PER_DEVICE_PER_WINDOW = 10_000L;

    private SensorPipeline() {}

    static void execute(CaptureEventSinkFactory innerFactory, String jobName) throws Exception {
        execute(innerFactory, jobName, "sensor-pipeline-e2e", ScenarioMode.NORMAL, CaptureMode.AGGREGATE);
    }

    static void execute(CaptureEventSinkFactory innerFactory, String jobName, String pipelineId) throws Exception {
        execute(innerFactory, jobName, pipelineId, ScenarioMode.NORMAL, CaptureMode.AGGREGATE);
    }

    static void execute(CaptureEventSinkFactory innerFactory, String jobName, String pipelineId,
            ScenarioMode mode) throws Exception {
        execute(innerFactory, jobName, pipelineId, mode, CaptureMode.AGGREGATE);
    }

    static void execute(CaptureEventSinkFactory innerFactory, String jobName, String pipelineId,
            ScenarioMode mode, CaptureMode captureMode) throws Exception {

        // ROTATING and entity capture mode both use the full entity config so that per-device
        // feature values are captured alongside aggregate stats throughout the job lifetime.
        boolean useEntityConfig = captureMode == CaptureMode.ENTITY || mode == ScenarioMode.ROTATING;
        FaroConfig<SensorReading> config = useEntityConfig
                ? FaroConfig.<SensorReading>builder()
                        .feature("temperature", FaroFeatureConfig.<SensorReading>builder()
                                .entityKey(r -> r.deviceId)
                                .featureValue(r -> r.temperature)
                                .valueType(CaptureEvent.FeatureValueType.SCALAR_DOUBLE)
                                .classification(DataClassification.NON_PERSONAL)
                                .build())
                        .features("window_throughput")
                        .build()
                : FaroConfig.<SensorReading>builder()
                        .features("temperature")
                        .build();

        CaptureEventSinkFactory sinkFactory =
                () -> new AsyncCaptureEventSink(innerFactory.create(), 1_000);
        FaroFlink faro = new FaroFlink(pipelineId, sinkFactory);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.enableCheckpointing(5_000L);

        // CARDINALITY_SPIKE uses 5× records per device per window to flood input cardinality.
        // Must be fixed at construction time as it affects the event-time layout.
        long recordsPerDevicePerWindow = mode == ScenarioMode.CARDINALITY_SPIKE
                ? RECORDS_PER_DEVICE_PER_WINDOW * 5
                : RECORDS_PER_DEVICE_PER_WINDOW;
        long recordsPerWindow = DEVICES.length * recordsPerDevicePerWindow;

        DataGeneratorSource<SensorReading> generatorSource = new DataGeneratorSource<>(
                index -> buildReading(index, recordsPerWindow, mode),
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(RECORDS_PER_SECOND),
                TypeInformation.of(SensorReading.class));

        // LATE_EVENTS and ROTATING both need idleness so the watermark advances even when a
        // device goes silent. Always enable it — a 5s idleness timeout is harmless otherwise.
        WatermarkStrategy<SensorReading> watermarks = WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((r, ts) -> r.eventTime)
                .withIdleness(Duration.ofSeconds(5));

        DataStream<SensorReading> source = env.fromSource(generatorSource, watermarks, "sensor-source");

        // Always wire the lateDataTag overload. When no phase emits late events the count stays 0.
        OutputTag<SensorReading> lateTag = new OutputTag<>("late-data", TypeInformation.of(SensorReading.class));

        Sink<SensorReading> fileSink = mode == ScenarioMode.SINK_BACKPRESSURE
                ? new SlowFileSink<>("/tmp/faro-output.txt")
                : new FileSink<>("/tmp/faro-output.txt");

        source
                .keyBy(r -> r.deviceId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE_MS)))
                .process(faro.windowTrace(new TemperatureSumFn(mode), config, lateTag))
                .returns(TypeInformation.of(SensorReading.class))
                .uid("window.temperature-sum")
                .sinkTo(new FaroSink<>(fileSink, pipelineId, config, sinkFactory, "sink.file"))
                .uid("sink.file");

        env.execute(jobName);
    }

    private static SensorReading buildReading(long index, long recordsPerWindow, ScenarioMode mode) {
        // Within each window block, records interleave across devices:
        // position within the window block determines both device and time offset.
        long windowSlot = index / recordsPerWindow;
        long posInWindow = index % recordsPerWindow;
        int deviceIndex = (int) (posInWindow % DEVICES.length);
        long recordSeqForDevice = posInWindow / DEVICES.length;  // 0..recordsPerDevicePerWindow-1
        long recordsPerDevicePerWindow = recordsPerWindow / DEVICES.length;
        // Spread records evenly across the window duration for this device.
        long eventTime = windowSlot * WINDOW_SIZE_MS
                + recordSeqForDevice * (WINDOW_SIZE_MS / recordsPerDevicePerWindow);

        ScenarioMode effective = mode == ScenarioMode.ROTATING
                ? ScenarioMode.forSlot(windowSlot)
                : mode;

        switch (effective) {
            case SILENT_DEVICE:
                if (deviceIndex == 2) deviceIndex = 0;
                break;

            case SLOW_DEVICE:
                if (deviceIndex == 1 && windowSlot % 4 != 0) deviceIndex = 0;
                break;

            case LATE_EVENTS:
                if (index % 4 == 0) eventTime -= (long) (2.5 * WINDOW_SIZE_MS);
                break;

            default:
                break;
        }

        double temperature = computeTemperature(deviceIndex, windowSlot, effective);
        return new SensorReading(DEVICES[deviceIndex], temperature, eventTime);
    }

    private static double computeTemperature(int deviceIndex, long windowSlot, ScenarioMode effective) {
        Random rng = new Random();
        switch (effective) {
            case GRADUAL_DRIFT:
                if (deviceIndex == 0) return 20.0 + windowSlot * 2.0;
                break;

            case STEP_CHANGE:
                if (deviceIndex == 0) return windowSlot < 10 ? 20.0 + rng.nextDouble() * 30.0 : 75.0;
                break;

            case NULL_RATE:
                // ~20% NaN — enough to surface null_rate violation
                if (rng.nextDouble() < 0.20) return Double.NaN;
                break;

            case CARDINALITY_DROP:
                // ~80% NaN so the window sum collapses and output is suppressed
                if (rng.nextDouble() < 0.80) return Double.NaN;
                break;

            default:
                break;
        }
        return 20.0 + rng.nextDouble() * 30.0;
    }

    private static final class TemperatureSumFn
            extends ProcessWindowFunction<SensorReading, SensorReading, String, TimeWindow> {

        private final ScenarioMode mode;

        TemperatureSumFn(ScenarioMode mode) {
            this.mode = mode;
        }

        @Override
        public void process(String deviceId, Context ctx,
                Iterable<SensorReading> elements, Collector<SensorReading> out) {
            long windowSlot = ctx.window().getStart() / WINDOW_SIZE_MS;
            ScenarioMode effective = mode == ScenarioMode.ROTATING
                    ? ScenarioMode.forSlot(windowSlot)
                    : mode;

            double sum = 0;
            for (SensorReading r : elements) {
                if (!Double.isNaN(r.temperature)) sum += r.temperature;
            }
            // In CARDINALITY_DROP ~80% of records are NaN so sum is ~20% of normal (~70k).
            // Suppress windows below 100k to drive output_cardinality to near 0.
            if (effective == ScenarioMode.CARDINALITY_DROP && sum <= 100_000.0) return;
            out.collect(new SensorReading(deviceId, sum, ctx.window().getStart()));
        }
    }

    private record FileSink<T>(String path) implements Sink<T> {

        @Override
        public SinkWriter<T> createWriter(InitContext context) throws IOException {
            PrintWriter writer = new PrintWriter(new FileWriter(path, true));
            return new SinkWriter<>() {
                @Override
                public void write(T element, Context ctx) {
                    writer.println(element);
                    writer.flush();
                }

                @Override
                public void flush(boolean endOfInput) {}

                @Override
                public void close() {
                    writer.close();
                }
            };
        }
    }

    /**
     * A sink that introduces a 50ms delay per record to simulate backpressure.
     * Fills the AsyncCaptureEventSink queue, causing capture_drop_since_last=true.
     */
    private record SlowFileSink<T>(String path) implements Sink<T> {

        @Override
        public SinkWriter<T> createWriter(InitContext context) throws IOException {
            PrintWriter writer = new PrintWriter(new FileWriter(path, true));
            return new SinkWriter<>() {
                @Override
                public void write(T element, Context ctx) throws InterruptedException {
                    Thread.sleep(50);
                    writer.println(element);
                    writer.flush();
                }

                @Override
                public void flush(boolean endOfInput) {}

                @Override
                public void close() {
                    writer.close();
                }
            };
        }
    }
}
