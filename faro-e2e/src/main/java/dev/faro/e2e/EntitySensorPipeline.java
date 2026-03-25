package dev.faro.e2e;

import dev.faro.core.CaptureEvent;
import dev.faro.core.DataClassification;
import dev.faro.flink.AsyncCaptureEventSink;
import dev.faro.flink.CaptureEventSinkFactory;
import dev.faro.flink.Faro;
import dev.faro.flink.FaroConfig;
import dev.faro.flink.FaroFeatureConfig;
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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.Random;

/**
 * Sensor pipeline demonstrating ENTITY-mode capture alongside the standard AGGREGATE capture.
 *
 * <p>The {@code temperature} feature is configured with {@code deviceId} as the entity key
 * and {@link DataClassification#NON_PERSONAL}, so each window fires both an AGGREGATE event
 * (one per feature per window) and an ENTITY event carrying the device's temperature sum and
 * its identifier. The {@code window_throughput} feature is AGGREGATE-only to show the two
 * modes coexisting on the same operator.
 */
final class EntitySensorPipeline {

    private static final String[] DEVICES = {"device-A", "device-B", "device-C", "device-D"};
    private static final long WINDOW_SIZE_MS = 10_000L;
    private static final double RECORDS_PER_SECOND = 8.0;

    private EntitySensorPipeline() {}

    static void execute(CaptureEventSinkFactory innerFactory, String jobName) throws Exception {
        execute(innerFactory, jobName, "sensor-pipeline-entity-e2e");
    }

    static void execute(CaptureEventSinkFactory innerFactory, String jobName, String pipelineId) throws Exception {
        FaroConfig<SensorReading> config = FaroConfig.<SensorReading>builder()
                .feature("temperature", FaroFeatureConfig.<SensorReading>builder()
                        .entityKey(r -> r.deviceId)
                        .featureValue(r -> r.temperature)
                        .valueType(CaptureEvent.FeatureValueType.SCALAR_DOUBLE)
                        .classification(DataClassification.NON_PERSONAL)
                        .build())
                .features("window_throughput")
                .build();

        CaptureEventSinkFactory sinkFactory =
                () -> new AsyncCaptureEventSink(innerFactory.create(), 1_000);
        Faro faro = new Faro(pipelineId, sinkFactory);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.enableCheckpointing(5_000L);

        DataGeneratorSource<SensorReading> generatorSource = new DataGeneratorSource<>(
                index -> {
                    int deviceIndex = (int) (index % DEVICES.length);
                    long windowSlot = index / DEVICES.length;
                    long eventTime = windowSlot * WINDOW_SIZE_MS + deviceIndex * (WINDOW_SIZE_MS / DEVICES.length);
                    return new SensorReading(DEVICES[deviceIndex], 20.0 + new Random().nextDouble() * 30.0, eventTime);
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(RECORDS_PER_SECOND),
                TypeInformation.of(SensorReading.class));

        DataStream<SensorReading> source = env.fromSource(
                generatorSource,
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((r, ts) -> r.eventTime),
                "sensor-source");

        source
                .keyBy(r -> r.deviceId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE_MS)))
                .process(faro.windowTrace(new TemperatureSumFn(), config))
                .returns(TypeInformation.of(SensorReading.class))
                .uid("window.temperature-sum")
                .sinkTo(new FaroSink<>(new FileSink<>("/tmp/faro-entity-output.txt"), pipelineId, config, sinkFactory, "sink.file"))
                .uid("sink.file");

        env.execute(jobName);
    }

    private static final class TemperatureSumFn
            extends ProcessWindowFunction<SensorReading, SensorReading, String, TimeWindow> {

        @Override
        public void process(String deviceId, Context ctx,
                Iterable<SensorReading> elements, Collector<SensorReading> out) {
            double sum = 0;
            for (SensorReading r : elements) {
                sum += r.temperature;
            }
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
}
