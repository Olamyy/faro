package dev.faro.e2e;

import dev.faro.flink.Faro;
import dev.faro.flink.FaroConfig;
import dev.faro.flink.FaroSink;
import dev.faro.flink.StdoutCaptureEventSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.Random;

/**
 * Unbounded end-to-end Faro demonstration job. Run until cancelled via the Flink UI or
 * {@code flink cancel}.
 */
public final class SensorPipelineJob {

    private static final String[] DEVICES = {"device-A", "device-B", "device-C", "device-D"};
    private static final long WINDOW_SIZE_MS = 10_000L;
    private static final double RECORDS_PER_SECOND = 8.0;

    public static void main(String[] args) throws Exception {
        FaroConfig config = FaroConfig.builder()
                .pipelineId("sensor-pipeline-e2e")
                .features("temperature")
                .build();

        StdoutCaptureEventSink sink = new StdoutCaptureEventSink();
        Faro faro = new Faro(config, sink);

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
                .process(faro.windowTrace(new TemperatureSumFn()))
                .returns(TypeInformation.of(SensorReading.class))
                .uid("window.temperature-sum")
                .sinkTo(new FaroSink<>(new FileSink<>("/tmp/faro-output.txt"), config, sink, "sink.file"))
                .uid("sink.file");

        env.execute("sensor-pipeline-e2e");
    }

    private static final class TemperatureSumFn
            extends ProcessWindowFunction<SensorReading, SensorReading, String, TimeWindow> {

        @Override
        public void process(String deviceId, Context ctx,
                Iterable<SensorReading> elements, Collector<SensorReading> out) {
            double sum = 0;
            long windowStart = ctx.window().getStart();
            for (SensorReading r : elements) {
                sum += r.temperature;
            }
            out.collect(new SensorReading(deviceId, sum, windowStart));
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
                    public void flush(boolean endOfInput) {
                    }

                    @Override
                    public void close() {
                        writer.close();
                    }
                };
            }
        }
}
