package dev.faro.spark;

import dev.faro.core.CaptureEvent;
import dev.faro.core.DataClassification;
import dev.faro.core.FaroConfig;
import dev.faro.core.FaroFeatureConfig;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Batch Spark sensor pipeline demonstrating {@link SparkFaro} with {@link DeltaCaptureEventSink}.
 *
 * <p>Generates synthetic sensor readings, filters outliers, and writes capture events —
 * one AGGREGATE event per feature per operator, plus one ENTITY event per reading for
 * {@code NON_PERSONAL} features — to a Delta table at {@code tablePath}.
 */
public final class SparkSensorJob {

    private static final String[] DEVICES = {"device-A", "device-B", "device-C", "device-D"};
    private static final int ROW_COUNT = 100;

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("faro-spark-sensor-job")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();
        run(spark, "/tmp/faro-spark-capture");
        spark.stop();
    }

    /**
     * Use this entry point from a Databricks notebook or any context where a SparkSession
     * already exists.
     */
    public static void run(SparkSession spark, String tablePath) {
        FaroConfig<Reading> config = FaroConfig.<Reading>builder()
                .feature("temperature", FaroFeatureConfig.<Reading>builder()
                        .entityKey(r -> r.getDeviceId())
                        .featureValue(r -> r.getTemperature())
                        .valueType(CaptureEvent.FeatureValueType.SCALAR_DOUBLE)
                        .classification(DataClassification.NON_PERSONAL)
                        .build())
                .features("reading_count")
                .build();

        SparkFaro faro = new SparkFaro(
                "spark-sensor-pipeline",
                DeltaCaptureEventSink.factory(spark, tablePath));

        Dataset<Reading> raw = spark.createDataset(generateReadings(), Encoders.bean(Reading.class));

        Dataset<Reading> filtered = faro.trace(
                "filter.outliers",
                CaptureEvent.OperatorType.FILTER,
                config,
                ds -> ds.filter((FilterFunction<Reading>) r -> r.getTemperature() <= 45.0))
                .apply(raw);

        filtered.show();
        faro.close();
    }

    private static List<Reading> generateReadings() {
        List<Reading> rows = new ArrayList<>(ROW_COUNT);
        Random rng = new Random(42);
        for (int i = 0; i < ROW_COUNT; i++) {
            String deviceId = DEVICES[i % DEVICES.length];
            double temperature = 20.0 + rng.nextDouble() * 40.0;
            rows.add(new Reading(deviceId, temperature, System.currentTimeMillis() + i));
        }
        return rows;
    }

    public static final class Reading implements Serializable {

        private String deviceId;
        private double temperature;
        private long eventTime;

        public Reading() {}

        public Reading(String deviceId, double temperature, long eventTime) {
            this.deviceId = deviceId;
            this.temperature = temperature;
            this.eventTime = eventTime;
        }

        public String getDeviceId() { return deviceId; }
        public void setDeviceId(String v) { this.deviceId = v; }

        public double getTemperature() { return temperature; }
        public void setTemperature(double v) { this.temperature = v; }

        public long getEventTime() { return eventTime; }
        public void setEventTime(long v) { this.eventTime = v; }
    }
}
