# Databricks notebook source
# MAGIC %md
# MAGIC # Faro Spark Streaming E2E
# MAGIC
# MAGIC Demonstrates `FaroSpark` with `withStreamingContext` in a Structured Streaming pipeline on Databricks.
# MAGIC Capture events are written to a Delta table after each micro-batch.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Upload `faro-spark/build/libs/faro-spark-databricks.jar` to your cluster as a library
# MAGIC    (Compute → your cluster → Libraries → Install new → JAR → upload)
# MAGIC 2. Set `CAPTURE_TABLE` and `OUTPUT_TABLE` below to Unity Catalog volume paths you have write access to

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import dev.faro.spark.{FaroSpark, FaroStreamingListener, DeltaCaptureEventSink}
# MAGIC import dev.faro.core.{FaroConfig, FaroFeatureConfig, CaptureEvent, DataClassification}
# MAGIC import org.apache.spark.sql.{Dataset, Row}
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.types._
# MAGIC
# MAGIC val CAPTURE_TABLE = "/Volumes/<catalog>/<schema>/<volume>/faro-streaming-capture"
# MAGIC val OUTPUT_TABLE  = "/Volumes/<catalog>/<schema>/<volume>/faro-streaming-output"
# MAGIC
# MAGIC val featureConfig = FaroFeatureConfig.builder[Row]()
# MAGIC   .entityKey(r => r.getAs[String]("deviceId"))
# MAGIC   .featureValue(r => r.getAs[Double]("temperature").asInstanceOf[Object])
# MAGIC   .valueType(CaptureEvent.FeatureValueType.SCALAR_DOUBLE)
# MAGIC   .classification(DataClassification.NON_PERSONAL)
# MAGIC   .build()
# MAGIC
# MAGIC val config = FaroConfig.builder[Row]()
# MAGIC   .feature("temperature", featureConfig)
# MAGIC   .features("record_count")
# MAGIC   .build()
# MAGIC
# MAGIC val listener = new FaroStreamingListener()
# MAGIC spark.streams.addListener(listener)
# MAGIC
# MAGIC val faro = new FaroSpark("sensor-streaming-pipeline", DeltaCaptureEventSink.factory(spark, CAPTURE_TABLE))
# MAGIC   .withStreamingContext("eventTime", null, listener)
# MAGIC
# MAGIC val stream = spark.readStream
# MAGIC   .format("rate")
# MAGIC   .option("rowsPerSecond", 10)
# MAGIC   .load()
# MAGIC   .withColumn("deviceId", concat(lit("device-"), col("value") % 4))
# MAGIC   .withColumn("temperature", rand() * 30 + 20)
# MAGIC   .withColumn("eventTime", col("timestamp"))
# MAGIC   .withWatermark("eventTime", "10 seconds")
# MAGIC
# MAGIC val query = stream.writeStream
# MAGIC   .foreachBatch { (batchDf: Dataset[Row], batchId: Long) =>
# MAGIC     val filtered = faro.trace(
# MAGIC       "filter.high-temp",
# MAGIC       CaptureEvent.OperatorType.FILTER,
# MAGIC       config,
# MAGIC       (ds: Dataset[Row]) => ds.filter(col("temperature") > 30.0)
# MAGIC     ).apply(batchDf)
# MAGIC
# MAGIC     filtered.write.format("delta").mode("append").save(OUTPUT_TABLE)
# MAGIC   }
# MAGIC   .option("checkpointLocation", OUTPUT_TABLE + "/_checkpoint")
# MAGIC   .start()
# MAGIC
# MAGIC query.awaitTermination(30000)
# MAGIC query.stop()
# MAGIC spark.streams.removeListener(listener)
# MAGIC faro.close()

# COMMAND ----------

# MAGIC %md ### Verify capture events landed

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val events = spark.read.format("delta").load(CAPTURE_TABLE)
# MAGIC println(s"Total capture events: ${events.count()}")
# MAGIC events.groupBy("captureMode", "featureName", "operatorId").count().show()

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC events.filter("captureMode = 'AGGREGATE'")
# MAGIC   .select("pipelineId", "operatorId", "featureName",
# MAGIC           "inputCardinality", "outputCardinality",
# MAGIC           "emitIntervalMs", "watermark", "eventTime", "processingTime")
# MAGIC   .show(truncate = false)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC events.filter("captureMode = 'ENTITY'")
# MAGIC   .select("pipelineId", "operatorId", "featureName",
# MAGIC           "entityId", "featureValueType", "processingTime")
# MAGIC   .show(truncate = false)
