# Databricks notebook source
# MAGIC %md
# MAGIC # Faro Spark E2E
# MAGIC
# MAGIC Demonstrates `SparkFaro` with `DeltaCaptureEventSink` on a Databricks cluster.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Upload `faro-spark/build/libs/faro-spark-databricks.jar` to your cluster as a library
# MAGIC    (Compute → your cluster → Libraries → Install new → JAR → upload)
# MAGIC 2. Set `TABLE_PATH` below to a Unity Catalog volume path you have write access to

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import dev.faro.spark.{SparkFaro, DeltaCaptureEventSink}
# MAGIC import dev.faro.core.{FaroConfig, FaroFeatureConfig, CaptureEvent, DataClassification}
# MAGIC import org.apache.spark.api.java.function.FilterFunction
# MAGIC import org.apache.spark.sql.Dataset
# MAGIC import java.util.Random
# MAGIC
# MAGIC val TABLE_PATH = "/Volumes/<catalog>/<schema>/<volume>/faro-capture-events"
# MAGIC
# MAGIC val featureConfig = FaroFeatureConfig.builder[String]()
# MAGIC   .entityKey(s => s)
# MAGIC   .featureValue(s => s.length.asInstanceOf[Object])
# MAGIC   .valueType(CaptureEvent.FeatureValueType.SCALAR_LONG)
# MAGIC   .classification(DataClassification.NON_PERSONAL)
# MAGIC   .build()
# MAGIC
# MAGIC val config = FaroConfig.builder[String]()
# MAGIC   .feature("length", featureConfig)
# MAGIC   .features("record_count")
# MAGIC   .build()
# MAGIC
# MAGIC val devices = Array("device-A", "device-B", "device-C", "device-D")
# MAGIC val rng = new Random(42)
# MAGIC val rows = (0 until 100).map(_ => devices(rng.nextInt(devices.length))).toSeq
# MAGIC val raw: Dataset[String] = spark.createDataset(rows)
# MAGIC
# MAGIC val faro = new SparkFaro("spark-sensor-pipeline", DeltaCaptureEventSink.factory(spark, TABLE_PATH))
# MAGIC
# MAGIC val filtered = faro.trace(
# MAGIC   "filter.short",
# MAGIC   CaptureEvent.OperatorType.FILTER,
# MAGIC   config,
# MAGIC   (ds: Dataset[String]) => ds.filter(new FilterFunction[String] { def call(s: String) = s.length > 6 })
# MAGIC ).apply(raw)
# MAGIC
# MAGIC filtered.show()
# MAGIC faro.close()

# COMMAND ----------

# MAGIC %md ### Verify capture events landed

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val events = spark.read.format("delta").load("/Volumes/<catalog>/<schema>/<volume>/faro-capture-events")
# MAGIC println(s"Total capture events: ${events.count()}")
# MAGIC events.groupBy("captureMode", "featureName", "operatorId").count().show()

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC events.filter("captureMode = 'AGGREGATE'")
# MAGIC   .select("pipelineId", "operatorId", "featureName", "inputCardinality", "outputCardinality", "processingTime")
# MAGIC   .show(truncate = false)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC events.filter("captureMode = 'ENTITY'")
# MAGIC   .select("pipelineId", "operatorId", "featureName", "entityId", "featureValueType", "processingTime")
# MAGIC   .show(truncate = false)
