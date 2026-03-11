# Databricks notebook source
# Module 12 — Notebook 03: Streaming Data with Structured Streaming
# Run on: Databricks Runtime 14.3 LTS (Spark 3.5)

# COMMAND ----------
# MAGIC %md
# MAGIC # Structured Streaming — Real-Time Order Processing
# MAGIC
# MAGIC This notebook reads from **Kafka** (orders topic), applies 5-minute windowed
# MAGIC aggregations with watermarks, and writes to **Delta Lake** with exactly-once semantics.
# MAGIC
# MAGIC Key concepts:
# MAGIC - Micro-batch processing (trigger every 30 seconds)
# MAGIC - Watermarks for late data handling
# MAGIC - Exactly-once via checkpointing
# MAGIC - Stateful aggregations

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, sum, count, current_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StringType, DoubleType, TimestampType, IntegerType
)

spark = SparkSession.builder.appName("OrderStreaming").getOrCreate()

KAFKA_SERVERS = "redpanda:9092"   # Redpanda (lightweight Kafka-compatible)
INPUT_TOPIC   = "orders"
CHECKPOINT    = "s3://checkpoints/orders-streaming/"
OUTPUT_PATH   = "s3://datalake/gold/streaming_revenue/"

# COMMAND ----------
# MAGIC %md ## Define Schema
# MAGIC
# MAGIC Always define explicit schemas for streaming — never use inferSchema with streams.

# COMMAND ----------
order_schema = StructType() \
    .add("order_id",   StringType()) \
    .add("customer_id", StringType()) \
    .add("revenue",    DoubleType()) \
    .add("region",     StringType()) \
    .add("status",     StringType()) \
    .add("event_time", TimestampType())

# COMMAND ----------
# MAGIC %md ## Read Stream from Kafka
# MAGIC
# MAGIC `readStream` is lazy — no connection until `writeStream.start()`.

# COMMAND ----------
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Deserialise JSON payload
parsed_stream = raw_stream \
    .select(from_json(col("value").cast("string"), order_schema).alias("d")) \
    .select("d.*") \
    .filter(col("order_id").isNotNull())

print("Stream defined (lazy). Schema:")
parsed_stream.printSchema()

# COMMAND ----------
# MAGIC %md ## Windowed Aggregation with Watermark
# MAGIC
# MAGIC **Watermark = 10 minutes**: Spark waits up to 10 min for late-arriving events
# MAGIC before finalising a window. Events >10 min late are silently dropped.
# MAGIC
# MAGIC **Window = 5 minutes**: Tumbling windows — each window is non-overlapping.

# COMMAND ----------
agg_stream = parsed_stream \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window("event_time", "5 minutes").alias("window"),
        "region"
    ) \
    .agg(
        sum("revenue").alias("revenue_5min"),
        count("*").alias("order_count_5min")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("region"),
        col("revenue_5min"),
        col("order_count_5min"),
        current_timestamp().alias("_written_at")
    )

# COMMAND ----------
# MAGIC %md ## Write Stream to Delta Lake
# MAGIC
# MAGIC `outputMode("append")` = only new windows are written (required with watermarks).
# MAGIC Checkpoint guarantees exactly-once even on driver crash.

# COMMAND ----------
query = agg_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT) \
    .trigger(processingTime="30 seconds") \
    .start(OUTPUT_PATH)

print(f"Streaming query started: {query.id}")
print(f"Status: {query.status}")

# COMMAND ----------
# MAGIC %md ## Monitor the Stream

# COMMAND ----------
import time

for _ in range(5):
    progress = query.lastProgress
    if progress:
        print(f"Micro-batch {progress['batchId']}: "
              f"{progress['numInputRows']:,} rows in "
              f"{progress['durationMs'].get('triggerExecution', 0)}ms")
    time.sleep(30)

# COMMAND ----------
# MAGIC %md ## Delivery Guarantees
# MAGIC
# MAGIC | Guarantee | How |
# MAGIC |---|---|
# MAGIC | **Exactly-once** | Checkpoint + idempotent Delta write |
# MAGIC | **No data loss** | `startingOffsets=latest` + Kafka retention 168h |
# MAGIC | **Late data** | Watermark allows 10 min grace period |
# MAGIC | **Fault tolerance** | Driver restarts resume from last checkpoint offset |

# COMMAND ----------
# Graceful stop (run this cell to stop the stream cleanly)
# query.stop()
# print("Stream stopped")
