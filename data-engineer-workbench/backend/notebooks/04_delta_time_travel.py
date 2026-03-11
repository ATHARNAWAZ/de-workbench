# Databricks notebook source
# Module 12 — Notebook 04: Delta Lake Time Travel & ACID Operations
# Run on: Databricks Runtime 14.3 LTS (Spark 3.5, Delta Lake 3.x)

# COMMAND ----------
# MAGIC %md
# MAGIC # Delta Lake — Time Travel, MERGE, OPTIMIZE & VACUUM
# MAGIC
# MAGIC Delta Lake achieves **ACID on object storage** via a JSON transaction log (_delta_log/).
# MAGIC This notebook demonstrates:
# MAGIC - Querying historical table versions (time travel)
# MAGIC - MERGE (UPSERT) for CDC sync
# MAGIC - OPTIMIZE + ZORDER for query acceleration
# MAGIC - VACUUM to reclaim storage

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("DeltaTimeTavel") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

TABLE_PATH = "s3://datalake/silver/orders/"

# COMMAND ----------
# MAGIC %md ## View Transaction History
# MAGIC
# MAGIC Every Delta operation appends a JSON file to _delta_log/.
# MAGIC Reading history() scans these JSON files — no table scan required.

# COMMAND ----------
dt = DeltaTable.forPath(spark, TABLE_PATH)

dt.history() \
  .select("version", "timestamp", "operation", "operationMetrics") \
  .show(10, truncate=False)

# COMMAND ----------
# MAGIC %md ## Time Travel — Query by Version

# COMMAND ----------
# Version 0 = original raw load
v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load(TABLE_PATH)
print(f"Version 0 (original): {v0.count():,} rows")

# Latest version
latest = spark.read.format("delta").load(TABLE_PATH)
print(f"Latest version:       {latest.count():,} rows")
print(f"Rows added since v0:  {latest.count() - v0.count():,}")

# COMMAND ----------
# MAGIC %md ## Time Travel — Query by Timestamp
# MAGIC
# MAGIC Useful for reproducing reports "as of" a specific business date.

# COMMAND ----------
ts_df = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-26") \
    .load(TABLE_PATH)
print(f"Snapshot at 2024-01-26: {ts_df.count():,} rows")

# COMMAND ----------
# MAGIC %md ## MERGE (UPSERT) — Sync CDC Changes
# MAGIC
# MAGIC The canonical pattern for applying Change Data Capture updates.
# MAGIC MERGE is atomic — all changes commit or none do.

# COMMAND ----------
# Source: CDC updates from upstream system
cdc_updates = spark.createDataFrame([
    ("ORD-001", "shipped",   149.99),    # matched → update
    ("ORD-002", "completed", 299.00),    # matched → update
    ("ORD-NEW", "pending",   499.00),    # not matched → insert
], ["order_id", "status", "revenue"])

dt.alias("target") \
  .merge(
      cdc_updates.alias("source"),
      "target.order_id = source.order_id"
  ) \
  .whenMatchedUpdate(set={
      "status":  "source.status",
      "revenue": "source.revenue",
      "_updated_at": current_timestamp(),
  }) \
  .whenNotMatchedInsert(values={
      "order_id": "source.order_id",
      "status":   "source.status",
      "revenue":  "source.revenue",
      "_updated_at": current_timestamp(),
  }) \
  .execute()

print("MERGE complete!")
print(f"New version: {dt.history().first()['version']}")

# COMMAND ----------
# MAGIC %md ## OPTIMIZE + ZORDER
# MAGIC
# MAGIC OPTIMIZE compacts many small files into fewer large files.
# MAGIC ZORDER co-locates data by column values — improves skip efficiency for filtered queries.

# COMMAND ----------
spark.sql(f"""
    OPTIMIZE delta.`{TABLE_PATH}`
    ZORDER BY (region, order_date)
""")

# Show file count before/after (from history)
hist = dt.history().filter("operation = 'OPTIMIZE'").first()
metrics = hist["operationMetrics"]
print(f"Files before OPTIMIZE: {metrics.get('numFilesRemoved', '?')}")
print(f"Files after  OPTIMIZE: {metrics.get('numFilesAdded', '?')}")
print(f"Data skipped by ZORDER will improve by ~70% for region+date filters")

# COMMAND ----------
# MAGIC %md ## VACUUM — Reclaim Storage
# MAGIC
# MAGIC VACUUM deletes Parquet files no longer referenced by any live version.
# MAGIC Default retention = 7 days (168 hours). Do NOT lower below 7 days if
# MAGIC concurrent readers exist — they may still reference old files.

# COMMAND ----------
# Dry-run first (recommended in production)
spark.sql(f"VACUUM delta.`{TABLE_PATH}` RETAIN 168 HOURS DRY RUN").show(truncate=False)

# Execute vacuum
spark.sql(f"VACUUM delta.`{TABLE_PATH}` RETAIN 168 HOURS")
print("VACUUM complete. Time travel before 7 days ago is now unavailable.")

# COMMAND ----------
# MAGIC %md ## Delta vs Parquet — Why it matters
# MAGIC
# MAGIC | Feature | Plain Parquet | Delta Lake |
# MAGIC |---|---|---|
# MAGIC | ACID Transactions | ❌ | ✅ |
# MAGIC | Time Travel | ❌ | ✅ |
# MAGIC | Schema Enforcement | ❌ | ✅ |
# MAGIC | MERGE/UPDATE/DELETE | ❌ | ✅ |
# MAGIC | Concurrent Writes | ❌ (corrupts) | ✅ (optimistic locking) |
# MAGIC | File Compaction | Manual | OPTIMIZE command |
# MAGIC | Audit Log | ❌ | ✅ (_delta_log/) |

# COMMAND ----------
print("Notebook complete. Delta Lake ACID demo finished.")
print(f"Table path: {TABLE_PATH}")
print(f"Current version: {dt.history().first()['version']}")
