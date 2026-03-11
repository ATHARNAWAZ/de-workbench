"""
Module 12 — Big Data & Databricks
All Spark operations are simulated using Pandas + realistic metrics.
No actual PySpark installation required.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import random
import datetime
import json

router = APIRouter()

# ─────────────────────────────────────────────────────
# SECTION A — Apache Spark Fundamentals
# ─────────────────────────────────────────────────────

@router.get("/bigdata/spark/concepts")
def get_spark_concepts():
    return {
        "abstractions": [
            {
                "name": "RDD",
                "full_name": "Resilient Distributed Dataset",
                "year": 2011,
                "description": "Low-level immutable distributed collection. Manual schema, no optimizer.",
                "use_when": "Fine-grained control, custom serialization, unstructured data",
                "api_langs": ["Python", "Scala", "Java"],
                "optimized": False,
                "color": "orange",
                "partition_count": 4,
            },
            {
                "name": "DataFrame",
                "full_name": "Distributed DataFrame",
                "year": 2015,
                "description": "High-level abstraction with named columns, schema, and Catalyst optimizer.",
                "use_when": "99% of use cases — structured/semi-structured data, SQL analytics",
                "api_langs": ["Python", "Scala", "Java", "R", "SQL"],
                "optimized": True,
                "color": "blue",
                "partition_count": 4,
            },
            {
                "name": "Dataset",
                "full_name": "Type-safe Dataset",
                "year": 2016,
                "description": "Compile-time type safety + Catalyst optimizer. Scala/Java only.",
                "use_when": "Scala/Java teams requiring compile-time type checking",
                "api_langs": ["Scala", "Java"],
                "optimized": True,
                "color": "green",
                "partition_count": 4,
            },
        ],
        "transformations": {
            "narrow": [
                {"name": "map()", "description": "Apply function element-wise", "shuffle": False},
                {"name": "filter()", "description": "Remove non-matching rows", "shuffle": False},
                {"name": "flatMap()", "description": "Map then flatten result", "shuffle": False},
                {"name": "select()", "description": "Project specific columns", "shuffle": False},
                {"name": "withColumn()", "description": "Add or replace a column", "shuffle": False},
                {"name": "union()", "description": "Combine two DataFrames (same schema)", "shuffle": False},
            ],
            "wide": [
                {"name": "groupBy().agg()", "description": "Group rows and aggregate", "shuffle": True},
                {"name": "join()", "description": "Combine two DFs on a key — shuffle both", "shuffle": True},
                {"name": "distinct()", "description": "Remove duplicates across partitions", "shuffle": True},
                {"name": "repartition(n)", "description": "Redistribute data evenly", "shuffle": True},
                {"name": "orderBy()", "description": "Global sort across all partitions", "shuffle": True},
                {"name": "window()", "description": "Window aggregation (partial shuffle)", "shuffle": True},
            ],
        },
        "lazy_evaluation_chain": [
            {"op": "spark.read.csv('orders.csv', header=True)", "type": "source", "note": "No data read yet — just a plan"},
            {"op": ".filter(col('status') == 'completed')", "type": "narrow", "note": "Narrow: each partition filtered independently"},
            {"op": ".withColumn('revenue', col('price') * col('qty'))", "type": "narrow", "note": "Narrow: computed column, no shuffle"},
            {"op": ".groupBy('region').agg(sum('revenue'))", "type": "wide", "note": "WIDE: data shuffled across network by 'region' key"},
            {"op": ".orderBy('revenue', ascending=False)", "type": "wide", "note": "WIDE: global sort requires all data to exchange"},
            {"op": ".show(10)  # ← ACTION triggers the whole plan!", "type": "action", "note": "Only NOW does Spark execute all transformations"},
        ],
        "dag_stages": [
            {"stage": 0, "name": "Stage 0: Read + Filter", "tasks": 8, "type": "narrow", "shuffle_out_mb": 0},
            {"stage": 1, "name": "Stage 1: Shuffle Write (groupBy)", "tasks": 8, "type": "shuffle_write", "shuffle_out_mb": 284},
            {"stage": 2, "name": "Stage 2: Shuffle Read + Aggregate", "tasks": 200, "type": "shuffle_read", "shuffle_out_mb": 12},
            {"stage": 3, "name": "Stage 3: Sort + Collect", "tasks": 1, "type": "action", "shuffle_out_mb": 0},
        ],
    }


class SparkJobRequest(BaseModel):
    code: str
    dataset_size: str = "medium"  # small=10k, medium=100k, large=1M


@router.post("/bigdata/spark/simulate")
def simulate_spark_job(req: SparkJobRequest):
    random.seed(hash(req.code[:50]) % 10000)
    size_map = {"small": 10_000, "medium": 100_000, "large": 1_000_000}
    row_count = size_map.get(req.dataset_size, 100_000)

    has_join = "join" in req.code.lower()
    has_groupby = "groupby" in req.code.lower() or "group_by" in req.code.lower()
    has_orderby = "orderby" in req.code.lower() or "order_by" in req.code.lower()
    has_filter = "filter" in req.code.lower() or "where" in req.code.lower()
    wide_ops = sum([has_join, has_groupby, has_orderby])
    num_stages = max(1, wide_ops + 1)
    num_executors = 4

    stages = []
    total_time = 0
    current_rows = row_count
    stage_names = ["Read & Filter", "Shuffle (Hash Partition)", "Aggregate & Join", "Sort & Write", "Final Collect"]

    for i in range(num_stages):
        tasks = num_executors * random.randint(2, 8)
        duration_ms = random.randint(600, 4200)
        shuffle_read = random.randint(128, 1024) if i > 0 else 0
        shuffle_write = random.randint(64, 512) if i < num_stages - 1 else 0
        input_rows = current_rows
        if i == 0 and has_filter:
            current_rows = int(current_rows * random.uniform(0.3, 0.7))
        elif i > 0:
            current_rows = int(current_rows * random.uniform(0.2, 0.8))
        stages.append({
            "stage_id": i,
            "name": stage_names[i] if i < len(stage_names) else f"Stage {i}",
            "tasks": tasks,
            "duration_ms": duration_ms,
            "shuffle_read_mb": shuffle_read,
            "shuffle_write_mb": shuffle_write,
            "input_rows": input_rows,
            "output_rows": current_rows,
            "status": "SUCCESS",
        })
        total_time += duration_ms

    executors = [
        {
            "id": f"executor-{i + 1}",
            "tasks_completed": random.randint(8, 30),
            "gc_time_ms": random.randint(40, 500),
            "peak_memory_mb": random.randint(512, 3072),
            "shuffle_read_mb": random.randint(50, 800),
            "shuffle_write_mb": random.randint(50, 400),
        }
        for i in range(num_executors)
    ]

    return {
        "job_id": f"job_{abs(hash(req.code[:20])) % 9999:04d}",
        "status": "SUCCEEDED",
        "total_duration_ms": total_time,
        "input_rows": row_count,
        "output_rows": current_rows,
        "stages": stages,
        "executors": executors,
        "shuffle_total_mb": sum(s["shuffle_write_mb"] for s in stages),
        "configs": {
            "spark.sql.shuffle.partitions": 200,
            "spark.executor.memory": "4g",
            "spark.executor.cores": 4,
            "spark.dynamicAllocation.enabled": True,
            "spark.sql.adaptive.enabled": True,
        },
    }


@router.get("/bigdata/spark/templates")
def get_spark_templates():
    return [
        {
            "id": "eda",
            "name": "EDA on Orders",
            "description": "Exploratory analysis of the orders dataset",
            "code": """from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, desc

spark = SparkSession.builder.appName("EDA_Orders").getOrCreate()

# Read data
df = spark.read.csv("s3://datalake/bronze/orders/", header=True, inferSchema=True)
print(f"Total rows: {df.count():,}")
df.printSchema()

# Revenue by region
df.groupBy("region") \\
  .agg(
    sum("revenue").alias("total_revenue"),
    avg("revenue").alias("avg_order_value"),
    count("*").alias("order_count")
  ) \\
  .orderBy(desc("total_revenue")) \\
  .show()

# Status distribution
df.groupBy("status").count().orderBy(desc("count")).show()""",
        },
        {
            "id": "medallion",
            "name": "Bronze → Silver → Gold",
            "description": "Full medallion architecture pipeline",
            "code": """from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp
from delta.tables import DeltaTable

spark = SparkSession.builder \\
    .appName("MedallionPipeline") \\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
    .getOrCreate()

# Bronze: raw ingestion (append-only)
raw_df = spark.read.json("s3://landing/orders/2024-01-27/")
raw_df.write.format("delta").mode("append").save("s3://datalake/bronze/orders/")

# Silver: cleanse + validate
bronze_df = spark.read.format("delta").load("s3://datalake/bronze/orders/")
silver_df = bronze_df \\
    .filter(col("order_id").isNotNull()) \\
    .withColumn("order_date", to_timestamp("order_date")) \\
    .withColumn("revenue", col("price") * col("quantity")) \\
    .dropDuplicates(["order_id"]) \\
    .withColumn("_processed_at", current_timestamp())

silver_df.write.format("delta").mode("overwrite") \\
    .option("overwriteSchema", "true") \\
    .save("s3://datalake/silver/orders/")

# Gold: business aggregations
gold_df = silver_df \\
    .groupBy("region", "product_category") \\
    .agg(sum("revenue").alias("total_revenue"))

gold_df.write.format("delta").mode("overwrite") \\
    .save("s3://datalake/gold/revenue_by_region/")
print("Pipeline complete!")""",
        },
        {
            "id": "streaming",
            "name": "Structured Streaming (Kafka)",
            "description": "Real-time order processing with watermarks",
            "code": """from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("OrderStreaming").getOrCreate()

schema = StructType() \\
    .add("order_id", StringType()) \\
    .add("customer_id", StringType()) \\
    .add("revenue", DoubleType()) \\
    .add("region", StringType()) \\
    .add("event_time", TimestampType())

# Read from Kafka
stream_df = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "kafka:9092") \\
    .option("subscribe", "orders") \\
    .load() \\
    .select(from_json(col("value").cast("string"), schema).alias("d")) \\
    .select("d.*")

# 5-minute windowed aggregation with 10-min late data tolerance
agg_df = stream_df \\
    .withWatermark("event_time", "10 minutes") \\
    .groupBy(window("event_time", "5 minutes"), "region") \\
    .agg(sum("revenue").alias("revenue_5min"))

# Write to Delta with exactly-once
query = agg_df.writeStream \\
    .format("delta") \\
    .outputMode("append") \\
    .option("checkpointLocation", "s3://checkpoints/orders/") \\
    .trigger(processingTime="30 seconds") \\
    .start("s3://datalake/gold/streaming_revenue/")

query.awaitTermination()""",
        },
        {
            "id": "delta_merge",
            "name": "Delta MERGE / Time Travel",
            "description": "UPSERT patterns and querying historical snapshots",
            "code": """from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("DeltaDemo").getOrCreate()
table_path = "s3://datalake/silver/orders/"

# View full transaction history
dt = DeltaTable.forPath(spark, table_path)
dt.history().select("version", "timestamp", "operation").show(10)

# Time travel: read version 0
v0 = spark.read.format("delta").option("versionAsOf", 0).load(table_path)
print(f"Version 0: {v0.count():,} rows")

# MERGE (UPSERT) — sync CDC updates
updates = spark.createDataFrame([
    ("ORD-001", "shipped", 149.99),
    ("ORD-NEW", "pending", 299.00),
], ["order_id", "status", "revenue"])

dt.alias("target").merge(
    updates.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll() \\
 .whenNotMatchedInsertAll() \\
 .execute()

# Compact small files + Z-order for faster region queries
spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY (region, order_date)")
print("OPTIMIZE complete!")""",
        },
    ]


# ─────────────────────────────────────────────────────
# SECTION B — Databricks Platform Simulator
# ─────────────────────────────────────────────────────

RUNTIMES = [
    "14.3 LTS (Spark 3.5, Scala 2.12)",
    "13.3 LTS (Spark 3.4, Scala 2.12)",
    "15.0 ML (Spark 3.5, GPU)",
    "12.2 LTS (Spark 3.3, Scala 2.12)",
]

NODE_TYPES = [
    {"id": "i3.xlarge", "vcpus": 4, "ram_gb": 30, "dbu_hour": 0.75, "label": "General Purpose"},
    {"id": "i3.2xlarge", "vcpus": 8, "ram_gb": 61, "dbu_hour": 1.5, "label": "General Purpose"},
    {"id": "r5.4xlarge", "vcpus": 16, "ram_gb": 128, "dbu_hour": 3.0, "label": "Memory Optimized"},
    {"id": "c5.9xlarge", "vcpus": 36, "ram_gb": 72, "dbu_hour": 4.5, "label": "Compute Optimized"},
]

_clusters: dict = {
    "cluster-001": {
        "id": "cluster-001",
        "name": "dev-analytics",
        "state": "RUNNING",
        "runtime": "14.3 LTS (Spark 3.5, Scala 2.12)",
        "node_type": "i3.xlarge",
        "min_workers": 2,
        "max_workers": 8,
        "current_workers": 3,
        "autoscaling": True,
        "dbu_hour": 0.75,
        "created_at": "2024-01-27T08:00:00",
        "events": [
            {"time": "08:00:00", "type": "STARTED", "message": "Cluster started with 2 workers"},
            {"time": "09:15:00", "type": "AUTOSCALE", "message": "Scaled up to 3 workers (high demand)"},
            {"time": "10:30:00", "type": "DRIVER", "message": "SparkContext initialized successfully"},
        ],
    },
    "cluster-002": {
        "id": "cluster-002",
        "name": "prod-etl-job-cluster",
        "state": "TERMINATED",
        "runtime": "13.3 LTS (Spark 3.4, Scala 2.12)",
        "node_type": "i3.2xlarge",
        "min_workers": 4,
        "max_workers": 4,
        "current_workers": 0,
        "autoscaling": False,
        "dbu_hour": 1.5,
        "created_at": "2024-01-26T22:00:00",
        "events": [
            {"time": "22:00:00", "type": "STARTED", "message": "Job cluster started"},
            {"time": "22:47:00", "type": "JOB", "message": "ETL pipeline completed successfully"},
            {"time": "22:48:00", "type": "TERMINATED", "message": "Auto-terminated after job completion"},
        ],
    },
}


@router.get("/bigdata/databricks/clusters")
def list_clusters():
    return list(_clusters.values())


@router.get("/bigdata/databricks/node-types")
def get_node_types():
    return {"node_types": NODE_TYPES, "runtimes": RUNTIMES}


class CreateClusterRequest(BaseModel):
    name: str
    node_type: str = "i3.xlarge"
    min_workers: int = 2
    max_workers: int = 8
    autoscaling: bool = True
    runtime: str = "14.3 LTS (Spark 3.5, Scala 2.12)"


@router.post("/bigdata/databricks/clusters")
def create_cluster(req: CreateClusterRequest):
    cluster_id = f"cluster-{len(_clusters) + 1:03d}"
    node = next((n for n in NODE_TYPES if n["id"] == req.node_type), NODE_TYPES[0])
    cluster = {
        "id": cluster_id,
        "name": req.name,
        "state": "STARTING",
        "runtime": req.runtime,
        "node_type": req.node_type,
        "min_workers": req.min_workers,
        "max_workers": req.max_workers,
        "current_workers": req.min_workers,
        "autoscaling": req.autoscaling,
        "dbu_hour": node["dbu_hour"],
        "created_at": datetime.datetime.now().isoformat(),
        "events": [
            {
                "time": datetime.datetime.now().strftime("%H:%M:%S"),
                "type": "STARTING",
                "message": f"Provisioning {req.min_workers} × {req.node_type} workers",
            }
        ],
    }
    _clusters[cluster_id] = cluster
    return cluster


@router.put("/bigdata/databricks/clusters/{cluster_id}/toggle")
def toggle_cluster(cluster_id: str):
    cluster = _clusters.get(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    now = datetime.datetime.now().strftime("%H:%M:%S")
    if cluster["state"] == "RUNNING":
        cluster["state"] = "TERMINATED"
        cluster["current_workers"] = 0
        cluster["events"].append({"time": now, "type": "TERMINATED", "message": "Cluster terminated by user"})
    else:
        cluster["state"] = "RUNNING"
        cluster["current_workers"] = cluster["min_workers"]
        cluster["events"].append({"time": now, "type": "STARTED", "message": "Cluster restarted"})
    return cluster


NOTEBOOKS = {
    "nb-01": {
        "id": "nb-01",
        "name": "01 — EDA on Orders Dataset",
        "description": "Exploratory Data Analysis with PySpark and Spark SQL",
        "cells": [
            {
                "id": "c1",
                "type": "markdown",
                "source": "# EDA — Orders Dataset\n\nThis notebook performs exploratory data analysis on the e-commerce orders dataset using **PySpark DataFrames** and **Spark SQL**.",
                "output": None,
                "duration_ms": None,
                "annotation": None,
            },
            {
                "id": "c2",
                "type": "python",
                "source": 'from pyspark.sql import SparkSession\nfrom pyspark.sql.functions import col, sum, avg, count, desc\n\nspark = SparkSession.builder.appName("EDA").getOrCreate()\ndf = spark.read.csv("/dbfs/FileStore/orders.csv", header=True, inferSchema=True)\nprint(f"Shape: {df.count():,} rows × {len(df.columns)} columns")\ndf.printSchema()',
                "output": "Shape: 5,000 rows × 12 columns\nroot\n |-- order_id: string (nullable = true)\n |-- customer_id: string (nullable = true)\n |-- product_category: string (nullable = true)\n |-- revenue: double (nullable = true)\n |-- status: string (nullable = true)\n |-- region: string (nullable = true)\n |-- order_date: timestamp (nullable = true)",
                "duration_ms": 1240,
                "annotation": "inferSchema scans the file in a separate Spark job to determine column types. In production, always define explicit schemas to avoid this extra scan.",
            },
            {
                "id": "c3",
                "type": "python",
                "source": "region_df = df.groupBy('region').agg(\n    sum('revenue').alias('total_revenue'),\n    avg('revenue').alias('avg_order_value'),\n    count('*').alias('order_count')\n).orderBy(desc('total_revenue'))\n\nregion_df.show()",
                "output": "+----------+---------------+------------------+-----------+\n|    region| total_revenue |  avg_order_value | order_count|\n+----------+---------------+------------------+-----------+\n|      West |    425,823.50 |           156.82 |      2,715 |\n|      East |    312,445.10 |           148.50 |      2,103 |\n|     South |    198,234.80 |           142.90 |      1,387 |\n|   Midwest |    145,602.30 |           138.45 |      1,051 |\n+----------+---------------+------------------+-----------+",
                "duration_ms": 2850,
                "annotation": "groupBy().agg() is a WIDE transformation — Spark shuffles data across the network partitioned by 'region'. This creates a new stage boundary in the DAG.",
            },
            {
                "id": "c4",
                "type": "sql",
                "source": "SELECT product_category,\n       COUNT(*) AS orders,\n       ROUND(SUM(revenue), 2) AS total_revenue\nFROM orders_view\nGROUP BY product_category\nORDER BY total_revenue DESC\nLIMIT 5",
                "output": "+------------------+--------+---------------+\n| product_category | orders | total_revenue |\n+------------------+--------+---------------+\n| Electronics      |  1,205 |   $312,450.80 |\n| Clothing         |  1,087 |   $198,234.10 |\n| Home & Garden    |    892 |   $167,892.50 |\n| Sports           |    756 |   $143,201.30 |\n| Books            |    543 |    $89,450.20 |\n+------------------+--------+---------------+",
                "duration_ms": 980,
                "annotation": "Spark SQL compiles to the same physical plan as the DataFrame API. Catalyst applies predicate pushdown and column pruning automatically.",
            },
            {
                "id": "c5",
                "type": "python",
                "source": "from pyspark.sql.functions import when\n\n# Single-pass null scan (efficient!)\nnull_df = df.select([\n    count(when(col(c).isNull(), c)).alias(c) for c in df.columns\n])\nnull_df.show()",
                "output": "+----------+-------------+----------------+---------+--------+--------+------------+\n| order_id | customer_id | product_category| revenue | status | region | order_date |\n+----------+-------------+----------------+---------+--------+--------+------------+\n|        0 |           3 |               0 |      12 |      0 |      1 |          0 |\n+----------+-------------+----------------+---------+--------+--------+------------+",
                "duration_ms": 3420,
                "annotation": "One Spark action for all columns vs. len(columns) separate actions. This 7x reduction in Spark jobs is a senior engineer pattern for efficient null scanning.",
            },
        ],
    },
    "nb-02": {
        "id": "nb-02",
        "name": "02 — Bronze → Silver → Gold Pipeline",
        "description": "Full medallion architecture with Delta Lake",
        "cells": [
            {
                "id": "c1",
                "type": "markdown",
                "source": "# Medallion Architecture Pipeline\n\n**Bronze** → raw ingestion | **Silver** → cleansed | **Gold** → aggregated for BI\n\nAll layers use **Delta Lake** for ACID, time travel, and schema evolution.",
                "output": None,
                "duration_ms": None,
                "annotation": None,
            },
            {
                "id": "c2",
                "type": "python",
                "source": "raw_df = spark.read.json('/dbfs/FileStore/raw/orders/')\nraw_df = raw_df.withColumn('_ingested_at', current_timestamp())\nraw_df.write.format('delta').mode('append').save('/delta/bronze/orders')\nprint(f'Bronze: {raw_df.count():,} rows ingested')",
                "output": "Bronze: 10,247 rows ingested\n_delta_log/ created with commit 0 (ADD 52 files)",
                "duration_ms": 4200,
                "annotation": "Bronze is APPEND-ONLY. Never modify raw data. _delta_log records every commit enabling ACID and time travel back to the original landing.",
            },
            {
                "id": "c3",
                "type": "python",
                "source": "bronze_df = spark.read.format('delta').load('/delta/bronze/orders')\nsilver_df = (\n  bronze_df\n  .filter(col('order_id').isNotNull())\n  .withColumn('revenue', col('price') * col('qty'))\n  .withColumn('order_date', to_timestamp('order_date'))\n  .dropDuplicates(['order_id'])\n)\nsilver_df.write.format('delta').mode('overwrite').save('/delta/silver/orders')\nprint(f'Silver: {bronze_df.count():,} → {silver_df.count():,} rows')",
                "output": "Silver: 10,247 → 9,891 rows\n(Removed: 312 nulls, 44 duplicates)",
                "duration_ms": 6800,
                "annotation": "dropDuplicates(['order_id']) is WIDE — shuffles all data by order_id hash to detect duplicates. This is the most expensive step.",
            },
            {
                "id": "c4",
                "type": "python",
                "source": "gold_df = (\n  spark.read.format('delta').load('/delta/silver/orders')\n  .groupBy('region', 'product_category', 'order_date')\n  .agg(sum('revenue').alias('daily_revenue'), count('*').alias('orders'))\n)\ngold_df.write.format('delta').mode('overwrite') \\\n  .partitionBy('order_date') \\\n  .save('/delta/gold/revenue')\nprint('Gold layer ready!')",
                "output": "Gold layer ready!\nPartitions: 365 (one per day)\nParquet files written: 1,460",
                "duration_ms": 8900,
                "annotation": "Gold is partitioned by date — BI tools can skip entire date partitions with predicate pushdown. Never expose Bronze/Silver to downstream consumers.",
            },
        ],
    },
    "nb-03": {
        "id": "nb-03",
        "name": "03 — Structured Streaming Demo",
        "description": "Real-time Kafka → Delta with watermarks and exactly-once",
        "cells": [
            {
                "id": "c1",
                "type": "markdown",
                "source": "# Structured Streaming\n\nProcesses real-time order events from **Kafka** using Spark Structured Streaming with **exactly-once guarantees** via checkpointing.",
                "output": None,
                "duration_ms": None,
                "annotation": None,
            },
            {
                "id": "c2",
                "type": "python",
                "source": "from pyspark.sql.functions import from_json, window, sum, col\nfrom pyspark.sql.types import StructType, StringType, DoubleType, TimestampType\n\nschema = (StructType()\n  .add('order_id', StringType())\n  .add('revenue', DoubleType())\n  .add('region', StringType())\n  .add('event_time', TimestampType()))\n\nstream_df = spark.readStream.format('kafka')\\\n  .option('kafka.bootstrap.servers', 'redpanda:9092')\\\n  .option('subscribe', 'orders').load()\\\n  .select(from_json(col('value').cast('string'), schema).alias('d'))\\\n  .select('d.*')\n\nprint('Stream reader defined (lazy — no data yet)')",
                "output": "Stream reader defined (lazy — no data yet)\nSchema inferred: {order_id: string, revenue: double, region: string, event_time: timestamp}",
                "duration_ms": 340,
                "annotation": "readStream is LAZY. No Kafka connection is made until writeStream.start(). Spark's lazy evaluation applies identically to batch and streaming.",
            },
            {
                "id": "c3",
                "type": "python",
                "source": "agg_df = stream_df\\\n  .withWatermark('event_time', '10 minutes')\\\n  .groupBy(window('event_time', '5 minutes'), 'region')\\\n  .agg(sum('revenue').alias('revenue_5min'))\n\nquery = agg_df.writeStream\\\n  .format('delta')\\\n  .outputMode('append')\\\n  .option('checkpointLocation', 's3://checkpoints/orders/')\\\n  .trigger(processingTime='30 seconds')\\\n  .start('/delta/gold/streaming_revenue/')\n\nprint(f'Query {query.id} running...')",
                "output": "Query a1b2c3d4-e5f6-7890 running...\nMicro-batch 0: 1,247 records in 2.3s\nMicro-batch 1: 892 records in 1.8s\nWatermark: 2024-01-27T10:18:00 (allows late data until 10:28:00)",
                "duration_ms": 2300,
                "annotation": "processingTime='30 seconds' = micro-batch interval. Watermark='10 minutes' means Spark waits 10 min for late events before finalizing a window. Checkpoint = exactly-once on crash recovery.",
            },
        ],
    },
    "nb-04": {
        "id": "nb-04",
        "name": "04 — Delta Time Travel & MERGE",
        "description": "ACID operations, version history, and UPSERT patterns",
        "cells": [
            {
                "id": "c1",
                "type": "markdown",
                "source": "# Delta Lake — Time Travel & MERGE\n\nDelta Lake achieves **ACID on object storage** (S3/ADLS/GCS) via a JSON transaction log. This notebook explores version history, time travel, and the MERGE pattern for CDC.",
                "output": None,
                "duration_ms": None,
                "annotation": None,
            },
            {
                "id": "c2",
                "type": "python",
                "source": "from delta.tables import DeltaTable\n\ndt = DeltaTable.forPath(spark, '/delta/silver/orders')\ndt.history().select('version', 'timestamp', 'operation', 'operationMetrics').show(5)",
                "output": "+-------+--------------------+----------+----------------------------------------------+\n|version|timestamp           |operation |operationMetrics                              |\n+-------+--------------------+----------+----------------------------------------------+\n|      3|2024-01-27 10:30:00 |MERGE     |{numUpdated: 892, numInserted: 45}            |\n|      2|2024-01-27 09:15:00 |OPTIMIZE  |{numFilesAdded: 1, numFilesRemoved: 47}       |\n|      1|2024-01-27 08:00:00 |WRITE     |{numFiles: 48, numOutputRows: 9891}           |\n|      0|2024-01-26 22:00:00 |WRITE     |{numFiles: 52, numOutputRows: 10247}          |\n+-------+--------------------+----------+----------------------------------------------+",
                "duration_ms": 1100,
                "annotation": "Every Delta operation appends a JSON file to _delta_log/. Reading history() queries these JSON files — no table scan required. This is the audit trail that enables ACID and time travel.",
            },
            {
                "id": "c3",
                "type": "python",
                "source": "v0 = spark.read.format('delta').option('versionAsOf', 0).load('/delta/silver/orders')\nv3 = spark.read.format('delta').option('versionAsOf', 3).load('/delta/silver/orders')\nprint(f'Version 0 (raw):         {v0.count():,} rows')\nprint(f'Version 3 (after MERGE): {v3.count():,} rows')",
                "output": "Version 0 (raw):         10,247 rows\nVersion 3 (after MERGE): 10,292 rows\n(+45 new rows from MERGE inserts)",
                "duration_ms": 2400,
                "annotation": "Time travel reads the Parquet files referenced by version 0's log — no data is duplicated physically. Delta tracks file additions/removals per version. VACUUM removes files no longer reachable.",
            },
            {
                "id": "c4",
                "type": "python",
                "source": "updates = spark.createDataFrame([\n  ('ORD-001', 'shipped', 149.99),\n  ('ORD-NEW', 'pending', 299.00),\n], ['order_id', 'status', 'revenue'])\n\ndt.alias('t').merge(updates.alias('s'), 't.order_id = s.order_id')\\\n  .whenMatchedUpdateAll()\\\n  .whenNotMatchedInsertAll()\\\n  .execute()\n\nprint('MERGE complete: 1 updated, 1 inserted')",
                "output": "MERGE complete: 1 updated, 1 inserted\nNew commit: version 4 → _delta_log/00000000000000000004.json\n{\"add\": [...], \"remove\": [...], \"commitInfo\": {\"operation\": \"MERGE\"}}",
                "duration_ms": 3800,
                "annotation": "MERGE is ATOMIC — all rows update/insert or none do. This is the CDC upsert pattern: source system sends changes, MERGE syncs them into the target Delta table.",
            },
        ],
    },
}


@router.get("/bigdata/databricks/notebooks")
def list_notebooks():
    return [{"id": v["id"], "name": v["name"], "description": v["description"]} for v in NOTEBOOKS.values()]


@router.get("/bigdata/databricks/notebooks/{notebook_id}")
def get_notebook(notebook_id: str):
    nb = NOTEBOOKS.get(notebook_id)
    if not nb:
        raise HTTPException(status_code=404, detail="Notebook not found")
    return nb


# Unity Catalog tree (static — no separate DB needed)
@router.get("/bigdata/databricks/unity-catalog")
def get_unity_catalog():
    return {
        "catalogs": [
            {
                "name": "prod",
                "schemas": [
                    {
                        "name": "bronze",
                        "tables": [
                            {"name": "raw_orders", "format": "Delta", "rows": 10247, "size_mb": 84, "owner": "data_eng", "pii": True, "tags": ["raw", "append-only"]},
                            {"name": "raw_customers", "format": "Delta", "rows": 8412, "size_mb": 32, "owner": "data_eng", "pii": True, "tags": ["raw"]},
                        ],
                    },
                    {
                        "name": "silver",
                        "tables": [
                            {"name": "orders", "format": "Delta", "rows": 9891, "size_mb": 64, "owner": "data_eng", "pii": False, "tags": ["cleansed", "deduplicated"]},
                            {"name": "customers", "format": "Delta", "rows": 8100, "size_mb": 28, "owner": "data_eng", "pii": False, "tags": ["cleansed"]},
                        ],
                    },
                    {
                        "name": "gold",
                        "tables": [
                            {"name": "revenue_by_region", "format": "Delta", "rows": 1460, "size_mb": 8, "owner": "analytics", "pii": False, "tags": ["aggregated", "bi-ready"]},
                            {"name": "daily_kpis", "format": "Delta", "rows": 365, "size_mb": 4, "owner": "analytics", "pii": False, "tags": ["kpi", "bi-ready"]},
                        ],
                    },
                ],
            },
            {
                "name": "dev",
                "schemas": [
                    {
                        "name": "sandbox",
                        "tables": [
                            {"name": "test_orders", "format": "Delta", "rows": 100, "size_mb": 1, "owner": "ather.nawaz", "pii": False, "tags": ["dev"]},
                        ],
                    }
                ],
            },
        ]
    }


# ─────────────────────────────────────────────────────
# SECTION C — Delta Lake Deep Dive
# ─────────────────────────────────────────────────────

_delta_state: dict = {
    "current_data": [
        {"order_id": "ORD-001", "status": "pending", "revenue": 149.99, "region": "West"},
        {"order_id": "ORD-002", "status": "completed", "revenue": 299.00, "region": "East"},
        {"order_id": "ORD-003", "status": "shipped", "revenue": 89.50, "region": "South"},
        {"order_id": "ORD-004", "status": "pending", "revenue": 450.00, "region": "West"},
        {"order_id": "ORD-005", "status": "completed", "revenue": 175.00, "region": "Midwest"},
    ]
}

_delta_log: list = [
    {
        "version": 0,
        "timestamp": "2024-01-26T22:00:00",
        "operation": "CREATE TABLE",
        "commit_info": {
            "operation": "CREATE TABLE",
            "schema": "order_id STRING, status STRING, revenue DOUBLE, region STRING",
        },
        "add_files": ["part-00000-a1b2.snappy.parquet", "part-00001-c3d4.snappy.parquet"],
        "remove_files": [],
        "rows_added": 5,
        "rows_removed": 0,
    }
]


class DeltaOpRequest(BaseModel):
    operation: str  # insert | update | delete | merge | optimize | vacuum


@router.post("/bigdata/delta/execute")
def execute_delta_op(req: DeltaOpRequest):
    op = req.operation.lower()
    version = len(_delta_log)
    ts = datetime.datetime.now().isoformat()

    if op == "insert":
        new_row = {
            "order_id": f"ORD-{100 + version:03d}",
            "status": "pending",
            "revenue": round(random.uniform(50, 500), 2),
            "region": random.choice(["West", "East", "South", "Midwest"]),
        }
        _delta_state["current_data"].append(new_row)
        log_entry = {
            "version": version, "timestamp": ts, "operation": "WRITE",
            "commit_info": {"operation": "INSERT", "numOutputRows": 1},
            "add_files": [f"part-{version:05d}-new.snappy.parquet"],
            "remove_files": [],
            "rows_added": 1, "rows_removed": 0,
        }
        _delta_log.append(log_entry)
        return {"status": "success", "version": version, "log_entry": log_entry,
                "data": _delta_state["current_data"],
                "message": f"Inserted 1 row: {new_row['order_id']} (revenue ${new_row['revenue']})"}

    elif op == "update":
        updated = 0
        for row in _delta_state["current_data"]:
            if row["status"] == "pending" and updated < 2:
                row["status"] = "shipped"
                updated += 1
        log_entry = {
            "version": version, "timestamp": ts, "operation": "UPDATE",
            "commit_info": {"operation": "UPDATE", "numUpdatedRows": updated},
            "add_files": [f"part-{version:05d}-upd.snappy.parquet"],
            "remove_files": [f"part-{version - 1:05d}-old.snappy.parquet"],
            "rows_added": updated, "rows_removed": updated,
        }
        _delta_log.append(log_entry)
        return {"status": "success", "version": version, "log_entry": log_entry,
                "data": _delta_state["current_data"],
                "message": f"Updated {updated} rows: pending → shipped"}

    elif op == "delete":
        before = len(_delta_state["current_data"])
        _delta_state["current_data"] = [r for r in _delta_state["current_data"] if r["revenue"] > 100]
        removed = before - len(_delta_state["current_data"])
        log_entry = {
            "version": version, "timestamp": ts, "operation": "DELETE",
            "commit_info": {"operation": "DELETE", "numDeletedRows": removed},
            "add_files": [],
            "remove_files": [f"part-{version - 1:05d}-del.snappy.parquet"],
            "rows_added": 0, "rows_removed": removed,
        }
        _delta_log.append(log_entry)
        return {"status": "success", "version": version, "log_entry": log_entry,
                "data": _delta_state["current_data"],
                "message": f"Deleted {removed} rows where revenue ≤ $100"}

    elif op == "merge":
        new_record = {"order_id": "ORD-MERGE", "status": "completed", "revenue": 999.99, "region": "West"}
        existing = next((r for r in _delta_state["current_data"] if r["order_id"] == "ORD-001"), None)
        if existing:
            existing["status"] = "completed"
        if not any(r["order_id"] == "ORD-MERGE" for r in _delta_state["current_data"]):
            _delta_state["current_data"].append(new_record)
        log_entry = {
            "version": version, "timestamp": ts, "operation": "MERGE",
            "commit_info": {"operation": "MERGE", "numTargetRowsUpdated": 1, "numTargetRowsInserted": 1},
            "add_files": [f"part-{version:05d}-mrg.snappy.parquet"],
            "remove_files": [f"part-{version - 1:05d}-old.snappy.parquet"],
            "rows_added": 2, "rows_removed": 1,
        }
        _delta_log.append(log_entry)
        return {"status": "success", "version": version, "log_entry": log_entry,
                "data": _delta_state["current_data"],
                "message": "MERGE: 1 matched+updated, 1 not-matched+inserted"}

    elif op == "optimize":
        before = random.randint(48, 120)
        after = random.randint(1, 4)
        log_entry = {
            "version": version, "timestamp": ts, "operation": "OPTIMIZE",
            "commit_info": {"operation": "OPTIMIZE", "numFilesAdded": after, "numFilesRemoved": before, "zOrderBy": ["region"]},
            "add_files": [f"part-opt-{i:03d}.snappy.parquet" for i in range(after)],
            "remove_files": [f"part-{i:05d}-small.snappy.parquet" for i in range(before)],
            "rows_added": 0, "rows_removed": 0,
        }
        _delta_log.append(log_entry)
        return {"status": "success", "version": version, "log_entry": log_entry,
                "data": _delta_state["current_data"],
                "message": f"OPTIMIZE: compacted {before} small files → {after} files (Z-ORDER by region)"}

    elif op == "vacuum":
        files_deleted = random.randint(15, 60)
        log_entry = {
            "version": version, "timestamp": ts, "operation": "VACUUM END",
            "commit_info": {"operation": "VACUUM END", "numDeletedFiles": files_deleted, "retentionHours": 168},
            "add_files": [],
            "remove_files": [f"part-old-{i:05d}.snappy.parquet" for i in range(files_deleted)],
            "rows_added": 0, "rows_removed": 0,
        }
        _delta_log.append(log_entry)
        return {"status": "success", "version": version, "log_entry": log_entry,
                "data": _delta_state["current_data"],
                "message": f"VACUUM: deleted {files_deleted} obsolete files (>168h retention). Time travel before v{max(0, version - 3)} no longer available."}

    raise HTTPException(status_code=400, detail=f"Unknown operation: {op}")


@router.get("/bigdata/delta/history")
def get_delta_history():
    return {
        "log": _delta_log,
        "current_version": len(_delta_log) - 1,
        "row_count": len(_delta_state["current_data"]),
        "current_data": _delta_state["current_data"],
    }


@router.get("/bigdata/delta/snapshot/{version}")
def get_delta_snapshot(version: int):
    if version >= len(_delta_log):
        version = len(_delta_log) - 1
    log_at = _delta_log[version]
    base = [
        {"order_id": "ORD-001", "status": "pending", "revenue": 149.99, "region": "West"},
        {"order_id": "ORD-002", "status": "completed", "revenue": 299.00, "region": "East"},
        {"order_id": "ORD-003", "status": "shipped", "revenue": 89.50, "region": "South"},
        {"order_id": "ORD-004", "status": "pending", "revenue": 450.00, "region": "West"},
        {"order_id": "ORD-005", "status": "completed", "revenue": 175.00, "region": "Midwest"},
    ]
    return {
        "version": version,
        "timestamp": log_at["timestamp"],
        "operation": log_at["operation"],
        "data": base,
        "row_count": len(base) + log_at.get("rows_added", 0),
    }


@router.get("/bigdata/delta/format-comparison")
def get_format_comparison():
    return {
        "formats": ["Delta Lake", "Apache Iceberg", "Apache Hudi"],
        "criteria": [
            {"name": "ACID Transactions", "values": ["Full", "Full", "Full"]},
            {"name": "Time Travel", "values": ["Yes (versions)", "Yes (snapshots)", "Yes (commits)"]},
            {"name": "Schema Evolution", "values": ["Add/rename cols", "Full (col reorder)", "Add cols only"]},
            {"name": "Upserts (MERGE)", "values": ["Native", "Native", "Native (optimized)"]},
            {"name": "Streaming Ingest", "values": ["Excellent", "Good", "Excellent (CoW/MoR)"]},
            {"name": "Engine Support", "values": ["Spark, Trino, Flink*", "Spark, Trino, Flink", "Spark, Flink, Hive"]},
            {"name": "Open Standard", "values": ["Linux Foundation", "Apache Foundation", "Apache Foundation"]},
            {"name": "Originated By", "values": ["Databricks (2019)", "Netflix (2020)", "Uber (2019)"]},
            {"name": "Best for", "values": ["Databricks shops", "Multi-engine, Snowflake", "CDC-heavy, near-realtime"]},
        ],
    }


# ─────────────────────────────────────────────────────
# SECTION D — Apache Kafka & Streaming
# ─────────────────────────────────────────────────────

KAFKA_TOPICS = [
    {"name": "orders", "partitions": 6, "replication_factor": 3, "retention_hours": 168, "msgs_per_sec": 850},
    {"name": "user-events", "partitions": 12, "replication_factor": 3, "retention_hours": 24, "msgs_per_sec": 4200},
    {"name": "sensor-readings", "partitions": 4, "replication_factor": 2, "retention_hours": 72, "msgs_per_sec": 2100},
    {"name": "payment-events", "partitions": 8, "replication_factor": 3, "retention_hours": 720, "msgs_per_sec": 320},
]

KAFKA_CONNECTORS = [
    {"id": "mysql-cdc-src", "name": "MySQL CDC Source", "type": "SOURCE", "class": "io.debezium.connector.mysql.MySqlConnector", "topics": ["orders", "customers"], "status": "RUNNING", "tasks": 1, "errors_last_hour": 0},
    {"id": "s3-sink", "name": "S3 Sink Connector", "type": "SINK", "class": "io.confluent.connect.s3.S3SinkConnector", "topics": ["orders", "user-events"], "status": "RUNNING", "tasks": 4, "errors_last_hour": 0},
    {"id": "delta-sink", "name": "Delta Lake Sink", "type": "SINK", "class": "io.delta.standalone.DeltaSinkConnector", "topics": ["orders"], "status": "PAUSED", "tasks": 2, "errors_last_hour": 3},
]


@router.get("/bigdata/kafka/topics")
def get_kafka_topics():
    result = []
    for topic in KAFKA_TOPICS:
        partitions = [
            {
                "partition": p,
                "leader": f"broker-{(p % 3) + 1}",
                "replicas": [(p % 3) + 1, (p + 1) % 3 + 1, (p + 2) % 3 + 1],
                "offset": random.randint(50_000, 500_000),
                "consumer_lag": random.randint(0, 5000),
            }
            for p in range(topic["partitions"])
        ]
        result.append({**topic, "partitions_detail": partitions, "total_lag": sum(p["consumer_lag"] for p in partitions)})
    return result


@router.get("/bigdata/kafka/messages/{topic}")
def get_kafka_messages(topic: str):
    generators = {
        "orders": lambda i: {"order_id": f"ORD-{9000 + i}", "customer_id": f"CUST-{random.randint(100, 999)}", "revenue": round(random.uniform(20, 500), 2), "status": random.choice(["pending", "completed", "shipped"]), "region": random.choice(["West", "East", "South", "Midwest"])},
        "user-events": lambda i: {"event_type": random.choice(["page_view", "click", "purchase", "search"]), "user_id": f"USER-{random.randint(1000, 9999)}", "page": random.choice(["/home", "/product", "/cart", "/checkout"]), "session_id": f"sess-{random.randint(10000, 99999)}"},
        "sensor-readings": lambda i: {"sensor_id": f"SENSOR-{random.randint(1, 20):03d}", "temperature": round(random.uniform(18, 40), 1), "humidity": round(random.uniform(30, 80), 1), "alert": random.choice(["OK", "OK", "OK", "WARNING", "CRITICAL"])},
        "payment-events": lambda i: {"payment_id": f"PAY-{random.randint(10000, 99999)}", "amount": round(random.uniform(10, 1000), 2), "currency": random.choice(["USD", "EUR", "GBP"]), "status": random.choice(["authorized", "captured", "failed"])},
    }
    gen = generators.get(topic, generators["orders"])
    base_offset = random.randint(10_000, 50_000)
    topic_info = next((t for t in KAFKA_TOPICS if t["name"] == topic), KAFKA_TOPICS[0])
    messages = [
        {
            "offset": base_offset + i,
            "partition": i % topic_info["partitions"],
            "timestamp": (datetime.datetime.now() - datetime.timedelta(seconds=15 - i)).isoformat(),
            "key": f"key-{random.randint(1, 100)}",
            "value": json.dumps(gen(i)),
            "size_bytes": random.randint(80, 400),
        }
        for i in range(15)
    ]
    return {"topic": topic, "messages": messages}


@router.get("/bigdata/kafka/connectors")
def get_kafka_connectors():
    return KAFKA_CONNECTORS


@router.get("/bigdata/kafka/streaming-metrics")
def get_streaming_metrics():
    return {
        "queries": [
            {"name": "orders-windowed-agg", "status": "ACTIVE", "records_per_sec": 847, "batches": 1247, "avg_batch_ms": 285, "watermark": "2024-01-27T10:28:00"},
            {"name": "user-events-enrichment", "status": "ACTIVE", "records_per_sec": 4120, "batches": 3891, "avg_batch_ms": 142, "watermark": "2024-01-27T10:29:45"},
        ],
        "throughput_series": [
            {"minute": f"10:{m:02d}", "orders": random.randint(40000, 55000), "user_events": random.randint(200000, 260000)}
            for m in range(0, 31, 5)
        ],
    }


# ─────────────────────────────────────────────────────
# SECTION E — Airflow + Spark Integration
# ─────────────────────────────────────────────────────

AIRFLOW_TEMPLATES = [
    {
        "id": "spark_submit",
        "name": "SparkSubmit ETL DAG",
        "description": "Submit a PySpark job to EMR/Dataproc via SparkSubmitOperator",
        "schedule": "0 2 * * *",
        "schedule_human": "Daily at 2:00 AM UTC",
        "tasks": [
            {"id": "check_s3", "type": "S3KeySensor", "upstream": [], "description": "Wait for daily data file in S3 landing zone"},
            {"id": "submit_spark", "type": "SparkSubmitOperator", "upstream": ["check_s3"], "description": "Submit orders_etl.py PySpark job to EMR cluster"},
            {"id": "quality_check", "type": "PythonOperator", "upstream": ["submit_spark"], "description": "Validate output row count and null rates"},
            {"id": "update_catalog", "type": "PythonOperator", "upstream": ["quality_check"], "description": "Update data catalog with new partition metadata"},
            {"id": "notify_slack", "type": "SlackWebhookOperator", "upstream": ["update_catalog"], "description": "Send success/failure notification to #data-alerts"},
        ],
    },
    {
        "id": "databricks_run",
        "name": "Databricks Run DAG",
        "description": "Trigger a Databricks notebook job via DatabricksRunNowOperator",
        "schedule": "0 * * * *",
        "schedule_human": "Every hour at :00",
        "tasks": [
            {"id": "sense_upstream", "type": "ExternalTaskSensor", "upstream": [], "description": "Wait for upstream ingestion DAG to complete"},
            {"id": "run_notebook", "type": "DatabricksRunNowOperator", "upstream": ["sense_upstream"], "description": "Run medallion pipeline notebook on job cluster"},
            {"id": "validate_gold", "type": "DatabricksSubmitRunOperator", "upstream": ["run_notebook"], "description": "Run DQ notebook on gold tables"},
            {"id": "trigger_reports", "type": "TriggerDagRunOperator", "upstream": ["validate_gold"], "description": "Trigger downstream reporting refresh DAG"},
        ],
    },
    {
        "id": "streaming_monitor",
        "name": "Streaming Health Monitor",
        "description": "Monitor Structured Streaming jobs, alert on lag or slow batches",
        "schedule": "*/15 * * * *",
        "schedule_human": "Every 15 minutes",
        "tasks": [
            {"id": "check_lag", "type": "PythonOperator", "upstream": [], "description": "Query Kafka consumer group lag via AdminClient API"},
            {"id": "check_batch", "type": "PythonOperator", "upstream": [], "description": "Check Spark streaming batch duration vs SLA (60s)"},
            {"id": "evaluate_sla", "type": "BranchPythonOperator", "upstream": ["check_lag", "check_batch"], "description": "Branch: alert if lag > 10k OR batch > 60s"},
            {"id": "send_alert", "type": "PagerDutyEventsOperator", "upstream": ["evaluate_sla"], "description": "Page on-call engineer with incident details"},
            {"id": "log_healthy", "type": "PythonOperator", "upstream": ["evaluate_sla"], "description": "Log healthy status to monitoring system"},
        ],
    },
]


@router.get("/bigdata/airflow/dag-templates")
def get_dag_templates():
    return AIRFLOW_TEMPLATES


@router.get("/bigdata/airflow/cost-calculator")
def get_cost_calculator():
    return {
        "job_cluster": {
            "label": "Job Cluster",
            "description": "Spins up for job, terminates immediately after",
            "dbu_rate": 0.20,
            "hours_per_run": 0.8,
            "runs_per_day": 24,
            "dbu_per_node": 4,
            "node_count": 4,
            "idle_pct": 0,
            "recommended_for": "Production ETL, scheduled pipelines",
            "daily_cost_usd": round(0.20 * 0.8 * 24 * 4, 2),
            "monthly_cost_usd": round(0.20 * 0.8 * 24 * 4 * 30, 2),
        },
        "all_purpose_cluster": {
            "label": "All-Purpose Cluster",
            "description": "Always-on cluster for interactive exploration",
            "dbu_rate": 0.55,
            "hours_per_day": 16,
            "dbu_per_node": 4,
            "node_count": 4,
            "idle_pct": 60,
            "recommended_for": "Interactive notebooks, development work",
            "daily_cost_usd": round(0.55 * 16 * 4, 2),
            "monthly_cost_usd": round(0.55 * 16 * 4 * 30, 2),
        },
    }


# ─────────────────────────────────────────────────────
# SECTION F — Performance Tuning Lab
# ─────────────────────────────────────────────────────

@router.get("/bigdata/performance/scenarios")
def get_perf_scenarios():
    return [
        {
            "id": "data_skew",
            "title": "Data Skew",
            "icon": "skew",
            "symptom": "One executor runs 10× longer than others — Spark UI shows 1 task = 14 min, others = 45s",
            "cause": "Uneven key distribution: 78% of rows have customer_id='unknown' in a join",
            "bad_code": "df.join(dim_customer, 'customer_id')  # partition 3 gets 4.2M rows",
            "fix": "Salting: append random int (0–9) to skewed key, explode join side",
            "fix_code": "salt_n = 10\ndf = df.withColumn('salt', (rand() * salt_n).cast('int'))\ndf = df.withColumn('customer_id_salted', concat(col('customer_id'), lit('_'), col('salt')))\n\ndim = dim_customer.withColumn('salt', explode(array([lit(i) for i in range(salt_n)])))\ndim = dim.withColumn('customer_id_salted', concat(col('id'), lit('_'), col('salt')))\n\ndf.join(dim, 'customer_id_salted')",
            "before": {"max_task_s": 840, "min_task_s": 12, "skew_ratio": 70, "spill_gb": 4.2, "total_s": 852},
            "after": {"max_task_s": 95, "min_task_s": 78, "skew_ratio": 1.2, "spill_gb": 0, "total_s": 97},
        },
        {
            "id": "small_files",
            "title": "Too Many Small Files",
            "icon": "files",
            "symptom": "File listing takes 38s. 1,200 tasks open for 2MB each — massive scheduler overhead",
            "cause": "Streaming micro-batches or frequent appends create thousands of tiny Parquet files",
            "bad_code": "# After 6 months of streaming: 1,200 files × 2MB = 2.4GB\n# Spark opens 1,200 connections just for metadata",
            "fix": "OPTIMIZE (Delta) compacts small files. Set target file size to 128–256MB.",
            "fix_code": "# Delta OPTIMIZE (recommended for Delta tables)\nDELTA.OPTIMIZE delta.`/path/to/table` ZORDER BY (region)\n\n# Or Spark coalesce for non-Delta:\ndf.coalesce(8).write.format('parquet').mode('overwrite').save(output_path)",
            "before": {"file_count": 1200, "avg_file_mb": 2, "query_s": 145, "list_overhead_s": 38},
            "after": {"file_count": 8, "avg_file_mb": 300, "query_s": 12, "list_overhead_s": 0},
        },
        {
            "id": "broadcast_join",
            "title": "Broadcast Join vs Sort-Merge Join",
            "icon": "join",
            "symptom": "Join on product_id is slow despite products table being only 8MB",
            "cause": "Spark's autoBroadcastJoinThreshold defaults to 10MB — products table at 12MB falls through",
            "bad_code": "# Sort-Merge Join: both tables shuffled by join key\norders_df.join(products_df, 'product_id')  # 2GB + 12MB both repartitioned",
            "fix": "Broadcast the small table explicitly OR raise the threshold",
            "fix_code": "from pyspark.sql.functions import broadcast\n\n# Option 1: Explicit broadcast hint\norders_df.join(broadcast(products_df), 'product_id')\n\n# Option 2: Raise threshold (careful — OOM risk if too high)\nspark.conf.set('spark.sql.autoBroadcastJoinThreshold', 50 * 1024 * 1024)  # 50MB",
            "before": {"shuffle_gb": 2.1, "stages": 3, "duration_s": 180, "memory_spill_mb": 0},
            "after": {"shuffle_gb": 0, "stages": 1, "duration_s": 8, "memory_spill_mb": 0},
        },
        {
            "id": "partition_tuning",
            "title": "Shuffle Partition Tuning",
            "icon": "partition",
            "symptom": "200 tiny shuffle tasks for a 2GB dataset — scheduler delay 450ms per task",
            "cause": "Default spark.sql.shuffle.partitions=200 creates 10MB partitions — too many for small data",
            "bad_code": "# Default: 200 partitions on 2GB = 10MB each\n# For a 16-core cluster with 32 slots, 200 tasks = 6 waves\nspark.conf.get('spark.sql.shuffle.partitions')  # → '200'",
            "fix": "Set partitions = 2–3× executor cores. Or enable AQE to auto-tune.",
            "fix_code": "# Manual: match cluster size (16 cores × 2 = 32 partitions)\nspark.conf.set('spark.sql.shuffle.partitions', 32)\n\n# Better: Adaptive Query Execution (Spark 3.0+)\nspark.conf.set('spark.sql.adaptive.enabled', True)\nspark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', True)\n# AQE auto-coalesces based on actual data size at runtime",
            "before": {"partitions": 200, "avg_partition_mb": 10, "duration_s": 95, "scheduler_delay_ms": 450},
            "after": {"partitions": 32, "avg_partition_mb": 63, "duration_s": 22, "scheduler_delay_ms": 12},
        },
    ]


@router.get("/bigdata/performance/catalyst")
def get_catalyst_plan():
    return {
        "query": "SELECT c.region, SUM(o.revenue)\nFROM orders o\nJOIN customers c ON o.customer_id = c.id\nWHERE o.status = 'completed'\nGROUP BY c.region",
        "phases": [
            {
                "name": "1. Unresolved Logical Plan",
                "color": "gray",
                "description": "SQL parsed into AST. Column and table names are unresolved symbols.",
                "nodes": [
                    "Project [region, sum(revenue)]",
                    "  Filter [status = 'completed']",
                    "    Join [customer_id = id]",
                    "      UnresolvedRelation [orders]",
                    "      UnresolvedRelation [customers]",
                ],
            },
            {
                "name": "2. Analyzed Logical Plan",
                "color": "blue",
                "description": "Catalog consulted — column types resolved, table schemas loaded.",
                "nodes": [
                    "Project [region: string, sum(revenue: double)]",
                    "  Filter [status: string = 'completed']",
                    "    Join Inner [customer_id: string = id: string]",
                    "      Relation [orders] delta (8 columns)",
                    "      Relation [customers] delta (5 columns)",
                ],
            },
            {
                "name": "3. Optimized Logical Plan",
                "color": "orange",
                "description": "Catalyst rules applied: predicate pushdown, column pruning, constant folding.",
                "nodes": [
                    "Aggregate [region] [sum(revenue)]",
                    "  Project [revenue, region]  ← column pruning applied",
                    "    Join Inner [customer_id = id]",
                    "      Filter [status = 'completed']  ← pushed below join",
                    "      Relation [orders] (only: customer_id, revenue, status)",
                    "      Relation [customers] (only: id, region)",
                ],
            },
            {
                "name": "4. Physical Plan",
                "color": "green",
                "description": "Optimal execution strategy chosen: broadcast join (customers = 8MB).",
                "nodes": [
                    "HashAggregate [region, sum#partial]",
                    "  Exchange hashpartitioning(region, 200)",
                    "    HashAggregate [region, sum#partial]",
                    "      BroadcastHashJoin [customer_id = id]",
                    "        Filter [status = completed]",
                    "          FileScan orders [customer_id, revenue, status]",
                    "        BroadcastExchange (customers → all executors)",
                ],
            },
        ],
        "optimizations": [
            "Predicate Pushdown — filter applied before join (scan fewer rows)",
            "Column Pruning — read only 3 of 8 columns from orders",
            "Broadcast Join — customers (8MB) broadcast instead of shuffle",
            "Partial Aggregation — pre-aggregate on each executor before final reduce",
        ],
    }


# ─────────────────────────────────────────────────────
# SECTION G — Ecosystem Map
# ─────────────────────────────────────────────────────

@router.get("/bigdata/ecosystem/tools")
def get_ecosystem_tools():
    return [
        {"id": "spark", "name": "Apache Spark", "category": "Processing", "description": "Unified analytics engine for large-scale data. In-memory computation, 100× faster than MapReduce.", "use_when": "Batch ETL, ML pipelines, SQL analytics at scale", "databricks_replaces": False, "databricks_note": "Databricks IS built on Spark — same engine with managed runtime + optimizations", "year": 2014, "color": "orange"},
        {"id": "kafka", "name": "Apache Kafka", "category": "Streaming", "description": "Distributed event streaming platform. High-throughput, fault-tolerant, ordered log.", "use_when": "Real-time streaming, CDC, microservice event bus", "databricks_replaces": False, "databricks_note": "Databricks integrates via Structured Streaming Kafka connector", "year": 2011, "color": "blue"},
        {"id": "flink", "name": "Apache Flink", "category": "Streaming", "description": "True streaming engine with native event-time processing. Lower latency than micro-batching.", "use_when": "Sub-second latency, complex event processing, fraud detection", "databricks_replaces": False, "databricks_note": "Databricks supports managed Flink. Project Lightspeed brings streaming improvements to Spark.", "year": 2014, "color": "purple"},
        {"id": "hive", "name": "Apache Hive", "category": "Query", "description": "SQL interface to HDFS/object storage. Batch-oriented, high latency (minutes per query).", "use_when": "Legacy Hadoop environments only", "databricks_replaces": True, "databricks_note": "Delta + SparkSQL replaces Hive with 10–100× better performance", "year": 2010, "color": "yellow"},
        {"id": "hdfs", "name": "HDFS", "category": "Storage", "description": "Hadoop Distributed File System. Stores data across commodity hardware on-premise.", "use_when": "On-premise Hadoop clusters (legacy)", "databricks_replaces": True, "databricks_note": "Cloud object storage (S3/ADLS/GCS) + Delta Lake replaces HDFS", "year": 2006, "color": "gray"},
        {"id": "yarn", "name": "Apache YARN", "category": "Resource Mgmt", "description": "Resource manager for Hadoop. Manages CPU/memory allocation across cluster nodes.", "use_when": "On-premise multi-tenant Hadoop clusters", "databricks_replaces": True, "databricks_note": "Databricks manages compute via its own runtime + cloud auto-scaling", "year": 2012, "color": "gray"},
        {"id": "iceberg", "name": "Apache Iceberg", "category": "Table Format", "description": "Open table format for huge analytic tables. Hidden partitioning, time travel, full schema evolution.", "use_when": "Multi-engine shops (Spark + Trino + Flink), Snowflake integration", "databricks_replaces": False, "databricks_note": "Databricks supports Iceberg via UniForm — Delta tables readable as Iceberg", "year": 2020, "color": "cyan"},
        {"id": "hudi", "name": "Apache Hudi", "category": "Table Format", "description": "Optimized for CDC and high-frequency upserts. Copy-on-Write and Merge-on-Read storage types.", "use_when": "CDC-heavy pipelines, near-realtime analytics from OLTP sources", "databricks_replaces": False, "databricks_note": "Databricks supports Hudi. Delta Lake is Databricks' preferred format.", "year": 2019, "color": "green"},
        {"id": "airflow", "name": "Apache Airflow", "category": "Orchestration", "description": "Platform to author, schedule, and monitor workflows as DAGs using Python.", "use_when": "Complex multi-step pipelines, dependency management, cross-system orchestration", "databricks_replaces": False, "databricks_note": "Databricks Workflows is a native alternative. Many teams use both together.", "year": 2015, "color": "teal"},
        {"id": "zookeeper", "name": "Apache ZooKeeper", "category": "Coordination", "description": "Distributed coordination service. Used by pre-KRaft Kafka and Hadoop services.", "use_when": "Legacy Kafka clusters (pre-3.0)", "databricks_replaces": True, "databricks_note": "Kafka KRaft mode (3.x) eliminates ZooKeeper. Databricks handles coordination internally.", "year": 2008, "color": "gray"},
        {"id": "ranger", "name": "Apache Ranger", "category": "Security", "description": "Policy-based access control for Hadoop ecosystem. Row/column-level security.", "use_when": "On-premise Hadoop with fine-grained access control", "databricks_replaces": True, "databricks_note": "Unity Catalog replaces Ranger with superior column/row-level security across workspaces", "year": 2014, "color": "red"},
        {"id": "superset", "name": "Apache Superset", "category": "BI", "description": "Open-source BI and data visualization. Connects to many SQL sources.", "use_when": "Self-hosted BI, open-source Tableau/Power BI alternative", "databricks_replaces": False, "databricks_note": "Databricks SQL serves as backend. Native integrations with Tableau, Power BI, Looker.", "year": 2016, "color": "blue"},
    ]


@router.get("/bigdata/ecosystem/timeline")
def get_ecosystem_timeline():
    return [
        {"year": 2006, "era": "Hadoop Era", "color": "gray", "event": "HDFS + MapReduce released — batch processing on commodity hardware", "tools": ["HDFS", "MapReduce", "ZooKeeper"]},
        {"year": 2010, "era": "Hadoop Era", "color": "gray", "event": "Hive & HBase mature — SQL finally comes to Hadoop", "tools": ["Hive", "HBase", "Pig", "YARN"]},
        {"year": 2011, "era": "Streaming Era", "color": "blue", "event": "Kafka open-sourced by LinkedIn — event streaming at scale", "tools": ["Kafka"]},
        {"year": 2014, "era": "Spark Era", "color": "orange", "event": "Apache Spark 1.0 — in-memory computation, 100× MapReduce", "tools": ["Spark", "Flink", "Storm"]},
        {"year": 2016, "era": "Cloud DW Era", "color": "cyan", "event": "Snowflake + cloud warehouses disrupt on-premise Hadoop", "tools": ["Snowflake", "BigQuery", "Redshift"]},
        {"year": 2019, "era": "Lakehouse Era", "color": "green", "event": "Delta Lake open-sourced — ACID on object storage, 'Lakehouse' coined", "tools": ["Delta Lake", "Hudi", "Databricks"]},
        {"year": 2021, "era": "Lakehouse Era", "color": "green", "event": "Databricks & Snowflake IPO — data infra goes mainstream, dbt explodes", "tools": ["Databricks", "Snowflake", "dbt"]},
        {"year": 2023, "era": "Open Format Era", "color": "purple", "event": "Apache Iceberg overtakes Hudi; open table format wars heat up", "tools": ["Iceberg", "Hudi", "Delta", "Unity Catalog"]},
        {"year": 2024, "era": "Open Format Era", "color": "purple", "event": "UniForm bridges Delta/Iceberg/Hudi. AI-native lakehouses emerge", "tools": ["UniForm", "Lakeflow", "AI/BI Genie"]},
    ]


@router.get("/bigdata/ecosystem/comparison")
def get_platform_comparison():
    return {
        "platforms": ["Databricks", "Snowflake", "BigQuery", "Redshift", "Azure Synapse"],
        "rows": [
            {"criterion": "Cost Model", "values": ["DBU + cloud compute", "Credit-based (virtual WH)", "Slot reservation / on-demand", "Node-hours (RA3/DC2)", "DWU-hours + Spark pools"]},
            {"criterion": "Spark Support", "values": ["Native (built on Spark)", "None", "Dataproc integration", "None", "Spark pools (separate)"]},
            {"criterion": "ML / AI", "values": ["MLflow + Feature Store", "Snowpark ML + Cortex", "Vertex AI + BQML", "SageMaker integration", "Azure ML integration"]},
            {"criterion": "Streaming", "values": ["Structured Streaming (excellent)", "Snowpipe Streaming (good)", "Dataflow + pub/sub", "Kinesis integration", "Event Hubs + Spark"]},
            {"criterion": "Open Standards", "values": ["Delta/Iceberg/Hudi (UniForm)", "Iceberg (external tables)", "Iceberg (limited)", "Iceberg (limited)", "Delta Lake"]},
            {"criterion": "Governance", "values": ["Unity Catalog (excellent)", "Snowflake RBAC + governance", "Dataplex / BigLake", "AWS Lake Formation", "Microsoft Purview"]},
            {"criterion": "SQL UX", "values": ["Databricks SQL (good)", "Best-in-class", "Excellent", "Good", "Good"]},
            {"criterion": "Serverless", "values": ["Serverless SQL + Jobs", "Fully serverless", "Fully serverless", "Serverless (limited)", "Serverless SQL pool"]},
            {"criterion": "Best For", "values": ["Spark/ML/Lakehouse", "Analytics/ELT/governed DW", "Google Cloud shops", "AWS native teams", "Microsoft/Azure shops"]},
        ],
    }