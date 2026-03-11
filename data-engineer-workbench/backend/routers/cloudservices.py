from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

router = APIRouter()

# Module 18 — Cloud-Native Data Services

CLOUD_SERVICE_MAP = [
    {
        "function": "Object Storage",
        "aws": {"name": "Amazon S3", "pricing": "$0.023/GB-month", "notes": "Industry standard. Storage classes: Standard, IA, Glacier. Event notifications via SNS/SQS."},
        "azure": {"name": "Azure Data Lake Storage Gen2", "pricing": "$0.018/GB-month", "notes": "HDFS-compatible. Hierarchical namespace for directory-level permissions. Best for Azure Synapse."},
        "gcp": {"name": "Google Cloud Storage", "pricing": "$0.020/GB-month", "notes": "Consistent performance. Strong consistency. Object versioning. Native BigQuery integration."},
    },
    {
        "function": "Managed Spark",
        "aws": {"name": "Amazon EMR", "pricing": "$0.096/vCPU-hour (m5.xlarge)", "notes": "EMR on EC2 or EKS. Spot instances reduce cost 70%. Integrates with AWS Glue Catalog."},
        "azure": {"name": "Azure HDInsight / Databricks", "pricing": "$0.12/DBU-hour", "notes": "HDInsight for OSS Spark. Azure Databricks for premium features. Delta Lake native support."},
        "gcp": {"name": "Google Dataproc", "pricing": "$0.048/vCPU-hour", "notes": "Fast cluster startup (90s). Preemptible VMs for 80% cost reduction. Native BigQuery connector."},
    },
    {
        "function": "Stream Processing",
        "aws": {"name": "Amazon Kinesis", "pricing": "$0.015/shard-hour + $0.014/million PUT records", "notes": "Kinesis Data Streams for ingest. Kinesis Firehose for delivery to S3/Redshift. KDA for Flink."},
        "azure": {"name": "Azure Event Hubs", "pricing": "$0.028/million events", "notes": "Kafka-compatible protocol. Capture to ADLS Gen2. Stream Analytics for SQL-based processing."},
        "gcp": {"name": "Google Pub/Sub + Dataflow", "pricing": "$0.04/GB ingested + $0.056/vCPU-hour", "notes": "Pub/Sub for messaging. Dataflow (managed Apache Beam) for stream + batch processing."},
    },
    {
        "function": "Data Warehouse",
        "aws": {"name": "Amazon Redshift", "pricing": "$0.25/hour (dc2.large)", "notes": "Columnar MPP. RA3 nodes with managed storage. Redshift Spectrum queries S3 data directly."},
        "azure": {"name": "Azure Synapse Analytics", "pricing": "$5.00/DWU-hour", "notes": "Dedicated SQL Pool (MPP) + Serverless SQL Pool (pay-per-query). Synapse Link for near-real-time HTAP."},
        "gcp": {"name": "Google BigQuery", "pricing": "$5.00/TB queried (on-demand)", "notes": "Serverless — no cluster management. Slot reservations for predictable cost. BI Engine for sub-second queries."},
    },
    {
        "function": "ETL / ELT",
        "aws": {"name": "AWS Glue", "pricing": "$0.44/DPU-hour", "notes": "Serverless Spark ETL. Glue DataBrew for no-code transforms. Glue Catalog as Hive metastore."},
        "azure": {"name": "Azure Data Factory", "pricing": "$1.00/1000 pipeline activity runs", "notes": "Visual pipeline canvas. 90+ connectors. Integration Runtime for on-premises connectivity."},
        "gcp": {"name": "Cloud Data Fusion", "pricing": "$0.48/hour (basic)", "notes": "CDAP-based visual ETL. 150+ connectors. Wrangler for interactive data prep."},
    },
    {
        "function": "Orchestration",
        "aws": {"name": "Amazon MWAA (Managed Airflow)", "pricing": "$0.49/hour (mw1.small)", "notes": "Fully managed Airflow. Auto-scaling workers. IAM integration for AWS service connections."},
        "azure": {"name": "Azure Data Factory Pipelines", "pricing": "Included in ADF pricing", "notes": "Trigger-based scheduling. Monitor pipeline runs. Activity-level retry policies."},
        "gcp": {"name": "Cloud Composer (Managed Airflow)", "pricing": "$0.10/vCPU-hour", "notes": "Managed Airflow on GKE. DAGs stored in GCS. Native GCP operator library."},
    },
    {
        "function": "Data Catalog",
        "aws": {"name": "AWS Glue Data Catalog", "pricing": "Free (first 1M objects)", "notes": "Hive-compatible metastore. Auto-discovery via crawlers. Lake Formation for permissions."},
        "azure": {"name": "Microsoft Purview", "pricing": "$0.40/vCore-hour", "notes": "Enterprise data governance. Automated data discovery. Business glossary and lineage."},
        "gcp": {"name": "Google Dataplex / Data Catalog", "pricing": "$0.10/asset-month", "notes": "Unified data management across lakes and warehouses. Auto-tagging with ML."},
    },
    {
        "function": "ML Platform",
        "aws": {"name": "Amazon SageMaker", "pricing": "$0.054-$32/hour (varies by instance)", "notes": "Full ML lifecycle. Built-in algorithms. SageMaker Feature Store. SageMaker Pipelines for MLOps."},
        "azure": {"name": "Azure Machine Learning", "pricing": "$0.10/hour + compute", "notes": "AutoML, MLflow tracking, Designer (no-code). Azure Arc for on-prem training."},
        "gcp": {"name": "Vertex AI", "pricing": "$0.08/hour + compute", "notes": "Unified platform (was CAIP + AutoML). Vertex Feature Store. Pipeline integration with Kubeflow."},
    },
]

COST_OPTIMIZATION_TIPS = [
    {"id": "tip-001", "service": "S3", "title": "Use S3 Intelligent-Tiering", "description": "Automatically moves objects between tiers based on access patterns. No retrieval fees.", "savings_pct": 40, "effort": "Low"},
    {"id": "tip-002", "service": "EC2/EMR", "title": "Switch to Reserved Instances", "description": "Commit to 1-3 years for predictable workloads. Combine with Spot for burst capacity.", "savings_pct": 60, "effort": "Medium"},
    {"id": "tip-003", "service": "Athena", "title": "Partition and compress data", "description": "Partition by date + use Parquet with Snappy. Reduces data scanned per query by 95%+.", "savings_pct": 90, "effort": "Medium"},
    {"id": "tip-004", "service": "Redshift", "title": "Use RA3 + Redshift Spectrum", "description": "Move cold data to S3, query via Spectrum. Pay for compute + S3 storage separately.", "savings_pct": 50, "effort": "High"},
    {"id": "tip-005", "service": "Databricks", "title": "Auto-terminate idle clusters", "description": "Set idle timeout to 30 minutes. Use job clusters (not all-purpose) for production.", "savings_pct": 35, "effort": "Low"},
    {"id": "tip-006", "service": "BigQuery", "title": "Use slot reservations for predictable loads", "description": "Flat-rate pricing better than on-demand for >2TB/day query volume.", "savings_pct": 45, "effort": "Medium"},
]


@router.get("/cloud/services")
def get_cloud_service_map():
    return {"services": CLOUD_SERVICE_MAP}


@router.get("/cloud/aws")
def get_aws_services():
    aws_services = [
        {
            "name": "Amazon S3", "category": "Storage",
            "description": "Object storage with 99.999999999% durability. Foundation of every AWS data lake.",
            "pricing": "$0.023/GB-month (Standard)", "use_case": "Data lake storage, backups, static assets",
            "storage_classes": [
                {"name": "S3 Standard", "cost_gb": 0.023, "retrieval": "Immediate", "use_case": "Frequently accessed data"},
                {"name": "S3 Standard-IA", "cost_gb": 0.0125, "retrieval": "Immediate", "use_case": "Infrequently accessed, > 30 days"},
                {"name": "S3 Glacier Instant", "cost_gb": 0.004, "retrieval": "Milliseconds", "use_case": "Archive, > 90 days"},
                {"name": "S3 Glacier Deep Archive", "cost_gb": 0.00099, "retrieval": "12 hours", "use_case": "Long-term archive, 7+ years"},
            ],
            "partitioning_impact": {
                "without_partition": {"query": "SELECT * FROM s3.orders WHERE date = '2024-01-01'", "data_scanned_gb": 450, "cost": "$2.25"},
                "with_partition": {"query": "SELECT * FROM s3.orders WHERE date = '2024-01-01'", "data_scanned_gb": 1.2, "cost": "$0.006"},
            },
        },
        {
            "name": "AWS Glue", "category": "ETL",
            "description": "Serverless ETL powered by Spark. Auto-generates PySpark code for common transformations.",
            "pricing": "$0.44/DPU-hour", "use_case": "Batch ETL, schema discovery, data catalog",
            "glue_catalog": {
                "databases": ["raw", "bronze", "silver", "gold"],
                "total_tables": 47,
                "crawlers_active": 3,
                "last_crawl": "2024-01-27T06:00:00Z",
            },
        },
        {
            "name": "Amazon Athena", "category": "Query",
            "description": "Serverless SQL on S3. Pay per query (TB scanned). No infrastructure to manage.",
            "pricing": "$5.00/TB scanned", "use_case": "Ad-hoc queries, data exploration, cost-efficient BI",
            "benchmark": {
                "query": "SELECT region, SUM(revenue) FROM orders GROUP BY region",
                "without_optimization": {"data_scanned_gb": 45.2, "duration_s": 18.4, "cost": "$0.226"},
                "with_parquet_partitioning": {"data_scanned_gb": 0.8, "duration_s": 2.1, "cost": "$0.004"},
                "improvement": "98.2% cost reduction with Parquet + partitioning",
            },
        },
        {
            "name": "Amazon Kinesis Data Streams", "category": "Streaming",
            "description": "Managed real-time data streaming. Partitioned shards, 7-day retention default.",
            "pricing": "$0.015/shard-hour + $0.014/million PUT records",
            "shards": [
                {"id": 0, "records_per_sec": 847, "bytes_per_sec_kb": 423},
                {"id": 1, "records_per_sec": 921, "bytes_per_sec_kb": 461},
                {"id": 2, "records_per_sec": 634, "bytes_per_sec_kb": 317},
            ],
        },
    ]
    return {"services": aws_services}


@router.get("/cloud/gcp")
def get_gcp_services():
    return {
        "services": [
            {
                "name": "BigQuery", "category": "Data Warehouse",
                "description": "Serverless, multi-cloud analytics warehouse. Petabyte-scale SQL.",
                "pricing": "$5/TB queried (on-demand) or flat-rate slot reservations",
                "slot_demo": {
                    "query": "SELECT customer_id, SUM(revenue) FROM orders GROUP BY customer_id",
                    "slots_used": 420, "bytes_processed_gb": 12.4, "duration_s": 2.8,
                    "on_demand_cost": "$0.062", "flat_rate_cost_equivalent": "$0.008",
                },
                "partitioning": {"column": "order_date", "type": "DATE", "benefit": "Query scans only relevant partitions — 95% cost reduction for date-filtered queries"},
            },
            {
                "name": "Google Pub/Sub", "category": "Messaging",
                "description": "Global, durable message bus. Push and pull delivery. At-least-once delivery.",
                "pricing": "$0.04/GB ingested",
                "topics": ["orders", "user-events", "sensor-data"],
                "delivery_modes": {
                    "push": "Pub/Sub HTTP POSTs to your endpoint. Good for serverless.",
                    "pull": "Subscriber polls for messages. Good for batch consumers.",
                },
            },
            {
                "name": "Cloud Dataflow", "category": "Stream + Batch",
                "description": "Managed Apache Beam. Auto-scaling, no cluster management.",
                "pricing": "$0.056/vCPU-hour + $0.003/GB memory-hour",
                "pipeline_demo": {
                    "steps": ["ReadFromPubSub", "ParseJSON", "AddTimestamp", "Window(5min)", "GroupByKey", "SumRevenue", "WriteToBigQuery"],
                    "workers": 4, "autoscaling": True,
                },
            },
        ]
    }


@router.get("/cloud/azure")
def get_azure_services():
    return {
        "services": [
            {
                "name": "Azure Data Factory", "category": "ETL/Orchestration",
                "description": "Visual data integration service. 90+ connectors. Hybrid (cloud + on-premises).",
                "pricing": "$1.00/1000 pipeline activity runs",
                "pipeline_demo": {
                    "activities": [
                        {"name": "Copy Data", "type": "CopyActivity", "source": "Azure SQL DB", "sink": "ADLS Gen2"},
                        {"name": "Data Flow", "type": "DataFlowActivity", "description": "Visual Spark transformation"},
                        {"name": "Notebook", "type": "DatabricksNotebook", "description": "Trigger Databricks job"},
                    ]
                },
            },
            {
                "name": "Azure Synapse Analytics", "category": "Data Warehouse",
                "description": "Unified analytics platform combining data warehousing and big data analytics.",
                "pricing": "Dedicated: $5/DWH-hour | Serverless: $5/TB processed",
                "pools": {
                    "dedicated": {"description": "Pre-provisioned MPP cluster. Predictable performance. Best for recurring BI workloads.", "dwu_range": "100-30000"},
                    "serverless": {"description": "On-demand SQL over ADLS Gen2. No cluster to manage. Pay per query.", "use_case": "Exploration, ad-hoc queries"},
                },
            },
        ]
    }


class CostCalculatorRequest(BaseModel):
    storage_gb: float
    queries_per_day: int = 100
    query_avg_gb_scanned: float = 10.0
    spark_hours_per_day: float = 4.0
    streaming_events_per_day: int = 1000000
    cloud: str = "aws"


@router.post("/cloud/cost-calculator")
def calculate_cost(req: CostCalculatorRequest):
    monthly = {}
    if req.cloud == "aws":
        storage = req.storage_gb * 0.023
        athena = req.queries_per_day * 30 * req.query_avg_gb_scanned * 0.005
        emr = req.spark_hours_per_day * 30 * 0.48
        kinesis = (req.streaming_events_per_day * 30 / 1_000_000) * 0.014
        total = storage + athena + emr + kinesis
        monthly = {"Storage (S3)": round(storage, 2), "Query (Athena)": round(athena, 2), "Spark (EMR)": round(emr, 2), "Streaming (Kinesis)": round(kinesis, 2), "TOTAL": round(total, 2)}
    elif req.cloud == "gcp":
        storage = req.storage_gb * 0.020
        bq = req.queries_per_day * 30 * req.query_avg_gb_scanned * 0.005
        dataproc = req.spark_hours_per_day * 30 * 0.048 * 4
        pubsub = (req.streaming_events_per_day * 30 / 1e9) * 40
        total = storage + bq + dataproc + pubsub
        monthly = {"Storage (GCS)": round(storage, 2), "Query (BigQuery)": round(bq, 2), "Spark (Dataproc)": round(dataproc, 2), "Streaming (Pub/Sub)": round(pubsub, 2), "TOTAL": round(total, 2)}
    else:
        storage = req.storage_gb * 0.018
        synapse = req.queries_per_day * 30 * req.query_avg_gb_scanned * 0.005
        spark = req.spark_hours_per_day * 30 * 0.12 * 4
        eventhub = (req.streaming_events_per_day * 30 / 1_000_000) * 0.028
        total = storage + synapse + spark + eventhub
        monthly = {"Storage (ADLS)": round(storage, 2), "Query (Synapse)": round(synapse, 2), "Spark (Databricks)": round(spark, 2), "Streaming (Event Hubs)": round(eventhub, 2), "TOTAL": round(total, 2)}

    return {"cloud": req.cloud, "monthly_cost_usd": monthly, "optimization_tips": COST_OPTIMIZATION_TIPS[:3]}
