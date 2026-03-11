from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
import random
import math

router = APIRouter()

# Module 25 — Capacity Planning & Performance Benchmarking

TPCDS_QUERIES = [
    {"id": "q1", "name": "TPC-DS Q1 — Customer Returns", "description": "Find customers whose total store returns exceed the average store returns for their home state by 20%", "complexity": "Medium"},
    {"id": "q7", "name": "TPC-DS Q7 — Item Revenue", "description": "Compute the average quantity, list price, discount, and sales price for promotional items sold in specific demographics", "complexity": "High"},
    {"id": "q19", "name": "TPC-DS Q19 — Catalog Sales by Employee", "description": "Report on managers with top catalog sales in electronics and books category", "complexity": "High"},
    {"id": "q42", "name": "TPC-DS Q42 — Electronics & Jewelry", "description": "Find top revenue items in electronics and jewelry categories for a specific year and quarter", "complexity": "Medium"},
    {"id": "q55", "name": "TPC-DS Q55 — Holiday Sales", "description": "Compute brand sales during Christmas holiday period for specific manager", "complexity": "Low"},
]

LOAD_TEST_SCENARIOS = [
    {"events_per_sec": 100, "latency_p99_ms": 45, "error_rate": 0.0, "cpu_pct": 12, "memory_pct": 18},
    {"events_per_sec": 500, "latency_p99_ms": 52, "error_rate": 0.0, "cpu_pct": 28, "memory_pct": 32},
    {"events_per_sec": 1000, "latency_p99_ms": 68, "error_rate": 0.01, "cpu_pct": 48, "memory_pct": 54},
    {"events_per_sec": 2000, "latency_p99_ms": 124, "error_rate": 0.02, "cpu_pct": 72, "memory_pct": 74},
    {"events_per_sec": 3000, "latency_p99_ms": 287, "error_rate": 0.08, "cpu_pct": 89, "memory_pct": 87},
    {"events_per_sec": 4000, "latency_p99_ms": 892, "error_rate": 0.24, "cpu_pct": 98, "memory_pct": 95},
    {"events_per_sec": 4500, "latency_p99_ms": 4200, "error_rate": 0.67, "cpu_pct": 100, "memory_pct": 99},
]


class StorageCalculatorRequest(BaseModel):
    current_gb_per_day: float = 10.0
    growth_rate_monthly_pct: float = 15.0
    retention_months: int = 24
    compression_ratio: float = 3.0
    replication_factor: int = 3


@router.post("/capacity/storage")
def storage_calculator(req: StorageCalculatorRequest):
    projections = []
    cumulative_raw_gb = 0.0
    for month in range(1, req.retention_months + 1):
        daily_gb = req.current_gb_per_day * ((1 + req.growth_rate_monthly_pct / 100) ** (month - 1))
        monthly_raw_gb = daily_gb * 30
        monthly_compressed_gb = monthly_raw_gb / req.compression_ratio
        monthly_total_gb = monthly_compressed_gb * req.replication_factor
        cumulative_raw_gb += monthly_raw_gb
        projections.append({
            "month": month,
            "daily_ingest_gb": round(daily_gb, 1),
            "monthly_raw_gb": round(monthly_raw_gb, 1),
            "monthly_compressed_gb": round(monthly_compressed_gb, 1),
            "monthly_with_replication_gb": round(monthly_total_gb, 1),
        })

    total_storage_gb = sum(p["monthly_with_replication_gb"] for p in projections)
    total_storage_tb = total_storage_gb / 1024

    aws_cost = total_storage_gb * 0.023
    gcp_cost = total_storage_gb * 0.020
    azure_cost = total_storage_gb * 0.018

    highlights = [projections[5], projections[11], projections[-1]] if len(projections) >= 12 else projections

    return {
        "inputs": req.dict(),
        "total_storage_tb": round(total_storage_tb, 2),
        "total_storage_gb": round(total_storage_gb, 1),
        "monthly_cloud_costs_usd": {"aws_s3": round(aws_cost / req.retention_months, 2), "gcp_gcs": round(gcp_cost / req.retention_months, 2), "azure_adls": round(azure_cost / req.retention_months, 2)},
        "total_24_month_cost_aws": round(aws_cost, 2),
        "highlights": [{"label": f"Month {p['month']}", **p} for p in highlights],
    }


class PipelineSizingRequest(BaseModel):
    source_rows_per_hour: int = 1000000
    avg_row_size_bytes: int = 512
    transformation_complexity: str = "medium"
    sla_max_latency_min: int = 30
    pipeline_type: str = "batch"


@router.post("/capacity/pipeline")
def pipeline_sizing(req: PipelineSizingRequest):
    complexity_multiplier = {"low": 1.0, "medium": 2.5, "high": 5.0}.get(req.transformation_complexity, 2.5)
    throughput_gb_hr = (req.source_rows_per_hour * req.avg_row_size_bytes) / (1024 ** 3)
    cpu_cores_needed = max(4, math.ceil(throughput_gb_hr * complexity_multiplier * 2))
    memory_gb_needed = max(8, math.ceil(throughput_gb_hr * complexity_multiplier * 4))
    spark_executors = max(2, math.ceil(cpu_cores_needed / 4))
    kafka_partitions = max(8, math.ceil(req.source_rows_per_hour / 100000))

    instance_map = {4: "m5.xlarge (4 vCPU, 16GB)", 8: "m5.2xlarge (8 vCPU, 32GB)", 16: "m5.4xlarge (16 vCPU, 64GB)", 32: "m5.8xlarge (32 vCPU, 128GB)"}
    instance_size = min([k for k in instance_map if k >= cpu_cores_needed], default=32)

    return {
        "inputs": req.dict(),
        "throughput_gb_per_hour": round(throughput_gb_hr, 2),
        "recommendations": {
            "spark": {
                "executors": spark_executors,
                "executor_cores": 4,
                "executor_memory_gb": max(8, memory_gb_needed // spark_executors),
                "driver_memory_gb": 4,
            },
            "kafka": {"partitions": kafka_partitions, "replication_factor": 3, "retention_hours": 168},
            "instance_type": instance_map.get(instance_size, "r5.4xlarge (16 vCPU, 128GB) — memory-optimized"),
            "estimated_monthly_cost_aws": round(spark_executors * 0.192 * 24 * 30, 2),
        },
        "sla_analysis": {
            "estimated_runtime_min": round(60 / max(1, throughput_gb_hr / complexity_multiplier), 1),
            "meets_sla": round(60 / max(1, throughput_gb_hr / complexity_multiplier), 1) <= req.sla_max_latency_min,
            "sla_target_min": req.sla_max_latency_min,
        },
    }


@router.get("/capacity/benchmarks")
def tpcds_benchmarks():
    r = random.Random(42)
    results = []
    for q in TPCDS_QUERIES:
        base_ms = {"Low": r.randint(800, 2000), "Medium": r.randint(3000, 8000), "High": r.randint(10000, 30000)}.get(q["complexity"], 5000)
        results.append({
            **q,
            "no_optimization": {"duration_ms": base_ms, "rows_scanned_M": r.randint(50, 500), "cost_usd": round(base_ms / 1000 * 0.05, 4)},
            "with_partitioning": {"duration_ms": base_ms // 4, "rows_scanned_M": r.randint(5, 50), "cost_usd": round(base_ms / 4 / 1000 * 0.05, 4), "improvement": "75% faster"},
            "with_stats_and_mv": {"duration_ms": base_ms // 12, "rows_scanned_M": r.randint(1, 10), "cost_usd": round(base_ms / 12 / 1000 * 0.05, 4), "improvement": "92% faster"},
        })
    return {"benchmarks": results, "environment": "Simulated 100-scale TPC-DS against gold.fact_orders + dimensions"}


class LoadTestRequest(BaseModel):
    target_events_per_sec: int = 2000
    sla_p99_ms: int = 200
    ramp_steps: int = 7


@router.post("/capacity/loadtest")
def load_test(req: LoadTestRequest):
    results = []
    for scenario in LOAD_TEST_SCENARIOS:
        results.append({
            **scenario,
            "meets_sla": scenario["latency_p99_ms"] <= req.sla_p99_ms,
            "degraded": scenario["error_rate"] > 0.05,
        })

    breaking_point = next((s for s in results if s["error_rate"] > 0.1), results[-1])

    return {
        "test_results": results,
        "breaking_point_events_per_sec": breaking_point["events_per_sec"],
        "sla_target_p99_ms": req.sla_p99_ms,
        "max_sustainable_throughput": next((s["events_per_sec"] for s in reversed(results) if s["meets_sla"] and not s["degraded"]), 500),
        "recommendation": f"Current infrastructure handles up to {next((s['events_per_sec'] for s in reversed(results) if not s['degraded']), 1000)} events/sec sustainably. Scale cluster by 3x to handle {req.target_events_per_sec} events/sec within {req.sla_p99_ms}ms SLA.",
    }


@router.get("/capacity/cost-recommendations")
def cost_recommendations():
    return {
        "recommendations": [
            {"id": "rec-01", "title": "Compress Parquet + partition by date", "current_cost_monthly": 2840, "optimized_cost_monthly": 142, "savings_pct": 95, "effort": "Medium", "detail": "Move from CSV to Parquet (5x compression) + partition by order_date. Athena scans 95% less data per query."},
            {"id": "rec-02", "title": "Auto-terminate idle Databricks clusters", "current_cost_monthly": 1200, "optimized_cost_monthly": 780, "savings_pct": 35, "effort": "Low", "detail": "Set idle timeout = 30 min on all-purpose clusters. Use job clusters for production workloads (auto-terminate after job)."},
            {"id": "rec-03", "title": "Reserved Instances for baseline compute", "current_cost_monthly": 3400, "optimized_cost_monthly": 1360, "savings_pct": 60, "effort": "Low", "detail": "Commit to 1-year Reserved Instances for your predictable baseline. Use Spot/Preemptible for burst workloads."},
            {"id": "rec-04", "title": "Use S3 Intelligent-Tiering for raw data", "current_cost_monthly": 920, "optimized_cost_monthly": 552, "savings_pct": 40, "effort": "Low", "detail": "Enable S3 Intelligent-Tiering on bronze/raw bucket. Objects not accessed for 30 days automatically move to cheaper IA tier."},
        ]
    }
