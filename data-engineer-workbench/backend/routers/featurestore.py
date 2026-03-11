from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import random
import uuid

router = APIRouter()

# Module 17 — Feature Store & MLOps Data Layer

FEATURES = [
    {"id": "feat-001", "name": "customer_ltv_30d", "entity": "customer_id", "dtype": "FLOAT64", "description": "Customer lifetime value over last 30 days", "owner": "ml-team", "freshness_sla_hours": 24, "last_computed": "2024-01-27T06:00:00Z", "status": "fresh", "source_table": "gold.fact_orders", "online_store": True, "offline_store": True, "tags": ["revenue", "customer"]},
    {"id": "feat-002", "name": "order_count_7d", "entity": "customer_id", "dtype": "INT64", "description": "Number of orders placed in last 7 days", "owner": "ml-team", "freshness_sla_hours": 1, "last_computed": "2024-01-27T11:00:00Z", "status": "fresh", "source_table": "gold.fact_orders", "online_store": True, "offline_store": True, "tags": ["orders", "customer"]},
    {"id": "feat-003", "name": "avg_order_value_90d", "entity": "customer_id", "dtype": "FLOAT64", "description": "Average order value over last 90 days", "owner": "data-eng-team", "freshness_sla_hours": 24, "last_computed": "2024-01-27T06:00:00Z", "status": "fresh", "source_table": "gold.fact_orders", "online_store": False, "offline_store": True, "tags": ["revenue", "customer"]},
    {"id": "feat-004", "name": "days_since_last_order", "entity": "customer_id", "dtype": "INT64", "description": "Days elapsed since customer's most recent order", "owner": "ml-team", "freshness_sla_hours": 24, "last_computed": "2024-01-26T06:00:00Z", "status": "stale", "source_table": "gold.dim_customer", "online_store": True, "offline_store": True, "tags": ["churn", "customer"]},
    {"id": "feat-005", "name": "product_view_count_24h", "entity": "product_id", "dtype": "INT64", "description": "Number of product page views in last 24 hours", "owner": "clickstream-team", "freshness_sla_hours": 1, "last_computed": "2024-01-27T11:30:00Z", "status": "fresh", "source_table": "silver.user_events", "online_store": True, "offline_store": True, "tags": ["engagement", "product"]},
    {"id": "feat-006", "name": "cart_abandonment_rate_30d", "entity": "customer_id", "dtype": "FLOAT64", "description": "Rate of cart abandonments in last 30 days (0-1)", "owner": "ml-team", "freshness_sla_hours": 24, "last_computed": "2024-01-27T06:00:00Z", "status": "fresh", "source_table": "silver.user_events", "online_store": True, "offline_store": True, "tags": ["churn", "customer", "engagement"]},
]

ML_PIPELINE_STAGES = [
    {"id": "raw", "name": "Raw Data", "description": "Unprocessed events from operational systems", "owner": "data-platform"},
    {"id": "features", "name": "Feature Engineering", "description": "Transform raw data into model-ready features", "owner": "data-engineering"},
    {"id": "feature_store", "name": "Feature Store", "description": "Offline (S3/Parquet) + Online (Redis) storage", "owner": "data-engineering"},
    {"id": "training", "name": "Model Training", "description": "Historical features + labels → trained model", "owner": "ml-engineering"},
    {"id": "serving", "name": "Model Serving", "description": "Real-time inference using online store features", "owner": "ml-engineering"},
    {"id": "prediction_log", "name": "Prediction Logging", "description": "Log all predictions for monitoring + retraining", "owner": "ml-engineering"},
    {"id": "monitoring", "name": "Model Monitoring", "description": "Detect feature drift, performance degradation", "owner": "ml-engineering / data-engineering"},
]


@router.get("/featurestore/features")
def list_features(entity: Optional[str] = None, tag: Optional[str] = None):
    features = FEATURES
    if entity:
        features = [f for f in features if f["entity"] == entity]
    if tag:
        features = [f for f in features if tag in f.get("tags", [])]
    return {"features": features, "total": len(features)}


class CreateFeatureRequest(BaseModel):
    name: str
    entity: str
    dtype: str
    description: str
    owner: str
    freshness_sla_hours: int = 24
    source_table: str
    feature_sql: str
    tags: List[str] = []


@router.post("/featurestore/features")
def create_feature(req: CreateFeatureRequest):
    fid = f"feat-{str(uuid.uuid4())[:8]}"
    feature = {
        "id": fid, "name": req.name, "entity": req.entity, "dtype": req.dtype,
        "description": req.description, "owner": req.owner,
        "freshness_sla_hours": req.freshness_sla_hours,
        "last_computed": None, "status": "pending",
        "source_table": req.source_table, "feature_sql": req.feature_sql,
        "online_store": False, "offline_store": True, "tags": req.tags,
    }
    return feature


class GenerateDatasetRequest(BaseModel):
    feature_ids: List[str]
    label_column: str = "churned"
    training_window_days: int = 180
    entity_column: str = "customer_id"


@router.post("/featurestore/dataset")
def generate_training_dataset(req: GenerateDatasetRequest):
    features = [f for f in FEATURES if f["id"] in req.feature_ids]
    if not features:
        raise HTTPException(400, "No valid feature IDs provided")

    r = random.Random(hash(",".join(sorted(req.feature_ids))))
    n_rows = r.randint(8000, 15000)
    feature_stats = {}
    for feat in features:
        if feat["dtype"] == "FLOAT64":
            feature_stats[feat["name"]] = {
                "mean": round(r.uniform(0.1, 100), 3), "std": round(r.uniform(0.01, 20), 3),
                "null_pct": round(r.uniform(0, 5), 2), "min": round(r.uniform(0, 1), 3), "max": round(r.uniform(50, 500), 1),
            }
        else:
            feature_stats[feat["name"]] = {
                "mean": round(r.uniform(1, 50), 1), "std": round(r.uniform(1, 15), 1),
                "null_pct": 0.0, "min": 0, "max": r.randint(100, 1000),
            }

    return {
        "dataset": {
            "rows": n_rows,
            "features": [f["name"] for f in features],
            "label": req.label_column,
            "entity": req.entity_column,
            "training_window_days": req.training_window_days,
            "point_in_time_correct": True,
            "export_formats": ["parquet", "csv"],
            "s3_path": f"s3://feature-store/training-datasets/dataset_{r.randint(10000,99999)}.parquet",
        },
        "feature_statistics": feature_stats,
        "point_in_time_explanation": "Each row represents the feature values AS OF the event timestamp — no future data leakage. A naive JOIN would give you revenue data after the churn event (data leakage). The feature store computes an as-of join using event_timestamp.",
        "leakage_example": {
            "naive_join": "SELECT * FROM customers c JOIN features f ON c.id = f.customer_id WHERE c.churned_at IS NOT NULL",
            "problem": "This includes feature values computed AFTER the churn event — the model learns from the future!",
            "correct_approach": "SELECT * FROM customers c JOIN features f ON c.id = f.customer_id AND f.feature_timestamp < c.churned_at",
        },
    }


@router.get("/featurestore/monitoring")
def get_feature_monitoring():
    r = random.Random(42)
    monitoring = []
    for feat in FEATURES:
        drift_score = round(r.uniform(0.01, 0.45), 3)
        monitoring.append({
            "feature_id": feat["id"],
            "feature_name": feat["name"],
            "status": "stale" if feat["status"] == "stale" else ("drift_detected" if drift_score > 0.3 else "healthy"),
            "freshness_hours_ago": r.randint(1, 30) if feat["status"] == "stale" else r.randint(0, 6),
            "drift_score": drift_score,
            "drift_detected": drift_score > 0.3,
            "null_pct_today": round(r.uniform(0, 3), 2),
            "null_pct_baseline": round(r.uniform(0, 2), 2),
            "mean_today": round(r.uniform(10, 100), 2),
            "mean_baseline": round(r.uniform(10, 100), 2),
        })
    return {"monitoring": monitoring}


@router.get("/featurestore/pipeline")
def get_ml_pipeline():
    return {"stages": ML_PIPELINE_STAGES, "de_boundary": "data-engineering owns stages 1-3 (raw → feature store). ML engineering owns stages 4-7."}


@router.get("/featurestore/online-serving")
def online_serving_demo():
    r = random.Random(42)
    return {
        "demo": {
            "request": {"entity": "customer_id", "entity_value": "CUST-12345", "features": ["customer_ltv_30d", "order_count_7d", "days_since_last_order"]},
            "response": {"customer_ltv_30d": 847.50, "order_count_7d": 3, "days_since_last_order": 5},
            "latency_ms": r.randint(2, 8),
            "store": "Redis",
        },
        "vs_offline": {
            "online_latency_ms": r.randint(2, 8),
            "offline_latency_ms": r.randint(200, 800),
            "online_use_case": "Real-time model serving, personalization APIs",
            "offline_use_case": "Model training, batch scoring, analytics",
        },
    }
