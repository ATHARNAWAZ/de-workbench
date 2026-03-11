from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
import random
import uuid
from datetime import datetime, timedelta

router = APIRouter()

DAGS = [
    {
        "dag_id": "daily_orders_pipeline",
        "name": "Daily Orders Pipeline",
        "description": "End-to-end orders ETL: extract → validate → transform → load to gold layer",
        "schedule": "0 2 * * *",
        "schedule_human": "Every day at 2:00 AM",
        "owner": "data-engineering@corp.com",
        "active": True,
        "tags": ["orders", "revenue", "gold"],
        "last_run": (datetime.now() - timedelta(hours=22)).isoformat(),
        "next_run": (datetime.now() + timedelta(hours=2)).isoformat(),
        "avg_duration_minutes": 12.4,
        "success_rate_30d": 97.8,
        "tasks": [
            {"task_id": "extract_orders", "type": "PythonOperator", "upstream": [], "retries": 3, "retry_delay_minutes": 5},
            {"task_id": "extract_customers", "type": "PythonOperator", "upstream": [], "retries": 3, "retry_delay_minutes": 5},
            {"task_id": "validate_orders", "type": "PythonOperator", "upstream": ["extract_orders"], "retries": 2, "retry_delay_minutes": 2},
            {"task_id": "transform_orders", "type": "PythonOperator", "upstream": ["validate_orders"], "retries": 2, "retry_delay_minutes": 5},
            {"task_id": "enrich_customers", "type": "PythonOperator", "upstream": ["extract_customers", "validate_orders"], "retries": 2, "retry_delay_minutes": 5},
            {"task_id": "build_fact_table", "type": "PythonOperator", "upstream": ["transform_orders", "enrich_customers"], "retries": 3, "retry_delay_minutes": 10},
            {"task_id": "update_catalog", "type": "PythonOperator", "upstream": ["build_fact_table"], "retries": 1, "retry_delay_minutes": 2},
            {"task_id": "send_alert", "type": "SlackOperator", "upstream": ["build_fact_table"], "retries": 1, "retry_delay_minutes": 1},
        ],
    },
    {
        "dag_id": "customer_360_refresh",
        "name": "Customer 360 Refresh",
        "description": "Build the customer 360 gold table with ML-derived segments and churn scores",
        "schedule": "0 4 * * 1",
        "schedule_human": "Every Monday at 4:00 AM",
        "owner": "analytics@corp.com",
        "active": True,
        "tags": ["customer", "ml", "gold"],
        "last_run": (datetime.now() - timedelta(days=3)).isoformat(),
        "next_run": (datetime.now() + timedelta(days=4)).isoformat(),
        "avg_duration_minutes": 45.2,
        "success_rate_30d": 100.0,
        "tasks": [
            {"task_id": "load_silver_data", "type": "PythonOperator", "upstream": [], "retries": 3, "retry_delay_minutes": 5},
            {"task_id": "compute_rfm", "type": "PythonOperator", "upstream": ["load_silver_data"], "retries": 2, "retry_delay_minutes": 5},
            {"task_id": "run_churn_model", "type": "PythonOperator", "upstream": ["load_silver_data"], "retries": 2, "retry_delay_minutes": 10},
            {"task_id": "merge_customer_360", "type": "PythonOperator", "upstream": ["compute_rfm", "run_churn_model"], "retries": 2, "retry_delay_minutes": 5},
            {"task_id": "publish_to_gold", "type": "PythonOperator", "upstream": ["merge_customer_360"], "retries": 3, "retry_delay_minutes": 5},
        ],
    },
    {
        "dag_id": "data_quality_monitor",
        "name": "Data Quality Monitor",
        "description": "Run automated quality checks across all gold tables and alert on violations",
        "schedule": "0 */6 * * *",
        "schedule_human": "Every 6 hours",
        "owner": "data-quality@corp.com",
        "active": True,
        "tags": ["quality", "monitoring"],
        "last_run": (datetime.now() - timedelta(hours=4)).isoformat(),
        "next_run": (datetime.now() + timedelta(hours=2)).isoformat(),
        "avg_duration_minutes": 8.7,
        "success_rate_30d": 91.3,
        "tasks": [
            {"task_id": "check_null_rates", "type": "PythonOperator", "upstream": [], "retries": 2, "retry_delay_minutes": 3},
            {"task_id": "check_row_counts", "type": "PythonOperator", "upstream": [], "retries": 2, "retry_delay_minutes": 3},
            {"task_id": "check_freshness", "type": "PythonOperator", "upstream": [], "retries": 2, "retry_delay_minutes": 3},
            {"task_id": "check_referential_integrity", "type": "PythonOperator", "upstream": ["check_null_rates", "check_row_counts"], "retries": 1, "retry_delay_minutes": 3},
            {"task_id": "generate_quality_report", "type": "PythonOperator", "upstream": ["check_null_rates", "check_row_counts", "check_freshness", "check_referential_integrity"], "retries": 1, "retry_delay_minutes": 2},
            {"task_id": "send_quality_alert", "type": "SlackOperator", "upstream": ["generate_quality_report"], "retries": 1, "retry_delay_minutes": 1},
        ],
    },
]

RUN_HISTORY = []

def _generate_history():
    history = []
    statuses = ["success", "success", "success", "success", "failed", "running"]
    for dag in DAGS:
        for i in range(20):
            start = datetime.now() - timedelta(hours=random.randint(1, 168))
            dur = random.uniform(5, dag["avg_duration_minutes"] * 1.5) * 60
            status = random.choice(statuses)
            history.append({
                "run_id": f"RUN-{uuid.uuid4().hex[:8].upper()}",
                "dag_id": dag["dag_id"],
                "dag_name": dag["name"],
                "status": status,
                "started_at": start.isoformat(),
                "completed_at": (start + timedelta(seconds=dur)).isoformat() if status != "running" else None,
                "duration_seconds": round(dur) if status != "running" else None,
                "trigger": random.choice(["scheduled", "manual", "backfill"]),
                "run_number": i + 1,
            })
    return sorted(history, key=lambda x: x["started_at"], reverse=True)

RUN_HISTORY = _generate_history()


class TriggerRequest(BaseModel):
    dag_id: str
    trigger_reason: Optional[str] = "manual"


@router.get("/orchestration/dags")
def list_dags():
    return {"dags": DAGS, "total": len(DAGS)}


@router.get("/orchestration/dags/{dag_id}")
def get_dag(dag_id: str):
    dag = next((d for d in DAGS if d["dag_id"] == dag_id), None)
    if not dag:
        return {"error": "DAG not found"}, 404
    return dag


@router.post("/orchestration/dags/{dag_id}/trigger")
def trigger_dag(dag_id: str, body: TriggerRequest = None):
    dag = next((d for d in DAGS if d["dag_id"] == dag_id), None)
    if not dag:
        return {"error": "DAG not found"}, 404

    run_id = f"RUN-{uuid.uuid4().hex[:8].upper()}"
    new_run = {
        "run_id": run_id,
        "dag_id": dag_id,
        "dag_name": dag["name"],
        "status": "running",
        "started_at": datetime.now().isoformat(),
        "completed_at": None,
        "duration_seconds": None,
        "trigger": body.trigger_reason if body else "manual",
    }
    RUN_HISTORY.insert(0, new_run)
    return {"success": True, "run": new_run, "message": f"DAG '{dag['name']}' triggered successfully"}


@router.get("/orchestration/runs")
def get_run_history(dag_id: str = None, limit: int = 50):
    history = RUN_HISTORY
    if dag_id:
        history = [r for r in history if r["dag_id"] == dag_id]
    return {"runs": history[:limit], "total": len(history)}


@router.get("/orchestration/cron-preview")
def preview_cron(expression: str = "0 2 * * *"):
    explanations = {
        "0 2 * * *": "Every day at 2:00 AM",
        "0 */6 * * *": "Every 6 hours",
        "0 4 * * 1": "Every Monday at 4:00 AM",
        "*/15 * * * *": "Every 15 minutes",
        "0 0 * * *": "Every day at midnight",
        "0 0 1 * *": "First day of every month at midnight",
        "0 0 1 1 *": "January 1st at midnight (yearly)",
    }

    human_readable = explanations.get(expression, "Custom schedule")
    now = datetime.now()
    next_runs = [
        (now + timedelta(hours=i * 6)).strftime("%Y-%m-%d %H:%M:%S")
        for i in range(1, 6)
    ]

    return {
        "expression": expression,
        "human_readable": human_readable,
        "next_5_runs": next_runs,
        "valid": True,
    }
