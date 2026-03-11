from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
import random
from datetime import datetime, timedelta

router = APIRouter()


class AlertRule(BaseModel):
    rule_name: str
    metric: str
    operator: str
    threshold: float
    severity: str


ALERT_HISTORY = [
    {"id": "ALT-001", "rule": "Pipeline Failure Alert", "severity": "critical", "triggered_at": (datetime.now() - timedelta(hours=2)).isoformat(), "resolved": True, "resolved_at": (datetime.now() - timedelta(hours=1, minutes=45)).isoformat(), "detail": "ingest_orders failed 3 times consecutively"},
    {"id": "ALT-002", "rule": "High Null Rate", "severity": "warning", "triggered_at": (datetime.now() - timedelta(hours=5)).isoformat(), "resolved": True, "resolved_at": (datetime.now() - timedelta(hours=4)).isoformat(), "detail": "dim_customer.phone null rate reached 12%"},
    {"id": "ALT-003", "rule": "Freshness SLA Breach", "severity": "critical", "triggered_at": (datetime.now() - timedelta(hours=1)).isoformat(), "resolved": False, "resolved_at": None, "detail": "gold.daily_revenue not updated in 7 hours"},
    {"id": "ALT-004", "rule": "Low Data Volume", "severity": "warning", "triggered_at": (datetime.now() - timedelta(minutes=30)).isoformat(), "resolved": False, "resolved_at": None, "detail": "bronze.raw_orders ingested only 423 rows (threshold: 1000)"},
]

SLA_DEFINITIONS = [
    {"pipeline": "ingest_orders", "sla_hours": 1, "compliance_30d": 97.8, "breaches_30d": 2, "last_run": (datetime.now() - timedelta(minutes=45)).isoformat()},
    {"pipeline": "transform_customers", "sla_hours": 2, "compliance_30d": 99.5, "breaches_30d": 0, "last_run": (datetime.now() - timedelta(hours=1)).isoformat()},
    {"pipeline": "build_gold_layer", "sla_hours": 6, "compliance_30d": 95.2, "breaches_30d": 5, "last_run": (datetime.now() - timedelta(hours=5)).isoformat()},
    {"pipeline": "sync_catalog", "sla_hours": 12, "compliance_30d": 100.0, "breaches_30d": 0, "last_run": (datetime.now() - timedelta(hours=3)).isoformat()},
    {"pipeline": "run_quality_checks", "sla_hours": 4, "compliance_30d": 91.3, "breaches_30d": 8, "last_run": (datetime.now() - timedelta(hours=2)).isoformat()},
]


def _generate_time_series(hours: int = 24, base: float = 100000, noise: float = 20000):
    now = datetime.now()
    return [
        {
            "timestamp": (now - timedelta(hours=hours - i)).strftime("%H:%M"),
            "value": max(0, round(base + random.gauss(0, noise))),
        }
        for i in range(hours)
    ]


def _gen_success_rate_series(hours: int = 24):
    now = datetime.now()
    series = []
    rate = 0.97
    for i in range(hours):
        rate = max(0.80, min(1.0, rate + random.gauss(0, 0.02)))
        series.append({
            "timestamp": (now - timedelta(hours=hours - i)).strftime("%H:%M"),
            "value": round(rate * 100, 1),
        })
    return series


@router.get("/monitoring/metrics")
def get_metrics():
    success_rate = round(random.uniform(94, 99.5), 1)
    avg_duration = round(random.uniform(45, 180), 1)
    total_ingested = random.randint(800000, 1200000)
    active_pipelines = random.randint(3, 8)
    failed_last_24h = random.randint(0, 5)

    pipeline_failures = [
        {"pipeline": "ingest_orders", "failures": random.randint(0, 3)},
        {"pipeline": "transform_customers", "failures": random.randint(0, 2)},
        {"pipeline": "build_gold_layer", "failures": random.randint(0, 4)},
        {"pipeline": "sync_catalog", "failures": random.randint(0, 1)},
        {"pipeline": "run_quality_checks", "failures": random.randint(0, 2)},
    ]

    table_freshness = [
        {"table": "bronze.raw_orders", "last_updated": (datetime.now() - timedelta(minutes=random.randint(5, 90))).isoformat(), "sla_hours": 1},
        {"table": "silver.orders", "last_updated": (datetime.now() - timedelta(hours=random.randint(1, 4))).isoformat(), "sla_hours": 3},
        {"table": "gold.daily_revenue", "last_updated": (datetime.now() - timedelta(hours=random.randint(3, 8))).isoformat(), "sla_hours": 6},
        {"table": "gold.customer_360", "last_updated": (datetime.now() - timedelta(hours=random.randint(20, 30))).isoformat(), "sla_hours": 24},
        {"table": "dim_customer", "last_updated": (datetime.now() - timedelta(hours=random.randint(2, 6))).isoformat(), "sla_hours": 8},
    ]

    for t in table_freshness:
        delta_h = (datetime.now() - datetime.fromisoformat(t["last_updated"])).total_seconds() / 3600
        t["hours_since_update"] = round(delta_h, 1)
        t["status"] = "fresh" if delta_h < t["sla_hours"] * 0.7 else ("stale" if delta_h < t["sla_hours"] else "breached")

    return {
        "summary": {
            "pipeline_success_rate": success_rate,
            "avg_duration_seconds": avg_duration,
            "total_rows_ingested_24h": total_ingested,
            "active_pipelines": active_pipelines,
            "failed_tasks_24h": failed_last_24h,
            "open_alerts": 2,
        },
        "success_rate_trend": _gen_success_rate_series(24),
        "duration_trend": _generate_time_series(24, 120, 40),
        "volume_trend": [
            {
                "timestamp": (datetime.now() - timedelta(hours=24 - i)).strftime("%H:%M"),
                "rows": max(0, random.randint(20000, 80000)),
            }
            for i in range(24)
        ],
        "pipeline_failures": pipeline_failures,
        "table_freshness": table_freshness,
        "generated_at": datetime.now().isoformat(),
    }


@router.get("/monitoring/alerts")
def get_alert_rules():
    return {
        "rules": [
            {"id": "RULE-001", "rule_name": "Pipeline Failure Alert", "metric": "pipeline_failure_rate", "operator": ">", "threshold": 0.1, "severity": "critical", "enabled": True},
            {"id": "RULE-002", "rule_name": "High Null Rate", "metric": "null_rate", "operator": ">", "threshold": 0.05, "severity": "warning", "enabled": True},
            {"id": "RULE-003", "rule_name": "Low Data Volume", "metric": "row_count", "operator": "<", "threshold": 1000, "severity": "warning", "enabled": True},
            {"id": "RULE-004", "rule_name": "Long Pipeline Duration", "metric": "duration_seconds", "operator": ">", "threshold": 600, "severity": "info", "enabled": True},
            {"id": "RULE-005", "rule_name": "Freshness SLA Breach", "metric": "hours_since_update", "operator": ">", "threshold": 6, "severity": "critical", "enabled": True},
        ],
        "alert_history": ALERT_HISTORY,
    }


@router.post("/monitoring/alerts")
def create_alert_rule(body: AlertRule):
    new_rule = {
        "id": f"RULE-{random.randint(100, 999)}",
        "rule_name": body.rule_name,
        "metric": body.metric,
        "operator": body.operator,
        "threshold": body.threshold,
        "severity": body.severity,
        "enabled": True,
        "created_at": datetime.now().isoformat(),
    }
    return {"success": True, "rule": new_rule}


@router.get("/monitoring/sla")
def get_sla():
    return {"slas": SLA_DEFINITIONS}
