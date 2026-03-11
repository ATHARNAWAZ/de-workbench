from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, Any
import random
from datetime import datetime, timedelta
import sqlite3

router = APIRouter()
DB_PATH = "workbench.db"


class AdhocQueryRequest(BaseModel):
    table: str
    columns: list[str]
    aggregation: Optional[str] = None
    group_by: Optional[str] = None
    filters: Optional[list[dict[str, Any]]] = None
    limit: int = 100


class ReportSchedule(BaseModel):
    name: str
    description: Optional[str] = ""
    schedule: str
    delivery: str
    recipients: list[str]
    query: str


SCHEDULED_REPORTS = [
    {
        "id": "rpt-001",
        "name": "Executive Revenue Dashboard",
        "description": "Daily GMV, order count, and top categories sent to leadership",
        "schedule": "0 8 * * 1-5",
        "schedule_human": "Weekdays at 8 AM",
        "delivery": "email",
        "recipients": ["ceo@corp.com", "cfo@corp.com", "vp-data@corp.com"],
        "last_sent": (datetime.now() - timedelta(hours=8)).isoformat(),
        "status": "active",
    },
    {
        "id": "rpt-002",
        "name": "Weekly Data Quality Report",
        "description": "Quality scores and issue summary for all gold tables",
        "schedule": "0 9 * * 1",
        "schedule_human": "Mondays at 9 AM",
        "delivery": "slack",
        "recipients": ["#data-engineering"],
        "last_sent": (datetime.now() - timedelta(days=3)).isoformat(),
        "status": "active",
    },
    {
        "id": "rpt-003",
        "name": "Monthly Finance Reconciliation",
        "description": "Revenue reconciliation report for finance team",
        "schedule": "0 6 1 * *",
        "schedule_human": "1st of every month at 6 AM",
        "delivery": "email",
        "recipients": ["finance@corp.com"],
        "last_sent": (datetime.now() - timedelta(days=15)).isoformat(),
        "status": "active",
    },
]


def _gen_revenue_trend(days: int = 30):
    base = 150000
    result = []
    for i in range(days):
        d = datetime.now() - timedelta(days=days - i)
        weekday_mult = 0.7 if d.weekday() >= 5 else 1.0
        trend = 1 + (i / days) * 0.15
        noise = random.gauss(0, 10000)
        result.append({
            "date": d.strftime("%Y-%m-%d"),
            "revenue": max(0, round((base * weekday_mult * trend) + noise, 2)),
            "orders": random.randint(800, 2500),
        })
    return result


def _gen_category_breakdown():
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books"]
    base_pct = [35, 25, 20, 12, 8]
    result = []
    for cat, pct in zip(categories, base_pct):
        revenue = round(pct * 15000 + random.gauss(0, 5000), 2)
        result.append({
            "category": cat,
            "revenue": max(0, revenue),
            "pct": pct,
            "orders": random.randint(200, 2000),
            "growth_pct": round(random.uniform(-5, 25), 1),
        })
    return result


@router.get("/reporting/kpis")
def get_kpis():
    gmv = round(random.uniform(4_500_000, 5_200_000), 2)
    prev_gmv = gmv * random.uniform(0.88, 0.97)
    dau = random.randint(28000, 35000)
    prev_dau = int(dau * random.uniform(0.92, 0.98))
    orders = random.randint(15000, 22000)
    aov = round(gmv / orders, 2)

    return {
        "kpis": {
            "gmv": {"value": gmv, "previous": round(prev_gmv, 2), "change_pct": round((gmv - prev_gmv) / prev_gmv * 100, 1), "label": "Gross Merchandise Value (30d)"},
            "dau": {"value": dau, "previous": prev_dau, "change_pct": round((dau - prev_dau) / prev_dau * 100, 1), "label": "Daily Active Users"},
            "orders": {"value": orders, "previous": int(orders * 0.94), "change_pct": round(random.uniform(2, 8), 1), "label": "Orders (30d)"},
            "aov": {"value": aov, "previous": round(aov * 0.97, 2), "change_pct": round(random.uniform(1, 5), 1), "label": "Average Order Value"},
            "pipeline_health": {"value": 97.8, "previous": 96.2, "change_pct": 1.6, "label": "Pipeline Success Rate (%)"},
            "data_freshness": {"value": 98.5, "previous": 97.1, "change_pct": 1.4, "label": "Data Freshness Score (%)"},
        },
        "revenue_trend": _gen_revenue_trend(30),
        "category_breakdown": _gen_category_breakdown(),
        "region_breakdown": [
            {"region": "North America", "revenue": round(random.uniform(2000000, 2500000), 2), "pct": 45},
            {"region": "Europe", "revenue": round(random.uniform(1200000, 1600000), 2), "pct": 28},
            {"region": "Asia Pacific", "revenue": round(random.uniform(700000, 1000000), 2), "pct": 18},
            {"region": "Latin America", "revenue": round(random.uniform(300000, 500000), 2), "pct": 9},
        ],
        "generated_at": datetime.now().isoformat(),
    }


@router.post("/reporting/adhoc")
def run_adhoc_query(body: AdhocQueryRequest):
    ALLOWED_TABLES = {
        "fact_orders": "fact_orders",
        "dim_customer": "dim_customer",
        "dim_product": "dim_product",
        "gold_daily_revenue": "gold_daily_revenue",
        "pipeline_runs": "pipeline_runs",
    }

    if body.table not in ALLOWED_TABLES:
        return {"error": f"Table '{body.table}' not available for ad-hoc queries"}

    safe_cols = [c for c in body.columns if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', c)]
    if not safe_cols:
        safe_cols = ["*"]

    if body.aggregation and body.group_by and re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', body.group_by):
        agg_col = safe_cols[0] if safe_cols != ["*"] else "revenue"
        col_str = f"{body.group_by}, {body.aggregation}({agg_col}) as {body.aggregation.lower()}_{agg_col}"
        sql = f"SELECT {col_str} FROM {ALLOWED_TABLES[body.table]} GROUP BY {body.group_by} LIMIT {min(body.limit, 500)}"
    else:
        col_str = ", ".join(safe_cols) if safe_cols != ["*"] else "*"
        sql = f"SELECT {col_str} FROM {ALLOWED_TABLES[body.table]} LIMIT {min(body.limit, 500)}"

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        import time
        start = time.time()
        cur.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
        elapsed = round((time.time() - start) * 1000, 2)
        columns = [d[0] for d in cur.description] if cur.description else []
        conn.close()

        return {
            "sql": sql,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "execution_time_ms": elapsed,
            "status": "success",
        }
    except Exception as e:
        return {"error": str(e), "sql": sql}


@router.get("/reporting/reports")
def list_reports():
    return {"reports": SCHEDULED_REPORTS, "total": len(SCHEDULED_REPORTS)}


@router.post("/reporting/reports")
def create_report(body: ReportSchedule):
    new_report = {
        "id": f"rpt-{random.randint(100, 999)}",
        "name": body.name,
        "description": body.description,
        "schedule": body.schedule,
        "delivery": body.delivery,
        "recipients": body.recipients,
        "last_sent": None,
        "status": "active",
        "created_at": datetime.now().isoformat(),
    }
    SCHEDULED_REPORTS.append(new_report)
    return {"success": True, "report": new_report}


@router.get("/reporting/available-tables")
def get_available_tables():
    return {
        "tables": [
            {"name": "fact_orders", "description": "Order transactions", "columns": ["order_id", "customer_id", "product_id", "quantity", "unit_price", "revenue", "status", "channel", "region"]},
            {"name": "dim_customer", "description": "Customer dimension", "columns": ["customer_id", "first_name", "last_name", "city", "country", "segment", "is_active"]},
            {"name": "dim_product", "description": "Product dimension", "columns": ["product_id", "product_name", "category", "subcategory", "brand", "unit_price", "cost"]},
            {"name": "gold_daily_revenue", "description": "Pre-aggregated daily revenue", "columns": ["date", "category", "region", "total_revenue", "order_count", "avg_order_value"]},
            {"name": "pipeline_runs", "description": "Pipeline execution history", "columns": ["run_id", "pipeline_name", "status", "started_at", "completed_at", "duration_seconds", "rows_processed"]},
        ]
    }


import re
