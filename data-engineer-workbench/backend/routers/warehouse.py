from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import sqlite3
import random
from datetime import datetime, timedelta

router = APIRouter()

DB_PATH = "workbench.db"

WAREHOUSE_TABLES = {
    "bronze": [
        {
            "id": "bronze_raw_orders",
            "name": "raw_orders",
            "full_name": "bronze.raw_orders",
            "layer": "bronze",
            "row_count": 2_450_000,
            "size_mb": 342.5,
            "partitioned_by": "ingested_date",
            "partition_count": 730,
            "last_updated": (datetime.now() - timedelta(hours=1)).isoformat(),
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False, "description": "Auto-increment surrogate key"},
                {"name": "raw_json", "type": "TEXT", "nullable": False, "description": "Raw ingested JSON payload"},
                {"name": "source", "type": "VARCHAR(50)", "nullable": False, "description": "Ingestion source identifier"},
                {"name": "ingested_at", "type": "TIMESTAMP", "nullable": False, "description": "UTC ingestion timestamp"},
                {"name": "batch_id", "type": "VARCHAR(20)", "nullable": False, "description": "Ingestion batch identifier"},
            ],
            "upstream": [],
            "downstream": ["silver.orders"],
        },
        {
            "id": "bronze_raw_events",
            "name": "raw_events",
            "full_name": "bronze.raw_events",
            "layer": "bronze",
            "row_count": 15_800_000,
            "size_mb": 1240.8,
            "partitioned_by": "event_date",
            "partition_count": 365,
            "last_updated": datetime.now().isoformat(),
            "columns": [
                {"name": "event_id", "type": "VARCHAR(36)", "nullable": False, "description": "UUID event identifier"},
                {"name": "user_id", "type": "VARCHAR(50)", "nullable": False, "description": "User identifier"},
                {"name": "event_type", "type": "VARCHAR(100)", "nullable": False, "description": "Event name"},
                {"name": "properties", "type": "TEXT", "nullable": True, "description": "JSON event properties"},
                {"name": "received_at", "type": "TIMESTAMP", "nullable": False, "description": "Server receipt timestamp"},
            ],
            "upstream": [],
            "downstream": ["silver.user_sessions"],
        },
    ],
    "silver": [
        {
            "id": "silver_orders",
            "name": "orders",
            "full_name": "silver.orders",
            "layer": "silver",
            "row_count": 2_380_000,
            "size_mb": 215.3,
            "partitioned_by": "order_date",
            "partition_count": 730,
            "last_updated": (datetime.now() - timedelta(hours=2)).isoformat(),
            "columns": [
                {"name": "order_id", "type": "VARCHAR(50)", "nullable": False, "description": "Business order identifier"},
                {"name": "customer_id", "type": "VARCHAR(50)", "nullable": False, "description": "FK to dim_customer"},
                {"name": "product_id", "type": "VARCHAR(50)", "nullable": False, "description": "FK to dim_product"},
                {"name": "order_date", "type": "DATE", "nullable": False, "description": "Order placement date"},
                {"name": "quantity", "type": "INTEGER", "nullable": False, "description": "Units ordered"},
                {"name": "unit_price", "type": "DECIMAL(10,2)", "nullable": False, "description": "Price per unit at time of order"},
                {"name": "revenue", "type": "DECIMAL(12,2)", "nullable": False, "description": "Calculated net revenue"},
                {"name": "status", "type": "VARCHAR(20)", "nullable": False, "description": "Order lifecycle status"},
                {"name": "validated_at", "type": "TIMESTAMP", "nullable": False, "description": "Quality check timestamp"},
            ],
            "upstream": ["bronze.raw_orders"],
            "downstream": ["gold.fact_orders"],
        },
    ],
    "gold": [
        {
            "id": "gold_daily_revenue",
            "name": "daily_revenue",
            "full_name": "gold.daily_revenue",
            "layer": "gold",
            "row_count": 125_400,
            "size_mb": 8.7,
            "partitioned_by": "date",
            "partition_count": 730,
            "last_updated": (datetime.now() - timedelta(hours=4)).isoformat(),
            "columns": [
                {"name": "date", "type": "DATE", "nullable": False, "description": "Revenue aggregation date"},
                {"name": "category", "type": "VARCHAR(50)", "nullable": False, "description": "Product category"},
                {"name": "region", "type": "VARCHAR(50)", "nullable": False, "description": "Geographic region"},
                {"name": "total_revenue", "type": "DECIMAL(14,2)", "nullable": False, "description": "Sum of net revenue"},
                {"name": "order_count", "type": "INTEGER", "nullable": False, "description": "Number of orders"},
                {"name": "avg_order_value", "type": "DECIMAL(10,2)", "nullable": False, "description": "AOV = revenue / order_count"},
                {"name": "updated_at", "type": "TIMESTAMP", "nullable": False, "description": "Last pipeline run timestamp"},
            ],
            "upstream": ["silver.orders"],
            "downstream": ["reporting.executive_dashboard"],
        },
        {
            "id": "gold_customer_360",
            "name": "customer_360",
            "full_name": "gold.customer_360",
            "layer": "gold",
            "row_count": 98_000,
            "size_mb": 45.2,
            "partitioned_by": None,
            "partition_count": 1,
            "last_updated": (datetime.now() - timedelta(days=1)).isoformat(),
            "columns": [
                {"name": "customer_id", "type": "VARCHAR(50)", "nullable": False, "description": "PK customer identifier"},
                {"name": "total_orders", "type": "INTEGER", "nullable": False, "description": "Lifetime order count"},
                {"name": "total_revenue", "type": "DECIMAL(12,2)", "nullable": False, "description": "Lifetime revenue"},
                {"name": "avg_order_value", "type": "DECIMAL(10,2)", "nullable": False, "description": "Customer AOV"},
                {"name": "last_order_date", "type": "DATE", "nullable": True, "description": "Most recent order date"},
                {"name": "customer_segment", "type": "VARCHAR(30)", "nullable": False, "description": "ML-derived segment"},
                {"name": "churn_risk_score", "type": "FLOAT", "nullable": True, "description": "ML churn probability 0-1"},
            ],
            "upstream": ["silver.orders", "silver.user_sessions"],
            "downstream": ["reporting.customer_dashboard", "ml.churn_model"],
        },
    ],
}


class QueryRequest(BaseModel):
    sql: str


@router.get("/warehouse/tables")
def list_warehouse_tables(layer: str = None):
    all_tables = []
    for layer_name, tables in WAREHOUSE_TABLES.items():
        if layer and layer_name != layer:
            continue
        all_tables.extend(tables)
    return {"tables": all_tables, "total": len(all_tables)}


@router.get("/warehouse/table/{table_id}")
def get_table(table_id: str):
    for tables in WAREHOUSE_TABLES.values():
        for t in tables:
            if t["id"] == table_id:
                return t
    raise HTTPException(404, "Table not found")


@router.post("/warehouse/query")
def execute_query(body: QueryRequest):
    sql = body.sql.strip()

    forbidden = ["DROP", "DELETE", "TRUNCATE", "ALTER", "CREATE", "INSERT", "UPDATE"]
    upper = sql.upper()
    for kw in forbidden:
        if kw in upper:
            raise HTTPException(400, f"Only SELECT queries are allowed (blocked keyword: {kw})")

    allowed_tables = {
        "fact_orders": "fact_orders",
        "dim_customer": "dim_customer",
        "dim_product": "dim_product",
        "dim_date": "dim_date",
        "gold_daily_revenue": "gold_daily_revenue",
        "bronze_raw_orders": "bronze_raw_orders",
        "silver_orders": "silver_orders",
        "pipeline_runs": "pipeline_runs",
    }

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        start = __import__("time").time()
        cur.execute(sql)
        rows = [dict(r) for r in cur.fetchmany(500)]
        elapsed = round((__import__("time").time() - start) * 1000, 2)
        columns = [d[0] for d in cur.description] if cur.description else []
        conn.close()

        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "execution_time_ms": elapsed,
            "query_plan": [
                f"SCAN TABLE {_extract_table(sql)}",
                "FILTER (WHERE clause applied)",
                f"RETURN {len(rows)} rows",
            ],
            "status": "success",
        }
    except Exception as e:
        raise HTTPException(400, str(e))


@router.get("/warehouse/lineage/{table_id}")
def get_lineage(table_id: str):
    all_tables = []
    for tables in WAREHOUSE_TABLES.values():
        all_tables.extend(tables)

    table = next((t for t in all_tables if t["id"] == table_id), None)
    if not table:
        raise HTTPException(404, "Table not found")

    nodes = []
    edges = []
    visited = set()

    def add_table(tbl_name: str, depth: int):
        if tbl_name in visited or depth > 3:
            return
        visited.add(tbl_name)
        found = next((t for t in all_tables if t["full_name"] == tbl_name), None)
        if found:
            nodes.append({"id": found["id"], "label": found["full_name"], "layer": found["layer"], "row_count": found["row_count"]})
            for upstream in found.get("upstream", []):
                edges.append({"from": upstream.replace(".", "_"), "to": found["id"]})
                add_table(upstream, depth + 1)
            for downstream in found.get("downstream", []):
                edges.append({"from": found["id"], "to": downstream.replace(".", "_")})
                ext_id = downstream.replace(".", "_")
                if ext_id not in visited:
                    visited.add(ext_id)
                    nodes.append({"id": ext_id, "label": downstream, "layer": "external", "row_count": None})

    add_table(table["full_name"], 0)
    return {"table_id": table_id, "nodes": nodes, "edges": edges}


def _extract_table(sql: str) -> str:
    import re
    match = re.search(r'FROM\s+(\w+)', sql, re.IGNORECASE)
    return match.group(1) if match else "unknown"
