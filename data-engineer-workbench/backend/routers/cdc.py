from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import random
import uuid
import datetime

router = APIRouter()

# Module 15 — Change Data Capture (CDC)

_connectors: dict = {
    "conn-001": {
        "id": "conn-001", "name": "postgres-orders-source", "type": "debezium-postgres",
        "status": "RUNNING", "host": "prod-postgres-01.internal", "port": 5432,
        "database": "orders_db", "table_whitelist": "public.orders,public.customers",
        "snapshot_mode": "initial", "lsn": "0/1A2B3C4D",
        "events_per_second": 47, "lag_ms": 120, "errors": 0,
        "created_at": "2024-01-10T09:00:00Z",
    },
    "conn-002": {
        "id": "conn-002", "name": "mysql-inventory-source", "type": "debezium-mysql",
        "status": "RUNNING", "host": "prod-mysql-01.internal", "port": 3306,
        "database": "inventory_db", "table_whitelist": "inventory.products,inventory.stock",
        "snapshot_mode": "schema_only", "binlog_pos": "mysql-bin.000012:4891234",
        "events_per_second": 12, "lag_ms": 85, "errors": 0,
        "created_at": "2024-01-15T14:00:00Z",
    },
    "conn-003": {
        "id": "conn-003", "name": "oracle-payments-source", "type": "debezium-oracle",
        "status": "DEGRADED", "host": "prod-oracle-01.internal", "port": 1521,
        "database": "PAYDB", "table_whitelist": "FINANCE.PAYMENTS",
        "snapshot_mode": "initial_only", "redo_log_pos": "thread 1, sequence 198",
        "events_per_second": 3, "lag_ms": 8400, "errors": 2,
        "created_at": "2023-12-01T00:00:00Z",
    },
}

DEBEZIUM_EVENT_TEMPLATE = {
    "schema": {
        "type": "struct",
        "fields": [
            {"field": "before", "type": "struct", "optional": True},
            {"field": "after", "type": "struct", "optional": True},
            {"field": "source", "type": "struct"},
            {"field": "op", "type": "string"},
            {"field": "ts_ms", "type": "int64"},
        ],
    }
}

SAMPLE_EVENTS = [
    {
        "op": "c", "op_name": "INSERT", "topic": "postgres.public.orders",
        "ts_ms": 1706400000000, "lsn": "0/1A2B3C01",
        "before": None,
        "after": {"order_id": "ORD-9991", "customer_id": "CUST-142", "revenue": 249.99, "status": "pending", "created_at": "2024-01-27T12:00:01Z"},
        "source": {"db": "orders_db", "table": "orders", "connector": "postgresql", "snapshot": "false"},
    },
    {
        "op": "u", "op_name": "UPDATE", "topic": "postgres.public.orders",
        "ts_ms": 1706400060000, "lsn": "0/1A2B3C02",
        "before": {"order_id": "ORD-9980", "status": "pending"},
        "after": {"order_id": "ORD-9980", "customer_id": "CUST-88", "revenue": 149.99, "status": "shipped", "created_at": "2024-01-26T09:00:00Z"},
        "source": {"db": "orders_db", "table": "orders", "connector": "postgresql", "snapshot": "false"},
    },
    {
        "op": "d", "op_name": "DELETE", "topic": "postgres.public.orders",
        "ts_ms": 1706400120000, "lsn": "0/1A2B3C03",
        "before": {"order_id": "ORD-9970", "customer_id": "CUST-55", "revenue": 19.99, "status": "cancelled"},
        "after": None,
        "source": {"db": "orders_db", "table": "orders", "connector": "postgresql", "snapshot": "false"},
    },
    {
        "op": "r", "op_name": "READ (snapshot)", "topic": "postgres.public.customers",
        "ts_ms": 1706399900000, "lsn": "0/1A2B3B00",
        "before": None,
        "after": {"customer_id": "CUST-001", "full_name": "Alice Johnson", "email": "alice@example.com", "segment": "Premium"},
        "source": {"db": "orders_db", "table": "customers", "connector": "postgresql", "snapshot": "true"},
    },
]


@router.get("/cdc/connectors")
def list_connectors():
    return {"connectors": list(_connectors.values())}


class CreateConnectorRequest(BaseModel):
    name: str
    type: str
    host: str
    port: int
    database: str
    table_whitelist: str
    snapshot_mode: str = "initial"


@router.post("/cdc/connectors")
def create_connector(req: CreateConnectorRequest):
    cid = f"conn-{str(uuid.uuid4())[:8]}"
    connector = {
        "id": cid, "name": req.name, "type": req.type,
        "status": "STARTING", "host": req.host, "port": req.port,
        "database": req.database, "table_whitelist": req.table_whitelist,
        "snapshot_mode": req.snapshot_mode, "lsn": "0/0",
        "events_per_second": 0, "lag_ms": 0, "errors": 0,
        "created_at": datetime.datetime.utcnow().isoformat() + "Z",
    }
    _connectors[cid] = connector
    return connector


@router.get("/cdc/events")
def get_cdc_events(connector_id: Optional[str] = None, limit: int = 20):
    events = []
    r = random.Random(42)
    for i in range(min(limit, 20)):
        base = SAMPLE_EVENTS[i % len(SAMPLE_EVENTS)]
        event = {**base, "ts_ms": base["ts_ms"] + i * 60000, "lsn": f"0/1A2B{3000+i:04X}"}
        events.append(event)
    return {"events": events, "total": len(events)}


class SimulateCDCRequest(BaseModel):
    operation: str  # INSERT, UPDATE, DELETE
    table: str = "orders"
    record_id: str = "ORD-9999"


@router.post("/cdc/simulate")
def simulate_cdc(req: SimulateCDCRequest):
    op_map = {"INSERT": "c", "UPDATE": "u", "DELETE": "d"}
    op_code = op_map.get(req.operation.upper(), "c")
    before = None
    after = None

    if op_code == "c":
        after = {"order_id": req.record_id, "customer_id": "CUST-999", "revenue": round(random.uniform(20, 500), 2), "status": "pending"}
    elif op_code == "u":
        before = {"order_id": req.record_id, "status": "pending"}
        after = {"order_id": req.record_id, "customer_id": "CUST-999", "revenue": round(random.uniform(20, 500), 2), "status": "shipped"}
    elif op_code == "d":
        before = {"order_id": req.record_id, "customer_id": "CUST-999", "revenue": 149.99, "status": "cancelled"}

    event = {
        "op": op_code, "op_name": req.operation.upper(),
        "topic": f"postgres.public.{req.table}",
        "ts_ms": int(datetime.datetime.utcnow().timestamp() * 1000),
        "lsn": f"0/1A{random.randint(0, 0xFFFFFF):06X}",
        "before": before, "after": after,
        "source": {"db": "orders_db", "table": req.table, "connector": "postgresql", "snapshot": "false"},
        "latency_ms": random.randint(50, 300),
    }
    return {"event": event, "kafka_topic": f"cdc.orders_db.{req.table}", "serialization": "Avro (Schema Registry ID: 42)"}


@router.get("/cdc/comparison")
def cdc_vs_polling():
    return {
        "comparison": {
            "cdc": {
                "method": "CDC (Debezium + WAL/binlog)",
                "latency_ms": 150,
                "source_db_load": "Near-zero (reads WAL, not data tables)",
                "data_loss_risk": "None — every change captured atomically",
                "setup_complexity": "High — requires DB permissions, Kafka, schema registry",
                "data_captured": "INSERT + UPDATE + DELETE + schema changes",
                "cost": "Higher initial setup, lower ongoing DB impact",
                "pros": ["Real-time (<1s latency)", "No missed updates", "Captures deletes", "Minimal source DB impact"],
                "cons": ["Complex setup", "Requires Kafka", "Schema changes need coordination"],
            },
            "polling": {
                "method": "Polling (SELECT WHERE updated_at > last_run)",
                "latency_ms": 300000,
                "source_db_load": "HIGH — full table scans every 5 minutes",
                "data_loss_risk": "HIGH — deletes are invisible, rapid changes may be missed",
                "setup_complexity": "Low — simple SQL query",
                "data_captured": "Only rows with updated_at column — CANNOT capture deletes",
                "cost": "Low setup, high ongoing DB impact",
                "pros": ["Simple to implement", "No Kafka needed", "Easy to debug"],
                "cons": ["High latency (minutes)", "Cannot capture deletes", "Puts load on source DB", "Misses rapid updates"],
            },
        },
        "recommendation": "Use CDC for production systems requiring <1min latency or where deletes matter. Use polling for simple batch pipelines with daily schedules.",
    }


@router.get("/cdc/wal-explainer")
def wal_explainer():
    return {
        "title": "How the Write-Ahead Log (WAL) Powers CDC",
        "explanation": "Every database change is written to the WAL BEFORE it's applied to data files. This guarantees durability. Debezium taps into this log stream — reading changes in order, without touching the actual data tables.",
        "databases": {
            "PostgreSQL": {"log_name": "WAL (Write-Ahead Log)", "debezium_method": "Logical Replication Slot (pgoutput or decoderbufs plugin)", "permission": "REPLICATION role required"},
            "MySQL": {"log_name": "binlog (binary log)", "debezium_method": "Reads binlog via MySQL replication protocol", "permission": "REPLICATION SLAVE + REPLICATION CLIENT grants"},
            "Oracle": {"log_name": "Redo Log", "debezium_method": "LogMiner utility", "permission": "LogMiner privileges + supplemental logging enabled"},
            "SQL Server": {"log_name": "Transaction Log", "debezium_method": "CDC tables (sys.fn_cdc_get_all_changes)", "permission": "CDC enabled per table, sysadmin or db_owner"},
        },
        "lsn_explained": "Log Sequence Number (LSN) is a monotonically increasing pointer into the WAL. Debezium stores the last processed LSN as a checkpoint. On restart, it resumes from that LSN — guaranteeing no events are missed.",
    }
