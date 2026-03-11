from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timedelta
import random
import uuid
from faker import Faker

fake = Faker()
router = APIRouter()

SOURCES = [
    {
        "id": "src-001",
        "name": "Orders PostgreSQL",
        "type": "database",
        "host": "prod-db-01.internal",
        "port": 5432,
        "database": "orders_prod",
        "status": "connected",
        "last_ingested": (datetime.now() - timedelta(minutes=30)).isoformat(),
        "row_count": 2_450_000,
        "schema": [
            {"column": "order_id", "type": "VARCHAR(50)", "nullable": False},
            {"column": "customer_id", "type": "VARCHAR(50)", "nullable": False},
            {"column": "product_id", "type": "VARCHAR(50)", "nullable": False},
            {"column": "order_date", "type": "TIMESTAMP", "nullable": False},
            {"column": "quantity", "type": "INTEGER", "nullable": False},
            {"column": "unit_price", "type": "DECIMAL(10,2)", "nullable": False},
            {"column": "status", "type": "VARCHAR(20)", "nullable": True},
        ],
        "description": "Primary transactional orders database. Contains all e-commerce orders since 2019.",
    },
    {
        "id": "src-002",
        "name": "User Events Kafka",
        "type": "stream",
        "host": "kafka-broker-01:9092",
        "port": 9092,
        "database": "user-events-topic",
        "status": "streaming",
        "last_ingested": datetime.now().isoformat(),
        "row_count": 15_800_000,
        "schema": [
            {"column": "event_id", "type": "STRING", "nullable": False},
            {"column": "user_id", "type": "STRING", "nullable": False},
            {"column": "event_type", "type": "STRING", "nullable": False},
            {"column": "timestamp", "type": "LONG", "nullable": False},
            {"column": "properties", "type": "MAP<STRING,STRING>", "nullable": True},
            {"column": "session_id", "type": "STRING", "nullable": True},
        ],
        "description": "Real-time clickstream events from web and mobile apps via Kafka.",
    },
    {
        "id": "src-003",
        "name": "Inventory REST API",
        "type": "api",
        "host": "api.inventory.internal",
        "port": 443,
        "database": "/v2/inventory",
        "status": "connected",
        "last_ingested": (datetime.now() - timedelta(hours=2)).isoformat(),
        "row_count": 125_000,
        "schema": [
            {"column": "sku", "type": "STRING", "nullable": False},
            {"column": "warehouse_id", "type": "STRING", "nullable": False},
            {"column": "quantity_on_hand", "type": "INTEGER", "nullable": False},
            {"column": "quantity_reserved", "type": "INTEGER", "nullable": False},
            {"column": "last_updated", "type": "ISO8601", "nullable": False},
        ],
        "description": "Inventory management system REST API. Paginated endpoint, rate-limited to 1000 req/min.",
    },
    {
        "id": "src-004",
        "name": "Finance CSV Drop",
        "type": "csv",
        "host": "s3://finance-data-lake",
        "port": None,
        "database": "/raw/finance/",
        "status": "idle",
        "last_ingested": (datetime.now() - timedelta(days=1)).isoformat(),
        "row_count": 48_200,
        "schema": [
            {"column": "transaction_id", "type": "STRING", "nullable": False},
            {"column": "account_id", "type": "STRING", "nullable": False},
            {"column": "amount", "type": "FLOAT", "nullable": False},
            {"column": "currency", "type": "STRING", "nullable": False},
            {"column": "transaction_date", "type": "DATE", "nullable": False},
            {"column": "category", "type": "STRING", "nullable": True},
        ],
        "description": "Daily finance exports dropped to S3 by the finance team. Processed every midnight.",
    },
    {
        "id": "src-005",
        "name": "Sensor Webhook",
        "type": "webhook",
        "host": "0.0.0.0",
        "port": 8001,
        "database": "/webhook/sensors",
        "status": "listening",
        "last_ingested": (datetime.now() - timedelta(minutes=5)).isoformat(),
        "row_count": 3_200_000,
        "schema": [
            {"column": "sensor_id", "type": "STRING", "nullable": False},
            {"column": "temperature", "type": "FLOAT", "nullable": True},
            {"column": "humidity", "type": "FLOAT", "nullable": True},
            {"column": "pressure", "type": "FLOAT", "nullable": True},
            {"column": "timestamp", "type": "EPOCH_MS", "nullable": False},
            {"column": "location", "type": "STRING", "nullable": False},
        ],
        "description": "IoT sensor readings pushed via webhook from 500+ factory floor sensors.",
    },
]

INGESTION_LOGS: dict = {}


class SourceCreate(BaseModel):
    name: str
    type: str
    host: str
    port: Optional[int] = None
    database: str
    description: Optional[str] = ""


@router.get("/sources")
def list_sources():
    return {"sources": SOURCES, "total": len(SOURCES)}


@router.post("/sources")
def create_source(body: SourceCreate):
    new_source = {
        "id": f"src-{uuid.uuid4().hex[:6]}",
        "name": body.name,
        "type": body.type,
        "host": body.host,
        "port": body.port,
        "database": body.database,
        "status": "pending",
        "last_ingested": None,
        "row_count": 0,
        "schema": [],
        "description": body.description,
    }
    SOURCES.append(new_source)
    return new_source


@router.post("/sources/{source_id}/test")
def test_connection(source_id: str):
    source = next((s for s in SOURCES if s["id"] == source_id), None)
    if not source:
        raise HTTPException(404, "Source not found")
    success = random.random() > 0.1
    latency = round(random.uniform(12, 250), 1)
    return {
        "success": success,
        "latency_ms": latency,
        "message": f"Connection successful in {latency}ms" if success else "Connection refused: timeout after 30s",
        "server_version": "PostgreSQL 15.2" if source["type"] == "database" else "OK",
        "timestamp": datetime.now().isoformat(),
    }


@router.post("/ingest/{source_id}")
def trigger_ingestion(source_id: str):
    source = next((s for s in SOURCES if s["id"] == source_id), None)
    if not source:
        raise HTTPException(404, "Source not found")

    batch_id = f"BATCH-{uuid.uuid4().hex[:8].upper()}"
    rows = random.randint(1000, 50000)
    duration = round(random.uniform(2.1, 45.8), 1)
    errors = random.randint(0, 5)

    logs = [
        {"ts": (datetime.now()).isoformat(), "level": "INFO", "msg": f"Starting ingestion batch {batch_id}"},
        {"ts": (datetime.now()).isoformat(), "level": "INFO", "msg": f"Connecting to {source['host']}"},
        {"ts": (datetime.now()).isoformat(), "level": "INFO", "msg": "Connection established"},
        {"ts": (datetime.now()).isoformat(), "level": "INFO", "msg": f"Reading data in chunks of 10,000 rows"},
        {"ts": (datetime.now()).isoformat(), "level": "INFO", "msg": f"Chunk 1/5: 10,000 rows extracted"},
        {"ts": (datetime.now()).isoformat(), "level": "INFO", "msg": f"Chunk 2/5: 10,000 rows extracted"},
        {"ts": (datetime.now()).isoformat(), "level": "INFO", "msg": f"Chunk 3/5: 10,000 rows extracted"},
        {"ts": (datetime.now()).isoformat(), "level": "INFO", "msg": f"Writing to bronze layer..."},
        {"ts": (datetime.now()).isoformat(), "level": "INFO", "msg": f"Committed {rows:,} rows to bronze.raw_{source['name'].lower().replace(' ', '_')}"},
        {"ts": (datetime.now()).isoformat(), "level": "INFO" if errors == 0 else "WARN",
         "msg": f"Ingestion complete. {rows:,} rows, {errors} parse errors, {duration}s elapsed"},
    ]

    INGESTION_LOGS[source_id] = logs

    for s in SOURCES:
        if s["id"] == source_id:
            s["status"] = "connected"
            s["last_ingested"] = datetime.now().isoformat()
            s["row_count"] = s["row_count"] + rows

    return {
        "batch_id": batch_id,
        "source_id": source_id,
        "status": "success",
        "rows_ingested": rows,
        "duration_seconds": duration,
        "parse_errors": errors,
        "destination": f"bronze.raw_{source['name'].lower().replace(' ', '_')}",
        "logs": logs,
    }


@router.get("/ingest/{source_id}/logs")
def get_ingestion_logs(source_id: str):
    logs = INGESTION_LOGS.get(source_id, [])
    return {"source_id": source_id, "logs": logs}
