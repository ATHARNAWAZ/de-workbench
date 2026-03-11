from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import json
import sqlite3
import os

from routers import (
    sources, quality, pipeline, warehouse, monitoring, catalog,
    governance, orchestration, reporting, bigdata,
    cicd, contracts, cdc, reversetl, featurestore, cloudservices,
    iac, incidents, datamesh, olap, nosql, versioning, capacity, leadership,
)
from services.fake_data import initialize_database

DB_PATH = "workbench.db"


@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_database(DB_PATH)
    yield


app = FastAPI(
    title="Senior Data Engineer Workbench API",
    description="A comprehensive data engineering simulation platform",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(sources.router, prefix="/api", tags=["sources"])
app.include_router(quality.router, prefix="/api", tags=["quality"])
app.include_router(pipeline.router, prefix="/api", tags=["pipeline"])
app.include_router(warehouse.router, prefix="/api", tags=["warehouse"])
app.include_router(monitoring.router, prefix="/api", tags=["monitoring"])
app.include_router(catalog.router, prefix="/api", tags=["catalog"])
app.include_router(governance.router, prefix="/api", tags=["governance"])
app.include_router(orchestration.router, prefix="/api", tags=["orchestration"])
app.include_router(reporting.router, prefix="/api", tags=["reporting"])
app.include_router(bigdata.router, prefix="/api", tags=["bigdata"])
app.include_router(cicd.router, prefix="/api", tags=["cicd"])
app.include_router(contracts.router, prefix="/api", tags=["contracts"])
app.include_router(cdc.router, prefix="/api", tags=["cdc"])
app.include_router(reversetl.router, prefix="/api", tags=["reversetl"])
app.include_router(featurestore.router, prefix="/api", tags=["featurestore"])
app.include_router(cloudservices.router, prefix="/api", tags=["cloud"])
app.include_router(iac.router, prefix="/api", tags=["iac"])
app.include_router(incidents.router, prefix="/api", tags=["incidents"])
app.include_router(datamesh.router, prefix="/api", tags=["datamesh"])
app.include_router(olap.router, prefix="/api", tags=["olap"])
app.include_router(nosql.router, prefix="/api", tags=["nosql"])
app.include_router(versioning.router, prefix="/api", tags=["versioning"])
app.include_router(capacity.router, prefix="/api", tags=["capacity"])
app.include_router(leadership.router, prefix="/api", tags=["leadership"])


@app.get("/health")
def health():
    return {"status": "healthy", "version": "1.0.0"}


active_ws_connections: list[WebSocket] = []


@app.websocket("/ws/pipeline-logs")
async def pipeline_logs_ws(websocket: WebSocket):
    await websocket.accept()
    active_ws_connections.append(websocket)
    try:
        steps = [
            {"step": "extract", "message": "Connecting to source database...", "status": "running"},
            {"step": "extract", "message": "Extracting 50,000 rows from orders table", "status": "running"},
            {"step": "extract", "message": "Extraction complete: 50,000 rows in 1.2s", "status": "success"},
            {"step": "validate", "message": "Running schema validation...", "status": "running"},
            {"step": "validate", "message": "Checking null constraints on 12 columns", "status": "running"},
            {"step": "validate", "message": "Validation passed: 0 constraint violations", "status": "success"},
            {"step": "transform", "message": "Applying filters: status IN ('completed', 'shipped')", "status": "running"},
            {"step": "transform", "message": "Joining with dim_customer (LEFT JOIN on customer_id)", "status": "running"},
            {"step": "transform", "message": "Aggregating revenue by product_category and date", "status": "running"},
            {"step": "transform", "message": "Transformation complete: 50,000 → 12,847 rows", "status": "success"},
            {"step": "load", "message": "Writing to gold.fact_daily_revenue...", "status": "running"},
            {"step": "load", "message": "Creating partition for 2024-01-15", "status": "running"},
            {"step": "load", "message": "Load complete: 12,847 rows written in 0.8s", "status": "success"},
            {"step": "done", "message": "Pipeline completed successfully. Total duration: 3.4s", "status": "done"},
        ]
        for step in steps:
            await websocket.send_text(json.dumps(step))
            await asyncio.sleep(0.7)
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in active_ws_connections:
            active_ws_connections.remove(websocket)


@app.websocket("/ws/dag-run")
async def dag_run_ws(websocket: WebSocket):
    await websocket.accept()
    try:
        tasks = [
            {"task_id": "extract_orders", "status": "running", "message": "Extracting orders from source DB"},
            {"task_id": "extract_customers", "status": "running", "message": "Extracting customer records"},
            {"task_id": "extract_orders", "status": "success", "message": "Extracted 45,231 order records"},
            {"task_id": "extract_customers", "status": "success", "message": "Extracted 8,412 customer records"},
            {"task_id": "validate_orders", "status": "running", "message": "Running Great Expectations suite"},
            {"task_id": "validate_orders", "status": "success", "message": "98.7% rows pass validation"},
            {"task_id": "transform_orders", "status": "running", "message": "Applying business logic transformations"},
            {"task_id": "enrich_customers", "status": "running", "message": "Joining with marketing segments"},
            {"task_id": "transform_orders", "status": "success", "message": "Transformed to silver layer"},
            {"task_id": "enrich_customers", "status": "success", "message": "Customer enrichment complete"},
            {"task_id": "build_fact_table", "status": "running", "message": "Building fact_orders table"},
            {"task_id": "build_fact_table", "status": "success", "message": "Loaded 45,231 rows to gold layer"},
            {"task_id": "update_catalog", "status": "running", "message": "Updating data catalog metadata"},
            {"task_id": "send_alert", "status": "running", "message": "Sending success notification"},
            {"task_id": "update_catalog", "status": "success", "message": "Catalog metadata refreshed"},
            {"task_id": "send_alert", "status": "success", "message": "Slack notification sent"},
            {"task_id": "__done__", "status": "done", "message": "DAG run completed successfully in 4m 12s"},
        ]
        for task in tasks:
            await websocket.send_text(json.dumps(task))
            await asyncio.sleep(0.8)
    except WebSocketDisconnect:
        pass


@app.websocket("/ws/kafka-stream")
async def kafka_stream_ws(websocket: WebSocket):
    await websocket.accept()
    try:
        import random as _r
        import datetime as _dt
        topics = ["orders", "user-events", "sensor-readings", "payment-events"]
        topic_gens = {
            "orders": lambda: {"order_id": f"ORD-{_r.randint(1000,9999)}", "revenue": round(_r.uniform(20,500),2), "region": _r.choice(["West","East","South","Midwest"]), "status": _r.choice(["pending","completed","shipped"])},
            "user-events": lambda: {"event_type": _r.choice(["page_view","click","purchase","search"]), "user_id": f"USER-{_r.randint(1000,9999)}", "page": _r.choice(["/home","/product","/cart","/checkout"])},
            "sensor-readings": lambda: {"sensor_id": f"SENSOR-{_r.randint(1,20):03d}", "temperature": round(_r.uniform(18,40),1), "alert": _r.choice(["OK","OK","OK","WARNING","CRITICAL"])},
            "payment-events": lambda: {"payment_id": f"PAY-{_r.randint(10000,99999)}", "amount": round(_r.uniform(10,1000),2), "status": _r.choice(["authorized","captured","failed"])},
        }
        offset = _r.randint(10000, 50000)
        while True:
            topic = _r.choice(topics)
            msg = {
                "topic": topic,
                "partition": _r.randint(0, 5),
                "offset": offset,
                "timestamp": _dt.datetime.now().isoformat(),
                "key": f"key-{_r.randint(1,100)}",
                "value": json.dumps(topic_gens[topic]()),
                "size_bytes": _r.randint(80, 400),
            }
            await websocket.send_text(json.dumps(msg))
            offset += 1
            await asyncio.sleep(0.8)
    except WebSocketDisconnect:
        pass
