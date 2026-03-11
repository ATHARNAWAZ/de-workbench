from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict
import random
import uuid
import datetime

router = APIRouter()

# Module 16 — Reverse ETL & Data Activation

DESTINATIONS = [
    {"id": "dest-salesforce", "name": "Salesforce", "type": "CRM", "icon": "cloud", "description": "Sync customer segments, scores, and attributes to Salesforce CRM", "required_fields": ["Id", "Email"], "auth": "OAuth2"},
    {"id": "dest-hubspot", "name": "HubSpot", "type": "CRM", "icon": "cloud", "description": "Push churn predictions and enrichment data to HubSpot contacts", "required_fields": ["email"], "auth": "API Key"},
    {"id": "dest-slack", "name": "Slack (Webhook)", "type": "Messaging", "icon": "message", "description": "Post aggregated alerts or daily summaries to Slack channels", "required_fields": ["message"], "auth": "Webhook URL"},
    {"id": "dest-sheets", "name": "Google Sheets", "type": "Spreadsheet", "icon": "table", "description": "Export query results to Google Sheets for business stakeholders", "required_fields": [], "auth": "Google OAuth"},
    {"id": "dest-rest", "name": "REST API", "type": "Generic", "icon": "globe", "description": "POST records to any REST endpoint with custom headers", "required_fields": ["url"], "auth": "Bearer Token / API Key"},
]

WAREHOUSE_TABLES = [
    {"id": "gold.customer_segments", "name": "customer_segments", "layer": "gold", "rows": 48200, "columns": ["customer_id", "email", "segment", "ltv_score", "churn_risk", "last_order_date"]},
    {"id": "gold.churn_predictions", "name": "churn_predictions", "layer": "gold", "rows": 12400, "columns": ["customer_id", "email", "churn_probability", "days_to_churn", "revenue_at_risk"]},
    {"id": "gold.product_recommendations", "name": "product_recommendations", "layer": "gold", "rows": 98700, "columns": ["customer_id", "recommended_product_id", "score", "category"]},
    {"id": "gold.daily_revenue", "name": "daily_revenue", "layer": "gold", "rows": 365, "columns": ["date", "region", "revenue", "orders", "avg_order_value"]},
]

_pipelines: Dict[str, Dict] = {
    "pipe-001": {
        "id": "pipe-001", "name": "High-Value Customers → Salesforce",
        "source_table": "gold.customer_segments", "source_filter": "ltv_score > 0.8",
        "destination": "dest-salesforce", "sync_mode": "incremental",
        "schedule": "hourly", "status": "active",
        "field_mappings": [
            {"source": "customer_id", "destination": "External_ID__c", "transform": None},
            {"source": "email", "destination": "Email", "transform": None},
            {"source": "segment", "destination": "Customer_Segment__c", "transform": None},
            {"source": "ltv_score", "destination": "LTV_Score__c", "transform": "multiply_100"},
        ],
        "last_sync": "2024-01-27T11:00:00Z", "records_synced": 4821, "errors": 0,
    },
    "pipe-002": {
        "id": "pipe-002", "name": "Churn Risk → HubSpot",
        "source_table": "gold.churn_predictions", "source_filter": "churn_probability > 0.7",
        "destination": "dest-hubspot", "sync_mode": "mirror",
        "schedule": "daily", "status": "active",
        "field_mappings": [
            {"source": "email", "destination": "email", "transform": None},
            {"source": "churn_probability", "destination": "churn_risk_score", "transform": "multiply_100"},
            {"source": "days_to_churn", "destination": "estimated_churn_days", "transform": None},
        ],
        "last_sync": "2024-01-27T02:00:00Z", "records_synced": 1247, "errors": 3,
    },
}

_sync_logs = [
    {"id": "log-001", "pipeline_id": "pipe-001", "started_at": "2024-01-27T11:00:00Z", "finished_at": "2024-01-27T11:02:34Z", "status": "success", "records_added": 128, "records_updated": 340, "records_deleted": 0, "errors": 0},
    {"id": "log-002", "pipeline_id": "pipe-001", "started_at": "2024-01-27T10:00:00Z", "finished_at": "2024-01-27T10:01:58Z", "status": "success", "records_added": 45, "records_updated": 210, "records_deleted": 0, "errors": 0},
    {"id": "log-003", "pipeline_id": "pipe-002", "started_at": "2024-01-27T02:00:00Z", "finished_at": "2024-01-27T02:15:22Z", "status": "partial", "records_added": 1210, "records_updated": 37, "records_deleted": 0, "errors": 3},
]


@router.get("/reversetl/pipelines")
def list_pipelines():
    return {"pipelines": list(_pipelines.values())}


@router.get("/reversetl/destinations")
def list_destinations():
    return {"destinations": DESTINATIONS}


@router.get("/reversetl/tables")
def list_tables():
    return {"tables": WAREHOUSE_TABLES}


class FieldMapping(BaseModel):
    source: str
    destination: str
    transform: Optional[str] = None


class CreatePipelineRequest(BaseModel):
    name: str
    source_table: str
    source_filter: str = ""
    destination: str
    sync_mode: str = "incremental"
    schedule: str = "hourly"
    field_mappings: List[FieldMapping]


@router.post("/reversetl/pipelines")
def create_pipeline(req: CreatePipelineRequest):
    pid = f"pipe-{str(uuid.uuid4())[:8]}"
    pipeline = {
        "id": pid, "name": req.name, "source_table": req.source_table,
        "source_filter": req.source_filter, "destination": req.destination,
        "sync_mode": req.sync_mode, "schedule": req.schedule, "status": "active",
        "field_mappings": [m.dict() for m in req.field_mappings],
        "last_sync": None, "records_synced": 0, "errors": 0,
    }
    _pipelines[pid] = pipeline
    return pipeline


class RunSyncRequest(BaseModel):
    pipeline_id: str


@router.post("/reversetl/sync")
def run_sync(req: RunSyncRequest):
    pipeline = _pipelines.get(req.pipeline_id)
    if not pipeline:
        raise HTTPException(404, "Pipeline not found")

    r = random.Random(hash(req.pipeline_id + datetime.datetime.utcnow().isoformat()[:13]))
    added = r.randint(50, 500)
    updated = r.randint(100, 1000)
    errors = r.randint(0, 3)
    duration_s = r.randint(30, 180)

    log = {
        "id": f"log-{str(uuid.uuid4())[:8]}", "pipeline_id": req.pipeline_id,
        "started_at": datetime.datetime.utcnow().isoformat() + "Z",
        "finished_at": (datetime.datetime.utcnow() + datetime.timedelta(seconds=duration_s)).isoformat() + "Z",
        "status": "success" if errors == 0 else "partial",
        "records_added": added, "records_updated": updated, "records_deleted": 0, "errors": errors,
    }
    _sync_logs.append(log)
    pipeline["last_sync"] = log["started_at"]
    pipeline["records_synced"] += added + updated
    return {"sync_result": log, "pipeline": pipeline}


@router.get("/reversetl/sync-log")
def get_sync_log(pipeline_id: Optional[str] = None):
    logs = _sync_logs if not pipeline_id else [l for l in _sync_logs if l["pipeline_id"] == pipeline_id]
    return {"logs": sorted(logs, key=lambda x: x["started_at"], reverse=True)}


@router.get("/reversetl/field-transforms")
def get_field_transforms():
    return {
        "transforms": [
            {"id": "multiply_100", "name": "Multiply × 100", "description": "Convert 0-1 float to 0-100 integer (e.g., scores)"},
            {"id": "uppercase", "name": "UPPERCASE", "description": "Convert string to uppercase"},
            {"id": "date_format", "name": "Format Date", "description": "Format date as MM/DD/YYYY for legacy systems"},
            {"id": "concat_name", "name": "Concat Full Name", "description": "Combine first_name + last_name into single field"},
            {"id": "lookup_enum", "name": "Lookup Enum", "description": "Map internal codes to destination enum values"},
            {"id": "truncate_255", "name": "Truncate 255", "description": "Truncate string to 255 characters max"},
        ]
    }
