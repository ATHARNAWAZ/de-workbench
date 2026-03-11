from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import random
import uuid

router = APIRouter()

# Module 14 — Data Contracts & Schema Registry

_contracts: Dict[str, Dict] = {
    "cnt-001": {
        "id": "cnt-001", "name": "orders_silver_contract", "version": "2.1.0",
        "producer": "ingestion-team", "consumer": "analytics-team",
        "dataset": "silver.orders", "sla_freshness_hours": 2, "sla_quality_pct": 98.5,
        "owner": "alice@company.com", "status": "active",
        "fields": [
            {"name": "order_id", "type": "STRING", "nullable": False, "pii": False, "description": "Unique order identifier", "example": "ORD-12345", "validation": "NOT NULL, MATCHES ^ORD-\\d+$"},
            {"name": "customer_id", "type": "STRING", "nullable": False, "pii": False, "description": "Customer reference", "example": "CUST-789", "validation": "NOT NULL, FK to dim_customer"},
            {"name": "revenue", "type": "DECIMAL(12,2)", "nullable": False, "pii": False, "description": "Order revenue in USD", "example": "149.99", "validation": ">= 0"},
            {"name": "region", "type": "STRING", "nullable": False, "pii": False, "description": "Sales region", "example": "West", "validation": "IN (West, East, South, Midwest)"},
            {"name": "order_date", "type": "TIMESTAMP", "nullable": False, "pii": False, "description": "Order creation timestamp", "example": "2024-01-15T10:30:00Z", "validation": "NOT NULL"},
            {"name": "customer_email", "type": "STRING", "nullable": True, "pii": True, "description": "Customer email address", "example": "user@example.com", "validation": "VALID EMAIL FORMAT"},
            {"name": "status", "type": "STRING", "nullable": False, "pii": False, "description": "Order status", "example": "completed", "validation": "IN (pending, completed, shipped, cancelled, refunded)"},
        ],
        "quality_rules": ["No nulls in order_id", "revenue >= 0", "status in allowed values", "order_date not in future"],
        "quality_score": 99.2, "last_checked": "2024-01-27T08:00:00Z",
    },
    "cnt-002": {
        "id": "cnt-002", "name": "customer_profile_contract", "version": "1.0.0",
        "producer": "crm-team", "consumer": "marketing-team",
        "dataset": "silver.dim_customer", "sla_freshness_hours": 24, "sla_quality_pct": 99.0,
        "owner": "bob@company.com", "status": "active",
        "fields": [
            {"name": "customer_id", "type": "STRING", "nullable": False, "pii": False, "description": "Unique customer ID", "example": "CUST-001", "validation": "NOT NULL, UNIQUE"},
            {"name": "full_name", "type": "STRING", "nullable": False, "pii": True, "description": "Customer full name", "example": "Jane Smith", "validation": "NOT NULL, LENGTH 2-100"},
            {"name": "email", "type": "STRING", "nullable": False, "pii": True, "description": "Primary email", "example": "jane@example.com", "validation": "VALID EMAIL, UNIQUE"},
            {"name": "segment", "type": "STRING", "nullable": True, "pii": False, "description": "Customer segment", "example": "Premium", "validation": "IN (Standard, Premium, VIP, Churn-risk)"},
            {"name": "ltv_score", "type": "FLOAT", "nullable": True, "pii": False, "description": "Lifetime value score 0-1", "example": "0.87", "validation": "BETWEEN 0 AND 1"},
        ],
        "quality_rules": ["No duplicate customer_id", "Valid email format", "ltv_score between 0 and 1"],
        "quality_score": 97.8, "last_checked": "2024-01-27T06:00:00Z",
    },
}

_schemas: Dict[str, Dict] = {
    "sch-001": {
        "id": "sch-001", "subject": "orders-value", "version": 1,
        "format": "AVRO", "compatibility": "BACKWARD",
        "schema": '{"type":"record","name":"Order","fields":[{"name":"order_id","type":"string"},{"name":"revenue","type":"double"},{"name":"status","type":"string"}]}',
        "registered_by": "ingestion-service", "registered_at": "2024-01-01T00:00:00Z",
    },
    "sch-002": {
        "id": "sch-002", "subject": "orders-value", "version": 2,
        "format": "AVRO", "compatibility": "BACKWARD",
        "schema": '{"type":"record","name":"Order","fields":[{"name":"order_id","type":"string"},{"name":"revenue","type":"double"},{"name":"status","type":"string"},{"name":"region","type":["null","string"],"default":null}]}',
        "registered_by": "ingestion-service", "registered_at": "2024-01-15T00:00:00Z",
    },
    "sch-003": {
        "id": "sch-003", "subject": "user-events-value", "version": 1,
        "format": "JSON", "compatibility": "FORWARD",
        "schema": '{"$schema":"http://json-schema.org/draft-07/schema","type":"object","required":["user_id","event_type","timestamp"],"properties":{"user_id":{"type":"string"},"event_type":{"type":"string"},"timestamp":{"type":"string","format":"date-time"}}}',
        "registered_by": "clickstream-service", "registered_at": "2024-01-10T00:00:00Z",
    },
}

COMPATIBILITY_RULES = {
    "BACKWARD": {
        "description": "New schema can read data written by old schema",
        "allowed": ["Add optional field (with default)", "Remove field consumers don't use"],
        "rejected": ["Remove required field", "Change field type", "Rename field", "Add required field without default"],
    },
    "FORWARD": {
        "description": "Old schema can read data written by new schema",
        "allowed": ["Add required field", "Remove optional field"],
        "rejected": ["Remove required field consumers need", "Change field type"],
    },
    "FULL": {
        "description": "Both BACKWARD and FORWARD compatible",
        "allowed": ["Add optional field with default"],
        "rejected": ["Remove any field", "Change field type", "Add required field", "Rename field"],
    },
    "NONE": {
        "description": "No compatibility checks enforced",
        "allowed": ["Any change"],
        "rejected": [],
    },
}


@router.get("/contracts")
def list_contracts():
    return {"contracts": list(_contracts.values()), "total": len(_contracts)}


class ContractField(BaseModel):
    name: str
    type: str
    nullable: bool = True
    pii: bool = False
    description: str = ""
    example: str = ""
    validation: str = ""


class CreateContractRequest(BaseModel):
    name: str
    producer: str
    consumer: str
    dataset: str
    sla_freshness_hours: int = 24
    sla_quality_pct: float = 99.0
    owner: str
    fields: List[ContractField]


@router.post("/contracts")
def create_contract(req: CreateContractRequest):
    cid = f"cnt-{str(uuid.uuid4())[:8]}"
    contract = {
        "id": cid, "name": req.name, "version": "1.0.0",
        "producer": req.producer, "consumer": req.consumer,
        "dataset": req.dataset, "sla_freshness_hours": req.sla_freshness_hours,
        "sla_quality_pct": req.sla_quality_pct, "owner": req.owner,
        "status": "active", "fields": [f.dict() for f in req.fields],
        "quality_rules": [f"No nulls in {f.name}" for f in req.fields if not f.nullable],
        "quality_score": 100.0, "last_checked": None,
    }
    _contracts[cid] = contract
    return contract


@router.get("/contracts/{contract_id}")
def get_contract(contract_id: str):
    contract = _contracts.get(contract_id)
    if not contract:
        raise HTTPException(404, "Contract not found")
    return contract


@router.get("/contracts/schemas/list")
def list_schemas():
    subjects = {}
    for s in _schemas.values():
        subj = s["subject"]
        if subj not in subjects:
            subjects[subj] = []
        subjects[subj].append({"version": s["version"], "id": s["id"], "format": s["format"], "compatibility": s["compatibility"]})
    return {"subjects": subjects, "total_schemas": len(_schemas)}


class RegisterSchemaRequest(BaseModel):
    subject: str
    format: str = "AVRO"
    compatibility: str = "BACKWARD"
    schema_definition: str


@router.post("/contracts/schemas/register")
def register_schema(req: RegisterSchemaRequest):
    sid = f"sch-{str(uuid.uuid4())[:8]}"
    existing_versions = [s["version"] for s in _schemas.values() if s["subject"] == req.subject]
    new_version = max(existing_versions, default=0) + 1
    schema = {
        "id": sid, "subject": req.subject, "version": new_version,
        "format": req.format, "compatibility": req.compatibility,
        "schema": req.schema_definition,
        "registered_by": "api-user", "registered_at": "2024-01-27T12:00:00Z",
    }
    _schemas[sid] = schema
    return {"registered": True, "schema_id": sid, "subject": req.subject, "version": new_version}


@router.get("/contracts/schemas/{schema_id}/diff")
def schema_diff(schema_id: str):
    schema = _schemas.get(schema_id)
    if not schema:
        raise HTTPException(404, "Schema not found")
    # Find previous version for same subject
    subject = schema["subject"]
    prev = None
    for s in _schemas.values():
        if s["subject"] == subject and s["version"] == schema["version"] - 1:
            prev = s
            break

    if not prev:
        return {"has_previous": False, "current": schema, "changes": [], "compatibility_verdict": "N/A (first version)"}

    changes = [
        {"field": "region", "change_type": "ADDED", "breaking": False, "detail": "Optional field 'region' added with default null — BACKWARD compatible"},
    ]
    return {
        "has_previous": True,
        "subject": subject,
        "from_version": prev["version"],
        "to_version": schema["version"],
        "changes": changes,
        "compatibility": schema["compatibility"],
        "compatibility_verdict": "COMPATIBLE",
        "rules": COMPATIBILITY_RULES.get(schema["compatibility"], {}),
    }


class CompatibilityCheckRequest(BaseModel):
    subject: str
    compatibility_mode: str
    proposed_change: str


@router.post("/contracts/compatibility-check")
def check_compatibility(req: CompatibilityCheckRequest):
    rules = COMPATIBILITY_RULES.get(req.compatibility_mode, COMPATIBILITY_RULES["BACKWARD"])
    change_lower = req.proposed_change.lower()
    is_breaking = any(word in change_lower for word in ["remove required", "rename", "change type", "delete column"])
    return {
        "subject": req.subject,
        "compatibility_mode": req.compatibility_mode,
        "proposed_change": req.proposed_change,
        "verdict": "REJECTED — BREAKING CHANGE" if is_breaking else "COMPATIBLE — ALLOWED",
        "breaking": is_breaking,
        "rules": rules,
        "recommendation": "Use a new schema subject or add field as optional with default" if is_breaking else "Safe to register this schema version",
    }


@router.get("/contracts/impact/{schema_id}")
def breaking_change_impact(schema_id: str):
    schema = _schemas.get(schema_id)
    if not schema:
        raise HTTPException(404, "Schema not found")
    return {
        "schema_id": schema_id,
        "subject": schema["subject"],
        "affected_consumers": [
            {"name": "analytics-pipeline", "team": "analytics", "impact": "HIGH", "action_required": "Update deserialization code to handle new field"},
            {"name": "ml-feature-pipeline", "team": "data-science", "impact": "MEDIUM", "action_required": "Add null handling for new optional region field"},
            {"name": "reporting-service", "team": "bi", "impact": "LOW", "action_required": "No action required — backward compatible change"},
        ],
        "migration_path": [
            "Notify all consumers of upcoming schema change",
            "Deploy consumers that handle both v1 and v2",
            "Switch producer to emit v2 schema",
            "Monitor consumer error rates for 24h",
            "Decommission v1 support after all consumers updated",
        ],
    }


@router.get("/contracts/compatibility-rules")
def get_compatibility_rules():
    return {"rules": COMPATIBILITY_RULES}
