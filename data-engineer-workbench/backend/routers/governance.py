from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
import random
import re
from datetime import datetime, timedelta

router = APIRouter()

PII_PATTERNS = {
    "email": r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    "ssn": r'\b\d{3}-\d{2}-\d{4}\b',
    "phone": r'\b[\+]?[(]?[0-9]{3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}\b',
    "credit_card": r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',
    "ip_address": r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
    "date_of_birth": r'\b(dob|date_of_birth|birth_date|birthdate)\b',
    "name": r'\b(first_name|last_name|full_name|fname|lname)\b',
    "address": r'\b(address|street|city|zip|postal_code|state)\b',
}

PII_COLUMN_HINTS = {
    "email": ("email", "e_mail", "user_email", "contact_email"),
    "phone": ("phone", "mobile", "cell", "telephone", "phone_number"),
    "ssn": ("ssn", "social_security", "tax_id", "national_id"),
    "name": ("first_name", "last_name", "full_name", "fname", "lname", "name"),
    "address": ("address", "street", "zip", "postal_code", "postcode"),
    "ip": ("ip", "ip_address", "remote_addr"),
    "date_of_birth": ("dob", "date_of_birth", "birth_date", "birthdate"),
    "credit_card": ("cc_number", "card_number", "credit_card", "payment_method"),
}

ACCESS_MATRIX = {
    "roles": ["Analyst", "Senior Analyst", "Data Engineer", "Admin"],
    "permissions": [
        {"table": "bronze.raw_orders", "Analyst": "DENY", "Senior Analyst": "SELECT", "Data Engineer": "SELECT, INSERT", "Admin": "ALL"},
        {"table": "silver.orders", "Analyst": "SELECT", "Senior Analyst": "SELECT", "Data Engineer": "SELECT, INSERT, UPDATE", "Admin": "ALL"},
        {"table": "gold.fact_orders", "Analyst": "SELECT", "Senior Analyst": "SELECT", "Data Engineer": "SELECT, INSERT, UPDATE", "Admin": "ALL"},
        {"table": "dim_customer", "Analyst": "SELECT (masked)", "Senior Analyst": "SELECT (masked)", "Data Engineer": "SELECT", "Admin": "ALL"},
        {"table": "dim_product", "Analyst": "SELECT", "Senior Analyst": "SELECT", "Data Engineer": "SELECT, INSERT", "Admin": "ALL"},
        {"table": "gold.daily_revenue", "Analyst": "SELECT", "Senior Analyst": "SELECT", "Data Engineer": "SELECT, INSERT", "Admin": "ALL"},
        {"table": "audit_log", "Analyst": "DENY", "Senior Analyst": "DENY", "Data Engineer": "SELECT", "Admin": "ALL"},
    ],
}

COMPLIANCE_CHECKLIST = [
    {"table": "dim_customer", "item": "Retention policy defined", "status": True, "detail": "90-day soft delete, 7-year hard delete"},
    {"table": "dim_customer", "item": "Consent tracking enabled", "status": True, "detail": "GDPR consent logged in consent_events table"},
    {"table": "dim_customer", "item": "Right-to-delete process", "status": True, "detail": "Automated via GDPR deletion API"},
    {"table": "dim_customer", "item": "Data minimization", "status": False, "detail": "Phone field collected but never used — review needed"},
    {"table": "dim_customer", "item": "Cross-border transfer documented", "status": True, "detail": "EU Standard Contractual Clauses in place"},
    {"table": "fact_orders", "item": "Retention policy defined", "status": True, "detail": "7-year financial data retention"},
    {"table": "fact_orders", "item": "PII removed from fact table", "status": True, "detail": "Only customer_id FK, no direct PII"},
    {"table": "fact_orders", "item": "Right-to-delete cascades", "status": False, "detail": "Deletion cascade from dim_customer to fact_orders not implemented"},
    {"table": "bronze.raw_orders", "item": "Raw PII purge policy", "status": False, "detail": "Raw JSON payloads contain email — purge after 30 days not configured"},
]


class PIIScanRequest(BaseModel):
    dataset_id: str
    sample_values: Optional[dict[str, list]] = None


class MaskRequest(BaseModel):
    column_name: str
    pii_type: str
    values: list[str]


@router.post("/governance/scan-pii")
def scan_pii(body: PIIScanRequest):
    column_profiles = {
        "ds-customers": [
            {"column": "customer_id", "sample": ["CUST-00001", "CUST-00045"]},
            {"column": "first_name", "sample": ["Alice", "Bob"]},
            {"column": "last_name", "sample": ["Johnson", "Smith"]},
            {"column": "email", "sample": ["alice@example.com", "bob@corp.com"]},
            {"column": "phone", "sample": ["+1-555-0100", "555-987-6543"]},
            {"column": "city", "sample": ["New York", "London"]},
            {"column": "country", "sample": ["United States", "UK"]},
            {"column": "segment", "sample": ["Enterprise", "SMB"]},
        ],
        "ds-orders": [
            {"column": "order_id", "sample": ["ORD-000001", "ORD-000002"]},
            {"column": "customer_id", "sample": ["CUST-00001", "CUST-00045"]},
            {"column": "revenue", "sample": [149.99, 89.50]},
            {"column": "status", "sample": ["completed", "shipped"]},
        ],
    }

    columns = column_profiles.get(body.dataset_id, [
        {"column": "id", "sample": ["ID-001"]},
        {"column": "value", "sample": ["some_value"]},
    ])

    results = []
    for col in columns:
        pii_type = None
        confidence = 0.0

        col_lower = col["column"].lower()
        for ptype, hints in PII_COLUMN_HINTS.items():
            if any(hint in col_lower for hint in hints):
                pii_type = ptype
                confidence = round(random.uniform(0.85, 0.99), 2)
                break

        sample_strs = [str(v) for v in col["sample"]]
        if pii_type is None:
            for ptype, pattern in PII_PATTERNS.items():
                if any(re.search(pattern, s, re.IGNORECASE) for s in sample_strs):
                    pii_type = ptype
                    confidence = round(random.uniform(0.70, 0.90), 2)
                    break

        results.append({
            "column": col["column"],
            "pii_detected": pii_type is not None,
            "pii_type": pii_type,
            "confidence": confidence if pii_type else None,
            "sample_values": col["sample"],
            "recommendation": _get_pii_recommendation(pii_type),
        })

    pii_count = sum(1 for r in results if r["pii_detected"])
    return {
        "dataset_id": body.dataset_id,
        "total_columns": len(results),
        "pii_columns": pii_count,
        "risk_level": "HIGH" if pii_count >= 3 else ("MEDIUM" if pii_count >= 1 else "LOW"),
        "columns": results,
        "scanned_at": datetime.now().isoformat(),
    }


def _get_pii_recommendation(pii_type: Optional[str]) -> Optional[str]:
    recs = {
        "email": "Hash with SHA-256 for analytics. Tokenize for CRM. Apply row-level security.",
        "phone": "Mask as +X-XXX-XXX-XXXX. Store only last 4 digits for display.",
        "ssn": "CRITICAL: Encrypt at rest with AES-256. Never expose in queries. Use tokenization service.",
        "name": "Pseudonymize with deterministic token. Maintain mapping in secure key vault.",
        "address": "Generalize to city/region level for analytics. Store full address only in secured PII vault.",
        "ip": "Anonymize by zeroing last octet (e.g., 192.168.1.x). Never log full IPs.",
        "date_of_birth": "Store as age buckets (18-24, 25-34) for analytics. Encrypt full DOB.",
        "credit_card": "CRITICAL: PCI-DSS requires tokenization. Never store full card numbers. Use payment processor tokens only.",
    }
    return recs.get(pii_type)


@router.post("/governance/mask-demo")
def demo_masking(body: MaskRequest):
    original = body.values
    masked = []

    for val in original:
        if body.pii_type == "email":
            parts = val.split("@")
            masked.append(f"{parts[0][:2]}***@{parts[1]}" if len(parts) == 2 else "***@***.***")
        elif body.pii_type == "phone":
            masked.append(re.sub(r'\d(?=\d{4})', '*', val))
        elif body.pii_type == "ssn":
            masked.append("***-**-" + val[-4:] if len(val) >= 4 else "***-**-****")
        elif body.pii_type == "name":
            masked.append(val[0] + "*" * (len(val) - 1))
        elif body.pii_type == "credit_card":
            digits = re.sub(r'\D', '', val)
            masked.append("*" * 12 + digits[-4:] if len(digits) >= 4 else "****-****-****-****")
        else:
            import hashlib
            masked.append("tok_" + hashlib.sha256(val.encode()).hexdigest()[:12])

    tokenized = ["tok_" + __import__("hashlib").sha256(v.encode()).hexdigest()[:16] for v in original]

    return {
        "column": body.column_name,
        "pii_type": body.pii_type,
        "rows": [
            {"original": o, "masked": m, "tokenized": t}
            for o, m, t in zip(original, masked, tokenized)
        ],
    }


@router.get("/governance/access-matrix")
def get_access_matrix():
    return ACCESS_MATRIX


@router.get("/governance/compliance")
def get_compliance():
    tables = {}
    for item in COMPLIANCE_CHECKLIST:
        if item["table"] not in tables:
            tables[item["table"]] = []
        tables[item["table"]].append(item)

    result = []
    for table, items in tables.items():
        passed = sum(1 for i in items if i["status"])
        result.append({
            "table": table,
            "score": round(passed / len(items) * 100),
            "items": items,
        })

    return {"compliance": result}


@router.get("/governance/audit-log")
def get_audit_log(limit: int = 50):
    import sqlite3
    try:
        conn = sqlite3.connect("workbench.db")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM audit_log ORDER BY timestamp DESC LIMIT ?", (limit,))
        logs = [dict(r) for r in cur.fetchall()]
        conn.close()
        return {"logs": logs, "total": len(logs)}
    except Exception:
        return {"logs": [], "total": 0}
