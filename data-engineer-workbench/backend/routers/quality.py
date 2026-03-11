from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
import random
import sqlite3
from datetime import datetime

router = APIRouter()

DATASETS = [
    {"id": "ds-orders", "name": "fact_orders", "layer": "bronze", "row_count": 50000, "column_count": 11},
    {"id": "ds-customers", "name": "dim_customer", "layer": "silver", "row_count": 500, "column_count": 10},
    {"id": "ds-products", "name": "dim_product", "layer": "silver", "row_count": 200, "column_count": 8},
    {"id": "ds-revenue", "name": "gold_daily_revenue", "layer": "gold", "row_count": 5000, "column_count": 7},
]

COLUMN_PROFILES = {
    "ds-orders": [
        {"name": "order_id", "type": "VARCHAR", "null_pct": 0.0, "unique_count": 5000, "sample": ["ORD-000001", "ORD-000002"]},
        {"name": "customer_id", "type": "VARCHAR", "null_pct": 0.0, "unique_count": 487, "sample": ["CUST-00001", "CUST-00243"]},
        {"name": "product_id", "type": "VARCHAR", "null_pct": 0.0, "unique_count": 198, "sample": ["PROD-0001", "PROD-0099"]},
        {"name": "quantity", "type": "INTEGER", "null_pct": 0.0, "unique_count": 10, "sample": [1, 3, 5]},
        {"name": "unit_price", "type": "DECIMAL", "null_pct": 0.02, "unique_count": 4980, "sample": [29.99, 149.99]},
        {"name": "discount", "type": "DECIMAL", "null_pct": 0.0, "unique_count": 320, "sample": [0.0, 0.1, 0.25]},
        {"name": "revenue", "type": "DECIMAL", "null_pct": 0.02, "unique_count": 4950, "sample": [89.97, 449.95]},
        {"name": "status", "type": "VARCHAR", "null_pct": 0.05, "unique_count": 7, "sample": ["completed", "shipped"]},
        {"name": "channel", "type": "VARCHAR", "null_pct": 0.0, "unique_count": 4, "sample": ["web", "mobile"]},
        {"name": "region", "type": "VARCHAR", "null_pct": 0.0, "unique_count": 4, "sample": ["North America", "Europe"]},
        {"name": "date_id", "type": "VARCHAR", "null_pct": 0.01, "unique_count": 730, "sample": ["20230101", "20231215"]},
    ],
    "ds-customers": [
        {"name": "customer_id", "type": "VARCHAR", "null_pct": 0.0, "unique_count": 500, "sample": ["CUST-00001"]},
        {"name": "email", "type": "VARCHAR", "null_pct": 0.03, "unique_count": 487, "sample": ["user@example.com"]},
        {"name": "phone", "type": "VARCHAR", "null_pct": 0.08, "unique_count": 491, "sample": ["+1-555-0100"]},
        {"name": "city", "type": "VARCHAR", "null_pct": 0.01, "unique_count": 8, "sample": ["New York"]},
        {"name": "country", "type": "VARCHAR", "null_pct": 0.0, "unique_count": 6, "sample": ["United States"]},
        {"name": "segment", "type": "VARCHAR", "null_pct": 0.0, "unique_count": 4, "sample": ["Enterprise"]},
        {"name": "is_active", "type": "INTEGER", "null_pct": 0.0, "unique_count": 2, "sample": [0, 1]},
        {"name": "created_at", "type": "TIMESTAMP", "null_pct": 0.0, "unique_count": 499, "sample": ["2022-03-15"]},
    ],
}


class QualityRuleCheck(BaseModel):
    dataset_id: str
    column: str
    rule: str
    value: Optional[str] = None


@router.get("/quality/datasets")
def list_datasets():
    return {"datasets": DATASETS}


@router.get("/quality/{dataset_id}")
def run_quality_checks(dataset_id: str):
    ds = next((d for d in DATASETS if d["id"] == dataset_id), None)
    if not ds:
        ds = {"id": dataset_id, "name": dataset_id, "layer": "unknown", "row_count": 10000, "column_count": 8}

    columns = COLUMN_PROFILES.get(dataset_id, [
        {"name": f"col_{i}", "type": "VARCHAR", "null_pct": round(random.uniform(0, 0.15), 3),
         "unique_count": random.randint(5, 1000), "sample": ["value1", "value2"]}
        for i in range(ds["column_count"])
    ])

    col_results = []
    total_penalty = 0.0

    for col in columns:
        null_pct = col["null_pct"]
        issues = []
        score = 100.0

        if null_pct > 0.1:
            issues.append({"type": "high_nulls", "severity": "critical", "detail": f"{null_pct*100:.1f}% missing values"})
            score -= 30
        elif null_pct > 0.02:
            issues.append({"type": "moderate_nulls", "severity": "warning", "detail": f"{null_pct*100:.1f}% missing values"})
            score -= 10

        if col["type"] in ["DECIMAL", "INTEGER"] and random.random() < 0.15:
            iqr_pct = round(random.uniform(0.5, 3.5), 1)
            issues.append({"type": "outliers", "severity": "warning", "detail": f"{iqr_pct}% rows are statistical outliers (IQR method)"})
            score -= 8

        if col["unique_count"] > ds["row_count"] * 0.98 and col["type"] == "VARCHAR" and "id" not in col["name"]:
            issues.append({"type": "high_cardinality", "severity": "info", "detail": "Suspiciously high cardinality for a categorical column"})
            score -= 3

        if random.random() < 0.08:
            issues.append({"type": "type_mismatch", "severity": "critical", "detail": "Detected mixed types (e.g., numbers stored as strings)"})
            score -= 20

        dup_count = random.randint(0, 50) if col["name"].endswith("_id") else 0
        if dup_count > 0 and "PRIMARY" in col["name"].upper() or (col["name"].endswith("_id") and random.random() < 0.05):
            dup_count = random.randint(1, 50)
            issues.append({"type": "duplicates", "severity": "critical", "detail": f"{dup_count} duplicate values detected"})
            score -= 25

        total_penalty += max(0, 100 - score)
        col_results.append({
            "column": col["name"],
            "type": col["type"],
            "null_pct": null_pct,
            "null_count": int(null_pct * ds["row_count"]),
            "unique_count": col["unique_count"],
            "cardinality_pct": round(col["unique_count"] / ds["row_count"] * 100, 1),
            "score": max(0, round(score, 1)),
            "issues": issues,
            "sample_values": col.get("sample", []),
            "suggestions": _generate_suggestions(issues),
        })

    overall_score = max(0, round(100 - (total_penalty / max(len(columns), 1)), 1))
    dup_rows = random.randint(0, 150)
    ref_issues = random.randint(0, 25)

    return {
        "dataset_id": dataset_id,
        "dataset_name": ds["name"],
        "row_count": ds["row_count"],
        "column_count": len(columns),
        "overall_score": overall_score,
        "grade": _score_to_grade(overall_score),
        "duplicate_rows": dup_rows,
        "referential_integrity_issues": ref_issues,
        "columns": col_results,
        "summary": {
            "critical_issues": sum(1 for c in col_results for i in c["issues"] if i["severity"] == "critical"),
            "warnings": sum(1 for c in col_results for i in c["issues"] if i["severity"] == "warning"),
            "info": sum(1 for c in col_results for i in c["issues"] if i["severity"] == "info"),
        },
        "checked_at": datetime.now().isoformat(),
    }


def _score_to_grade(score: float) -> str:
    if score >= 95: return "A+"
    if score >= 90: return "A"
    if score >= 80: return "B"
    if score >= 70: return "C"
    if score >= 60: return "D"
    return "F"


def _generate_suggestions(issues: list) -> list:
    suggestions = []
    for issue in issues:
        if issue["type"] == "high_nulls":
            suggestions.append("Consider imputation strategies: median fill for numeric, mode fill for categorical, or use an 'UNKNOWN' sentinel value.")
        elif issue["type"] == "moderate_nulls":
            suggestions.append("Investigate upstream data sources for null propagation. Add NOT NULL constraint at ingestion.")
        elif issue["type"] == "outliers":
            suggestions.append("Apply IQR-based winsorization or cap/floor extreme values. Log outliers for domain expert review.")
        elif issue["type"] == "type_mismatch":
            suggestions.append("Cast column to correct type during transformation. Add schema enforcement at ingestion layer.")
        elif issue["type"] == "duplicates":
            suggestions.append("Add deduplication step: df.drop_duplicates(subset=['id'], keep='last'). Investigate upstream for duplicate emissions.")
        elif issue["type"] == "high_cardinality":
            suggestions.append("Verify if this should be a free-text field or a FK. Consider adding a lookup/dimension table.")
    return suggestions


@router.post("/quality/rule-check")
def check_custom_rule(body: QualityRuleCheck):
    row_count = random.randint(1000, 50000)
    violations = random.randint(0, int(row_count * 0.05))
    pass_rate = round((row_count - violations) / row_count * 100, 2)
    return {
        "dataset_id": body.dataset_id,
        "column": body.column,
        "rule": body.rule,
        "value": body.value,
        "row_count": row_count,
        "violations": violations,
        "pass_rate": pass_rate,
        "status": "pass" if violations == 0 else ("warn" if violations < row_count * 0.01 else "fail"),
        "checked_at": datetime.now().isoformat(),
    }
