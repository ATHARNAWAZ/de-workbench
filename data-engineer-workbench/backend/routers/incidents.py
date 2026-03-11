from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import random
import uuid

router = APIRouter()

# Module 20 — Incident Management & On-Call

INCIDENT_SCENARIOS = [
    {
        "id": "inc-001", "title": "Pipeline Failing Silently for 3 Hours", "severity": "P1",
        "category": "Pipeline Failure",
        "alert": "Data freshness SLA breached: orders_silver table not updated in 3h (SLA: 1h)",
        "symptoms": ["Dashboard showing stale data as of 3 hours ago", "No error notifications received", "Pipeline status shows 'running' but no progress"],
        "investigation_steps": [
            "Check pipeline logs: kubectl logs -l app=orders-pipeline --since=3h",
            "Check Airflow task status: task shows 'running' but executor logs show OOM kill",
            "Check cluster metrics: memory utilization at 98%, multiple task restarts",
            "Check source row count: SELECT COUNT(*) FROM bronze.raw_orders WHERE _ingested_at > NOW()-3h",
        ],
        "root_cause": "Spark executor running out of memory due to a data skew introduced by a new customer segment with 10x more orders than others. OOMKilled tasks retried silently.",
        "resolution": ["Increase executor memory from 4GB to 8GB", "Add salting to the skewed join key", "Set spark.task.maxFailures=2 to fail fast instead of silent retry"],
        "prevention": ["Add memory utilization alert (threshold: 80%)", "Implement data skew detection in pipeline validation", "Add explicit SLA breach notification at 30min overdue"],
        "detection_delay_min": 180, "resolution_time_min": 45,
    },
    {
        "id": "inc-002", "title": "Data Skew Causing Spark Job to Hang on Last 1%", "severity": "P2",
        "category": "Performance",
        "alert": "Spark job running for 4h (expected: 45min). Stage 8/9 stuck at 99% — 1 task remaining.",
        "symptoms": ["Spark UI shows Stage 8 at 99% — 1 task running for 3h", "99 tasks completed in 5min, 1 task still running", "Executor logs show single partition with 15GB of data"],
        "investigation_steps": [
            "Open Spark UI → Stages → Stage 8 → click 'tasks' tab",
            "Find the straggler task: note partition index and shuffle read bytes",
            "Run: SELECT customer_id, COUNT(*) FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            "Identify hot key: customer 'GUEST' has 2.3M orders vs 500 avg",
        ],
        "root_cause": "NULL customer_id values coerced to 'GUEST' string in a recent data cleaning step, creating a 2.3M-row hot partition in the groupBy stage.",
        "resolution": ["Filter out NULL customer_id before join: .filter(col('customer_id').isNotNull())", "Or apply salting: concat(customer_id, (rand() * 10).cast(int))"],
        "prevention": ["Add partition size check after each stage", "Alert if any partition > 3x median partition size"],
        "detection_delay_min": 240, "resolution_time_min": 30,
    },
    {
        "id": "inc-003", "title": "Source Database Schema Changed, Breaking Ingestion", "severity": "P1",
        "category": "Schema Change",
        "alert": "Pipeline orders_bronze FAILED: Column 'customer_name' not found in source schema",
        "symptoms": ["Ingestion pipeline throwing: pyspark.sql.utils.AnalysisException: Column 'customer_name' not found", "Source DB team released schema migration that split name into first_name + last_name", "No advance notice or data contract change request"],
        "investigation_steps": [
            "Check pipeline error: 'Column customer_name not found in struct schema'",
            "Connect to source DB: SELECT column_name FROM information_schema.columns WHERE table_name = 'orders'",
            "Confirm: customer_name removed, first_name + last_name added",
            "Check data contract: contract version still references customer_name",
        ],
        "root_cause": "Source team changed schema without following data contract change process. No schema registry validation at write time.",
        "resolution": ["Update bronze ingestion to read first_name + last_name", "Add derived column: customer_name = concat(first_name, ' ', last_name)", "Update data contract v1.2 and register new schema version"],
        "prevention": ["Enforce schema registry for all CDC sources", "Add schema validation step at start of pipeline", "Create schema change approval workflow with impact analysis"],
        "detection_delay_min": 5, "resolution_time_min": 60,
    },
    {
        "id": "inc-004", "title": "Dashboard Showing Wrong Revenue Figures", "severity": "P1",
        "category": "Data Correctness",
        "alert": "Stakeholder reports: total revenue $2.1M today, but CFO dashboard shows $4.2M — exactly double.",
        "symptoms": ["Revenue doubled in gold layer since yesterday", "Row count in gold table doubled", "Pipeline completed successfully — no errors"],
        "investigation_steps": [
            "Compare gold row count: SELECT COUNT(*) FROM gold.daily_revenue WHERE date = '2024-01-27'",
            "Check pipeline run count today: found 2 runs (01:00 and 01:05 — duplicate trigger)",
            "Check orchestration logs: DAG triggered twice due to Airflow scheduler restart during maintenance",
            "Confirm: pipeline is NOT idempotent — uses INSERT not MERGE/UPSERT",
        ],
        "root_cause": "Pipeline triggered twice due to Airflow restart (both triggers completed successfully). INSERT mode caused data duplication. No idempotency guard.",
        "resolution": ["Truncate and reload: TRUNCATE TABLE gold.daily_revenue; re-run pipeline once", "Fix pipeline: change INSERT to MERGE/DELETE+INSERT with unique key", "Add deduplication check: alert if row count increases > 20% unexpectedly"],
        "prevention": ["All pipelines MUST be idempotent (safe to run twice)", "Use MERGE not INSERT for gold layer", "Add pre-run check: fail if target already has data for this date"],
        "detection_delay_min": 480, "resolution_time_min": 90,
    },
    {
        "id": "inc-005", "title": "PII Data Written to Wrong Table", "severity": "P0",
        "category": "Data Security",
        "alert": "Governance scan detected: table analytics.user_debug contains email, phone, ssn columns — accessible to all analysts",
        "symptoms": ["PII scan triggered by governance tool at 03:00", "Table analytics.user_debug created by a pipeline debug run last night", "Table permissions: PUBLIC — any analyst can query it"],
        "investigation_steps": [
            "Run PII scan: SELECT column_name FROM information_schema.columns WHERE column_name ILIKE '%email%' OR column_name ILIKE '%phone%'",
            "Check table owner and creation time",
            "Check access log: how many users have queried this table in last 8h",
            "Identify which pipeline created this debug table",
        ],
        "root_cause": "Engineer added debug output step in pipeline that wrote full customer records including PII to a public schema. Code review didn't catch it. No automated PII guardrails at write time.",
        "resolution": ["Immediately: DROP TABLE analytics.user_debug", "Notify security team and DPO within 1 hour", "Identify all users who accessed the table — assess notification obligation", "Revoke write permissions on analytics schema for pipeline service accounts"],
        "prevention": ["Block CREATE TABLE in public schemas for pipeline roles", "Add PII column detection at write time — fail pipeline if PII detected in non-governed table", "Mandatory PII review in code review checklist"],
        "detection_delay_min": 480, "resolution_time_min": 20,
    },
]

RUNBOOKS = [
    {
        "id": "rb-001", "title": "Pipeline Failure — General Recovery", "category": "Pipeline",
        "overview": "Standard runbook for investigating and recovering from a failed data pipeline",
        "alert_conditions": ["Pipeline status = FAILED", "Data freshness SLA breached", "Row count < 10% of expected"],
        "investigation_steps": [
            {"step": 1, "description": "Check pipeline logs", "command": "kubectl logs -l app={pipeline_name} --since=2h | tail -200", "expected": "Find the exact error message and stack trace"},
            {"step": 2, "description": "Check last successful run", "command": "SELECT MAX(run_date), status, error_message FROM pipeline_runs WHERE pipeline_id = '{id}' ORDER BY 1 DESC", "expected": "Identify when it last succeeded"},
            {"step": 3, "description": "Check source data availability", "command": "SELECT COUNT(*) FROM {source_table} WHERE _ingested_at > NOW() - INTERVAL '2 hours'", "expected": "Confirm source has data"},
            {"step": 4, "description": "Check cluster resources", "command": "kubectl top pods -l app={pipeline_name}", "expected": "Memory/CPU within limits"},
        ],
        "resolution_steps": [
            {"step": 1, "description": "Fix the identified root cause (see investigation)"},
            {"step": 2, "description": "Trigger manual backfill: airflow dags backfill {dag_id} -s {start_date} -e {end_date}"},
            {"step": 3, "description": "Verify output: SELECT COUNT(*), MAX(_processed_at) FROM {target_table}"},
            {"step": 4, "description": "Update incident ticket with root cause and resolution"},
        ],
        "escalation": [{"level": "L1", "contact": "On-call DE", "timeout_min": 15}, {"level": "L2", "contact": "Senior DE", "timeout_min": 30}, {"level": "L3", "contact": "Engineering Manager", "timeout_min": 60}],
    },
    {
        "id": "rb-002", "title": "Data Quality Alert — Null Rate Spike",
        "category": "Data Quality",
        "overview": "Handle unexpected spike in null values in a key column",
        "alert_conditions": ["Null rate for {column} exceeds {threshold}% (baseline: {baseline}%)", "Great Expectations check failed: column.null_count > max_null_count"],
        "investigation_steps": [
            {"step": 1, "description": "Quantify the issue", "command": "SELECT COUNT(*) total, SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) nulls FROM {table} WHERE partition_date = CURRENT_DATE", "expected": "Get current null count"},
            {"step": 2, "description": "Check if issue is in source", "command": "SELECT COUNT(*) total, SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) nulls FROM {bronze_table} WHERE _ingested_at > NOW() - INTERVAL '24 hours'", "expected": "Determine if nulls originate in bronze (source issue) or appear in silver (transformation bug)"},
            {"step": 3, "description": "Check recent pipeline changes", "command": "SELECT * FROM pipeline_runs WHERE pipeline_id = '{id}' ORDER BY run_date DESC LIMIT 5", "expected": "Was there a code deployment recently?"},
        ],
        "resolution_steps": [
            {"step": 1, "description": "If source issue: notify upstream team, file data quality ticket"},
            {"step": 2, "description": "If transformation bug: revert pipeline to last known-good version"},
            {"step": 3, "description": "Re-run pipeline for affected partitions"},
        ],
        "escalation": [{"level": "L1", "contact": "On-call DE", "timeout_min": 30}, {"level": "L2", "contact": "Data Owner", "timeout_min": 60}],
    },
]

ONCALL_SCHEDULE = [
    {"week": "2024-W04", "primary": "Alice Johnson", "secondary": "Bob Chen", "escalation": "Carol Williams (Manager)"},
    {"week": "2024-W05", "primary": "Bob Chen", "secondary": "David Kim", "escalation": "Carol Williams (Manager)"},
    {"week": "2024-W06", "primary": "David Kim", "secondary": "Emma Davis", "escalation": "Carol Williams (Manager)"},
]


@router.get("/incidents/scenarios")
def list_scenarios():
    return {"scenarios": [{"id": s["id"], "title": s["title"], "severity": s["severity"], "category": s["category"]} for s in INCIDENT_SCENARIOS]}


@router.post("/incidents/trigger")
def trigger_incident(scenario_id: Optional[str] = None):
    if scenario_id:
        scenario = next((s for s in INCIDENT_SCENARIOS if s["id"] == scenario_id), None)
        if not scenario:
            raise HTTPException(404, "Scenario not found")
    else:
        scenario = random.choice(INCIDENT_SCENARIOS)
    return {"incident": scenario}


@router.get("/incidents/runbooks")
def list_runbooks():
    return {"runbooks": [{"id": r["id"], "title": r["title"], "category": r["category"], "overview": r["overview"]} for r in RUNBOOKS]}


@router.get("/incidents/runbooks/{runbook_id}")
def get_runbook(runbook_id: str):
    rb = next((r for r in RUNBOOKS if r["id"] == runbook_id), None)
    if not rb:
        raise HTTPException(404, "Runbook not found")
    return rb


class PostmortemRequest(BaseModel):
    incident_title: str
    timeline: str
    root_cause: str
    impact: str
    contributing_factors: List[str]
    action_items: List[str]


@router.post("/incidents/postmortem")
def generate_postmortem(req: PostmortemRequest):
    whys = [
        f"Why did this happen? {req.root_cause}",
        "Why wasn't it caught earlier? Monitoring thresholds were too coarse",
        "Why were the thresholds too coarse? Alert tuning process hasn't been run in 6 months",
        "Why hasn't alert tuning been done? No owner assigned, no recurring task",
        "Why is there no owner? On-call rotation doesn't include reliability improvements",
    ]
    doc = f"""# Postmortem: {req.incident_title}
**Date**: 2024-01-27 | **Severity**: P1 | **Status**: Resolved

## Impact
{req.impact}

## Timeline
{req.timeline}

## Root Cause
{req.root_cause}

## Contributing Factors
{chr(10).join(f'- {f}' for f in req.contributing_factors)}

## 5-Why Analysis
{chr(10).join(f'{i+1}. {w}' for i, w in enumerate(whys))}

## Action Items
| Action | Owner | Due Date |
|--------|-------|----------|
{chr(10).join(f'| {a} | TBD | TBD |' for a in req.action_items)}

## What Went Well
- On-call engineer responded within SLA
- Rollback procedure worked as documented
- Communication to stakeholders was timely

*This is a blameless postmortem. Individuals are not blamed — systems and processes are improved.*"""
    return {"postmortem_markdown": doc, "five_whys": whys}


@router.get("/incidents/oncall")
def get_oncall():
    return {"schedule": ONCALL_SCHEDULE, "current_week": ONCALL_SCHEDULE[0], "alert_routing": [
        {"alert": "Pipeline failure (P1)", "routing": "PagerDuty → Primary on-call", "escalation_timeout_min": 15},
        {"alert": "Data quality (P2)", "routing": "Slack #data-alerts → Primary on-call", "escalation_timeout_min": 30},
        {"alert": "Freshness warning (P3)", "routing": "Slack #data-alerts", "escalation_timeout_min": 60},
    ]}
