from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Optional
import random

router = APIRouter()

# Module 26 — Senior Soft Skills & Leadership

TDD_TEMPLATE = {
    "title": "Technical Design Document Template",
    "sections": [
        {"name": "Problem Statement", "placeholder": "What problem are we solving? Why does it matter? What is the impact of not solving it?"},
        {"name": "Goals", "placeholder": "What does success look like? List 3-5 measurable goals. Example: Reduce pipeline latency from 4h to 30min."},
        {"name": "Non-Goals", "placeholder": "What are we explicitly NOT solving in this document? This prevents scope creep."},
        {"name": "Proposed Solution", "placeholder": "The recommended approach. Why was this chosen over alternatives?"},
        {"name": "Alternatives Considered", "placeholder": "What else was evaluated? Why was it rejected? Shows thoroughness."},
        {"name": "Data Model", "placeholder": "Table schemas, field definitions, relationships. Include sample data."},
        {"name": "API Design", "placeholder": "Endpoint definitions, request/response shapes, error codes."},
        {"name": "Risks & Mitigations", "placeholder": "What could go wrong? How will you handle each risk?"},
        {"name": "Rollout Plan", "placeholder": "How will this be deployed? Dark launch? Feature flag? Gradual rollout?"},
        {"name": "Success Metrics", "placeholder": "How will you know this worked? KPIs, dashboards, alerts."},
        {"name": "Open Questions", "placeholder": "Decisions not yet made. Who will decide? By when?"},
    ],
    "example": {
        "title": "TDD: Migrate from Airflow 1.x to Airflow 2.x",
        "problem": "Airflow 1.10 reaches EOL in December 2024. 47 DAGs currently running. No upgrade path to new features (TaskFlow API, dynamic task mapping).",
        "goals": ["Zero DAG failures during migration", "All 47 DAGs migrated within 6 weeks", "P99 scheduler latency < 2s (currently 8s in Airflow 1)", "Engineers trained on TaskFlow API"],
        "non_goals": ["Migrating to a different orchestrator (Prefect, Dagster) — evaluated in Q3, decided against", "Refactoring all DAG business logic (separate project)"],
        "proposed_solution": "Blue/green migration: run Airflow 2 in parallel, migrate DAGs one-by-one, validate outputs match, cut over domain by domain.",
        "risks": [{"risk": "DAG incompatibility", "likelihood": "Medium", "mitigation": "Run migration linter on all 47 DAGs week 1. Fix issues before migration sprint."}],
    },
}

COMMUNICATION_TEMPLATES = [
    {
        "id": "tmpl-incident", "title": "Data Incident Notification (Exec-Friendly)",
        "audience": "Executive stakeholders (non-technical)",
        "template": """Subject: [RESOLVED] Data Issue Affecting {Dashboard Name} — {Date}

We identified and resolved a data quality issue that affected the {Dashboard Name} dashboard today.

IMPACT:
- {Which reports/dashboards} showed incorrect data for {time period}
- Business impact: {describe in plain terms, e.g., "Revenue figures were inflated by approximately 15%"}

WHAT HAPPENED:
{2-3 sentences max, in plain English. No technical jargon.}

WHAT WE DID:
- At {time}, our monitoring system detected the issue
- Data was corrected by {time}
- All downstream reports have been updated with accurate numbers

WHAT CHANGES:
We have implemented {brief description of prevention measure} to prevent this from happening again.

Please feel free to reach out if you need any clarification.
{Your name} | Data Engineering Team""",
    },
    {
        "id": "tmpl-deprecation", "title": "Pipeline Deprecation Notice (With Migration Guide)",
        "audience": "Internal engineering consumers",
        "template": """Subject: [Action Required] {Pipeline Name} deprecated — migrate by {Date}

The {pipeline_name} pipeline will be decommissioned on {date} (in {N} weeks).

REPLACEMENT: {new_pipeline_name}
Docs: {link_to_docs}

WHY: {1-2 sentences on reason — e.g., "The new pipeline provides 10x better performance and supports Delta Lake time travel."}

MIGRATION STEPS:
1. Update your {config/dbt model/query} to use {new_table_name} instead of {old_table_name}
2. Schema changes: {list any column renames or type changes}
3. Test your pipeline against new data source by {test_deadline}

WHAT HAPPENS IF YOU DON'T MIGRATE:
{old_pipeline_name} will stop running on {date}. Any downstream jobs depending on it will fail.

NEED HELP?
Schedule a 30-min pairing session: {calendar_link}
Data Eng team Slack: #data-engineering""",
    },
    {
        "id": "tmpl-quality-report", "title": "Data Quality Issue Report",
        "audience": "Data owner + business stakeholders",
        "template": """Subject: [Data Quality] {table_name} — {issue_summary}

SUMMARY:
A data quality issue was identified in {table_name} affecting {affected_period}.

IMPACT:
- Rows affected: {N} rows ({pct}% of total)
- Affected columns: {column_names}
- Downstream affected: {list of dashboards/pipelines}
- Business impact: {business description}

ROOT CAUSE:
{1-2 paragraphs}

STATUS: {Resolved / In Progress / Under Investigation}
Fixed as of: {timestamp} (if resolved)

PREVENTION:
{What was added to prevent recurrence}

ACTION REQUIRED FROM DATA OWNER:
{Any validation or sign-off needed from data owner}""",
    },
    {
        "id": "tmpl-new-dataset", "title": "New Dataset Announcement",
        "audience": "Data consumers across the organization",
        "template": """Subject: [New] {dataset_name} now available in the Data Warehouse

We've published a new dataset that may be useful to your team.

DATASET: {catalog_link} — {one_sentence_description}

KEY FIELDS:
{field_name} — {description}
{field_name} — {description}

USE CASES:
- {use_case_1}
- {use_case_2}

HOW TO USE:
SQL: SELECT * FROM {table_name} WHERE {example_filter}
Refresh: {schedule}
SLA: {freshness_guarantee}

OWNER: {owner_name} | {owner_team} | {slack_channel}

Questions? Reply to this email or post in #data-help.""",
    },
]

INTERVIEW_QUESTIONS = [
    {"id": "iq-01", "category": "System Design", "question": "Design a real-time fraud detection pipeline for a payment processor handling 10,000 transactions per second.", "key_components": ["Kafka for ingestion (low-latency, durable)", "Stream processor (Flink/Spark Streaming) for feature computation", "Feature store (Redis) for sub-5ms feature lookup", "ML model serving (ONNX or dedicated serving layer)", "Alert routing (PagerDuty/Kafka topic for fraud alerts)", "Feedback loop: confirmed fraud → retraining pipeline"], "trade_offs": ["Exactly-once vs at-least-once semantics", "Latency vs accuracy (simpler model = faster, complex model = better accuracy)", "False positive rate impact on customer experience"], "follow_ups": ["How do you handle model drift?", "What happens when Redis goes down?", "How do you backtest the model with historical data?"]},
    {"id": "iq-02", "category": "System Design", "question": "Design Twitter's trending topics system. Topics should update every 5 minutes based on last 24 hours.", "key_components": ["Kafka for tweet stream ingestion", "Spark Streaming with 5-min micro-batches", "Sliding window aggregation (24h window, 5min slide)", "HyperLogLog for unique user counts (not just tweet count)", "Redis Sorted Set for top-N trending topics", "CDN + cache for API serving (trending doesn't change per-request)"], "trade_offs": ["Approximate unique counts (HLL) vs exact counts (too much memory)", "Global trending vs geo-based trending", "Real-time vs batch (trade-off: latency vs cost)"], "follow_ups": ["How do you handle trending manipulation?", "How do you make it work per-country?"]},
    {"id": "iq-03", "category": "System Design", "question": "Design a data platform for a company with 1 billion daily events from mobile apps.", "key_components": ["Kafka with 100+ partitions for ingest", "Exactly-once Kafka → Delta Lake pipeline", "Medallion architecture (Bronze/Silver/Gold)", "Apache Iceberg or Delta for ACID + time travel", "Data catalog (Apache Atlas or Databricks Unity Catalog)", "Governance layer (column masking, row-level security)", "Query layer: Trino for ad-hoc, ClickHouse for product analytics"], "trade_offs": ["Centralized vs federated (data mesh) ownership", "Storage cost vs query performance (compression vs partitioning strategy)", "Batch vs streaming for gold layer aggregations"], "follow_ups": ["How do you handle late-arriving data?", "How do you implement GDPR right-to-be-forgotten at 1B events/day?"]},
    {"id": "iq-04", "category": "System Design", "question": "Design a data warehouse migration from on-premises Oracle to Snowflake.", "key_components": ["Assessment: catalog 500+ tables, identify dependencies with lineage tool", "CDC (Debezium) for live sync during migration", "Parallel run: Oracle and Snowflake running simultaneously for validation", "dbt for transformation layer rebuild (replace Oracle stored procedures)", "Data validation framework: row counts, checksums, business metrics", "Cutover plan: feature flags in BI tools to switch data source"], "trade_offs": ["Big bang vs phased migration (risk vs speed)", "Lift-and-shift vs redesign (speed vs technical debt)", "Downtime window vs zero-downtime migration"], "follow_ups": ["How do you handle Oracle-specific SQL syntax?", "How do you validate numeric precision differences?"]},
    {"id": "iq-05", "category": "Architecture", "question": "Your company's Airflow scheduler is taking 30 seconds to schedule tasks. How do you diagnose and fix this?", "key_components": ["Check scheduler heartbeat interval (default 1s — increase if many DAGs)", "Profile DAG parsing: dags > 1s to parse slow down scheduler", "Reduce number of active DAGs (archive unused)", "Use DAG serialization (Airflow 2.x — store DAGs in DB not filesystem)", "Scale scheduler horizontally (Airflow 2.x supports multiple schedulers)", "Executor: switch from LocalExecutor to CeleryExecutor or KubernetesExecutor"], "follow_ups": ["When would you switch from Airflow to Prefect or Dagster?"]},
]

CAREER_LADDER = [
    {"level": "L3", "title": "Junior Data Engineer", "years_typical": "0-2", "impact_scope": "Individual tasks", "autonomy": "Directed — given clear tasks with expected outcomes", "technical": ["Writes SQL and Python pipelines with guidance", "Operates existing pipelines", "Fixes known bug patterns"], "leadership": ["No people management", "Participates in code reviews"], "promotion_criteria": ["Can complete well-defined tasks independently", "Understands the data stack end-to-end", "Writes clean, tested code"]},
    {"level": "L4", "title": "Data Engineer", "years_typical": "2-5", "impact_scope": "Team — owns multiple pipelines", "autonomy": "Independent — given goals, determines approach", "technical": ["Designs end-to-end pipelines", "Handles production incidents independently", "Writes and reviews TDDs"], "leadership": ["Mentors interns/junior engineers", "Leads features within a sprint"], "promotion_criteria": ["Designs and delivers complete features without guidance", "Proactively identifies and fixes infrastructure issues", "Reliable on-call responder"]},
    {"level": "L5", "title": "Senior Data Engineer", "years_typical": "5-8", "impact_scope": "Team + cross-team — drives technical direction", "autonomy": "Self-directed — identifies what needs to be done", "technical": ["Architects multi-team data systems", "Sets technical standards and patterns", "Owns platform reliability"], "leadership": ["Mentors L3/L4 engineers", "Drives cross-team technical decisions", "Writes RFCs/TDDs adopted company-wide"], "promotion_criteria": ["Multiplies team output (not just own output)", "Designs systems other teams depend on", "Recognized as go-to expert in 2+ domains"]},
    {"level": "L6", "title": "Staff Data Engineer", "years_typical": "8-12", "impact_scope": "Organization — shapes data strategy", "autonomy": "Defines direction — identifies company-level technical needs", "technical": ["Defines data architecture for the company", "Evaluates and selects major technology investments", "Drives data platform as a product"], "leadership": ["Influences org-wide engineering practices", "Partners with VPs on technical roadmap", "Grows senior engineers"], "promotion_criteria": ["Organization-level technical impact", "Drives major initiatives that span multiple teams", "Seen as thought leader internally and externally"]},
    {"level": "Staff/Principal", "title": "Principal / Distinguished Engineer", "years_typical": "12+", "impact_scope": "Industry — shapes how data engineering is done", "autonomy": "Creates direction — defines industry patterns", "technical": ["Technical strategy across business units", "Open source contributions adopted industry-wide", "Public thought leadership"], "leadership": ["Trusted advisor to CTO/CEO", "Defines engineering culture", "Represents company at conferences"], "promotion_criteria": ["Industry-level impact", "Named in external publications/talks", "Multiple company-defining technical decisions"]},
]

ASSESSMENT_DIMENSIONS = [
    {"id": "a01", "category": "Technical", "dimension": "SQL & Query Optimization", "description": "Write efficient queries, explain plans, optimize for cost and performance"},
    {"id": "a02", "category": "Technical", "dimension": "Pipeline Architecture", "description": "Design end-to-end batch and streaming pipelines with appropriate trade-offs"},
    {"id": "a03", "category": "Technical", "dimension": "Data Modeling", "description": "Design dimensional models, understand normalization vs denormalization trade-offs"},
    {"id": "a04", "category": "Technical", "dimension": "Distributed Systems", "description": "Understand Spark internals, partitioning, and performance tuning"},
    {"id": "a05", "category": "Technical", "dimension": "Data Quality", "description": "Design quality checks, handle schema changes, implement SLA monitoring"},
    {"id": "a06", "category": "Technical", "dimension": "Cloud Platforms", "description": "AWS/GCP/Azure native services, cost optimization, IaC"},
    {"id": "a07", "category": "Technical", "dimension": "Streaming & CDC", "description": "Kafka, exactly-once semantics, watermarks, Debezium patterns"},
    {"id": "a08", "category": "Technical", "dimension": "Security & Governance", "description": "PII handling, RBAC, compliance, audit trails"},
    {"id": "a09", "category": "Leadership", "dimension": "Technical Communication", "description": "Write TDDs, communicate incidents, present to stakeholders"},
    {"id": "a10", "category": "Leadership", "dimension": "Mentoring", "description": "Help junior engineers grow, give constructive feedback"},
    {"id": "a11", "category": "Leadership", "dimension": "Project Planning", "description": "Estimate work accurately, identify risks, manage dependencies"},
    {"id": "a12", "category": "Leadership", "dimension": "Influence Without Authority", "description": "Drive adoption of standards across teams without direct reports"},
]


@router.get("/leadership/tdd-template")
def get_tdd_template():
    return TDD_TEMPLATE


@router.get("/leadership/communication-templates")
def get_communication_templates():
    return {"templates": COMMUNICATION_TEMPLATES}


@router.get("/leadership/communication-templates/{template_id}")
def get_template(template_id: str):
    tmpl = next((t for t in COMMUNICATION_TEMPLATES if t["id"] == template_id), None)
    if not tmpl:
        from fastapi import HTTPException
        raise HTTPException(404, "Template not found")
    return tmpl


class EstimateRequest(BaseModel):
    project_name: str = "Build new data pipeline"
    tasks: List[str]


@router.post("/leadership/estimate")
def estimate_project(req: EstimateRequest):
    r = random.Random(hash(req.project_name))
    task_estimates = []
    for task in req.tasks:
        optimistic = r.randint(1, 3)
        realistic = r.randint(optimistic + 1, optimistic + 5)
        pessimistic = r.randint(realistic + 2, realistic + 8)
        expected = round((optimistic + 4 * realistic + pessimistic) / 6, 1)
        task_estimates.append({"task": task, "optimistic_days": optimistic, "realistic_days": realistic, "pessimistic_days": pessimistic, "expected_days": expected})

    total_expected = sum(t["expected_days"] for t in task_estimates)
    risk_buffer = round(total_expected * 0.25, 1)

    return {
        "project": req.project_name,
        "task_estimates": task_estimates,
        "total_expected_days": round(total_expected, 1),
        "risk_buffer_days": risk_buffer,
        "total_with_buffer_days": round(total_expected + risk_buffer, 1),
        "formula_explanation": "Expected = (Optimistic + 4×Realistic + Pessimistic) ÷ 6 (PERT estimation). +25% risk buffer for unknowns.",
    }


@router.get("/leadership/interview-questions")
def get_interview_questions(category: Optional[str] = None):
    qs = INTERVIEW_QUESTIONS
    if category:
        qs = [q for q in qs if q["category"].lower() == category.lower()]
    return {"questions": qs, "total": len(qs)}


@router.get("/leadership/career-ladder")
def get_career_ladder():
    return {"ladder": CAREER_LADDER}


@router.get("/leadership/assessment-dimensions")
def get_assessment_dimensions():
    return {"dimensions": ASSESSMENT_DIMENSIONS}


class SelfAssessmentRequest(BaseModel):
    scores: dict  # {dimension_id: score 1-5}


@router.post("/leadership/self-assessment")
def self_assessment(req: SelfAssessmentRequest):
    valid_dims = {d["id"] for d in ASSESSMENT_DIMENSIONS}
    scored = {k: v for k, v in req.scores.items() if k in valid_dims and 1 <= int(v) <= 5}
    avg = sum(scored.values()) / len(scored) if scored else 0

    strengths = [ASSESSMENT_DIMENSIONS[[d["id"] for d in ASSESSMENT_DIMENSIONS].index(k)]["dimension"] for k, v in scored.items() if v >= 4]
    gaps = [ASSESSMENT_DIMENSIONS[[d["id"] for d in ASSESSMENT_DIMENSIONS].index(k)]["dimension"] for k, v in scored.items() if v <= 2]

    level = "L3" if avg < 2.5 else ("L4" if avg < 3.5 else ("L5" if avg < 4.2 else "L6"))

    return {
        "average_score": round(avg, 2),
        "estimated_level": level,
        "strengths": strengths[:5],
        "development_areas": gaps[:5],
        "next_steps": [f"Deep dive: {g} — target 1 project or course in next 30 days" for g in gaps[:3]],
    }
