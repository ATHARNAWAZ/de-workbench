from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import random

router = APIRouter()

# Module 13 — CI/CD for Data Pipelines

PIPELINE_STAGES = [
    {"id": "commit", "name": "Code Commit", "description": "Developer pushes code to feature branch", "duration_s": 2, "checks": ["Pre-commit hooks: black, isort, flake8"], "icon": "git-commit"},
    {"id": "lint", "name": "Lint & Format", "description": "Static analysis and code style enforcement", "duration_s": 45, "checks": ["flake8 (PEP8 compliance)", "black (formatting)", "isort (import ordering)", "mypy (type hints)"], "tools": ["flake8", "black", "isort", "mypy"]},
    {"id": "unit", "name": "Unit Tests", "description": "Test individual transformation functions with mocked data", "duration_s": 120, "checks": ["pytest: 47 unit tests", "coverage > 80%", "mock all external DB calls", "assert transformation logic"], "tools": ["pytest", "pytest-mock", "coverage"]},
    {"id": "integration", "name": "Integration Tests", "description": "Test full pipeline end-to-end against test database", "duration_s": 300, "checks": ["Pipeline run on 1,000-row test DB", "Assert output schema matches contract", "Assert row counts within tolerance", "Test idempotency (run twice = same result)"], "tools": ["pytest", "testcontainers", "SQLite test DB"]},
    {"id": "staging", "name": "Staging Deploy", "description": "Deploy pipeline to staging environment", "duration_s": 180, "checks": ["Docker build & push", "Deploy to staging cluster", "Smoke test: 100-row run", "Validate environment configs"], "environment": "staging"},
    {"id": "quality_gate", "name": "Data Quality Gate", "description": "Validate output data quality before production", "duration_s": 240, "checks": ["Great Expectations suite (12 expectations)", "Schema contract validation", "Row count within ±5% of expected", "Zero null violations on NOT NULL columns", "No referential integrity errors"], "tools": ["Great Expectations", "dbt tests", "custom assertions"]},
    {"id": "production", "name": "Production Deploy", "description": "Blue/green deploy to production with rollback capability", "duration_s": 120, "checks": ["Blue/green deployment", "Health check endpoint responds 200", "Canary: 5% traffic to new version", "Automated rollback if error rate > 1%"], "environment": "production"},
]

TEST_SUITES = {
    "unit": {
        "name": "Unit Tests", "description": "Test individual transformation functions with mock input/output",
        "tests": [
            {"id": "u1", "name": "test_revenue_calculation", "description": "revenue = price * quantity for all rows"},
            {"id": "u2", "name": "test_null_filter", "description": "Rows with null order_id are removed"},
            {"id": "u3", "name": "test_date_parsing", "description": "ISO8601 dates correctly parsed to datetime"},
            {"id": "u4", "name": "test_region_trim", "description": "Leading/trailing whitespace stripped from region"},
            {"id": "u5", "name": "test_deduplication", "description": "Duplicate order_ids removed, latest record kept"},
            {"id": "u6", "name": "test_negative_revenue_rejected", "description": "Negative revenue values flagged as invalid"},
        ],
    },
    "integration": {
        "name": "Integration Tests", "description": "Test full pipeline end-to-end against a test database",
        "tests": [
            {"id": "i1", "name": "test_bronze_ingestion", "description": "1,000 rows ingested into bronze table correctly"},
            {"id": "i2", "name": "test_silver_cleaning", "description": "Silver output matches expected cleaned schema"},
            {"id": "i3", "name": "test_gold_aggregation", "description": "Gold revenue totals match source data sum"},
            {"id": "i4", "name": "test_pipeline_idempotency", "description": "Running pipeline twice produces identical output"},
            {"id": "i5", "name": "test_partition_creation", "description": "Daily partitions created for each date in input"},
        ],
    },
    "contract": {
        "name": "Data Contract Tests", "description": "Validate output schema matches agreed contract exactly",
        "tests": [
            {"id": "c1", "name": "test_required_columns_present", "description": "All 12 required columns present in output"},
            {"id": "c2", "name": "test_column_types_match", "description": "Column types match contract definitions"},
            {"id": "c3", "name": "test_nullable_constraints", "description": "Non-nullable columns have zero nulls"},
            {"id": "c4", "name": "test_revenue_non_negative", "description": "Revenue values are all >= 0"},
            {"id": "c5", "name": "test_status_enum_values", "description": "Status field only contains allowed enum values"},
        ],
    },
    "regression": {
        "name": "Regression Tests", "description": "Compare current run output against golden dataset baseline",
        "tests": [
            {"id": "r1", "name": "test_row_count_regression", "description": "Row count within ±5% of baseline 9,891 rows"},
            {"id": "r2", "name": "test_revenue_sum_regression", "description": "Total revenue within ±0.1% of $2,847,301 baseline"},
            {"id": "r3", "name": "test_category_distribution", "description": "Category breakdown matches baseline proportions"},
            {"id": "r4", "name": "test_null_rate_regression", "description": "Null rates have not increased vs baseline"},
            {"id": "r5", "name": "test_top_customers_unchanged", "description": "Top 10 customers by revenue unchanged"},
        ],
    },
}

BRANCHING_STRATEGY = {
    "branches": [
        {"name": "feature/add-revenue-column", "type": "feature", "created_from": "dev", "status": "open", "commits": 3, "author": "alice@company.com"},
        {"name": "dev", "type": "dev", "description": "Integration branch — all features merged here first", "protection": "1 required review", "env": {"row_limit": 1000, "db": "dev-db", "schedule": "manual"}},
        {"name": "staging", "type": "staging", "description": "Pre-production — mirrors production config except data subset", "protection": "2 required reviews + CI pass", "env": {"row_limit": 100000, "db": "staging-db", "schedule": "on_merge"}},
        {"name": "main", "type": "production", "description": "Production branch — protected, requires 2 approvals", "protection": "2 required reviews + CI pass + manager approval", "env": {"row_limit": "UNLIMITED", "db": "prod-db", "schedule": "daily_02:00_UTC"}},
    ],
    "env_configs": {
        "dev": {"DATA_SOURCE": "s3://dev-bucket/sample/", "ROW_LIMIT": "1000", "DB_URL": "postgresql://dev-db/analytics", "NOTIFICATIONS": "none", "LOG_LEVEL": "DEBUG"},
        "staging": {"DATA_SOURCE": "s3://staging-bucket/", "ROW_LIMIT": "100000", "DB_URL": "postgresql://staging-db/analytics", "NOTIFICATIONS": "slack-data-eng", "LOG_LEVEL": "INFO"},
        "production": {"DATA_SOURCE": "s3://prod-bucket/", "ROW_LIMIT": "UNLIMITED", "DB_URL": "postgresql://prod-db/analytics", "NOTIFICATIONS": "pagerduty+slack", "LOG_LEVEL": "WARNING"},
    },
    "blue_green": {
        "description": "Run old and new pipeline versions in parallel. Compare outputs before switching traffic.",
        "steps": [
            "Deploy new pipeline version as 'green' (old version = 'blue' stays live)",
            "Route 0% traffic to green — run both on same inputs in parallel",
            "Compare outputs: row counts, revenue totals, checksums, business metrics",
            "If diff < 0.1%: gradually shift traffic 5% → 25% → 100% to green",
            "If diff > threshold: alert on-call, keep blue running, rollback green automatically",
            "Once green handles 100% traffic: decommission blue after 24h monitoring window",
        ],
    },
}


@router.get("/cicd/pipeline-stages")
def get_pipeline_stages():
    return {"stages": PIPELINE_STAGES}


class RunStageRequest(BaseModel):
    stage_id: str
    force_fail: bool = False


@router.post("/cicd/run-stage")
def run_stage(req: RunStageRequest):
    stage = next((s for s in PIPELINE_STAGES if s["id"] == req.stage_id), None)
    if not stage:
        raise HTTPException(404, "Stage not found")

    r = random.Random(hash(req.stage_id + str(req.force_fail)))
    if req.force_fail:
        failed_check = r.choice(stage["checks"])
        logs = [
            f"[00:00] Starting {stage['name']}...",
            f"[00:02] Installing dependencies...",
            f"[00:05] Running: {failed_check}",
            f"[00:08] ERROR: Check failed — {failed_check}",
            f"[00:08] Exit code: 1 — Stage FAILED",
        ]
        status = "failed"
    else:
        logs = [f"[00:00] Starting {stage['name']}..."]
        for i, check in enumerate(stage["checks"], 1):
            logs.append(f"[00:{i*2:02d}] Running: {check}")
            logs.append(f"[00:{i*2+1:02d}] PASSED: {check}")
        logs.append(f"[00:{len(stage['checks'])*2+2:02d}] All checks passed. Duration: {stage['duration_s']}s")
        status = "passed"

    return {"stage_id": req.stage_id, "status": status, "duration_s": stage["duration_s"], "logs": logs}


@router.get("/cicd/test-suites")
def get_test_suites():
    return {"suites": TEST_SUITES}


class RunTestsRequest(BaseModel):
    suite: str


@router.post("/cicd/run-tests")
def run_tests(req: RunTestsRequest):
    suite = TEST_SUITES.get(req.suite)
    if not suite:
        raise HTTPException(404, "Test suite not found")

    results = []
    passed = failed = 0
    for test in suite["tests"]:
        r = random.Random(hash(test["id"] + req.suite + "v2"))
        outcome = "passed" if r.random() > 0.12 else "failed"
        duration_ms = r.randint(40, 600)
        error_msg = None
        if outcome == "failed":
            error_msg = f"AssertionError: {test['description']} — actual value did not match expected"
        results.append({**test, "status": outcome, "duration_ms": duration_ms, "error": error_msg})
        if outcome == "passed":
            passed += 1
        else:
            failed += 1

    return {
        "suite": req.suite,
        "suite_name": suite["name"],
        "total": len(results),
        "passed": passed,
        "failed": failed,
        "coverage": f"{random.randint(78, 94)}%",
        "duration_ms": sum(r["duration_ms"] for r in results),
        "results": results,
    }


@router.get("/cicd/branching-strategy")
def get_branching_strategy():
    return BRANCHING_STRATEGY


class GenerateYAMLRequest(BaseModel):
    steps: List[str]
    trigger_branch: str = "main"
    python_version: str = "3.11"
    use_dbt: bool = False
    use_great_expectations: bool = True


@router.post("/cicd/generate-yaml")
def generate_yaml(req: GenerateYAMLRequest):
    steps_yaml = ""
    if "lint" in req.steps:
        steps_yaml += """
      - name: Lint & Format Check
        run: |
          pip install black isort flake8 mypy
          black --check .
          isort --check .
          flake8 . --max-line-length=120
          mypy pipeline/ --ignore-missing-imports
"""
    if "unit" in req.steps:
        steps_yaml += """
      - name: Unit Tests
        run: |
          pip install pytest pytest-cov pytest-mock
          pytest tests/unit/ -v --cov=pipeline --cov-report=xml
          python -m coverage report --fail-under=80
"""
    if "integration" in req.steps:
        steps_yaml += """
      - name: Integration Tests
        run: |
          pytest tests/integration/ -v --tb=short
        env:
          TEST_DB_URL: sqlite:///test.db
          ROW_LIMIT: "1000"
"""
    if req.use_dbt:
        steps_yaml += """
      - name: dbt Tests
        run: |
          pip install dbt-core dbt-postgres
          dbt deps
          dbt run --target ci
          dbt test --target ci
"""
    if req.use_great_expectations:
        steps_yaml += """
      - name: Data Quality Gate (Great Expectations)
        run: |
          pip install great_expectations
          great_expectations checkpoint run orders_quality_suite
"""
    yaml_content = f"""name: Data Pipeline CI/CD

on:
  push:
    branches: [ {req.trigger_branch}, dev, staging ]
  pull_request:
    branches: [ {req.trigger_branch} ]

jobs:
  pipeline-ci:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python {req.python_version}
        uses: actions/setup-python@v4
        with:
          python-version: '{req.python_version}'

      - name: Cache pip dependencies
        uses: actions/cache@v3
        with:
          path: ~/.cache/pip
          key: ${{{{ runner.os }}}}-pip-${{{{ hashFiles('requirements.txt') }}}}
{steps_yaml}
      - name: Upload test results
        uses: actions/upload-artifact@v3
        if: always()
        with:
          name: test-results
          path: |
            coverage.xml
            test-results.xml

  deploy-staging:
    needs: pipeline-ci
    if: github.ref == 'refs/heads/staging'
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - uses: actions/checkout@v4
      - name: Deploy to Staging
        run: |
          docker build -t pipeline:staging .
          docker push $REGISTRY/pipeline:staging
          kubectl apply -f k8s/staging/

  deploy-production:
    needs: pipeline-ci
    if: github.ref == 'refs/heads/{req.trigger_branch}'
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4
      - name: Blue/Green Deploy to Production
        run: |
          docker build -t pipeline:${{{{ github.sha }}}} .
          docker push $REGISTRY/pipeline:${{{{ github.sha }}}}
          ./scripts/blue_green_deploy.sh ${{{{ github.sha }}}}
"""
    return {"yaml": yaml_content, "filename": ".github/workflows/pipeline.yml"}
