from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import random
import uuid

router = APIRouter()

# Module 24 — Data Versioning & Reproducibility

_dataset_versions = [
    {"id": "ds-v1", "dataset": "orders_training", "version": 1, "tag": None, "rows": 45200, "columns": 12, "size_mb": 38.4, "checksum": "sha256:a1b2c3d4...", "created_by": "alice", "created_at": "2024-01-01T00:00:00Z", "description": "Initial training dataset", "remote": "s3://ml-datasets/orders/v1/", "changes": {"rows_added": 45200, "rows_removed": 0, "columns_added": 12, "columns_removed": 0}},
    {"id": "ds-v2", "dataset": "orders_training", "version": 2, "tag": "model-v1-training", "rows": 52100, "columns": 12, "size_mb": 44.1, "checksum": "sha256:e5f6g7h8...", "created_by": "alice", "created_at": "2024-01-08T00:00:00Z", "description": "Added 2 weeks of new orders data", "remote": "s3://ml-datasets/orders/v2/", "changes": {"rows_added": 6900, "rows_removed": 0, "columns_added": 0, "columns_removed": 0}},
    {"id": "ds-v3", "dataset": "orders_training", "version": 3, "tag": None, "rows": 51800, "columns": 13, "size_mb": 46.2, "checksum": "sha256:i9j0k1l2...", "created_by": "bob", "created_at": "2024-01-15T00:00:00Z", "description": "Added churn_label column, removed 300 rows with null revenue", "remote": "s3://ml-datasets/orders/v3/", "changes": {"rows_added": 0, "rows_removed": 300, "columns_added": 1, "columns_removed": 0}},
    {"id": "ds-v4", "dataset": "orders_training", "version": 4, "tag": "model-v2-training", "rows": 68400, "columns": 13, "size_mb": 58.7, "checksum": "sha256:m3n4o5p6...", "created_by": "alice", "created_at": "2024-01-22T00:00:00Z", "description": "Extended training window to 6 months", "remote": "s3://ml-datasets/orders/v4/", "changes": {"rows_added": 16600, "rows_removed": 0, "columns_added": 0, "columns_removed": 0}},
]

_experiments = [
    {"id": "exp-001", "name": "churn-model-v1", "dataset_version": "ds-v2", "dataset_tag": "model-v1-training", "model": "XGBoost", "params": {"n_estimators": 100, "max_depth": 6, "learning_rate": 0.1}, "metrics": {"auc": 0.847, "precision": 0.721, "recall": 0.683, "f1": 0.701}, "created_at": "2024-01-09T10:00:00Z"},
    {"id": "exp-002", "name": "churn-model-v2", "dataset_version": "ds-v4", "dataset_tag": "model-v2-training", "model": "XGBoost", "params": {"n_estimators": 200, "max_depth": 8, "learning_rate": 0.05}, "metrics": {"auc": 0.891, "precision": 0.764, "recall": 0.741, "f1": 0.752}, "created_at": "2024-01-23T14:00:00Z"},
    {"id": "exp-003", "name": "churn-model-v2-lgbm", "dataset_version": "ds-v4", "dataset_tag": "model-v2-training", "model": "LightGBM", "params": {"n_estimators": 300, "num_leaves": 63, "learning_rate": 0.05}, "metrics": {"auc": 0.903, "precision": 0.789, "recall": 0.758, "f1": 0.773}, "created_at": "2024-01-24T09:00:00Z"},
]

DVC_CONCEPTS = {
    "dvc_file_structure": {
        "description": ".dvc files are small metadata files stored in Git. They point to actual data in remote storage.",
        "example_dvc_file": """# data/orders_training.csv.dvc
outs:
- md5: a1b2c3d4e5f6789012345678
  size: 40265472
  path: data/orders_training.csv
  remote: myremote""",
        "workflow": [
            "dvc add data/orders_training.csv  # Creates .dvc file, adds data/ to .gitignore",
            "git add data/orders_training.csv.dvc .gitignore",
            "git commit -m 'Add training dataset v1'",
            "dvc push  # Uploads actual data to S3/GCS/Azure",
        ],
        "benefit": "Git stores the .dvc metadata file (tiny). S3 stores the actual dataset. Anyone who clones the repo runs 'dvc pull' to download the data.",
    },
    "dvc_diff": {
        "command": "dvc diff HEAD~1 HEAD",
        "output": """
modified: data/orders_training.csv
    size: 40265472 -> 43417600 (+3152128 bytes, +7.8%)
    hash: a1b2c3d4... -> e5f6g7h8...

modified: data/features/customer_features.parquet
    size: 12845056 -> 14680064 (+1835008 bytes, +14.3%)""",
        "explanation": "Shows what data files changed between commits. File hashes ensure integrity.",
    },
    "dvc_repro": {
        "description": "dvc repro re-runs only the pipeline stages whose inputs have changed. Like make for data pipelines.",
        "pipeline_yaml": """stages:
  prepare:
    cmd: python src/prepare.py
    deps:
      - data/raw/orders.csv
    outs:
      - data/processed/orders_clean.csv

  featurize:
    cmd: python src/featurize.py
    deps:
      - data/processed/orders_clean.csv
    outs:
      - data/features/features.parquet
    params:
      - params.yaml:
          - featurize.window_days

  train:
    cmd: python src/train.py
    deps:
      - data/features/features.parquet
    metrics:
      - metrics.json""",
        "smart_caching": "If only 'train.py' changes, DVC re-runs only the train stage. It uses the cached output of 'featurize' — no wasted compute.",
    },
}

REPRODUCIBILITY_CHECKLIST = [
    {"id": "chk-01", "category": "Data", "item": "Pin exact dataset version with hash verification", "code": "dvc pull data/training.csv.dvc && dvc status --cloud", "importance": "Critical"},
    {"id": "chk-02", "category": "Dependencies", "item": "Pin all package versions in requirements.txt", "code": "pip freeze > requirements.txt  # Not just pandas, but ALL transitive deps", "importance": "Critical"},
    {"id": "chk-03", "category": "Environment", "item": "Use Docker image pinned by digest (not tag)", "code": "FROM python:3.11.4-slim@sha256:abc123...  # digest is immutable, tag is not", "importance": "High"},
    {"id": "chk-04", "category": "Randomness", "item": "Set all random seeds explicitly", "code": "random.seed(42); np.random.seed(42); torch.manual_seed(42)", "importance": "Critical"},
    {"id": "chk-05", "category": "Pipeline", "item": "Use DVC to track pipeline stages and outputs", "code": "dvc repro  # Only re-runs stages with changed inputs", "importance": "High"},
    {"id": "chk-06", "category": "Config", "item": "Externalize all config to params.yaml (not hardcoded)", "code": "params: {window_days: 90, min_revenue: 10.0}  # params.yaml", "importance": "Medium"},
    {"id": "chk-07", "category": "Logging", "item": "Log all parameters and metrics to experiment tracker", "code": "mlflow.log_params(params); mlflow.log_metrics(metrics)", "importance": "High"},
]


@router.get("/versioning/datasets")
def list_dataset_versions(dataset: str = "orders_training"):
    return {"dataset": dataset, "versions": _dataset_versions, "total": len(_dataset_versions)}


class CreateVersionRequest(BaseModel):
    dataset: str
    description: str
    tag: Optional[str] = None
    changes_description: str = ""


@router.post("/versioning/datasets")
def create_version(req: CreateVersionRequest):
    last = _dataset_versions[-1]
    r = random.Random(hash(req.description))
    new_rows = last["rows"] + r.randint(-500, 5000)
    vid = f"ds-v{last['version'] + 1}"
    version = {
        "id": vid, "dataset": req.dataset, "version": last["version"] + 1,
        "tag": req.tag, "rows": new_rows, "columns": last["columns"],
        "size_mb": round(new_rows * 44.1 / 52100, 1),
        "checksum": f"sha256:{str(uuid.uuid4()).replace('-', '')[:16]}...",
        "created_by": "api-user", "created_at": "2024-01-27T12:00:00Z",
        "description": req.description, "remote": f"s3://ml-datasets/{req.dataset}/v{last['version'] + 1}/",
        "changes": {"rows_added": max(0, new_rows - last["rows"]), "rows_removed": max(0, last["rows"] - new_rows), "columns_added": 0, "columns_removed": 0},
    }
    _dataset_versions.append(version)
    return version


@router.get("/versioning/diff")
def dataset_diff(from_version: str = "ds-v1", to_version: str = "ds-v2"):
    v1 = next((v for v in _dataset_versions if v["id"] == from_version), None)
    v2 = next((v for v in _dataset_versions if v["id"] == to_version), None)
    if not v1 or not v2:
        raise HTTPException(404, "Version not found")
    return {
        "from": {"version": v1["version"], "rows": v1["rows"], "checksum": v1["checksum"]},
        "to": {"version": v2["version"], "rows": v2["rows"], "checksum": v2["checksum"]},
        "changes": v2["changes"],
        "size_delta_mb": round(v2["size_mb"] - v1["size_mb"], 1),
        "compatible": v2["columns"] >= v1["columns"],
    }


class RollbackRequest(BaseModel):
    dataset: str
    target_version: str


@router.post("/versioning/rollback")
def rollback(req: RollbackRequest):
    version = next((v for v in _dataset_versions if v["id"] == req.target_version), None)
    if not version:
        raise HTTPException(404, "Version not found")
    return {
        "status": "rolled_back",
        "dataset": req.dataset,
        "active_version": version["version"],
        "checksum": version["checksum"],
        "remote": version["remote"],
        "command": f"dvc checkout {version['id']}.dvc && dvc pull",
    }


@router.get("/versioning/experiments")
def list_experiments():
    return {"experiments": _experiments}


@router.get("/versioning/dvc-concepts")
def get_dvc_concepts():
    return DVC_CONCEPTS


@router.get("/versioning/reproducibility-checklist")
def get_checklist():
    return {"checklist": REPRODUCIBILITY_CHECKLIST}
