from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict
import random

router = APIRouter()

# Module 19 — Infrastructure as Code (IaC)

TERRAFORM_MODULES = [
    {
        "id": "mod-s3", "name": "S3 Data Lake Bucket", "provider": "aws",
        "description": "S3 bucket with versioning, encryption (AES-256), lifecycle rules, and access logging",
        "variables": [
            {"name": "bucket_name", "type": "string", "description": "Globally unique bucket name", "default": "my-data-lake"},
            {"name": "environment", "type": "string", "description": "Deployment environment", "default": "dev"},
            {"name": "retention_days", "type": "number", "description": "Glacier transition after N days", "default": 90},
            {"name": "enable_versioning", "type": "bool", "description": "Enable S3 versioning", "default": True},
        ],
        "resources_created": ["aws_s3_bucket", "aws_s3_bucket_versioning", "aws_s3_bucket_server_side_encryption_configuration", "aws_s3_bucket_lifecycle_configuration", "aws_s3_bucket_logging"],
    },
    {
        "id": "mod-glue", "name": "AWS Glue Database + Crawler", "provider": "aws",
        "description": "Glue database, crawler with S3 target, and IAM role for crawling",
        "variables": [
            {"name": "database_name", "type": "string", "description": "Glue catalog database name", "default": "analytics"},
            {"name": "s3_target_path", "type": "string", "description": "S3 path for crawler to discover", "default": "s3://my-data-lake/silver/"},
            {"name": "schedule", "type": "string", "description": "Cron schedule for crawler", "default": "cron(0 6 * * ? *)"},
        ],
        "resources_created": ["aws_glue_catalog_database", "aws_glue_crawler", "aws_iam_role", "aws_iam_role_policy_attachment"],
    },
    {
        "id": "mod-redshift", "name": "Redshift Cluster", "provider": "aws",
        "description": "Redshift cluster with subnet group, security group, and parameter group",
        "variables": [
            {"name": "cluster_identifier", "type": "string", "description": "Unique cluster ID", "default": "analytics-warehouse"},
            {"name": "node_type", "type": "string", "description": "Node type (ra3.xlplus recommended)", "default": "ra3.xlplus"},
            {"name": "number_of_nodes", "type": "number", "description": "Number of compute nodes", "default": 2},
            {"name": "master_username", "type": "string", "description": "Admin username", "default": "admin"},
            {"name": "database_name", "type": "string", "description": "Initial database name", "default": "analytics"},
        ],
        "resources_created": ["aws_redshift_cluster", "aws_redshift_subnet_group", "aws_security_group", "aws_redshift_parameter_group"],
    },
    {
        "id": "mod-databricks", "name": "Databricks Workspace (AWS)", "provider": "databricks/aws",
        "description": "Databricks workspace with VPC, cross-account IAM role, S3 root storage",
        "variables": [
            {"name": "workspace_name", "type": "string", "description": "Workspace display name", "default": "analytics-workspace"},
            {"name": "region", "type": "string", "description": "AWS region", "default": "us-east-1"},
            {"name": "tier", "type": "string", "description": "Pricing tier (standard/premium/enterprise)", "default": "premium"},
        ],
        "resources_created": ["databricks_mws_workspaces", "databricks_mws_networks", "databricks_mws_storage_configurations", "databricks_mws_credentials", "aws_vpc", "aws_iam_role"],
    },
    {
        "id": "mod-snowflake", "name": "Snowflake Data Warehouse", "provider": "snowflake",
        "description": "Snowflake database, schema, warehouse, user, and role with privilege grants",
        "variables": [
            {"name": "database_name", "type": "string", "description": "Snowflake database name", "default": "ANALYTICS"},
            {"name": "warehouse_name", "type": "string", "description": "Virtual warehouse name", "default": "ANALYTICS_WH"},
            {"name": "warehouse_size", "type": "string", "description": "Warehouse size", "default": "SMALL"},
            {"name": "role_name", "type": "string", "description": "Role for data engineers", "default": "DATA_ENGINEER"},
        ],
        "resources_created": ["snowflake_database", "snowflake_schema", "snowflake_warehouse", "snowflake_role", "snowflake_user", "snowflake_grant_privileges_to_role"],
    },
    {
        "id": "mod-kinesis", "name": "Kinesis Data Stream", "provider": "aws",
        "description": "Kinesis stream with enhanced fan-out and CloudWatch alarms",
        "variables": [
            {"name": "stream_name", "type": "string", "description": "Stream name", "default": "orders-stream"},
            {"name": "shard_count", "type": "number", "description": "Number of shards (1 shard = 1MB/s write, 2MB/s read)", "default": 4},
            {"name": "retention_hours", "type": "number", "description": "Data retention period in hours", "default": 168},
        ],
        "resources_created": ["aws_kinesis_stream", "aws_kinesis_stream_consumer", "aws_cloudwatch_metric_alarm"],
    },
]

TF_TEMPLATES = {
    "mod-s3": '''resource "aws_s3_bucket" "{bucket_name}" {{
  bucket = "{bucket_name}-{environment}"

  tags = {{
    Environment = "{environment}"
    ManagedBy   = "terraform"
  }}
}}

resource "aws_s3_bucket_versioning" "{bucket_name}_versioning" {{
  bucket = aws_s3_bucket.{bucket_name}.id
  versioning_configuration {{
    status = {enable_versioning_str}
  }}
}}

resource "aws_s3_bucket_server_side_encryption_configuration" "{bucket_name}_encryption" {{
  bucket = aws_s3_bucket.{bucket_name}.id
  rule {{
    apply_server_side_encryption_by_default {{
      sse_algorithm = "AES256"
    }}
  }}
}}

resource "aws_s3_bucket_lifecycle_configuration" "{bucket_name}_lifecycle" {{
  bucket = aws_s3_bucket.{bucket_name}.id
  rule {{
    id     = "glacier_transition"
    status = "Enabled"
    transition {{
      days          = {retention_days}
      storage_class = "GLACIER_IR"
    }}
  }}
}}''',

    "mod-snowflake": '''resource "snowflake_database" "this" {{
  name    = "{database_name}"
  comment = "Managed by Terraform"
}}

resource "snowflake_schema" "analytics" {{
  database = snowflake_database.this.name
  name     = "ANALYTICS"
}}

resource "snowflake_warehouse" "this" {{
  name           = "{warehouse_name}"
  warehouse_size = "{warehouse_size}"
  auto_suspend   = 120
  auto_resume    = true
}}

resource "snowflake_role" "data_engineer" {{
  name = "{role_name}"
}}

resource "snowflake_grant_privileges_to_role" "schema_grants" {{
  privileges = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  role_name  = snowflake_role.data_engineer.name
  on_schema {{
    schema_name = "\\"{database_name}\\".\\"{snowflake_schema.analytics.name}\\""
  }}
}}''',
}

GITOPS_FLOW = {
    "steps": [
        {"order": 1, "name": "Feature Branch", "description": "Engineer creates feature/add-kinesis-stream branch from main"},
        {"order": 2, "name": "Local Plan", "description": "terraform init && terraform plan -var-file=dev.tfvars > plan.txt"},
        {"order": 3, "name": "Pull Request", "description": "Open PR with plan output as comment. Team reviews changes."},
        {"order": 4, "name": "CI: terraform plan", "description": "GitHub Actions runs terraform plan automatically. Plan posted as PR comment."},
        {"order": 5, "name": "Approval & Merge", "description": "2 approvals required. Merge to main triggers apply."},
        {"order": 6, "name": "CD: terraform apply", "description": "GitHub Actions runs terraform apply on merge. State locked via DynamoDB."},
        {"order": 7, "name": "Drift Detection", "description": "Nightly job runs terraform plan. Alerts if state drifts from real infra."},
    ],
    "state_management": {
        "local": {"description": "terraform.tfstate stored locally", "problem": "Cannot share with team. Lost if disk fails. No locking = concurrent apply = corruption."},
        "remote_s3": {
            "description": "State stored in S3, lock in DynamoDB",
            "config": '''terraform {{
  backend "s3" {{
    bucket         = "my-terraform-state"
    key            = "data-platform/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-state-lock"
    encrypt        = true
  }}
}}''',
            "benefits": ["Team can share state", "Encryption at rest", "DynamoDB lock prevents concurrent applies", "Versioned — can roll back"],
        },
    },
}

SECRETS_DEMO = {
    "vault": {
        "description": "HashiCorp Vault — dynamic secrets, PKI, encryption as a service",
        "patterns": [
            {
                "name": "Dynamic DB Credentials",
                "description": "Vault generates a unique DB username/password per request. Credentials auto-expire after TTL.",
                "request": "vault read database/creds/data-engineer-role",
                "response": {"username": "v-data-eng-abc123", "password": "A1b2C3d4-auto-rotated", "lease_duration": "1h", "renewable": True},
                "benefit": "No shared passwords. Audit log per request. Compromise is isolated to one TTL window.",
            },
            {
                "name": "AWS Dynamic Credentials",
                "description": "Vault generates temporary AWS IAM credentials scoped to a policy",
                "request": "vault read aws/creds/data-pipeline-role",
                "response": {"access_key": "ASIA...", "secret_key": "...", "security_token": "...", "lease_duration": "30m"},
                "benefit": "No long-lived IAM access keys. Automatic rotation.",
            },
        ],
    },
    "aws_secrets_manager": {
        "description": "AWS native secret storage with automatic rotation",
        "vs_parameter_store": {
            "secrets_manager": ["Automatic rotation via Lambda", "JSON multi-key secrets", "$0.40/secret/month", "Built-in RDS rotation"],
            "parameter_store": ["Manual rotation", "Single string value (or SecureString)", "Free tier available", "Simpler for config values"],
        },
        "retrieve_example": '''import boto3
client = boto3.client("secretsmanager")
secret = client.get_secret_value(SecretId="prod/db/credentials")
creds = json.loads(secret["SecretString"])
# NEVER: db_password = "hardcoded_password_123"''',
    },
}


@router.get("/iac/modules")
def list_modules():
    return {"modules": TERRAFORM_MODULES}


class GenerateTerraformRequest(BaseModel):
    module_id: str
    variables: Dict[str, str]


@router.post("/iac/generate")
def generate_terraform(req: GenerateTerraformRequest):
    module = next((m for m in TERRAFORM_MODULES if m["id"] == req.module_id), None)
    if not module:
        raise HTTPException(404, "Module not found")

    template = TF_TEMPLATES.get(req.module_id)
    if template:
        vars_with_defaults = {v["name"]: req.variables.get(v["name"], str(v["default"])) for v in module["variables"]}
        if req.module_id == "mod-s3":
            vars_with_defaults["enable_versioning_str"] = '"Enabled"' if vars_with_defaults.get("enable_versioning", "true").lower() == "true" else '"Suspended"'
        try:
            hcl = template.format(**vars_with_defaults)
        except KeyError:
            hcl = template
    else:
        lines = [f'# Terraform module: {module["name"]}\n# Generated from template\n']
        for res in module["resources_created"]:
            name = req.variables.get("bucket_name", req.variables.get("cluster_identifier", "this")).replace("-", "_")
            lines.append(f'resource "{res}" "{name}" {{\n  # Configure {res}\n}}\n')
        hcl = "\n".join(lines)

    return {
        "module_id": req.module_id,
        "module_name": module["name"],
        "hcl": hcl,
        "variables_used": req.variables,
        "resources_created": module["resources_created"],
        "filename": f"modules/{req.module_id.replace('mod-', '')}/main.tf",
    }


@router.get("/iac/plan")
def terraform_plan(module_id: str = "mod-s3"):
    r = random.Random(hash(module_id))
    module = next((m for m in TERRAFORM_MODULES if m["id"] == module_id), TERRAFORM_MODULES[0])
    plan_lines = ["Terraform will perform the following actions:"]
    for i, res in enumerate(module["resources_created"]):
        plan_lines.append(f'\n  # {res}.this will be created')
        plan_lines.append(f'  + resource "{res}" "this" {{')
        plan_lines.append(f'      + id = (known after apply)')
        plan_lines.append(f'      + arn = (known after apply)')
        plan_lines.append(f'    }}')
    plan_lines.append(f'\nPlan: {len(module["resources_created"])} to add, 0 to change, 0 to destroy.')
    return {"plan_output": "\n".join(plan_lines), "adds": len(module["resources_created"]), "changes": 0, "destroys": 0}


@router.get("/iac/gitops-flow")
def get_gitops_flow():
    return GITOPS_FLOW


@router.get("/iac/secrets")
def get_secrets_demo():
    return SECRETS_DEMO
