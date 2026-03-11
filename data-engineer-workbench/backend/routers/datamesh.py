from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import random
import uuid

router = APIRouter()

# Module 21 — Data Mesh & Data Products

DOMAINS = [
    {"id": "dom-orders", "name": "Orders Domain", "description": "End-to-end order lifecycle data: creation, fulfillment, returns", "team": "Platform Engineering", "team_size": 8, "product_count": 4, "data_products": ["dp-orders-silver", "dp-daily-revenue", "dp-order-analytics"]},
    {"id": "dom-customer", "name": "Customer Domain", "description": "Customer profiles, segments, LTV, churn predictions", "team": "Customer Intelligence", "team_size": 5, "product_count": 3, "data_products": ["dp-customer-360", "dp-churn-model"]},
    {"id": "dom-inventory", "name": "Inventory Domain", "description": "Product catalog, stock levels, warehouse operations", "team": "Supply Chain", "team_size": 6, "product_count": 2, "data_products": ["dp-inventory-snapshot", "dp-demand-forecast"]},
    {"id": "dom-marketing", "name": "Marketing Domain", "description": "Campaign performance, attribution, audience segments", "team": "Growth Engineering", "team_size": 4, "product_count": 3, "data_products": ["dp-campaign-metrics", "dp-attribution"]},
]

_data_products = {
    "dp-orders-silver": {
        "id": "dp-orders-silver", "name": "orders_silver", "domain": "dom-orders",
        "description": "Cleansed and validated orders data, deduplicated with business rules applied",
        "output_ports": [
            {"type": "Table", "address": "silver.orders", "format": "Delta Lake", "sla_freshness_hours": 1},
            {"type": "API", "address": "https://data-api.company.com/v1/orders", "format": "JSON REST", "sla_freshness_hours": 1},
        ],
        "owner": "platform-eng@company.com", "version": "2.3.0", "status": "active",
        "sla_freshness_hours": 1, "sla_quality_pct": 99.5,
        "consumers": ["marketing-attribution-pipeline", "ml-churn-model", "finance-reporting"],
        "quality_score": 99.7, "last_updated": "2024-01-27T11:00:00Z",
        "schema": [
            {"name": "order_id", "type": "STRING", "nullable": False, "description": "Unique order ID"},
            {"name": "customer_id", "type": "STRING", "nullable": False, "description": "Customer reference"},
            {"name": "revenue", "type": "DECIMAL(12,2)", "nullable": False, "description": "Order revenue USD"},
            {"name": "region", "type": "STRING", "nullable": False, "description": "Sales region"},
            {"name": "order_date", "type": "TIMESTAMP", "nullable": False, "description": "Order timestamp"},
            {"name": "status", "type": "STRING", "nullable": False, "description": "Order status"},
        ],
        "access_required": False, "subscribers": 3,
    },
    "dp-customer-360": {
        "id": "dp-customer-360", "name": "customer_360", "domain": "dom-customer",
        "description": "Unified customer profile combining CRM, orders, and behavioral data",
        "output_ports": [
            {"type": "Table", "address": "gold.customer_360", "format": "Delta Lake", "sla_freshness_hours": 24},
            {"type": "Stream", "address": "kafka://customer-events", "format": "Avro", "sla_freshness_hours": 0.1},
        ],
        "owner": "customer-team@company.com", "version": "1.5.0", "status": "active",
        "sla_freshness_hours": 24, "sla_quality_pct": 98.0,
        "consumers": ["marketing-personalization", "sales-crm-sync", "ml-recommendation-model"],
        "quality_score": 98.4, "last_updated": "2024-01-27T06:00:00Z",
        "schema": [
            {"name": "customer_id", "type": "STRING", "nullable": False, "description": "Customer ID"},
            {"name": "email", "type": "STRING", "nullable": False, "description": "Primary email (PII)"},
            {"name": "segment", "type": "STRING", "nullable": True, "description": "Customer segment"},
            {"name": "ltv_score", "type": "FLOAT", "nullable": True, "description": "Lifetime value 0-1"},
            {"name": "churn_risk", "type": "FLOAT", "nullable": True, "description": "Churn probability 0-1"},
            {"name": "total_orders", "type": "INT", "nullable": False, "description": "Total orders placed"},
        ],
        "access_required": True, "subscribers": 3,
    },
    "dp-daily-revenue": {
        "id": "dp-daily-revenue", "name": "daily_revenue", "domain": "dom-orders",
        "description": "Daily revenue aggregates by region, category, and customer segment. BI-ready gold table.",
        "output_ports": [
            {"type": "Table", "address": "gold.daily_revenue", "format": "Delta Lake", "sla_freshness_hours": 2},
            {"type": "File", "address": "s3://data-products/daily-revenue/", "format": "Parquet", "sla_freshness_hours": 24},
        ],
        "owner": "platform-eng@company.com", "version": "3.0.0", "status": "active",
        "sla_freshness_hours": 2, "sla_quality_pct": 99.9,
        "consumers": ["finance-dashboard", "exec-reporting", "marketing-budget-model"],
        "quality_score": 99.9, "last_updated": "2024-01-27T02:00:00Z",
        "schema": [{"name": "date", "type": "DATE"}, {"name": "region", "type": "STRING"}, {"name": "revenue", "type": "DECIMAL(14,2)"}, {"name": "order_count", "type": "INT"}],
        "access_required": False, "subscribers": 3,
    },
}

MESH_PRINCIPLES = [
    {
        "id": 1, "name": "Domain Ownership", "icon": "users",
        "description": "Data is owned by the domain team that best understands it, not a central data team.",
        "traditional_problem": "Central data team becomes a bottleneck. 6-week ticket wait to add a column.",
        "mesh_solution": "Orders team owns orders data. They build and maintain it. No waiting for central team.",
        "how_to_apply": ["Identify domain boundaries (Orders, Customer, Inventory, Marketing)", "Each domain has dedicated data engineers", "Domain team is accountable for SLA, quality, and documentation"],
    },
    {
        "id": 2, "name": "Data as a Product", "icon": "package",
        "description": "Data sets are treated as products with owners, consumers, SLAs, docs, and versioning.",
        "traditional_problem": "Raw tables with no documentation. Consumers don't know if data is fresh or trustworthy.",
        "mesh_solution": "Each data product has: owner, SLA, schema contract, quality score, changelog.",
        "how_to_apply": ["Define output ports (Table, API, Stream, File)", "Publish SLA and quality metrics publicly", "Version data products like software (semver)", "Maintain consumer-facing documentation"],
    },
    {
        "id": 3, "name": "Self-Serve Data Platform", "icon": "server",
        "description": "A platform team provides tools so domain teams can build, deploy, and monitor their own data products without deep infrastructure knowledge.",
        "traditional_problem": "Domain teams can't work independently — they need ops to provision every resource.",
        "mesh_solution": "1-click provisioning: new domain gets S3 bucket, Delta table, catalog entry, and monitoring in 5 minutes.",
        "how_to_apply": ["Provide Terraform modules for standard data resources", "Self-service catalog registration", "Built-in data quality framework teams plug into", "Standardized CI/CD pipeline template"],
    },
    {
        "id": 4, "name": "Federated Computational Governance", "icon": "shield",
        "description": "Global policies (PII, access control, compliance) enforced by the platform, while domain teams control domain-specific rules.",
        "traditional_problem": "Either no governance (chaos) or centralized governance (bottleneck).",
        "mesh_solution": "Platform enforces: PII masking, access logging, retention. Domains define: quality rules, business logic.",
        "how_to_apply": ["Platform-level: automated PII detection at table creation", "Platform-level: RBAC policy enforcement", "Domain-level: domain-specific quality rules", "Interoperability: standard metadata schema all products follow"],
    },
]


@router.get("/datamesh/domains")
def list_domains():
    return {"domains": DOMAINS}


@router.get("/datamesh/products")
def list_products(domain_id: Optional[str] = None):
    products = list(_data_products.values())
    if domain_id:
        products = [p for p in products if p["domain"] == domain_id]
    return {"products": products, "total": len(products)}


@router.get("/datamesh/products/{product_id}")
def get_product(product_id: str):
    product = _data_products.get(product_id)
    if not product:
        raise HTTPException(404, "Data product not found")
    return product


class CreateProductRequest(BaseModel):
    name: str
    domain: str
    description: str
    owner: str
    sla_freshness_hours: int = 24
    sla_quality_pct: float = 99.0
    output_port_type: str = "Table"
    output_address: str = "gold.new_product"


@router.post("/datamesh/products")
def create_product(req: CreateProductRequest):
    pid = f"dp-{req.name.replace('_', '-')}"
    product = {
        "id": pid, "name": req.name, "domain": req.domain,
        "description": req.description, "owner": req.owner,
        "output_ports": [{"type": req.output_port_type, "address": req.output_address, "format": "Delta Lake", "sla_freshness_hours": req.sla_freshness_hours}],
        "version": "1.0.0", "status": "provisioning",
        "sla_freshness_hours": req.sla_freshness_hours, "sla_quality_pct": req.sla_quality_pct,
        "consumers": [], "quality_score": 100.0, "last_updated": None,
        "schema": [], "access_required": False, "subscribers": 0,
    }
    _data_products[pid] = product
    return product


class SubscribeRequest(BaseModel):
    product_id: str
    consumer_name: str
    use_case: str


@router.post("/datamesh/subscribe")
def subscribe(req: SubscribeRequest):
    product = _data_products.get(req.product_id)
    if not product:
        raise HTTPException(404, "Product not found")
    if product.get("access_required"):
        status = "pending_approval"
        message = f"Access request submitted for {product['name']}. Owner {product['owner']} will review within 2 business days."
    else:
        product["consumers"].append(req.consumer_name)
        product["subscribers"] = len(product["consumers"])
        status = "approved"
        message = f"Access granted to {product['name']}. Use the output port at {product['output_ports'][0]['address']}."
    return {"status": status, "message": message, "product_id": req.product_id, "consumer": req.consumer_name}


@router.get("/datamesh/principles")
def get_principles():
    return {"principles": MESH_PRINCIPLES}


class APIBuilderRequest(BaseModel):
    table: str = "gold.daily_revenue"
    endpoint_path: str = "/v1/revenue"
    filters: List[str] = ["date", "region"]
    auth: str = "bearer_token"
    rate_limit_per_min: int = 100


@router.post("/datamesh/api-builder")
def build_data_api(req: APIBuilderRequest):
    filter_params = "\n".join([f"    {f}: Optional[str] = Query(None, description='Filter by {f}')" for f in req.filters])
    filter_conditions = "\n".join([f"    if {f}:\n        query += f' AND {f} = :\\'{{{f}}}\\'' " for f in req.filters])
    code = f'''from fastapi import FastAPI, Query, Depends, HTTPException
from fastapi.security import HTTPBearer
from typing import Optional
import sqlite3

app = FastAPI(title="Data Product API — {req.table}")
security = HTTPBearer()

RATE_LIMIT = {req.rate_limit_per_min}  # requests per minute

@app.get("{req.endpoint_path}")
async def get_data(
{filter_params},
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
    token = Depends(security),
):
    """
    Query {req.table} with optional filters.
    Rate limit: {req.rate_limit_per_min} requests/min per API key.
    """
    query = "SELECT * FROM {req.table} WHERE 1=1"
{filter_conditions}
    query += f" LIMIT {{limit}} OFFSET {{offset}}"
    # Execute query and return results
    ...
'''
    return {
        "code": code,
        "endpoint": req.endpoint_path,
        "auth_type": req.auth,
        "rate_limit": req.rate_limit_per_min,
        "openapi_spec": {
            "path": req.endpoint_path,
            "method": "GET",
            "parameters": [{"name": f, "in": "query", "required": False, "schema": {"type": "string"}} for f in req.filters],
            "responses": {"200": {"description": "Data rows matching filters"}, "429": {"description": "Rate limit exceeded"}},
        },
    }
