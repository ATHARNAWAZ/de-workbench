from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timedelta
import random

router = APIRouter()

CATALOG = [
    {
        "id": "cat-001",
        "table_name": "fact_orders",
        "full_name": "gold.fact_orders",
        "layer": "gold",
        "description": "Central fact table containing all completed orders. Primary source for revenue reporting and business analytics.",
        "owner": "data-engineering@corp.com",
        "tags": ["Revenue", "Financial", "Core"],
        "last_profiled": (datetime.now() - timedelta(hours=6)).isoformat(),
        "row_count": 2_450_000,
        "size_mb": 342.5,
        "upstream": ["silver.orders"],
        "downstream": ["reporting.executive_dashboard", "ml.revenue_forecast"],
        "columns": [
            {"name": "order_id", "type": "VARCHAR(50)", "nullable": False, "pii": False, "description": "Unique order identifier (business key)", "example_values": ["ORD-001234", "ORD-009876"], "min": None, "max": None, "cardinality": 2450000, "mean": None},
            {"name": "customer_id", "type": "VARCHAR(50)", "nullable": False, "pii": False, "description": "FK to dim_customer", "example_values": ["CUST-00045", "CUST-00102"], "min": None, "max": None, "cardinality": 98000, "mean": None},
            {"name": "revenue", "type": "DECIMAL(12,2)", "nullable": False, "pii": False, "description": "Net revenue after discounts", "example_values": [149.99, 89.50], "min": 0.01, "max": 9999.99, "cardinality": None, "mean": 127.45},
            {"name": "status", "type": "VARCHAR(20)", "nullable": True, "pii": False, "description": "Order lifecycle status: completed|shipped|processing|cancelled", "example_values": ["completed", "shipped"], "min": None, "max": None, "cardinality": 7, "mean": None},
            {"name": "order_date", "type": "DATE", "nullable": False, "pii": False, "description": "Date order was placed", "example_values": ["2024-01-15"], "min": "2023-01-01", "max": "2024-12-31", "cardinality": 730, "mean": None},
        ],
        "glossary_links": ["Revenue", "Order"],
    },
    {
        "id": "cat-002",
        "table_name": "dim_customer",
        "full_name": "silver.dim_customer",
        "layer": "silver",
        "description": "Customer dimension table with full profile. Contains PII — access restricted to approved roles.",
        "owner": "customer-data@corp.com",
        "tags": ["PII", "Customer", "Marketing"],
        "last_profiled": (datetime.now() - timedelta(hours=12)).isoformat(),
        "row_count": 98_000,
        "size_mb": 12.4,
        "upstream": ["bronze.raw_customers"],
        "downstream": ["gold.fact_orders", "gold.customer_360"],
        "columns": [
            {"name": "customer_id", "type": "VARCHAR(50)", "nullable": False, "pii": False, "description": "Unique customer identifier", "example_values": ["CUST-00001"], "min": None, "max": None, "cardinality": 98000, "mean": None},
            {"name": "email", "type": "VARCHAR(255)", "nullable": True, "pii": True, "description": "Customer email address (PII)", "example_values": ["user@example.com"], "min": None, "max": None, "cardinality": 97800, "mean": None},
            {"name": "phone", "type": "VARCHAR(20)", "nullable": True, "pii": True, "description": "Customer phone number (PII)", "example_values": ["+1-555-0100"], "min": None, "max": None, "cardinality": 97100, "mean": None},
            {"name": "segment", "type": "VARCHAR(30)", "nullable": False, "pii": False, "description": "Customer business segment", "example_values": ["Enterprise", "SMB"], "min": None, "max": None, "cardinality": 4, "mean": None},
        ],
        "glossary_links": ["Customer", "Segment"],
    },
    {
        "id": "cat-003",
        "table_name": "dim_product",
        "full_name": "silver.dim_product",
        "layer": "silver",
        "description": "Product catalog dimension. Contains pricing, categorization and cost basis for all active SKUs.",
        "owner": "product-data@corp.com",
        "tags": ["Product", "Catalog"],
        "last_profiled": (datetime.now() - timedelta(days=1)).isoformat(),
        "row_count": 45_000,
        "size_mb": 4.2,
        "upstream": ["bronze.raw_products"],
        "downstream": ["gold.fact_orders"],
        "columns": [
            {"name": "product_id", "type": "VARCHAR(50)", "nullable": False, "pii": False, "description": "Unique product identifier (SKU)", "example_values": ["PROD-0001"], "min": None, "max": None, "cardinality": 45000, "mean": None},
            {"name": "category", "type": "VARCHAR(50)", "nullable": False, "pii": False, "description": "Top-level product category", "example_values": ["Electronics", "Clothing"], "min": None, "max": None, "cardinality": 12, "mean": None},
            {"name": "unit_price", "type": "DECIMAL(10,2)", "nullable": False, "pii": False, "description": "Current list price", "example_values": [29.99, 149.99], "min": 0.99, "max": 9999.99, "cardinality": None, "mean": 87.50},
        ],
        "glossary_links": ["Product", "SKU", "Revenue"],
    },
    {
        "id": "cat-004",
        "table_name": "gold_daily_revenue",
        "full_name": "gold.daily_revenue",
        "layer": "gold",
        "description": "Pre-aggregated daily revenue by product category and region. Primary source for executive dashboards.",
        "owner": "analytics@corp.com",
        "tags": ["Revenue", "Financial", "Aggregated", "Executive"],
        "last_profiled": (datetime.now() - timedelta(hours=4)).isoformat(),
        "row_count": 125_400,
        "size_mb": 8.7,
        "upstream": ["gold.fact_orders"],
        "downstream": ["reporting.executive_dashboard"],
        "columns": [
            {"name": "date", "type": "DATE", "nullable": False, "pii": False, "description": "Revenue date", "example_values": ["2024-01-15"], "min": "2023-01-01", "max": "2024-12-31", "cardinality": 730, "mean": None},
            {"name": "total_revenue", "type": "DECIMAL(14,2)", "nullable": False, "pii": False, "description": "Sum of net revenue for the day/category/region combination", "example_values": [125430.50], "min": 0, "max": 1250000, "cardinality": None, "mean": 48750.25},
            {"name": "order_count", "type": "INTEGER", "nullable": False, "pii": False, "description": "Number of orders contributing to revenue", "example_values": [892, 1243], "min": 1, "max": 5000, "cardinality": None, "mean": 342},
        ],
        "glossary_links": ["Revenue", "GMV"],
    },
]

GLOSSARY = [
    {"term": "Revenue", "definition": "Net revenue = (unit_price × quantity) × (1 - discount). Does not include taxes or shipping.", "owner": "Finance", "linked_columns": ["fact_orders.revenue", "gold_daily_revenue.total_revenue"], "created_at": "2023-01-15"},
    {"term": "GMV", "definition": "Gross Merchandise Value. Total value of orders before any deductions. Used as top-level business metric.", "owner": "Finance", "linked_columns": ["fact_orders.unit_price"], "created_at": "2023-01-15"},
    {"term": "Active User", "definition": "A user who has placed at least one order in the last 30 days.", "owner": "Product", "linked_columns": ["dim_customer.customer_id"], "created_at": "2023-03-01"},
    {"term": "Order", "definition": "A confirmed purchase transaction. Excludes carts and wishlists. An order may contain multiple line items.", "owner": "Commerce", "linked_columns": ["fact_orders.order_id"], "created_at": "2023-01-15"},
    {"term": "Customer", "definition": "A registered user who has completed at least one purchase. Distinct from 'User' (registered but no purchase).", "owner": "CRM", "linked_columns": ["dim_customer.customer_id"], "created_at": "2023-02-01"},
    {"term": "Segment", "definition": "Business classification of customer: Enterprise (>500 employees), SMB (11-500), Consumer (<11 or individual), Government.", "owner": "Sales", "linked_columns": ["dim_customer.segment"], "created_at": "2023-04-01"},
    {"term": "SKU", "definition": "Stock Keeping Unit. Unique identifier for each distinct product variant (size, color, etc.).", "owner": "Product", "linked_columns": ["dim_product.product_id"], "created_at": "2023-01-20"},
]


class CatalogUpdate(BaseModel):
    description: Optional[str] = None
    tags: Optional[list[str]] = None
    owner: Optional[str] = None


@router.get("/catalog/search")
def search_catalog(q: str = Query("", description="Search query"), tag: str = Query("", description="Filter by tag"), layer: str = Query("", description="Filter by layer")):
    results = CATALOG.copy()

    if q:
        q_lower = q.lower()
        results = [
            item for item in results
            if q_lower in item["table_name"].lower()
            or q_lower in item["description"].lower()
            or any(q_lower in col["name"].lower() or q_lower in col["description"].lower() for col in item["columns"])
        ]

    if tag:
        results = [item for item in results if tag in item["tags"]]

    if layer:
        results = [item for item in results if item["layer"] == layer]

    return {"results": results, "total": len(results)}


@router.get("/catalog/tables")
def list_catalog():
    return {"tables": CATALOG, "total": len(CATALOG)}


@router.get("/catalog/table/{table_id}")
def get_catalog_entry(table_id: str):
    item = next((t for t in CATALOG if t["id"] == table_id), None)
    if not item:
        return {"error": "Not found"}, 404
    return item


@router.put("/catalog/table/{table_id}")
def update_catalog_entry(table_id: str, body: CatalogUpdate):
    for item in CATALOG:
        if item["id"] == table_id:
            if body.description is not None:
                item["description"] = body.description
            if body.tags is not None:
                item["tags"] = body.tags
            if body.owner is not None:
                item["owner"] = body.owner
            item["last_profiled"] = datetime.now().isoformat()
            return {"success": True, "item": item}
    return {"error": "Not found"}


@router.get("/catalog/glossary")
def get_glossary():
    return {"terms": GLOSSARY, "total": len(GLOSSARY)}


@router.get("/catalog/tags")
def get_all_tags():
    tags = set()
    for item in CATALOG:
        tags.update(item["tags"])
    return {"tags": sorted(list(tags))}
