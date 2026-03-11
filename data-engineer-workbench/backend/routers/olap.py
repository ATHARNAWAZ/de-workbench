from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
import random

router = APIRouter()

# Module 22 — Real-Time OLAP & Query Engines

OLAP_COMPARISON = [
    {
        "engine": "ClickHouse",
        "by": "Yandex (open-source)",
        "use_case": "User-facing analytics, operational dashboards, log analytics",
        "ingestion_model": "Batch inserts (optimized for bulk loads) + Kafka table engine for streaming",
        "query_latency_ms": 50,
        "max_cardinality": "Billions",
        "approximate_results": False,
        "strengths": ["Extremely fast columnar aggregations", "Simple architecture (no JVM)", "Best compression (LZ4/ZSTD)", "Materialized views auto-update on insert"],
        "weaknesses": ["Not ideal for JOINs across large tables", "No ACID transactions", "Limited UPDATE/DELETE support"],
        "best_for": "When you need sub-100ms queries on 100M-10B rows. Product analytics, dashboards, log search.",
        "avoid_if": "Complex multi-table JOINs, OLTP operations, frequent updates.",
    },
    {
        "engine": "Apache Druid",
        "by": "Apache (open-source, originally MetaMarkets)",
        "use_case": "Real-time event analytics, interactive dashboards on streaming data",
        "ingestion_model": "Native streaming (Kafka), batch (S3/HDFS). Data pre-aggregated at ingest.",
        "query_latency_ms": 100,
        "max_cardinality": "High (with approximation)",
        "approximate_results": True,
        "strengths": ["Real-time + batch in one system", "HyperLogLog for approximate unique counts", "Automatic rollup reduces storage 100x", "Highly concurrent (many users simultaneously)"],
        "weaknesses": ["Complex operational overhead (many services)", "Rollup means raw data not preserved", "Approximate algorithms can mislead"],
        "best_for": "Real-time event analytics where approximate answers are acceptable. SaaS product metrics at scale.",
        "avoid_if": "Exact counts required, complex JOINs, or team lacks operational expertise.",
    },
    {
        "engine": "Apache Pinot",
        "by": "LinkedIn (open-source)",
        "use_case": "User-facing analytics at LinkedIn/Uber scale — trillion events/day",
        "ingestion_model": "Real-time (Kafka) + batch. Segment-based, each segment immutable.",
        "query_latency_ms": 30,
        "max_cardinality": "Trillions",
        "approximate_results": True,
        "strengths": ["Lowest latency at extreme scale", "StarTree index for fast multi-dimensional queries", "Real-time upserts supported", "LinkedIn/Uber battle-tested"],
        "weaknesses": ["Complex setup and operations", "Steeper learning curve", "Approximate for some queries"],
        "best_for": "External-facing analytics APIs where you serve data to end users at massive scale.",
        "avoid_if": "Team size < 5 engineers, moderate scale (<1B events/day).",
    },
    {
        "engine": "StarRocks",
        "by": "CelerData (Apache License)",
        "use_case": "Unified analytics — OLAP + data lake queries + streaming",
        "ingestion_model": "Kafka, S3 external tables, MySQL-compatible batch loads",
        "query_latency_ms": 80,
        "max_cardinality": "Billions",
        "approximate_results": False,
        "strengths": ["MySQL-compatible (easy migration)", "Data lake queries via external tables", "Full JOINs supported (unlike Druid)", "MPP architecture scales linearly"],
        "weaknesses": ["Smaller community than ClickHouse/Druid", "Fewer managed cloud options"],
        "best_for": "When you need ClickHouse speed but also need complex JOINs and SQL compatibility.",
        "avoid_if": "Very large streaming volumes where Druid/Pinot's native streaming is better.",
    },
]

CLICKHOUSE_DEMO = {
    "mergetree_demo": {
        "description": "MergeTree is ClickHouse's core storage engine. Data is written in sorted parts and periodically merged.",
        "ddl": """CREATE TABLE clickstream (
    user_id     UInt64,
    event_type  LowCardinality(String),
    page_url    String,
    session_id  String,
    event_time  DateTime,
    revenue     Nullable(Decimal(10,2))
)
ENGINE = MergeTree()
ORDER BY (user_id, event_time)
PARTITION BY toYYYYMM(event_time)
TTL event_time + INTERVAL 90 DAY;""",
        "what_happens_on_insert": [
            "Data written as a new 'part' (sorted directory of column files)",
            "Multiple small parts are merged in background into larger sorted parts",
            "Each column stored separately (columnar) — query only reads needed columns",
            "Partition pruning: queries with WHERE event_time = '2024-01' only read Jan partition",
        ],
        "benchmark": {
            "query": "SELECT event_type, COUNT(*), SUM(revenue) FROM clickstream WHERE event_time >= '2024-01-01' GROUP BY event_type",
            "clickhouse_ms": 47,
            "postgres_ms": 42000,
            "speedup": "893x faster",
            "reason": "ClickHouse reads only 2 columns (event_type, revenue) vs PostgreSQL reading all columns. Columnar compression + vectorized execution.",
        },
    },
    "materialized_views": {
        "description": "Materialized views in ClickHouse auto-update when source table receives inserts. Zero-latency pre-aggregation.",
        "sql": """-- Create pre-aggregation: hourly revenue by event type
CREATE MATERIALIZED VIEW hourly_revenue_mv
ENGINE = SummingMergeTree()
PARTITION BY toDate(hour)
ORDER BY (hour, event_type)
AS SELECT
    toStartOfHour(event_time) AS hour,
    event_type,
    sum(revenue) AS total_revenue,
    count() AS event_count
FROM clickstream
GROUP BY hour, event_type;

-- Query the materialized view (pre-aggregated — blazing fast)
SELECT hour, sum(total_revenue) FROM hourly_revenue_mv
WHERE hour >= today() - 7 GROUP BY hour ORDER BY hour;""",
        "benefit": "The GROUP BY query above runs against pre-aggregated data (hours, not raw rows) — 1000x fewer rows to scan.",
    },
}

DRUID_DEMO = {
    "ingestion_spec": {
        "type": "kafka",
        "spec": {
            "dataSchema": {
                "dataSource": "orders",
                "timestampSpec": {"column": "event_time", "format": "iso"},
                "dimensionsSpec": {"dimensions": ["region", "product_category", "status", "customer_segment"]},
                "metricsSpec": [
                    {"name": "revenue_sum", "type": "doubleSum", "fieldName": "revenue"},
                    {"name": "order_count", "type": "count"},
                    {"name": "unique_customers", "type": "HLLSketchBuild", "fieldName": "customer_id"},
                ],
                "granularitySpec": {"queryGranularity": "minute", "segmentGranularity": "hour", "rollup": True},
            },
            "ioConfig": {"inputSource": {"type": "kafka", "bootstrapServers": "kafka:9092", "topic": "orders"}},
        },
    },
    "rollup_explanation": "With rollup=true, all events in the same minute with the same dimensions are pre-aggregated at ingest. 1000 orders → 40 rows. Storage 25x smaller. Queries 25x faster. But individual order details are lost.",
    "approximate_algorithms": {
        "HyperLogLog": {"purpose": "Approximate COUNT(DISTINCT customer_id)", "error_rate": "~2%", "storage": "12KB instead of millions of IDs", "use_case": "Unique user counts — 2% error acceptable for product metrics"},
        "ThetaSketch": {"purpose": "Set operations — intersections and unions of user sets", "error_rate": "~1%", "use_case": "How many users in segment A are also in segment B?"},
        "QuantilesSketch": {"purpose": "Approximate percentiles (p50, p95, p99)", "error_rate": "<0.5%", "use_case": "Revenue distribution, latency percentiles"},
    },
}

TRINO_DEMO = {
    "connectors": [
        {"name": "Hive (S3/HDFS)", "catalog": "hive", "query_example": "SELECT * FROM hive.silver.orders LIMIT 10", "pushdown": "Partition + column pushdown"},
        {"name": "PostgreSQL", "catalog": "postgresql", "query_example": "SELECT * FROM postgresql.public.customers LIMIT 10", "pushdown": "Predicate and projection pushdown"},
        {"name": "Apache Kafka", "catalog": "kafka", "query_example": "SELECT * FROM kafka.default.orders LIMIT 10", "pushdown": "Offset range pushdown"},
        {"name": "Delta Lake", "catalog": "delta", "query_example": "SELECT * FROM delta.gold.revenue WHERE date = '2024-01-01'", "pushdown": "Time travel + partition pruning"},
    ],
    "federated_query": {
        "sql": """-- Query across S3, PostgreSQL, and Kafka in one SQL
SELECT
    o.order_id,
    c.email,           -- from PostgreSQL CRM
    o.revenue,         -- from Delta Lake (S3)
    k.event_type       -- from Kafka (real-time)
FROM delta.gold.orders o
JOIN postgresql.crm.customers c ON o.customer_id = c.id
LEFT JOIN kafka.events.clickstream k ON k.session_id = o.session_id
WHERE o.order_date = CURRENT_DATE""",
        "how_it_works": [
            "Trino coordinator receives query, generates distributed execution plan",
            "Predicate pushdown: WHERE clause pushed to each connector — only relevant data fetched",
            "PostgreSQL: only fetches matched customer IDs (not full table scan)",
            "Delta Lake: partition pruning on order_date — scans only today's partition",
            "Data serialized to Trino's internal format, joined in-memory across workers",
        ],
        "gotchas": [
            "Cross-connector JOINs can be slow — connector with fewer rows should be on the right side",
            "No pushdown for complex expressions — may cause full scans",
            "Network bandwidth is the bottleneck — co-locate Trino workers near data sources",
        ],
    },
}


@router.get("/olap/comparison")
def get_comparison():
    return {"engines": OLAP_COMPARISON}


class OLAPQueryRequest(BaseModel):
    sql: str = "SELECT region, SUM(revenue) FROM orders GROUP BY region ORDER BY 2 DESC"
    engine: str = "clickhouse"
    rows_in_table: int = 1000000


@router.post("/olap/query")
def simulate_olap_query(req: OLAPQueryRequest):
    r = random.Random(hash(req.sql + req.engine))
    latency_map = {"clickhouse": r.randint(20, 80), "druid": r.randint(60, 150), "pinot": r.randint(15, 60), "starrocks": r.randint(40, 120)}
    pg_latency = r.randint(8000, 45000)
    latency_ms = latency_map.get(req.engine, r.randint(50, 200))

    results = [
        {"region": "West", "total_revenue": round(r.uniform(800000, 1200000), 2), "order_count": r.randint(8000, 15000)},
        {"region": "East", "total_revenue": round(r.uniform(600000, 900000), 2), "order_count": r.randint(6000, 12000)},
        {"region": "South", "total_revenue": round(r.uniform(400000, 700000), 2), "order_count": r.randint(4000, 9000)},
        {"region": "Midwest", "total_revenue": round(r.uniform(300000, 500000), 2), "order_count": r.randint(3000, 6000)},
    ]
    return {
        "engine": req.engine,
        "sql": req.sql,
        "rows_scanned": req.rows_in_table,
        "latency_ms": latency_ms,
        "rows_returned": len(results),
        "results": results,
        "vs_postgres_ms": pg_latency,
        "speedup": f"{pg_latency // latency_ms}x faster than PostgreSQL",
    }


@router.get("/olap/clickhouse")
def get_clickhouse_demo():
    return CLICKHOUSE_DEMO


@router.get("/olap/druid")
def get_druid_demo():
    return DRUID_DEMO


@router.get("/olap/trino")
def get_trino_demo():
    return TRINO_DEMO


@router.post("/olap/benchmark")
def run_benchmark(rows_millions: int = 100, engine: str = "clickhouse"):
    r = random.Random(hash(f"{rows_millions}{engine}"))
    latency = {"clickhouse": r.randint(30, 100), "druid": r.randint(80, 200), "pinot": r.randint(20, 70), "postgresql": r.randint(30000, 120000)}
    return {
        "rows_scanned": rows_millions * 1_000_000,
        "results": {eng: {"latency_ms": ms, "rows_per_ms": round(rows_millions * 1_000_000 / ms)} for eng, ms in latency.items()},
        "winner": min(latency, key=latency.get),
    }
