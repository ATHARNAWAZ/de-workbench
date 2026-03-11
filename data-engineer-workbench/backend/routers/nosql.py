from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
import random

router = APIRouter()

# Module 23 — NoSQL & Polyglot Persistence

DB_PROFILES = {
    "redis": {
        "name": "Redis", "type": "In-Memory Key-Value / Multi-Model",
        "description": "Sub-millisecond data structures in memory. The fastest cache layer.",
        "use_cases": ["Feature store online serving (< 5ms)", "Session store", "Rate limiting", "Real-time leaderboards", "Pub/Sub messaging"],
        "cap": {"consistency": "Strong (single node)", "availability": "High (replication)", "partition_tolerance": "No (single-region primary)"},
        "query_latency_ms": 0.5, "data_model": "Key-Value with rich types",
        "best_for": "Caching, real-time serving, session management",
        "anti_patterns": ["Primary datastore (no persistence guarantees)", "Complex relational queries", "Data > available RAM"],
    },
    "mongodb": {
        "name": "MongoDB", "type": "Document Database",
        "description": "Flexible JSON documents with rich query capabilities. Schema-flexible.",
        "use_cases": ["Operational data with flexible schema", "Content management", "Product catalogs", "IoT device state"],
        "cap": {"consistency": "Tunable (read/write concerns)", "availability": "High (replica sets)", "partition_tolerance": "Yes"},
        "query_latency_ms": 2, "data_model": "BSON Documents (nested JSON)",
        "best_for": "Operational data where schema evolves frequently",
        "anti_patterns": ["Multi-collection JOINs (no foreign keys)", "ACID transactions (limited)", "Reporting/analytics workloads"],
    },
    "cassandra": {
        "name": "Apache Cassandra", "type": "Wide-Column Store",
        "description": "Linearly scalable, masterless distributed database. No single point of failure.",
        "use_cases": ["Time-series data (IoT sensors, metrics)", "Write-heavy workloads at scale", "Geographically distributed data"],
        "cap": {"consistency": "Tunable (via CL)", "availability": "Highest (AP system)", "partition_tolerance": "Yes"},
        "query_latency_ms": 5, "data_model": "Tables with partition key + clustering columns",
        "best_for": "High-write, time-series data at massive scale (Netflix, Uber, Instagram)",
        "anti_patterns": ["Ad-hoc queries (must know query pattern upfront)", "Relational joins", "ACID transactions", "Small datasets < 1M rows"],
    },
    "elasticsearch": {
        "name": "Elasticsearch", "type": "Search + Analytics Engine",
        "description": "Distributed search engine built on Lucene. Full-text search + aggregations.",
        "use_cases": ["Full-text search", "Log analytics (ELK stack)", "Faceted product search", "Application monitoring"],
        "cap": {"consistency": "Eventual", "availability": "High", "partition_tolerance": "Yes"},
        "query_latency_ms": 20, "data_model": "JSON documents with inverted index",
        "best_for": "Full-text search, log analytics, and near-real-time dashboards",
        "anti_patterns": ["Primary datastore (use as secondary index)", "ACID transactions", "Complex aggregations replacing a warehouse"],
    },
    "neo4j": {
        "name": "Neo4j", "type": "Graph Database",
        "description": "Native graph storage. Traversals are O(1) per relationship (not table scans).",
        "use_cases": ["Fraud detection (find suspicious relationship patterns)", "Social networks", "Recommendation engines", "Knowledge graphs"],
        "cap": {"consistency": "Strong (single master)", "availability": "High (causal clustering)", "partition_tolerance": "Limited"},
        "query_latency_ms": 10, "data_model": "Nodes + Relationships + Properties",
        "best_for": "Highly connected data where relationship traversal is the primary operation",
        "anti_patterns": ["Tabular data", "Simple key-value lookups", "Bulk analytics"],
    },
}

DECISION_QUESTIONS = [
    {"id": "q1", "question": "What is the primary access pattern?", "options": [
        {"id": "q1a", "text": "Key lookup (get value by ID)", "next": "q2"},
        {"id": "q1b", "text": "Full-text search", "next": "recommend_es"},
        {"id": "q1c", "text": "Graph traversal (follow relationships)", "next": "recommend_neo4j"},
        {"id": "q1d", "text": "Time-series / append-only writes", "next": "q3"},
    ]},
    {"id": "q2", "question": "What is your latency requirement?", "options": [
        {"id": "q2a", "text": "< 1ms (real-time serving)", "next": "recommend_redis"},
        {"id": "q2b", "text": "< 10ms (operational)", "next": "q4"},
        {"id": "q2c", "text": "< 100ms (analytics acceptable)", "next": "recommend_mongo"},
    ]},
    {"id": "q3", "question": "What is your scale?", "options": [
        {"id": "q3a", "text": "> 10M events/day, global distribution", "next": "recommend_cassandra"},
        {"id": "q3b", "text": "< 10M events/day, single region", "next": "recommend_postgres"},
    ]},
    {"id": "q4", "question": "Does your schema change frequently?", "options": [
        {"id": "q4a", "text": "Yes — flexible schema needed", "next": "recommend_mongo"},
        {"id": "q4b", "text": "No — fixed schema", "next": "recommend_postgres"},
    ]},
]

RECOMMENDATIONS = {
    "recommend_redis": {"db": "redis", "reason": "Sub-millisecond latency requirement → Redis is the only option. Use for feature store, session cache, or real-time counters."},
    "recommend_es": {"db": "elasticsearch", "reason": "Full-text search → Elasticsearch with BM25 relevance scoring. Add Kibana for log analytics dashboards."},
    "recommend_neo4j": {"db": "neo4j", "reason": "Graph traversal → Neo4j. Cypher query language makes relationship queries intuitive. Perfect for fraud rings, recommendations."},
    "recommend_cassandra": {"db": "cassandra", "reason": "High-scale time-series writes → Cassandra. Design partition key as (sensor_id, month) to distribute writes evenly."},
    "recommend_mongo": {"db": "mongodb", "reason": "Flexible schema + operational reads → MongoDB. Use Atlas for managed hosting. Add compound indexes on frequent query patterns."},
    "recommend_postgres": {"db": "postgresql", "reason": "Fixed schema, moderate scale → PostgreSQL with TimescaleDB extension handles time-series well. No need for NoSQL complexity."},
}


@router.get("/nosql/decision-tree")
def get_decision_tree():
    return {"questions": DECISION_QUESTIONS, "recommendations": RECOMMENDATIONS}


@router.get("/nosql/profiles")
def get_profiles():
    return {"databases": list(DB_PROFILES.values())}


class RedisRequest(BaseModel):
    operation: str  # STRING, HASH, LIST, SET, SORTED_SET, STREAM


@router.post("/nosql/redis/demo")
def redis_demo(req: RedisRequest):
    demos = {
        "STRING": {
            "description": "Simple key-value. Foundation of all Redis data types.",
            "commands": [
                {"cmd": "SET session:user123 '{\"user_id\":\"u123\",\"cart\":[\"prod-1\",\"prod-2\"]}' EX 3600", "result": "OK", "explanation": "Store session with 1-hour TTL"},
                {"cmd": "GET session:user123", "result": '{"user_id":"u123","cart":["prod-1","prod-2"]}', "explanation": "Retrieve in < 0.5ms"},
                {"cmd": "INCR rate_limit:api_key:abc123", "result": "42", "explanation": "Atomic increment — perfect for rate limiting"},
            ],
            "use_case_highlight": "Cache: reduces warehouse query from 500ms → 0.5ms for repeated requests",
            "cache_comparison": {"without_cache_ms": 487, "with_cache_ms": 0.4, "speedup": "1218x"},
        },
        "HASH": {
            "description": "Dictionary of field-value pairs. Store structured objects efficiently.",
            "commands": [
                {"cmd": "HSET feature:customer:123 ltv_30d 847.50 churn_risk 0.12 order_count_7d 3", "result": "3", "explanation": "Store ML features for customer 123"},
                {"cmd": "HGETALL feature:customer:123", "result": '{"ltv_30d":"847.50","churn_risk":"0.12","order_count_7d":"3"}', "explanation": "Fetch all features in one command"},
                {"cmd": "HMGET feature:customer:123 ltv_30d churn_risk", "result": '["847.50","0.12"]', "explanation": "Fetch specific features — used by ML serving"},
            ],
            "use_case_highlight": "Feature Store Online Serving: store ML features per entity, fetch in < 1ms at inference time",
        },
        "SORTED_SET": {
            "description": "Set of unique strings, each with a numeric score. Auto-sorted by score.",
            "commands": [
                {"cmd": "ZADD leaderboard 9821 'user:alice' 8743 'user:bob' 7654 'user:carol'", "result": "3", "explanation": "Add scores to leaderboard"},
                {"cmd": "ZREVRANGE leaderboard 0 9 WITHSCORES", "result": "[alice:9821, bob:8743, carol:7654, ...]", "explanation": "Get top 10 — O(log N + M)"},
                {"cmd": "ZRANK leaderboard 'user:bob'", "result": "1", "explanation": "Get rank of any user in O(log N)"},
            ],
            "use_case_highlight": "Real-time leaderboards, priority queues, rate limiting with sliding window",
        },
        "LIST": {
            "description": "Ordered list. Push/pop from head or tail. Use as queue or stack.",
            "commands": [
                {"cmd": "RPUSH notifications:user123 'Your order shipped' 'Flash sale starts in 1h'", "result": "2"},
                {"cmd": "LPOP notifications:user123", "result": "Your order shipped"},
                {"cmd": "LRANGE notifications:user123 0 -1", "result": '["Flash sale starts in 1h"]'},
            ],
            "use_case_highlight": "Message queues, activity feeds, job queues",
        },
        "SET": {
            "description": "Unordered set of unique strings. Set operations in O(N).",
            "commands": [
                {"cmd": "SADD segment:premium_customers user:1 user:2 user:3 user:4", "result": "4"},
                {"cmd": "SINTERSTORE segment:premium_churners segment:premium_customers segment:churn_risk", "result": "12", "explanation": "Find premium customers who are also churn risk — one command"},
                {"cmd": "SCARD segment:premium_churners", "result": "12"},
            ],
            "use_case_highlight": "Audience segmentation, set operations across customer groups",
        },
        "STREAM": {
            "description": "Append-only log with consumer groups. Kafka-like in Redis.",
            "commands": [
                {"cmd": "XADD orders * order_id ORD-9999 revenue 249.99 status pending", "result": "1706400000000-0", "explanation": "Append event to stream with auto-ID"},
                {"cmd": "XREAD COUNT 10 STREAMS orders 0", "result": "[order events...]", "explanation": "Read last 10 events"},
                {"cmd": "XGROUP CREATE orders analytics-group $ MKSTREAM", "result": "OK", "explanation": "Create consumer group starting from now"},
            ],
            "use_case_highlight": "Lightweight event streaming, real-time notifications, activity tracking",
        },
    }
    return {"operation": req.operation, "demo": demos.get(req.operation, demos["STRING"])}


class CassandraRequest(BaseModel):
    scenario: str  # partition_design, replication, ttl


@router.post("/nosql/cassandra/demo")
def cassandra_demo(req: CassandraRequest):
    demos = {
        "partition_design": {
            "title": "Partition Key Design — The Most Important Cassandra Decision",
            "bad_example": {
                "table": "CREATE TABLE sensor_readings (sensor_id TEXT, reading_time TIMESTAMP, temperature FLOAT, PRIMARY KEY (sensor_id));",
                "problem": "All readings for sensor-001 in one partition. After 1 year of 1-sec readings = 31M rows in one partition. Hot partition!",
                "symptoms": ["Node overloaded with one sensor's data", "READ timeouts for popular sensors", "Uneven data distribution across nodes"],
            },
            "good_example": {
                "table": "CREATE TABLE sensor_readings (sensor_id TEXT, month TEXT, reading_time TIMESTAMP, temperature FLOAT, PRIMARY KEY ((sensor_id, month), reading_time)) WITH CLUSTERING ORDER BY (reading_time DESC);",
                "benefit": "Each (sensor, month) is one partition. Bounded size. Even distribution. Sorted by time for efficient range queries.",
                "query": "SELECT * FROM sensor_readings WHERE sensor_id = 'SENSOR-001' AND month = '2024-01' AND reading_time >= '2024-01-01' LIMIT 100",
            },
        },
        "replication": {
            "title": "Replication Factor + Consistency Level Matrix",
            "setup": "RF=3 means each piece of data is stored on 3 nodes across the ring",
            "matrix": [
                {"write_cl": "ANY", "read_cl": "ONE", "consistency": "Eventual", "availability": "Highest", "use_case": "Fire-and-forget logging"},
                {"write_cl": "ONE", "read_cl": "ONE", "consistency": "Eventual", "availability": "Very high", "use_case": "High-write IoT streams"},
                {"write_cl": "QUORUM", "read_cl": "QUORUM", "consistency": "Strong", "availability": "High", "use_case": "Financial transactions, user profiles"},
                {"write_cl": "ALL", "read_cl": "ALL", "consistency": "Linearizable", "availability": "Low", "use_case": "Rarely used — single node failure breaks writes"},
            ],
            "quorum_math": "QUORUM = floor(RF/2) + 1 = floor(3/2) + 1 = 2. Write to 2 nodes, read from 2 nodes → always see the latest write.",
            "node_failure": "With RF=3, QUORUM: can lose 1 node and keep reading + writing. Can lose 2 nodes and still read (but not write at QUORUM).",
        },
        "ttl": {
            "title": "TTL (Time-To-Live) for Automatic Data Expiry",
            "use_case": "IoT sensor data with 90-day retention policy",
            "insert_with_ttl": "INSERT INTO sensor_readings (sensor_id, month, reading_time, temperature) VALUES ('SENSOR-001', '2024-01', '2024-01-01T00:00:00', 23.4) USING TTL 7776000;",
            "explanation": "7,776,000 seconds = 90 days. Cassandra automatically deletes the row after TTL expires — no cron job needed.",
            "tombstones_warning": "Deletes in Cassandra create 'tombstones' (deletion markers) that slow down reads. TTL is the recommended way to expire data — tombstones are compacted away during normal compaction.",
            "default_ttl": "CREATE TABLE sensor_readings (...) WITH default_time_to_live = 7776000;",
        },
    }
    return demos.get(req.scenario, demos["partition_design"])


class ESRequest(BaseModel):
    operation: str  # search, aggregation, mapping


@router.post("/nosql/elasticsearch/demo")
def elasticsearch_demo(req: ESRequest):
    demos = {
        "search": {
            "title": "Full-Text Search with BM25 Relevance Scoring",
            "index_request": '{"mappings": {"properties": {"title": {"type": "text", "analyzer": "english"}, "description": {"type": "text"}, "category": {"type": "keyword"}, "price": {"type": "float"}}}}',
            "search_request": '{"query": {"multi_match": {"query": "wireless bluetooth headphones", "fields": ["title^3", "description"], "type": "best_fields"}}, "size": 5}',
            "results": [
                {"_score": 18.7, "_source": {"title": "Sony WH-1000XM5 Wireless Bluetooth Headphones", "price": 349.99, "category": "Electronics"}},
                {"_score": 14.2, "_source": {"title": "Bose QuietComfort 45 Bluetooth Headphones", "price": 279.99, "category": "Electronics"}},
                {"_score": 8.1, "_source": {"title": "JBL Wireless Speaker Bluetooth", "price": 89.99, "category": "Electronics"}},
            ],
            "explanation": "title^3 = title field weighted 3x more than description. BM25 considers term frequency + inverse document frequency.",
        },
        "aggregation": {
            "title": "Aggregation Pipeline — Faceted Search + Date Histogram",
            "request": '{"aggs": {"by_category": {"terms": {"field": "category", "size": 10}, "aggs": {"avg_price": {"avg": {"field": "price"}}}}, "orders_over_time": {"date_histogram": {"field": "order_date", "calendar_interval": "day"}, "aggs": {"daily_revenue": {"sum": {"field": "revenue"}}}}}}',
            "results": {
                "by_category": [
                    {"key": "Electronics", "doc_count": 4821, "avg_price": {"value": 287.40}},
                    {"key": "Clothing", "doc_count": 3102, "avg_price": {"value": 67.20}},
                    {"key": "Home & Garden", "doc_count": 2847, "avg_price": {"value": 129.80}},
                ],
                "orders_over_time": "28 daily buckets with revenue sums",
            },
            "use_case": "E-commerce faceted search — show category counts + prices while user types in search box",
        },
        "mapping": {
            "title": "Index Mapping — Field Types and Analyzers",
            "mapping": {
                "properties": {
                    "order_id": {"type": "keyword", "note": "Exact match only — no analysis"},
                    "description": {"type": "text", "analyzer": "english", "note": "Analyzed for full-text search (stop words removed, stemming applied)"},
                    "status": {"type": "keyword", "note": "Enum — always exact match"},
                    "revenue": {"type": "float", "note": "Numeric — supports range queries"},
                    "order_date": {"type": "date", "format": "strict_date_optional_time", "note": "Date — supports date histograms and range queries"},
                    "customer": {"type": "object", "properties": {"id": {"type": "keyword"}, "name": {"type": "text"}}},
                }
            },
            "keyword_vs_text": "keyword = exact match, sorting, aggregations. text = analyzed for full-text search. NEVER use text for aggregations — use keyword or fielddata (expensive).",
        },
    }
    return demos.get(req.operation, demos["search"])
