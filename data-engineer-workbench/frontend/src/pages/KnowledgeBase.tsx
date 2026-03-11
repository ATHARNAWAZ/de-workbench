import { useState } from 'react'
import ExplanationPanel from '../components/ExplanationPanel'
import { Search, ChevronDown, ChevronUp, Code, Zap } from 'lucide-react'
import clsx from 'clsx'

interface ConceptCard {
  id: string
  title: string
  category: string
  explanation: string
  analogy: string
  whenToUse: string
  pitfalls: string
  code?: string
}

const CONCEPTS: ConceptCard[] = [
  {
    id: 'cap', title: 'CAP Theorem', category: 'Distributed Systems',
    explanation: "A distributed system can guarantee at most 2 of 3 properties: Consistency (every read returns the most recent write), Availability (every request gets a response), Partition Tolerance (system operates despite network failures). Since network partitions are unavoidable, you choose between CP (consistency) or AP (availability).",
    analogy: "Like a bank with multiple branches: if the network between branches goes down, you can either refuse all transactions (CP — consistent but unavailable) or allow local transactions that may be out of sync (AP — available but potentially inconsistent).",
    whenToUse: "Use CP systems (HBase, Zookeeper) for financial data where consistency is non-negotiable. Use AP systems (Cassandra, DynamoDB) for user activity or shopping carts where availability matters more than perfect consistency.",
    pitfalls: "Most data engineers make the mistake of assuming they need strict consistency everywhere. In practice, eventual consistency is sufficient for 90% of analytics workloads — saving huge complexity.",
    code: `# CP vs AP in practice
# CP: PostgreSQL - will reject write if replica unreachable
psql -c "SET synchronous_commit = 'remote_apply';"

# AP: Cassandra - always accepts writes (eventual consistency)
cass.execute("INSERT INTO events...", consistency_level=ONE)`
  },
  {
    id: 'cdc', title: 'Change Data Capture (CDC)', category: 'Ingestion',
    explanation: "CDC captures every INSERT, UPDATE, DELETE that occurs in a source database and streams these changes to downstream systems. Instead of bulk-loading the full table every pipeline run, CDC processes only what changed — reducing latency from hours to seconds.",
    analogy: "Like a bank statement vs. checking your balance: CDC is the real-time transaction ledger (every change), bulk ingestion is the monthly statement (snapshot). CDC gives you sub-minute data freshness vs. hourly batch.",
    whenToUse: "Use CDC when: (1) You need near-real-time data, (2) Source tables are very large (billions of rows), (3) You need to track what changed, not just current state, (4) You're building event-driven architectures.",
    pitfalls: "CDC requires database-level configuration (binlog in MySQL, WAL in Postgres). DDL changes (schema changes) can break CDC streams. Also, primary key deletes are invisible in snapshots — only CDC captures them.",
    code: `# Debezium CDC config for PostgreSQL
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.server.name": "prod-postgres",
  "table.include.list": "public.orders",
  "plugin.name": "pgoutput",
  "slot.name": "debezium_slot"
  # This emits every change to a Kafka topic
}`
  },
  {
    id: 'idempotency', title: 'Idempotency', category: 'Pipeline Design',
    explanation: "An idempotent operation produces the same result whether run once or a hundred times. In data engineering, idempotent pipelines mean: if a pipeline fails and retries, or if you manually re-run it, the output data is identical to a successful first run. No duplicate data, no missing data.",
    analogy: "Like an elevator button: pressing it 10 times doesn't call 10 elevators. Once pressed, additional presses have no effect. A non-idempotent pipeline is like a 'Buy Now' button — clicking twice buys twice.",
    whenToUse: "Every production pipeline should be idempotent. This is especially critical for: daily aggregations (INSERT OVERWRITE, not INSERT INTO), backfill scenarios, and any retry logic in orchestrators.",
    pitfalls: "The #1 mistake: using INSERT INTO instead of INSERT OVERWRITE / REPLACE INTO. If a daily pipeline fails at step 3 and retries, INSERT INTO creates duplicate rows for step 1 and 2.",
    code: `# NON-idempotent (BAD) - creates duplicates on retry
df.to_sql('daily_revenue', con, if_exists='append')

# Idempotent (GOOD) - safe to re-run
df.to_sql('daily_revenue', con, if_exists='replace')

# Idempotent with partitions (BEST)
spark.sql("""
  INSERT OVERWRITE TABLE daily_revenue
  PARTITION (date='2024-01-15')
  SELECT * FROM transformed_data WHERE date='2024-01-15'
""")`
  },
  {
    id: 'exactly-once', title: 'Exactly-Once Semantics', category: 'Streaming',
    explanation: "In streaming systems, message delivery has three guarantees: at-most-once (messages may be lost), at-least-once (messages may be duplicated on failure/retry), or exactly-once (each message is processed exactly one time). Exactly-once is the hardest to achieve but required for financial accuracy.",
    analogy: "Imagine processing payment events: at-most-once means some payments might not get processed (lost data). At-least-once means some payments might get charged twice (duplicates). Exactly-once means every payment is processed exactly one time.",
    whenToUse: "Revenue and financial event processing. Order state machines where duplicate processing = charging twice. Inventory updates where duplicates cause negative stock.",
    pitfalls: "Exactly-once has a significant performance cost (2-3x lower throughput vs at-least-once). Many systems advertise exactly-once but achieve it only within their own system — not end-to-end. Kafka Streams and Flink offer true end-to-end exactly-once.",
    code: `# Kafka exactly-once producer config
producer = KafkaProducer(
    enable_idempotence=True,  # idempotent producer
    transactional_id='my-transactional-id',
    acks='all',
    retries=sys.maxsize,
)
producer.init_transactions()
producer.begin_transaction()
producer.send('orders', key=order_id, value=payload)
producer.commit_transaction()`
  },
  {
    id: 'data-mesh', title: 'Data Mesh', category: 'Architecture',
    explanation: "Data Mesh is an organizational and architectural paradigm where data ownership is decentralized to domain teams (e.g., Orders team owns orders data, Marketing team owns campaign data). Each domain treats data as a product and is responsible for its quality, documentation, and SLAs. A central platform team provides self-service infrastructure.",
    analogy: "Instead of one central kitchen (central data team) that cooks for the whole company (leading to bottlenecks), Data Mesh gives every department its own kitchen with standard equipment (central platform), so they can cook and serve their own data.",
    whenToUse: "Organizations with 50+ data engineers, multiple business domains, where a central data team is becoming a bottleneck, and domain teams have sufficient technical maturity to own their data.",
    pitfalls: "Data Mesh is an organizational transformation, not a technology choice. Most failures come from implementing 'federated data' without addressing ownership, accountability, and skill gaps. Do not attempt without executive sponsorship.",
    code: `# Data Product contract (data-as-a-product principle)
data_product = {
    "name": "orders",
    "owner": "orders-team@company.com",
    "sla": {"freshness_hours": 1, "availability_pct": 99.9},
    "schema_version": "2.1.0",
    "output_ports": {
        "batch": "s3://data-mesh/orders/gold/",
        "stream": "kafka://orders-events",
        "api": "https://data.internal/api/orders"
    }
}`
  },
  {
    id: 'backpressure', title: 'Backpressure', category: 'Streaming',
    explanation: "Backpressure is a mechanism where a downstream system signals to an upstream system to slow down because it cannot keep up with the data rate. Without backpressure, fast producers overwhelm slow consumers, causing out-of-memory errors, data loss, or cascading failures.",
    analogy: "Like a traffic management system: when a highway is congested, entrance ramps are metered (slowed down) to prevent gridlock. Without metering, every car enters freely and the highway grinds to a halt.",
    whenToUse: "Any streaming pipeline where production and consumption rates can diverge: API rate limits, database write limits, slow downstream transformations.",
    pitfalls: "Ignoring backpressure is the #1 cause of streaming pipeline OOM crashes. Adding more memory is a symptom fix — proper backpressure is the cure.",
    code: `# Backpressure in Flink
env.setBufferTimeout(100)  # ms
env.setParallelism(4)

# RxJava backpressure strategy
Flowable.fromIterable(events)
    .onBackpressureBuffer(1000,
        () -> log.warn("Buffer full!"),
        BackpressureOverflow.DROP_OLDEST)
    .observeOn(Schedulers.io())`
  },
  {
    id: 'lambda-kappa', title: 'Lambda vs Kappa Architecture', category: 'Architecture',
    explanation: "Lambda Architecture has two processing paths: a batch layer (reprocesses all historical data for accuracy) and a speed layer (processes recent data for low latency). Results are merged. Kappa Architecture eliminates the batch layer — everything is a stream, and historical data is just a very long stream.",
    analogy: "Lambda is like having a super-accurate accountant do the books monthly (batch) while a cashier tracks today's sales (speed). Kappa is like having one accountant tracking everything in real-time with a time machine for corrections.",
    whenToUse: "Lambda: when you need guaranteed exactly-correct historical data and can tolerate the complexity of two codebases. Kappa: when you want simplicity and your streaming system supports efficient historical replay (Kafka with long retention).",
    pitfalls: "Lambda's biggest pitfall is maintaining two separate codebases (batch and streaming) that must produce identical results — they inevitably diverge. Kappa's pitfall is reprocessing large historical datasets through a stream processor is expensive.",
    code: `# Kappa: everything is a stream
# Historical replay by seeking Kafka to the beginning
consumer = KafkaConsumer('orders-events')
consumer.seek_to_beginning()  # replay from t=0

# Process all events through the same pipeline
for record in consumer:
    transform_and_write(record.value)`
  },
  {
    id: 'data-contracts', title: 'Data Contracts', category: 'Pipeline Design',
    explanation: "A data contract is a formal, versioned agreement between a data producer (e.g., the Orders microservice) and data consumers (e.g., the data platform). It specifies: schema, field descriptions, data types, quality guarantees (null rates, cardinality), SLAs (freshness, availability), and breaking change policies.",
    analogy: "Like a software API contract (OpenAPI spec), a data contract ensures producers cannot silently break downstream consumers. Without a contract, the marketing team's schema change can silently break the BI dashboard.",
    whenToUse: "Any data pipeline where a separate team controls the source system. Data contracts are essential at the boundary between microservices and the data platform.",
    pitfalls: "Contracts without enforcement are useless. You need automated schema validation (Schema Registry for Kafka, pydantic for Python) that rejects schema-breaking changes before they reach production.",
    code: `# Data contract as code (Python/pydantic)
class OrderEvent(BaseModel):
    order_id: str = Field(..., description="UUID order identifier")
    customer_id: str = Field(..., min_length=1)
    revenue: float = Field(..., gt=0, description="Net revenue in USD")
    status: Literal["completed","shipped","cancelled"]
    created_at: datetime

    class Config:
        # Breaking change = new required field or type change
        # Non-breaking = new optional field
        schema_extra = {"version": "2.1.0"}`
  },
  {
    id: 'schema-registry', title: 'Schema Registry', category: 'Streaming',
    explanation: "A Schema Registry is a centralized service that stores and enforces Avro/Protobuf/JSON Schema schemas for Kafka topics. Producers register their schema before publishing; consumers retrieve it to deserialize. The registry enforces compatibility rules (BACKWARD, FORWARD, FULL) preventing schema-breaking deployments.",
    analogy: "Like TypeScript for your event bus. Without a schema registry, Kafka topics are like any[] — anything can be published. With a registry, it's like string[] — the schema is enforced at compile time (publish time).",
    whenToUse: "Any production Kafka deployment with more than one team publishing or consuming events. Essential for preventing the 'JSON soup' anti-pattern where fields silently appear/disappear.",
    pitfalls: "Schema Registry can become a single point of failure — deploy in HA mode. Also, FULL_TRANSITIVE compatibility is safest but most restrictive. Most teams start with BACKWARD and regret it when consumers need to handle old messages.",
    code: `# Confluent Schema Registry
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro

schema_str = """
{"type": "record", "name": "Order", "fields": [
  {"name": "order_id", "type": "string"},
  {"name": "revenue", "type": "double"},
  {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"}
]}"""

producer = AvroProducer({
    'schema.registry.url': 'http://schema-registry:8081'
}, default_value_schema=avro.loads(schema_str))`
  },
  {
    id: 'bloom-filters', title: 'Bloom Filters', category: 'Performance',
    explanation: "A Bloom filter is a space-efficient probabilistic data structure that tests set membership. It can definitively say 'NOT IN set' but only 'probably IN set' (false positives are possible, false negatives are not). Used in databases to avoid expensive disk lookups for rows that definitely don't exist.",
    analogy: "Like a quick smell test before tasting food: if it smells bad, definitely don't eat it (no false negatives). But a good smell doesn't guarantee it's edible (false positives possible). The smell test is cheap; the full evaluation is expensive.",
    whenToUse: "Delta Lake and Iceberg use Bloom filters on high-cardinality columns (customer_id, product_id) to skip data files during SELECT queries. This can reduce query time by 10-100x for point lookups.",
    pitfalls: "Bloom filters are only useful for equality filters (=, IN). Range queries (BETWEEN, >, <) cannot use Bloom filters. Also, they increase file size and write time — only add them to frequently-queried columns.",
    code: `# Delta Lake Bloom filter
(df.write
  .format("delta")
  .option("delta.bloomFilter.enabled", "true")
  .option("delta.bloomFilter.numItems", "10000000")  # expected distinct values
  .option("delta.bloomFilter.fpp", "0.01")           # false positive probability
  .mode("overwrite")
  .save("/gold/fact_orders"))`
  },
  {
    id: 'z-ordering', title: 'Z-Ordering (Data Clustering)', category: 'Performance',
    explanation: "Z-ordering (also called data skipping or multi-dimensional clustering) co-locates related data in the same files using a Z-curve space-filling curve. When you query with multiple filter predicates (e.g., WHERE country='US' AND category='Electronics'), Delta Lake can skip entire files that don't match.",
    analogy: "Like organizing a library by both author and genre simultaneously, so you can find all mystery novels by British authors without scanning the whole library. Partitioning is like organizing by author only.",
    whenToUse: "Multi-column filter patterns that don't warrant partitioning (cardinality too high). Common example: OPTIMIZE with ZORDER BY (country, product_category) for a global e-commerce table.",
    pitfalls: "Z-ordering is optimized at write time — you must run OPTIMIZE periodically. It's not free: OPTIMIZE reads all existing files and rewrites them. Schedule during off-peak hours.",
    code: `# Delta Lake Z-ordering (run periodically)
spark.sql("""
  OPTIMIZE gold.fact_orders
  ZORDER BY (customer_id, product_category)
""")
# Before: query scans 1000 files
# After: query skips 950 files, reads only 50`
  },
  {
    id: 'reverse-etl', title: 'Reverse ETL', category: 'Architecture',
    explanation: "Traditional ETL moves data FROM operational systems TO the warehouse. Reverse ETL moves data FROM the warehouse back TO operational systems (CRM, marketing tools, product databases). Example: sync customer churn scores computed in the warehouse back to Salesforce for the sales team to act on.",
    analogy: "The warehouse is like a brain that processes everything. Reverse ETL lets the brain's conclusions (e.g., 'this customer is about to churn') flow back to the hands (CRM, email platform, customer success tools) to take action.",
    whenToUse: "Syncing ML predictions back to CRM. Powering personalized product recommendations from warehouse data. Sending high-value customer segments to ad platforms (Facebook, Google Ads).",
    pitfalls: "Reverse ETL creates operational dependencies on the warehouse — if your warehouse pipeline is down, your Salesforce data is stale. Also, data freshness expectations differ wildly between warehouse consumers and operational consumers.",
    code: `# Census / Hightouch model (Reverse ETL)
model = {
    "source": "snowflake",
    "query": """
      SELECT customer_id, churn_risk_score, recommended_action
      FROM gold.customer_360
      WHERE churn_risk_score > 0.7
    """,
    "destination": "salesforce",
    "destination_object": "Contact",
    "mapping": {
        "customer_id": "External_ID__c",
        "churn_risk_score": "Churn_Risk__c",
    }
}`
  },
]

const INTERVIEW_QUESTIONS = [
  { q: "What is the difference between ETL and ELT?", a: "ETL (Extract-Transform-Load) transforms data before loading into the warehouse. ELT (Extract-Load-Transform) loads raw data first, then transforms within the warehouse using SQL. Modern cloud data warehouses (Snowflake, BigQuery) made ELT dominant because compute is cheap and storage is cheap — there's no reason to transform before loading." },
  { q: "How would you design a pipeline to handle late-arriving data?", a: "Use watermarking in streaming systems (Spark Structured Streaming, Flink) to define how late data can arrive. For batch systems, implement a re-processing window: keep 'event_date' and 'processing_date' separately, and re-run yesterday's aggregation for 2-3 days to catch late data. Use INSERT OVERWRITE to update partitions idempotently." },
  { q: "Explain the difference between a fact table and a dimension table.", a: "Fact tables contain measurable business events (orders, transactions, pageviews) with foreign keys to dimensions and numeric measures. Dimension tables provide context (who, what, where, when) with descriptive attributes. Facts are large and narrow; dimensions are small and wide. Star schema = 1 fact + multiple dimensions." },
  { q: "What is SCD Type 2 and when would you use it?", a: "Slowly Changing Dimension Type 2 adds a new row when a dimension attribute changes, with effective_from, effective_to, and is_current columns. Use it when you need point-in-time accuracy — e.g., 'What was the customer's segment when they placed this order in 2022?' Without SCD2, segment updates retroactively change historical reports." },
  { q: "How do you ensure data quality in a production pipeline?", a: "Multi-layer approach: (1) Schema validation at ingestion (pydantic, Avro Schema Registry), (2) Great Expectations suites on raw data, (3) dbt tests on transformed models (not_null, unique, accepted_values, relationships), (4) Volume monitoring (row count anomaly detection), (5) Business metric monitoring (revenue should not drop 80% overnight)." },
  { q: "What is data lineage and why does it matter?", a: "Data lineage tracks data's journey from source to consumption — which tables feed which, what transformations were applied. It matters for: (1) Impact analysis ('if I change this column, what breaks?'), (2) Debugging ('why is this revenue number wrong?'), (3) Compliance ('can I prove this GDPR-deleted user's data is fully removed?')." },
  { q: "Explain partitioning strategies for a 10TB orders table.", a: "Partition by the most common filter column — typically order_date. This enables partition pruning: a query for 'last 7 days' reads 7/3650 = 0.2% of data instead of 100%. For high-cardinality secondary filters (e.g., customer_id), use Z-ordering within partitions. Never partition by high-cardinality columns (UUID) — this creates millions of tiny files." },
  { q: "What happens when a critical pipeline fails at 2am?", a: "This tests on-call process and alerting design. Answer: PagerDuty alert fires within 2 minutes (SLA monitoring). On-call engineer checks monitoring dashboard, reviews pipeline logs, identifies root cause. For transient failure: retry manually. For data issue: fix upstream, re-run from checkpoint (not beginning). Post-incident: 5-why analysis, add regression test, improve alerting specificity." },
  { q: "How would you migrate from batch to streaming for a critical pipeline?", a: "Never big-bang migrate. Use the strangler fig pattern: (1) Build streaming pipeline in parallel, writing to a shadow table, (2) Compare output of batch and streaming for 2-4 weeks, (3) Validate business metrics match (within acceptable tolerance), (4) Gradually shift consumers from batch table to streaming table, (5) Decommission batch pipeline." },
  { q: "What is the medallion architecture and why use it?", a: "Bronze/Silver/Gold layered architecture. Bronze = raw, immutable source data. Silver = cleaned, validated, standardized. Gold = business-ready aggregations. Benefits: (1) Each layer can be reprocessed independently from the layer below, (2) Clear data quality expectations per layer, (3) Bronze serves as disaster recovery — you can always re-derive Silver and Gold from Bronze." },
]

const TECH_COMPARISONS = [
  {
    category: 'Orchestration',
    tools: [
      { name: 'Apache Airflow', pros: 'Mature, huge community, extensive operators, Python-native DAGs', cons: 'Complex setup, poor data-aware scheduling, historical baggage', bestFor: 'Large teams with complex dependency graphs' },
      { name: 'Prefect', pros: 'Modern UI, dynamic DAGs, cloud-native, Python functions as flows', cons: 'Smaller ecosystem, newer (less battle-tested)', bestFor: 'Python teams wanting modern developer experience' },
      { name: 'Dagster', pros: 'Asset-centric (focuses on data assets not tasks), excellent observability, type-safe', cons: 'Steeper learning curve, more opinionated', bestFor: 'Teams that think in data assets not task graphs' },
    ]
  },
  {
    category: 'Stream Processing',
    tools: [
      { name: 'Apache Spark (Structured Streaming)', pros: 'Unified batch+streaming, Python/Scala/SQL, huge ecosystem', cons: 'Micro-batch (not true streaming), high latency for low-latency needs', bestFor: 'Teams already using Spark for batch, moderate latency OK (seconds)' },
      { name: 'Apache Flink', pros: 'True event-time streaming, lowest latency, exactly-once natively', cons: 'Steeper learning curve, less Python support historically', bestFor: 'Financial services, real-time fraud detection, sub-second latency' },
      { name: 'dbt (batch SQL transforms)', pros: 'SQL-first, version-controlled, built-in testing, excellent docs', cons: 'Not streaming — batch only, no Python transforms (core)', bestFor: 'Analytics engineering, warehouse-centric transformations' },
    ]
  },
  {
    category: 'Cloud Data Warehouses',
    tools: [
      { name: 'Snowflake', pros: 'Zero-copy cloning, perfect SQL compatibility, auto-scaling, Time Travel', cons: 'Most expensive at scale, limited control over compute', bestFor: 'Enterprise teams wanting managed simplicity, SaaS companies' },
      { name: 'Google BigQuery', pros: 'Serverless (no cluster management), ML built-in, cheapest at rare large queries', cons: 'Per-byte scanned pricing can surprise, less SQL compatibility', bestFor: 'GCP-native organizations, variable workloads, ML use cases' },
      { name: 'Amazon Redshift', pros: 'Deep AWS integration, mature, AQUA hardware acceleration', cons: 'Less elastic than Snowflake, requires more tuning (vacuuming)', bestFor: 'AWS-native organizations already deep in the ecosystem' },
    ]
  },
]

export default function KnowledgeBase() {
  const [search, setSearch] = useState('')
  const [expandedConcept, setExpandedConcept] = useState<string | null>(null)
  const [expandedQ, setExpandedQ] = useState<number | null>(null)
  const [activeCategory, setActiveCategory] = useState('all')
  const [activeTab, setActiveTab] = useState('concepts')

  const categories = ['all', ...Array.from(new Set(CONCEPTS.map((c) => c.category)))]

  const filtered = CONCEPTS.filter((c) => {
    const matchSearch = !search || c.title.toLowerCase().includes(search.toLowerCase()) || c.explanation.toLowerCase().includes(search.toLowerCase())
    const matchCat = activeCategory === 'all' || c.category === activeCategory
    return matchSearch && matchCat
  })

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Senior Engineer Knowledge Base</h1>
        <p className="text-sm text-gray-400 mt-1">Concept cards, interview prep, and technology comparisons</p>
      </div>

      <ExplanationPanel
        title="Senior Data Engineer Mastery"
        what="Senior data engineers don't just write pipelines — they design systems that are reliable, scalable, observable, and maintainable. They understand distributed systems theory, data modeling principles, and the organizational dynamics of data teams."
        why="The gap between a mid-level and senior data engineer is not about knowing more tools — it's about understanding the tradeoffs. Why use Flink instead of Spark? When is SCD Type 2 overkill? When does a data contract prevent a 3am incident? This knowledge takes years to acquire without a structured learning path."
        how="Study distributed systems fundamentals (CAP theorem, consensus algorithms), data modeling theory (Kimball, Data Vault), streaming semantics (exactly-once, watermarks), and systems design patterns. Read engineering blogs from Airbnb, Uber, Lyft, Netflix — they share real-world architectures and failures."
        tools={['Designing Data-Intensive Applications (Kleppmann)', 'The Data Warehouse Toolkit (Kimball)', 'Fundamentals of Data Engineering (Reis/Housley)']}
        seniorTip="The most important skill is asking 'what happens when this fails?' for every design decision. Run game days (chaos engineering) on your pipelines. Delete a partition and see if your pipeline recovers. Simulate a Kafka broker failure. The engineer who has broken their pipeline on purpose never gets paged by it in production."
      />

      {/* Tabs */}
      <div className="flex gap-1 border-b border-gray-800">
        {['concepts', 'interview', 'comparison'].map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={clsx('px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors', activeTab === tab ? 'border-blue-500 text-blue-400' : 'border-transparent text-gray-500 hover:text-gray-300')}
          >
            {tab === 'concepts' ? 'Concept Cards' : tab === 'interview' ? 'Interview Prep' : 'Tech Comparisons'}
          </button>
        ))}
      </div>

      {/* Concept Cards */}
      {activeTab === 'concepts' && (
        <div className="space-y-5 animate-slide-in">
          <div className="flex gap-3 flex-wrap">
            <div className="relative flex-1 min-w-48">
              <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" />
              <input className="input pl-9" placeholder="Search concepts..." value={search} onChange={(e) => setSearch(e.target.value)} />
            </div>
            <div className="flex flex-wrap gap-2">
              {categories.map((cat) => (
                <button
                  key={cat}
                  onClick={() => setActiveCategory(cat)}
                  className={clsx('text-xs px-3 py-1.5 rounded-full border capitalize transition-colors', activeCategory === cat ? 'bg-blue-600 text-white border-blue-600' : 'bg-gray-800 text-gray-400 border-gray-700 hover:border-gray-600')}
                >
                  {cat}
                </button>
              ))}
            </div>
          </div>

          <div className="grid grid-cols-1 gap-4">
            {filtered.map((concept) => {
              const isExp = expandedConcept === concept.id
              return (
                <div key={concept.id} className="card overflow-hidden">
                  <button
                    onClick={() => setExpandedConcept(isExp ? null : concept.id)}
                    className="w-full flex items-center justify-between px-5 py-4 text-left hover:bg-gray-800/30 transition-colors"
                  >
                    <div className="flex items-center gap-4">
                      <div className="p-2 bg-blue-900/20 rounded-lg">
                        <Zap size={16} className="text-blue-400" />
                      </div>
                      <div>
                        <div className="font-semibold text-gray-100">{concept.title}</div>
                        <div className="text-xs text-gray-500 mt-0.5">{concept.category}</div>
                      </div>
                    </div>
                    {isExp ? <ChevronUp size={16} className="text-gray-500" /> : <ChevronDown size={16} className="text-gray-500" />}
                  </button>

                  {isExp && (
                    <div className="border-t border-gray-800 px-5 py-5 space-y-5 bg-gray-950/30 animate-slide-in">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                        <div>
                          <div className="text-xs font-semibold text-blue-400 uppercase tracking-wider mb-2">What it is</div>
                          <p className="text-sm text-gray-400 leading-relaxed">{concept.explanation}</p>
                        </div>
                        <div>
                          <div className="text-xs font-semibold text-amber-400 uppercase tracking-wider mb-2">Real-world analogy</div>
                          <p className="text-sm text-gray-400 leading-relaxed">{concept.analogy}</p>
                        </div>
                        <div>
                          <div className="text-xs font-semibold text-green-400 uppercase tracking-wider mb-2">When to use it</div>
                          <p className="text-sm text-gray-400 leading-relaxed">{concept.whenToUse}</p>
                        </div>
                        <div>
                          <div className="text-xs font-semibold text-red-400 uppercase tracking-wider mb-2">Common pitfalls</div>
                          <p className="text-sm text-gray-400 leading-relaxed">{concept.pitfalls}</p>
                        </div>
                      </div>
                      {concept.code && (
                        <div>
                          <div className="flex items-center gap-2 mb-2">
                            <Code size={13} className="text-gray-400" />
                            <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Code Example</div>
                          </div>
                          <pre className="code-block text-xs leading-relaxed overflow-x-auto">{concept.code}</pre>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Interview Questions */}
      {activeTab === 'interview' && (
        <div className="space-y-3 animate-slide-in">
          <div className="text-sm text-gray-400 mb-4">Top {INTERVIEW_QUESTIONS.length} Senior Data Engineer Interview Questions — with detailed answers</div>
          {INTERVIEW_QUESTIONS.map((item, i) => (
            <div key={i} className="card overflow-hidden">
              <button
                onClick={() => setExpandedQ(expandedQ === i ? null : i)}
                className="w-full flex items-start justify-between px-5 py-4 text-left hover:bg-gray-800/30 transition-colors"
              >
                <div className="flex items-start gap-3">
                  <span className="text-xs font-mono text-gray-600 mt-0.5 flex-shrink-0">Q{String(i + 1).padStart(2, '0')}</span>
                  <span className="text-sm font-medium text-gray-200">{item.q}</span>
                </div>
                {expandedQ === i ? <ChevronUp size={15} className="text-gray-500 flex-shrink-0 mt-0.5" /> : <ChevronDown size={15} className="text-gray-500 flex-shrink-0 mt-0.5" />}
              </button>
              {expandedQ === i && (
                <div className="border-t border-gray-800 px-5 py-4 bg-gray-950/30 animate-slide-in">
                  <p className="text-sm text-gray-400 leading-relaxed">{item.a}</p>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Tech Comparisons */}
      {activeTab === 'comparison' && (
        <div className="space-y-6 animate-slide-in">
          {TECH_COMPARISONS.map((section) => (
            <div key={section.category} className="space-y-4">
              <h3 className="text-sm font-bold text-gray-300 uppercase tracking-wider">{section.category}</h3>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                {section.tools.map((tool) => (
                  <div key={tool.name} className="card p-5">
                    <h4 className="font-bold text-gray-100 mb-3">{tool.name}</h4>
                    <div className="space-y-3 text-xs">
                      <div>
                        <div className="text-green-400 font-semibold mb-1">Pros</div>
                        <p className="text-gray-400 leading-relaxed">{tool.pros}</p>
                      </div>
                      <div>
                        <div className="text-red-400 font-semibold mb-1">Cons</div>
                        <p className="text-gray-400 leading-relaxed">{tool.cons}</p>
                      </div>
                      <div className="bg-blue-900/10 border border-blue-800/30 rounded p-2">
                        <div className="text-blue-400 font-semibold mb-1">Best For</div>
                        <p className="text-gray-400">{tool.bestFor}</p>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
