# Senior Data Engineer Workbench

A production-grade, full-stack learning platform covering **all 26 modules** of senior data engineering — from raw data ingestion through Big Data, CDC, Data Mesh, MLOps, and leadership skills. Every module uses realistic simulated data, no placeholders.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 18, TypeScript, Tailwind CSS, Recharts, React Router v6 |
| Backend | FastAPI (Python 3.11), SQLite, Pandas, Faker |
| Charts | Recharts |
| Icons | Lucide React |
| Containerization | Docker + Docker Compose |

## Quick Start

### Option 1: Docker (Recommended)

```bash
git clone <repo>
cd data-engineer-workbench
cp .env.example .env
docker-compose up
```

Then open:
- Frontend: http://localhost:3000
- Backend API: http://localhost:8000
- API Docs: http://localhost:8000/docs

### Option 2: Local Development

**Backend:**
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

**Frontend:**
```bash
cd frontend
npm install
npm run dev
```

Frontend runs at http://localhost:5173, proxying API calls to the backend.

## Module Overview

### Data Foundation
| # | Module | What You Learn |
|---|--------|---------------|
| 01 | **Data Ingestion** | REST, CSV, DB, Streaming, Webhook connectors; batch vs streaming; ELT vs ETL; fault tolerance |
| 02 | **Data Quality** | Null analysis, outlier detection (IQR), duplicate detection, custom rule builder, quality scoring |
| 03 | **Pipeline Builder** | Visual DAG builder, dbt project view, test runner, Python code generation |
| 04 | **Data Modeling** | Star vs Snowflake schema, ER diagrams, DDL generation, SCD Type 1/2/3 |
| 05 | **Orchestration** | DAG visualization, cron scheduling, task dependency graphs, retry logic simulation |

### Platform & Storage
| # | Module | What You Learn |
|---|--------|---------------|
| 06 | **Storage Architecture** | Bronze/Silver/Gold medallion layers, SQL query editor, data lineage graph |
| 07 | **Monitoring** | Real-time metrics, Z-score anomaly detection, cost monitoring, alert rules, SLA tracking |
| 08 | **Data Catalog** | Searchable metadata, column-level docs, PII tagging, business glossary |
| 09 | **Governance** | PII scanner, data masking, RBAC manager, GDPR compliance, retention policies |
| 10 | **Reporting** | Executive KPI dashboard, ad-hoc query builder, scheduled reports |
| 11 | **Knowledge Base** | Concept cards, interview questions, tool comparisons, glossary |
| 12 | **Big Data & Databricks** | Spark, Delta Lake ACID, Kafka streaming, Airflow at scale, performance tuning |

### Pipeline Engineering
| # | Module | What You Learn |
|---|--------|---------------|
| 13 | **CI/CD Pipelines** | Pipeline testing, branching strategy, GitHub Actions YAML generation |
| 14 | **Data Contracts** | Schema Registry, Avro/Protobuf, compatibility rules, breaking change impact |
| 15 | **Change Data Capture** | Debezium connectors, WAL/binlog, LSN checkpointing, CDC vs polling |
| 16 | **Reverse ETL** | Sync warehouse to Salesforce/HubSpot/Sheets, field mapping, sync modes |
| 17 | **Feature Store & MLOps** | Point-in-time joins, online/offline store, training-serving skew, feature drift |

### Advanced Architecture
| # | Module | What You Learn |
|---|--------|---------------|
| 18 | **Cloud-Native Services** | AWS/Azure/GCP comparison matrix, Athena, BigQuery, Kinesis, cost calculator |
| 19 | **Infrastructure as Code** | Terraform modules, GitOps flow, Vault secrets, state management |
| 20 | **Incident Management** | Runbooks, postmortem generator, 5-Why analysis, blameless review, on-call |
| 21 | **Data Mesh** | Domain ownership, data products, federated governance, API builder |
| 22 | **Real-Time OLAP** | ClickHouse MergeTree, Druid HLL/sketch, Trino federation, sub-second analytics |
| 23 | **NoSQL & Polyglot** | CAP theorem, Redis 6 data structures, Cassandra partition design, Elasticsearch |

### Career & Leadership
| # | Module | What You Learn |
|---|--------|---------------|
| 24 | **Data Versioning** | DVC, dataset changelog, experiment tracking (MLflow), reproducibility checklist |
| 25 | **Capacity Planning** | Storage growth calculator, pipeline sizing (Spark/Kafka), TPC-DS benchmarks, load testing |
| 26 | **Leadership & Soft Skills** | TDD builder, communication templates, PERT estimation, 50 interview questions, career ladder L3→Principal |

## Learning Paths (in-app)

Three guided paths are available in the sidebar with progress tracking:

- **Beginner** — Modules 01–05, 08: Core DE fundamentals
- **Intermediate** — Modules 06–12, 15, 19: Platform engineering & advanced patterns
- **Senior** — Modules 13–14, 17–18, 21–22, 25–26: Architecture, MLOps, leadership & scale

Progress is persisted in `localStorage` — pick up where you left off.

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/sources` | List all data sources |
| POST | `/api/sources/{id}/test` | Test source connection |
| POST | `/api/ingest/{source_id}` | Trigger data ingestion |
| GET | `/api/quality/{dataset_id}` | Run quality checks |
| POST | `/api/pipeline/run` | Execute transformation pipeline |
| GET | `/api/warehouse/tables` | List warehouse tables |
| POST | `/api/warehouse/query` | Execute SQL query |
| GET | `/api/monitoring/metrics` | Real-time pipeline metrics |
| GET | `/api/catalog/search?q=...` | Search data catalog |
| GET | `/api/warehouse/lineage/{table_id}` | Data lineage graph |
| WS | `/ws/pipeline-logs` | Real-time pipeline log stream |
| WS | `/ws/dag-run` | Real-time DAG execution stream |

## Sample Datasets

| File | Description | Rows |
|------|-------------|------|
| `data/orders.csv` | E-commerce orders with revenue, status, channel | 25+ |
| `data/user_events.json` | Clickstream events from web/mobile apps | 20 |
| `data/sensor_readings.csv` | IoT factory sensor data with anomalies | 30 |

Generate more data:
```bash
cd data && pip install faker && python generate_data.py
```

## Architecture Decisions

- **SQLite over PostgreSQL**: No external dependencies for the demo. In production, replace with Snowflake/BigQuery/Redshift.
- **Faker library**: Generates realistic synthetic data — never expose real customer data in demos.
- **WebSockets**: Real-time pipeline log streaming uses FastAPI's native WebSocket support.
- **No Redux**: React Context + useState is sufficient for this demo app scope.
- **Tailwind CSS**: Utility-first CSS enables rapid dark-theme UI development.
