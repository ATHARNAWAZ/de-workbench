import axios from 'axios'

const BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

export const api = axios.create({
  baseURL: BASE_URL,
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
})

api.interceptors.response.use(
  (res) => res,
  (err) => {
    console.error('API Error:', err.response?.data || err.message)
    return Promise.reject(err)
  }
)

export const WS_BASE = import.meta.env.VITE_WS_URL || 'ws://localhost:8000'

export const endpoints = {
  // Sources
  sources: '/api/sources',
  testSource: (id: string) => `/api/sources/${id}/test`,
  ingest: (id: string) => `/api/ingest/${id}`,
  ingestLogs: (id: string) => `/api/ingest/${id}/logs`,

  // Quality
  qualityDatasets: '/api/quality/datasets',
  quality: (id: string) => `/api/quality/${id}`,
  qualityRuleCheck: '/api/quality/rule-check',

  // Pipeline
  pipelineRun: '/api/pipeline/run',
  pipelineCode: '/api/pipeline/generate-code',
  pipelineTemplates: '/api/pipeline/templates',

  // Warehouse
  warehouseTables: '/api/warehouse/tables',
  warehouseTable: (id: string) => `/api/warehouse/table/${id}`,
  warehouseQuery: '/api/warehouse/query',
  lineage: (id: string) => `/api/warehouse/lineage/${id}`,

  // Monitoring
  metrics: '/api/monitoring/metrics',
  alertRules: '/api/monitoring/alerts',
  sla: '/api/monitoring/sla',

  // Catalog
  catalogSearch: '/api/catalog/search',
  catalogTables: '/api/catalog/tables',
  catalogEntry: (id: string) => `/api/catalog/table/${id}`,
  glossary: '/api/catalog/glossary',
  catalogTags: '/api/catalog/tags',

  // Governance
  scanPII: '/api/governance/scan-pii',
  maskDemo: '/api/governance/mask-demo',
  accessMatrix: '/api/governance/access-matrix',
  compliance: '/api/governance/compliance',
  auditLog: '/api/governance/audit-log',

  // Orchestration
  dags: '/api/orchestration/dags',
  dag: (id: string) => `/api/orchestration/dags/${id}`,
  triggerDag: (id: string) => `/api/orchestration/dags/${id}/trigger`,
  runs: '/api/orchestration/runs',
  cronPreview: '/api/orchestration/cron-preview',

  // Reporting
  kpis: '/api/reporting/kpis',
  adhocQuery: '/api/reporting/adhoc',
  reports: '/api/reporting/reports',
  availableTables: '/api/reporting/available-tables',

  // Big Data & Databricks (Module 12)
  sparkConcepts: '/api/bigdata/spark/concepts',
  sparkSimulate: '/api/bigdata/spark/simulate',
  sparkTemplates: '/api/bigdata/spark/templates',
  dbClusters: '/api/bigdata/databricks/clusters',
  dbNodeTypes: '/api/bigdata/databricks/node-types',
  dbNotebooks: '/api/bigdata/databricks/notebooks',
  dbNotebook: (id: string) => `/api/bigdata/databricks/notebooks/${id}`,
  dbClusterToggle: (id: string) => `/api/bigdata/databricks/clusters/${id}/toggle`,
  unityCatalog: '/api/bigdata/databricks/unity-catalog',
  deltaExecute: '/api/bigdata/delta/execute',
  deltaHistory: '/api/bigdata/delta/history',
  deltaSnapshot: (v: number) => `/api/bigdata/delta/snapshot/${v}`,
  deltaFormatComparison: '/api/bigdata/delta/format-comparison',
  kafkaTopics: '/api/bigdata/kafka/topics',
  kafkaMessages: (topic: string) => `/api/bigdata/kafka/messages/${topic}`,
  kafkaConnectors: '/api/bigdata/kafka/connectors',
  kafkaStreamingMetrics: '/api/bigdata/kafka/streaming-metrics',
  airflowDagTemplates: '/api/bigdata/airflow/dag-templates',
  airflowCostCalculator: '/api/bigdata/airflow/cost-calculator',
  perfScenarios: '/api/bigdata/performance/scenarios',
  perfCatalyst: '/api/bigdata/performance/catalyst',
  ecosystemTools: '/api/bigdata/ecosystem/tools',
  ecosystemTimeline: '/api/bigdata/ecosystem/timeline',
  ecosystemComparison: '/api/bigdata/ecosystem/comparison',

  // CI/CD (Module 13)
  cicdStages: '/api/cicd/pipeline-stages',
  cicdRunStage: '/api/cicd/run-stage',
  cicdRunTests: '/api/cicd/run-tests',
  cicdBranching: '/api/cicd/branching-strategy',
  cicdGenerateYaml: '/api/cicd/generate-yaml',

  // Data Contracts (Module 14)
  contracts: '/api/contracts',
  contractSchemas: '/api/contracts/schemas',
  registerSchema: '/api/contracts/schemas/register',
  schemaCompatibility: '/api/contracts/compatibility-check',
  schemaImpact: (id: string) => `/api/contracts/impact/${id}`,
  schemaDiff: (id: string) => `/api/contracts/schemas/${id}/diff`,

  // CDC (Module 15)
  cdcConnectors: '/api/cdc/connectors',
  cdcEvents: '/api/cdc/events',
  cdcSimulate: '/api/cdc/simulate',
  cdcComparison: '/api/cdc/comparison',
  cdcWalExplainer: '/api/cdc/wal-explainer',

  // Reverse ETL (Module 16)
  reversetlPipelines: '/api/reversetl/pipelines',
  reversetlSync: '/api/reversetl/sync',
  reversetlSyncLog: '/api/reversetl/sync-log',
  reversetlFieldTransforms: '/api/reversetl/field-transforms',

  // Feature Store (Module 17)
  featureStoreFeatures: '/api/featurestore/features',
  featureStoreDataset: '/api/featurestore/dataset',
  featureStoreMonitoring: '/api/featurestore/monitoring',
  featureStoreOnlineServing: '/api/featurestore/online-serving',

  // Cloud Services (Module 18)
  cloudServices: '/api/cloud/services',
  cloudAws: '/api/cloud/aws',
  cloudGcp: '/api/cloud/gcp',
  cloudAzure: '/api/cloud/azure',
  cloudCostCalculator: '/api/cloud/cost-calculator',

  // IaC (Module 19)
  iacModules: '/api/iac/modules',
  iacGenerate: '/api/iac/generate',
  iacPlan: '/api/iac/plan',
  iacGitopsFlow: '/api/iac/gitops-flow',
  iacSecrets: '/api/iac/secrets',

  // Incidents (Module 20)
  incidentScenarios: '/api/incidents/scenarios',
  incidentTrigger: '/api/incidents/trigger',
  incidentRunbook: (id: string) => `/api/incidents/runbooks/${id}`,
  incidentPostmortem: '/api/incidents/postmortem',
  incidentOncall: '/api/incidents/oncall',

  // Data Mesh (Module 21)
  datameshDomains: '/api/datamesh/domains',
  datameshProducts: '/api/datamesh/products',
  datameshSubscribe: '/api/datamesh/subscribe',
  datameshPrinciples: '/api/datamesh/principles',
  datameshApiBuilder: '/api/datamesh/api-builder',

  // Real-Time OLAP (Module 22)
  olapComparison: '/api/olap/comparison',
  olapQuery: '/api/olap/query',
  olapClickhouse: '/api/olap/clickhouse',
  olapDruid: '/api/olap/druid',
  olapTrino: '/api/olap/trino',
  olapBenchmark: '/api/olap/benchmark',

  // NoSQL (Module 23)
  nosqlDecisionTree: '/api/nosql/decision-tree',
  nosqlProfiles: '/api/nosql/profiles',
  nosqlRedisDemo: '/api/nosql/redis/demo',
  nosqlCassandraDemo: '/api/nosql/cassandra/demo',
  nosqlElasticsearchDemo: '/api/nosql/elasticsearch/demo',

  // Data Versioning (Module 24)
  versioningDatasets: '/api/versioning/datasets',
  versioningDiff: '/api/versioning/diff',
  versioningRollback: '/api/versioning/rollback',
  versioningExperiments: '/api/versioning/experiments',
  versioningDvcConcepts: '/api/versioning/dvc-concepts',

  // Capacity Planning (Module 25)
  capacityStorage: '/api/capacity/storage',
  capacityPipeline: '/api/capacity/pipeline',
  capacityBenchmarks: '/api/capacity/benchmarks',
  capacityLoadtest: '/api/capacity/loadtest',
  capacityCostRecs: '/api/capacity/cost-recommendations',

  // Leadership (Module 26)
  leadershipTdd: '/api/leadership/tdd-template',
  leadershipGenerateTdd: '/api/leadership/generate-tdd',
  leadershipCommunication: '/api/leadership/communication-templates',
  leadershipEstimate: '/api/leadership/estimate',
  leadershipInterviews: '/api/leadership/interview-questions',
  leadershipCareerLadder: '/api/leadership/career-ladder',
  leadershipSelfAssessment: '/api/leadership/self-assessment',
}
