import { useState, useEffect, useRef, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { Search, X } from 'lucide-react'
import clsx from 'clsx'

interface SearchEntry {
  title: string
  description: string
  path: string
  module: string
  tags: string[]
}

const SEARCH_INDEX: SearchEntry[] = [
  // Module 01
  { title: 'Data Ingestion', description: 'REST, database, file, and streaming data sources', path: '/sources', module: '01', tags: ['fivetran', 'airbyte', 'connector', 'source', 'ingest'] },
  // Module 02
  { title: 'Data Quality', description: 'Great Expectations, null checks, schema validation', path: '/quality', module: '02', tags: ['quality', 'validation', 'expectations', 'null', 'schema'] },
  // Module 03
  { title: 'Pipeline Builder', description: 'ETL/ELT pipeline design, dbt, transformations', path: '/pipeline', module: '03', tags: ['etl', 'elt', 'dbt', 'transform', 'pipeline'] },
  // Module 04
  { title: 'Data Modeling', description: 'Star schema, Kimball, dimensional modeling, ERD', path: '/modeling', module: '04', tags: ['star schema', 'kimball', 'dimension', 'fact', 'modeling'] },
  // Module 05
  { title: 'Orchestration', description: 'Airflow DAGs, scheduling, task dependencies', path: '/orchestration', module: '05', tags: ['airflow', 'dag', 'schedule', 'orchestration', 'cron'] },
  // Module 06
  { title: 'Storage Layers', description: 'Data warehouse, data lake, medallion architecture', path: '/storage', module: '06', tags: ['warehouse', 'lake', 'medallion', 'bronze', 'silver', 'gold', 's3'] },
  // Module 07
  { title: 'Monitoring & Observability', description: 'Pipeline metrics, anomaly detection, SLA, cost monitoring', path: '/monitoring', module: '07', tags: ['monitoring', 'alert', 'sla', 'freshness', 'anomaly', 'zscore', 'cost'] },
  // Module 08
  { title: 'Data Catalog', description: 'Metadata management, lineage, data discovery', path: '/catalog', module: '08', tags: ['catalog', 'metadata', 'lineage', 'discovery', 'glossary'] },
  // Module 09
  { title: 'Governance & Compliance', description: 'PII scanning, RBAC, data masking, retention policies', path: '/governance', module: '09', tags: ['governance', 'pii', 'gdpr', 'rbac', 'compliance', 'masking', 'retention'] },
  // Module 10
  { title: 'Reporting & Analytics', description: 'KPIs, dashboards, ad-hoc SQL queries', path: '/reporting', module: '10', tags: ['reporting', 'kpi', 'dashboard', 'analytics', 'sql'] },
  // Module 11
  { title: 'Knowledge Base', description: 'Data engineering concepts, glossary, daily digest', path: '/learn', module: '11', tags: ['learn', 'concepts', 'glossary', 'interview', 'study'] },
  // Module 12
  { title: 'Big Data & Databricks', description: 'Spark, Delta Lake, Kafka, Airflow at scale', path: '/bigdata', module: '12', tags: ['spark', 'databricks', 'delta', 'kafka', 'streaming', 'rdd', 'dataframe'] },
  // Module 13
  { title: 'CI/CD Pipelines', description: 'Pipeline testing, branching strategy, GitHub Actions YAML', path: '/cicd', module: '13', tags: ['cicd', 'testing', 'github actions', 'branch', 'deploy', 'yaml'] },
  // Module 14
  { title: 'Data Contracts', description: 'Schema registry, compatibility rules, impact analysis', path: '/contracts', module: '14', tags: ['contracts', 'schema registry', 'avro', 'protobuf', 'compatibility', 'breaking change'] },
  // Module 15
  { title: 'Change Data Capture', description: 'Debezium, WAL, binlog, CDC connectors', path: '/cdc', module: '15', tags: ['cdc', 'debezium', 'wal', 'binlog', 'change data capture', 'replication'] },
  // Module 16
  { title: 'Reverse ETL', description: 'Sync data warehouse to SaaS tools (Salesforce, HubSpot)', path: '/reversetl', module: '16', tags: ['reverse etl', 'salesforce', 'hubspot', 'sync', 'census', 'hightouch'] },
  // Module 17
  { title: 'Feature Store & MLOps', description: 'Point-in-time joins, online/offline store, feature drift', path: '/featurestore', module: '17', tags: ['feature store', 'mlops', 'feast', 'point in time', 'training', 'serving skew'] },
  // Module 18
  { title: 'Cloud-Native Services', description: 'AWS Glue, BigQuery, Azure ADF, multi-cloud cost', path: '/cloud', module: '18', tags: ['aws', 'gcp', 'azure', 'glue', 'bigquery', 'athena', 'kinesis', 'cloud'] },
  // Module 19
  { title: 'Infrastructure as Code', description: 'Terraform, GitOps, Vault secrets, state management', path: '/iac', module: '19', tags: ['terraform', 'iac', 'gitops', 'vault', 'secrets', 'infrastructure'] },
  // Module 20
  { title: 'Incident Management', description: 'Runbooks, postmortem, on-call, blameless incident review', path: '/incidents', module: '20', tags: ['incident', 'runbook', 'postmortem', 'oncall', 'p0', 'p1', 'alert'] },
  // Module 21
  { title: 'Data Mesh', description: 'Domain ownership, data products, federated governance', path: '/datamesh', module: '21', tags: ['data mesh', 'domain', 'data product', 'self-serve', 'federated'] },
  // Module 22
  { title: 'Real-Time OLAP', description: 'ClickHouse, Druid, Trino, sub-second analytics', path: '/olap', module: '22', tags: ['clickhouse', 'druid', 'trino', 'olap', 'real-time', 'analytics', 'mergetree'] },
  // Module 23
  { title: 'NoSQL & Polyglot Persistence', description: 'Redis, Cassandra, Elasticsearch, CAP theorem', path: '/nosql', module: '23', tags: ['nosql', 'redis', 'cassandra', 'elasticsearch', 'mongodb', 'cap theorem'] },
  // Module 24
  { title: 'Data Versioning', description: 'DVC, experiment tracking, dataset changelog, reproducibility', path: '/versioning', module: '24', tags: ['dvc', 'versioning', 'mlflow', 'experiment', 'reproducibility', 'git'] },
  // Module 25
  { title: 'Capacity Planning', description: 'Storage calculator, pipeline sizing, TPC-DS, load testing', path: '/capacity', module: '25', tags: ['capacity', 'storage', 'sizing', 'benchmark', 'tpcds', 'load test', 'cost'] },
  // Module 26
  { title: 'Leadership & Soft Skills', description: 'TDD, communication templates, PERT estimation, career ladder', path: '/leadership', module: '26', tags: ['leadership', 'tdd', 'estimation', 'pert', 'communication', 'interview', 'career'] },
]

function highlight(text: string, query: string): React.ReactNode {
  if (!query) return text
  const regex = new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi')
  const parts = text.split(regex)
  return parts.map((part, i) =>
    regex.test(part) ? <mark key={i} className="bg-yellow-500/30 text-yellow-200 rounded px-0.5">{part}</mark> : part
  )
}

export default function GlobalSearch() {
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const [cursor, setCursor] = useState(0)
  const inputRef = useRef<HTMLInputElement>(null)
  const navigate = useNavigate()

  const results = query.trim().length < 1
    ? SEARCH_INDEX.slice(0, 8)
    : SEARCH_INDEX.filter(entry => {
        const q = query.toLowerCase()
        return (
          entry.title.toLowerCase().includes(q) ||
          entry.description.toLowerCase().includes(q) ||
          entry.tags.some(t => t.includes(q)) ||
          entry.module === q
        )
      })

  const go = useCallback((path: string) => {
    navigate(path)
    setOpen(false)
    setQuery('')
    setCursor(0)
  }, [navigate])

  // Cmd+K / Ctrl+K to open
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault()
        setOpen(o => !o)
      }
      if (e.key === 'Escape') setOpen(false)
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [])

  // Focus input when opened
  useEffect(() => {
    if (open) {
      setTimeout(() => inputRef.current?.focus(), 50)
      setCursor(0)
    }
  }, [open])

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setCursor(c => Math.min(c + 1, results.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setCursor(c => Math.max(c - 1, 0))
    } else if (e.key === 'Enter' && results[cursor]) {
      go(results[cursor].path)
    }
  }

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="flex items-center gap-2 px-3 py-1.5 bg-gray-800 hover:bg-gray-700 border border-gray-700 rounded-lg text-xs text-gray-400 transition-colors"
      >
        <Search size={13} />
        <span>Search modules...</span>
        <span className="ml-1 font-mono text-[10px] bg-gray-700 px-1.5 py-0.5 rounded">⌘K</span>
      </button>
    )
  }

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center pt-20 px-4" onClick={() => setOpen(false)}>
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" />

      {/* Panel */}
      <div
        className="relative w-full max-w-xl bg-gray-900 border border-gray-700 rounded-xl shadow-2xl overflow-hidden"
        onClick={e => e.stopPropagation()}
      >
        {/* Input */}
        <div className="flex items-center gap-3 px-4 py-3 border-b border-gray-800">
          <Search size={16} className="text-gray-400 flex-shrink-0" />
          <input
            ref={inputRef}
            value={query}
            onChange={e => { setQuery(e.target.value); setCursor(0) }}
            onKeyDown={handleKeyDown}
            placeholder="Search modules, concepts, tools..."
            className="flex-1 bg-transparent text-white placeholder-gray-500 outline-none text-sm"
          />
          {query && (
            <button onClick={() => setQuery('')} className="text-gray-500 hover:text-gray-300">
              <X size={14} />
            </button>
          )}
          <kbd className="text-[10px] text-gray-600 font-mono bg-gray-800 px-1.5 py-0.5 rounded">ESC</kbd>
        </div>

        {/* Results */}
        <div className="max-h-96 overflow-y-auto">
          {results.length === 0 ? (
            <div className="px-4 py-8 text-center text-sm text-gray-500">No modules found for "{query}"</div>
          ) : (
            <>
              <div className="px-4 pt-2 pb-1 text-[10px] text-gray-600 uppercase tracking-widest">
                {query ? `${results.length} results` : 'All modules'}
              </div>
              <ul>
                {results.map((entry, i) => (
                  <li key={entry.path}>
                    <button
                      className={clsx(
                        'w-full flex items-center gap-3 px-4 py-3 text-left transition-colors',
                        i === cursor ? 'bg-blue-600/20 border-l-2 border-blue-500' : 'hover:bg-gray-800 border-l-2 border-transparent'
                      )}
                      onClick={() => go(entry.path)}
                      onMouseEnter={() => setCursor(i)}
                    >
                      <span className="text-[10px] font-mono text-gray-600 w-6 flex-shrink-0">{entry.module}</span>
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-medium text-white truncate">
                          {highlight(entry.title, query)}
                        </p>
                        <p className="text-xs text-gray-400 truncate">
                          {highlight(entry.description, query)}
                        </p>
                      </div>
                      <span className="text-[10px] text-gray-600 font-mono flex-shrink-0">{entry.path}</span>
                    </button>
                  </li>
                ))}
              </ul>
            </>
          )}
        </div>

        {/* Footer */}
        <div className="px-4 py-2 border-t border-gray-800 flex items-center gap-3 text-[10px] text-gray-600">
          <span><kbd className="font-mono bg-gray-800 px-1 rounded">↑↓</kbd> navigate</span>
          <span><kbd className="font-mono bg-gray-800 px-1 rounded">↵</kbd> open</span>
          <span><kbd className="font-mono bg-gray-800 px-1 rounded">esc</kbd> close</span>
        </div>
      </div>
    </div>
  )
}
