import { useState, useEffect, useRef, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import { cn } from '../../lib/utils'

interface SearchEntry {
  title: string
  description: string
  path: string
  module: string
  tags: string[]
  group: string
}

const SEARCH_INDEX: SearchEntry[] = [
  { title: 'Data Ingestion', description: 'REST, database, file, and streaming data sources', path: '/sources', module: '01', tags: ['fivetran', 'airbyte', 'connector', 'source', 'ingest'], group: 'Data Foundation' },
  { title: 'Data Quality', description: 'Great Expectations, null checks, schema validation', path: '/quality', module: '02', tags: ['quality', 'validation', 'expectations', 'null', 'schema'], group: 'Data Foundation' },
  { title: 'Pipeline Builder', description: 'ETL/ELT pipeline design, dbt, transformations', path: '/pipeline', module: '03', tags: ['etl', 'elt', 'dbt', 'transform', 'pipeline'], group: 'Data Foundation' },
  { title: 'Data Modeling', description: 'Star schema, Kimball, dimensional modeling, ERD', path: '/modeling', module: '04', tags: ['star schema', 'kimball', 'dimension', 'fact', 'modeling'], group: 'Data Foundation' },
  { title: 'Orchestration', description: 'Airflow DAGs, scheduling, task dependencies', path: '/orchestration', module: '05', tags: ['airflow', 'dag', 'schedule', 'orchestration', 'cron'], group: 'Data Foundation' },
  { title: 'Storage Layers', description: 'Data warehouse, data lake, medallion architecture', path: '/storage', module: '06', tags: ['warehouse', 'lake', 'medallion', 'bronze', 'silver', 'gold', 's3'], group: 'Platform & Storage' },
  { title: 'Monitoring & Observability', description: 'Pipeline metrics, anomaly detection, SLA, cost monitoring', path: '/monitoring', module: '07', tags: ['monitoring', 'alert', 'sla', 'freshness', 'anomaly', 'zscore', 'cost'], group: 'Platform & Storage' },
  { title: 'Data Catalog', description: 'Metadata management, lineage, data discovery', path: '/catalog', module: '08', tags: ['catalog', 'metadata', 'lineage', 'discovery', 'glossary'], group: 'Platform & Storage' },
  { title: 'Governance & Compliance', description: 'PII scanning, RBAC, data masking, retention policies', path: '/governance', module: '09', tags: ['governance', 'pii', 'gdpr', 'rbac', 'compliance', 'masking', 'retention'], group: 'Platform & Storage' },
  { title: 'Reporting & Analytics', description: 'KPIs, dashboards, ad-hoc SQL queries', path: '/reporting', module: '10', tags: ['reporting', 'kpi', 'dashboard', 'analytics', 'sql'], group: 'Platform & Storage' },
  { title: 'Knowledge Base', description: 'Data engineering concepts, glossary, daily digest', path: '/learn', module: '11', tags: ['learn', 'concepts', 'glossary', 'interview', 'study'], group: 'Platform & Storage' },
  { title: 'Big Data & Databricks', description: 'Spark, Delta Lake, Kafka, Airflow at scale', path: '/bigdata', module: '12', tags: ['spark', 'databricks', 'delta', 'kafka', 'streaming', 'rdd', 'dataframe'], group: 'Platform & Storage' },
  { title: 'CI/CD Pipelines', description: 'Pipeline testing, branching strategy, GitHub Actions YAML', path: '/cicd', module: '13', tags: ['cicd', 'testing', 'github actions', 'branch', 'deploy', 'yaml'], group: 'Pipeline Engineering' },
  { title: 'Data Contracts', description: 'Schema registry, compatibility rules, impact analysis', path: '/contracts', module: '14', tags: ['contracts', 'schema registry', 'avro', 'protobuf', 'compatibility', 'breaking change'], group: 'Pipeline Engineering' },
  { title: 'Change Data Capture', description: 'Debezium, WAL, binlog, CDC connectors', path: '/cdc', module: '15', tags: ['cdc', 'debezium', 'wal', 'binlog', 'change data capture', 'replication'], group: 'Pipeline Engineering' },
  { title: 'Reverse ETL', description: 'Sync data warehouse to SaaS tools', path: '/reversetl', module: '16', tags: ['reverse etl', 'salesforce', 'hubspot', 'sync', 'census', 'hightouch'], group: 'Pipeline Engineering' },
  { title: 'Feature Store & MLOps', description: 'Point-in-time joins, online/offline store, feature drift', path: '/featurestore', module: '17', tags: ['feature store', 'mlops', 'feast', 'point in time', 'training', 'serving skew'], group: 'Pipeline Engineering' },
  { title: 'Cloud-Native Services', description: 'AWS Glue, BigQuery, Azure ADF, multi-cloud cost', path: '/cloud', module: '18', tags: ['aws', 'gcp', 'azure', 'glue', 'bigquery', 'athena', 'kinesis', 'cloud'], group: 'Advanced Architecture' },
  { title: 'Infrastructure as Code', description: 'Terraform, GitOps, Vault secrets, state management', path: '/iac', module: '19', tags: ['terraform', 'iac', 'gitops', 'vault', 'secrets', 'infrastructure'], group: 'Advanced Architecture' },
  { title: 'Incident Management', description: 'Runbooks, postmortem, on-call, blameless incident review', path: '/incidents', module: '20', tags: ['incident', 'runbook', 'postmortem', 'oncall', 'p0', 'p1', 'alert'], group: 'Advanced Architecture' },
  { title: 'Data Mesh', description: 'Domain ownership, data products, federated governance', path: '/datamesh', module: '21', tags: ['data mesh', 'domain', 'data product', 'self-serve', 'federated'], group: 'Advanced Architecture' },
  { title: 'Real-Time OLAP', description: 'ClickHouse, Druid, Trino, sub-second analytics', path: '/olap', module: '22', tags: ['clickhouse', 'druid', 'trino', 'olap', 'real-time', 'analytics', 'mergetree'], group: 'Advanced Architecture' },
  { title: 'NoSQL & Polyglot', description: 'Redis, Cassandra, Elasticsearch, CAP theorem', path: '/nosql', module: '23', tags: ['nosql', 'redis', 'cassandra', 'elasticsearch', 'mongodb', 'cap theorem'], group: 'Advanced Architecture' },
  { title: 'Data Versioning', description: 'DVC, experiment tracking, dataset changelog, reproducibility', path: '/versioning', module: '24', tags: ['dvc', 'versioning', 'mlflow', 'experiment', 'reproducibility', 'git'], group: 'Career & Leadership' },
  { title: 'Capacity Planning', description: 'Storage calculator, pipeline sizing, TPC-DS, load testing', path: '/capacity', module: '25', tags: ['capacity', 'storage', 'sizing', 'benchmark', 'tpcds', 'load test', 'cost'], group: 'Career & Leadership' },
  { title: 'Leadership & Soft Skills', description: 'TDD, communication templates, PERT estimation, career ladder', path: '/leadership', module: '26', tags: ['leadership', 'tdd', 'estimation', 'pert', 'communication', 'interview', 'career'], group: 'Career & Leadership' },
]

function highlight(text: string, query: string): React.ReactNode {
  if (!query) return text
  const regex = new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi')
  const parts = text.split(regex)
  return parts.map((part, i) =>
    regex.test(part)
      ? <mark key={i} className="bg-[rgba(124,58,237,0.2)] text-[--purple-light] rounded-sm not-italic">{part}</mark>
      : part
  )
}

const groupColors: Record<string, string> = {
  'Data Foundation':       'text-[--green]',
  'Platform & Storage':    'text-[--cyan]',
  'Pipeline Engineering':  'text-[--amber]',
  'Advanced Architecture': 'text-[--purple-light]',
  'Career & Leadership':   'text-[--cyan]',
}

export default function CommandPalette({ open, onClose }: { open: boolean; onClose: () => void }) {
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
    onClose()
    setQuery('')
    setCursor(0)
  }, [navigate, onClose])

  useEffect(() => {
    if (open) {
      setTimeout(() => inputRef.current?.focus(), 60)
      setCursor(0)
    } else {
      setQuery('')
    }
  }, [open])

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); setCursor(c => Math.min(c + 1, results.length - 1)) }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setCursor(c => Math.max(c - 1, 0)) }
    else if (e.key === 'Enter' && results[cursor]) { go(results[cursor].path) }
    else if (e.key === 'Escape') { onClose() }
  }

  return (
    <AnimatePresence>
      {open && (
        <motion.div
          className="fixed inset-0 z-50 flex items-start justify-center pt-[20vh] px-4"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.1 }}
          onClick={onClose}
        >
          <div className="absolute inset-0 bg-black/70 backdrop-blur-[8px]" />

          <motion.div
            className="relative w-full max-w-[640px] bg-[--bg-floating] border border-[--border-loud] rounded-[10px] overflow-hidden"
            style={{ boxShadow: '0 24px 80px rgba(0,0,0,0.8), 0 4px 16px rgba(0,0,0,0.6)' }}
            initial={{ scale: 0.96, opacity: 0, y: -8 }}
            animate={{ scale: 1, opacity: 1, y: 0 }}
            exit={{ scale: 0.96, opacity: 0, y: -8 }}
            transition={{ duration: 0.15, ease: [0.175, 0.885, 0.32, 1.275] }}
            onClick={e => e.stopPropagation()}
          >
            {/* Input */}
            <div className="flex items-center gap-3 px-5 py-4 border-b border-[--border-soft]">
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none" className="flex-shrink-0 text-[--text-tertiary]">
                <circle cx="6.5" cy="6.5" r="4.5" stroke="currentColor" strokeWidth="1.5"/>
                <path d="M10.5 10.5L13.5 13.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
              </svg>
              <input
                ref={inputRef}
                value={query}
                onChange={e => { setQuery(e.target.value); setCursor(0) }}
                onKeyDown={handleKeyDown}
                placeholder="Search modules, run pipelines, find tables..."
                className="flex-1 bg-transparent text-[--text-primary] font-body text-base placeholder:text-[--text-tertiary] outline-none"
              />
              {query && (
                <button onClick={() => setQuery('')} className="text-[--text-tertiary] hover:text-[--text-secondary] transition-colors">
                  <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M1 1l12 12M13 1L1 13" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/></svg>
                </button>
              )}
              <kbd className="font-mono text-[10px] text-[--text-tertiary] bg-[--bg-overlay] border border-[--border-soft] px-1.5 py-0.5 rounded flex-shrink-0">ESC</kbd>
            </div>

            {/* Results */}
            <div className="max-h-[360px] overflow-y-auto">
              {results.length === 0 ? (
                <div className="px-5 py-10 text-center">
                  <p className="text-sm text-[--text-tertiary]">Nothing found for "{query}"</p>
                  <p className="text-xs text-[--text-tertiary] mt-1 opacity-60">Try searching by module name, tool, or concept</p>
                </div>
              ) : (
                <>
                  <div className="px-5 pt-3 pb-1 label-section">
                    {query ? `${results.length} result${results.length !== 1 ? 's' : ''}` : 'All modules'}
                  </div>
                  <ul>
                    {results.map((entry, i) => (
                      <li key={entry.path}>
                        <button
                          className={cn(
                            'w-full flex items-center gap-3 px-5 py-2.5 text-left transition-colors border-l-2',
                            i === cursor
                              ? 'bg-[rgba(124,58,237,0.12)] border-[--purple]'
                              : 'border-transparent hover:bg-black/[0.03]'
                          )}
                          onClick={() => go(entry.path)}
                          onMouseEnter={() => setCursor(i)}
                        >
                          <span className={cn('font-mono text-[10px] w-6 flex-shrink-0', groupColors[entry.group] || 'text-[--text-tertiary]')}>
                            {entry.module}
                          </span>
                          <div className="flex-1 min-w-0">
                            <p className="text-sm font-body font-medium text-[--text-primary] truncate">
                              {highlight(entry.title, query)}
                            </p>
                            <p className="text-xs text-[--text-tertiary] truncate">
                              {highlight(entry.description, query)}
                            </p>
                          </div>
                          <div className="flex items-center gap-2 flex-shrink-0">
                            <span className={cn('text-[10px] font-medium', groupColors[entry.group] || 'text-[--text-tertiary]')}>
                              {entry.group}
                            </span>
                            {i === cursor && (
                              <kbd className="font-mono text-[10px] text-[--text-tertiary] bg-[--bg-overlay] border border-[--border-soft] px-1 py-0.5 rounded">&#x21b5;</kbd>
                            )}
                          </div>
                        </button>
                      </li>
                    ))}
                  </ul>
                </>
              )}
            </div>

            {/* Footer */}
            <div className="px-5 py-2.5 border-t border-[--border-dim] flex items-center gap-4 label-section">
              <span><kbd className="font-mono bg-[--bg-overlay] px-1 rounded border border-[--border-dim]">&#x2191;&#x2193;</kbd> navigate</span>
              <span><kbd className="font-mono bg-[--bg-overlay] px-1 rounded border border-[--border-dim]">&#x21b5;</kbd> open</span>
              <span><kbd className="font-mono bg-[--bg-overlay] px-1 rounded border border-[--border-dim]">esc</kbd> close</span>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  )
}
