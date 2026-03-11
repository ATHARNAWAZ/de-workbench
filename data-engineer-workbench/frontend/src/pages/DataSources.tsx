import { useState, useEffect } from 'react'
import { api, endpoints } from '../utils/api'
import ExplanationPanel from '../components/ExplanationPanel'
import {
  Database, Wifi, FileText, Globe, Webhook,
  Play, CheckCircle, XCircle, Clock, RefreshCw, ChevronDown, ChevronUp
} from 'lucide-react'
import clsx from 'clsx'

const TYPE_ICONS: Record<string, React.ElementType> = {
  database: Database,
  stream: Wifi,
  csv: FileText,
  api: Globe,
  webhook: Webhook,
}

const STATUS_CONFIG: Record<string, { label: string; color: string; dot: string }> = {
  connected: { label: 'Connected', color: 'text-green-400', dot: 'bg-green-500' },
  streaming: { label: 'Streaming', color: 'text-blue-400', dot: 'bg-blue-500 animate-pulse' },
  idle: { label: 'Idle', color: 'text-gray-400', dot: 'bg-gray-500' },
  listening: { label: 'Listening', color: 'text-purple-400', dot: 'bg-purple-500 animate-pulse' },
  pending: { label: 'Pending', color: 'text-yellow-400', dot: 'bg-yellow-500' },
  failed: { label: 'Failed', color: 'text-red-400', dot: 'bg-red-500' },
}

interface Source {
  id: string
  name: string
  type: string
  host: string
  port: number | null
  database: string
  status: string
  last_ingested: string | null
  row_count: number
  schema: Array<{ column: string; type: string; nullable: boolean }>
  description: string
}

interface IngestionResult {
  batch_id: string
  rows_ingested: number
  duration_seconds: number
  parse_errors: number
  logs: Array<{ ts: string; level: string; msg: string }>
}

export default function DataSources() {
  const [sources, setSources] = useState<Source[]>([])
  const [loading, setLoading] = useState(true)
  const [expanded, setExpanded] = useState<string | null>(null)
  const [testing, setTesting] = useState<string | null>(null)
  const [ingesting, setIngesting] = useState<string | null>(null)
  const [testResults, setTestResults] = useState<Record<string, { success: boolean; latency_ms: number; message: string }>>({})
  const [ingestionResults, setIngestionResults] = useState<Record<string, IngestionResult>>({})

  useEffect(() => {
    api.get(endpoints.sources).then((r) => {
      setSources(r.data.sources)
      setLoading(false)
    })
  }, [])

  const handleTest = async (id: string) => {
    setTesting(id)
    try {
      const r = await api.post(endpoints.testSource(id))
      setTestResults((prev) => ({ ...prev, [id]: r.data }))
    } finally {
      setTesting(null)
    }
  }

  const handleIngest = async (id: string) => {
    setIngesting(id)
    try {
      const r = await api.post(endpoints.ingest(id))
      setIngestionResults((prev) => ({ ...prev, [id]: r.data }))
      setSources((prev) =>
        prev.map((s) => s.id === id ? { ...s, last_ingested: new Date().toISOString(), row_count: s.row_count + r.data.rows_ingested } : s)
      )
    } finally {
      setIngesting(null)
    }
  }

  const formatDate = (iso: string | null) => {
    if (!iso) return 'Never'
    const d = new Date(iso)
    const diff = Math.floor((Date.now() - d.getTime()) / 60000)
    if (diff < 60) return `${diff}m ago`
    if (diff < 1440) return `${Math.floor(diff / 60)}h ago`
    return `${Math.floor(diff / 1440)}d ago`
  }

  if (loading) {
    return (
      <div className="space-y-6">
        <div className="h-8 w-64 skeleton rounded" />
        {[1, 2, 3].map((i) => <div key={i} className="card p-6 h-32 skeleton" />)}
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Data Ingestion</h1>
          <p className="text-sm text-gray-400 mt-1">Manage and monitor all data source connectors</p>
        </div>
        <div className="flex items-center gap-3">
          <span className="badge-success">{sources.filter((s) => ['connected', 'streaming', 'listening'].includes(s.status)).length} Active</span>
          <span className="badge-info">{sources.length} Total Sources</span>
        </div>
      </div>

      <ExplanationPanel
        title="Data Collection & Ingestion"
        what="Data ingestion is the process of transporting data from source systems into your data platform. Sources include databases, APIs, event streams, file drops, and webhooks."
        why="Without reliable ingestion, downstream analytics and ML are built on incomplete or stale data. Ingestion failures cascade — a 1-hour delay in raw data can mean missing an SLA by 6 hours after transformation and reporting."
        how="Senior engineers design ingestion with fault tolerance (retry logic, dead-letter queues), observability (row counts, schema drift alerts), and idempotency (re-running the same batch produces the same result). They separate concerns: ingestion writes raw to Bronze, transformation happens separately."
        tools={['Apache Kafka', 'Fivetran', 'Airbyte', 'dlt', 'Apache NiFi', 'AWS Glue']}
        seniorTip="Always validate schema at ingestion time. A silent schema change (e.g., VARCHAR(50) → VARCHAR(10)) can silently truncate data. Use Schema Registry with Avro/Protobuf for event streams and alert on schema evolution."
      />

      <div className="grid gap-4">
        {sources.map((source) => {
          const Icon = TYPE_ICONS[source.type] || Database
          const status = STATUS_CONFIG[source.status] || STATUS_CONFIG.pending
          const isExpanded = expanded === source.id
          const testResult = testResults[source.id]
          const ingestResult = ingestionResults[source.id]

          return (
            <div key={source.id} className="card overflow-hidden">
              {/* Header */}
              <div className="flex items-start gap-4 p-5">
                <div className="p-2.5 bg-gray-800 rounded-xl flex-shrink-0">
                  <Icon size={20} className="text-blue-400" />
                </div>

                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-3 flex-wrap">
                    <h3 className="font-semibold text-gray-100">{source.name}</h3>
                    <span className="badge-info">{source.type.toUpperCase()}</span>
                    <span className="flex items-center gap-1.5 text-xs">
                      <span className={clsx('w-1.5 h-1.5 rounded-full', status.dot)} />
                      <span className={status.color}>{status.label}</span>
                    </span>
                  </div>
                  <p className="text-xs text-gray-500 mt-1">{source.description}</p>
                  <div className="flex items-center gap-4 mt-2 text-xs text-gray-500">
                    <span>{source.host}{source.port ? `:${source.port}` : ''}</span>
                    <span>•</span>
                    <span>{source.row_count.toLocaleString()} rows</span>
                    <span>•</span>
                    <span>Last ingested: {formatDate(source.last_ingested)}</span>
                  </div>
                </div>

                <div className="flex items-center gap-2 flex-shrink-0">
                  <button
                    onClick={() => handleTest(source.id)}
                    disabled={testing === source.id}
                    className="btn-secondary text-xs"
                  >
                    {testing === source.id ? <RefreshCw size={13} className="animate-spin" /> : <Wifi size={13} />}
                    Test
                  </button>
                  <button
                    onClick={() => handleIngest(source.id)}
                    disabled={ingesting === source.id}
                    className="btn-primary text-xs"
                  >
                    {ingesting === source.id ? <RefreshCw size={13} className="animate-spin" /> : <Play size={13} />}
                    Ingest
                  </button>
                  <button
                    onClick={() => setExpanded(isExpanded ? null : source.id)}
                    className="p-2 rounded-lg hover:bg-gray-800 text-gray-500 transition-colors"
                  >
                    {isExpanded ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
                  </button>
                </div>
              </div>

              {/* Test result banner */}
              {testResult && (
                <div className={clsx(
                  'mx-5 mb-3 px-3 py-2 rounded-lg text-xs flex items-center gap-2',
                  testResult.success ? 'bg-green-900/20 border border-green-800/40 text-green-400' : 'bg-red-900/20 border border-red-800/40 text-red-400'
                )}>
                  {testResult.success ? <CheckCircle size={13} /> : <XCircle size={13} />}
                  {testResult.message}
                </div>
              )}

              {/* Expanded details */}
              {isExpanded && (
                <div className="border-t border-gray-800 p-5 space-y-5 bg-gray-950/30 animate-slide-in">
                  {/* Schema */}
                  <div>
                    <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Schema Preview</h4>
                    <div className="overflow-x-auto">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="border-b border-gray-800">
                            <th className="table-header pb-2 pr-4">Column</th>
                            <th className="table-header pb-2 pr-4">Type</th>
                            <th className="table-header pb-2">Nullable</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-800/50">
                          {source.schema.map((col) => (
                            <tr key={col.column} className="hover:bg-gray-800/20">
                              <td className="py-1.5 pr-4 font-mono text-blue-300">{col.column}</td>
                              <td className="py-1.5 pr-4 text-gray-400">{col.type}</td>
                              <td className="py-1.5">{col.nullable ? <span className="text-yellow-500">Yes</span> : <span className="text-green-500">No</span>}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  {/* Ingestion log */}
                  {ingestResult && (
                    <div>
                      <div className="flex items-center justify-between mb-3">
                        <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Ingestion Log</h4>
                        <div className="flex gap-3 text-xs text-gray-500">
                          <span><span className="text-green-400 font-mono">{ingestResult.rows_ingested.toLocaleString()}</span> rows</span>
                          <span><span className="text-blue-400 font-mono">{ingestResult.duration_seconds}s</span> duration</span>
                          {ingestResult.parse_errors > 0 && <span><span className="text-yellow-400 font-mono">{ingestResult.parse_errors}</span> errors</span>}
                        </div>
                      </div>
                      <div className="code-block space-y-1 text-xs max-h-40 overflow-y-auto">
                        {ingestResult.logs.map((log, i) => (
                          <div key={i} className="flex gap-3">
                            <span className={clsx(
                              'flex-shrink-0',
                              log.level === 'INFO' ? 'text-blue-400' : 'text-yellow-400'
                            )}>[{log.level}]</span>
                            <span className="text-gray-300">{log.msg}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}
