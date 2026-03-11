import { useState, useEffect } from 'react'
import { api, endpoints } from '../utils/api'
import ExplanationPanel from '../components/ExplanationPanel'
import { Play, Database, ArrowRight, GitBranch, RefreshCw } from 'lucide-react'
import clsx from 'clsx'

interface Column {
  name: string
  type: string
  nullable: boolean
  description: string
}

interface Table {
  id: string
  name: string
  full_name: string
  layer: string
  row_count: number
  size_mb: number
  partitioned_by: string | null
  partition_count: number
  last_updated: string
  columns: Column[]
  upstream: string[]
  downstream: string[]
}

interface QueryResult {
  columns: string[]
  rows: Record<string, unknown>[]
  row_count: number
  execution_time_ms: number
  query_plan: string[]
  status: string
}

interface LineageNode {
  id: string
  label: string
  layer: string
  row_count: number | null
}

interface LineageEdge {
  from: string
  to: string
}

const LAYER_CONFIG = {
  bronze: { label: 'Bronze Layer', color: 'text-orange-400', bg: 'bg-orange-900/20', border: 'border-orange-800/40', description: 'Raw, unprocessed data. Write-once. Exact copy of source.' },
  silver: { label: 'Silver Layer', color: 'text-gray-300', bg: 'bg-gray-800/30', border: 'border-gray-700', description: 'Cleaned, validated, standardized. Schema enforced.' },
  gold: { label: 'Gold Layer', color: 'text-yellow-400', bg: 'bg-yellow-900/20', border: 'border-yellow-800/40', description: 'Business-ready. Aggregated, enriched, optimized for queries.' },
}

const SAMPLE_QUERIES = [
  { label: 'Daily Revenue', sql: 'SELECT date, category, SUM(total_revenue) as revenue, SUM(order_count) as orders FROM gold_daily_revenue GROUP BY date, category ORDER BY date DESC LIMIT 20' },
  { label: 'Top Customers', sql: 'SELECT customer_id, segment, city FROM dim_customer WHERE is_active = 1 LIMIT 20' },
  { label: 'Pipeline History', sql: "SELECT pipeline_name, status, duration_seconds, rows_processed FROM pipeline_runs ORDER BY started_at DESC LIMIT 20" },
  { label: 'Order Sampling', sql: 'SELECT order_id, customer_id, revenue, status FROM fact_orders ORDER BY RANDOM() LIMIT 20' },
]

export default function StorageArchitecture() {
  const [tables, setTables] = useState<Table[]>([])
  const [selectedLayer, setSelectedLayer] = useState<string>('all')
  const [selectedTable, setSelectedTable] = useState<Table | null>(null)
  const [sql, setSql] = useState(SAMPLE_QUERIES[0].sql)
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null)
  const [queryLoading, setQueryLoading] = useState(false)
  const [queryError, setQueryError] = useState<string | null>(null)
  const [lineage, setLineage] = useState<{ nodes: LineageNode[]; edges: LineageEdge[] } | null>(null)
  const [showLineage, setShowLineage] = useState(false)

  useEffect(() => {
    api.get(endpoints.warehouseTables).then((r) => {
      setTables(r.data.tables)
      setSelectedTable(r.data.tables[0])
    })
  }, [])

  const runQuery = async () => {
    setQueryLoading(true)
    setQueryError(null)
    try {
      const r = await api.post(endpoints.warehouseQuery, { sql })
      if (r.data.error) {
        setQueryError(r.data.error)
      } else {
        setQueryResult(r.data)
      }
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } }
      setQueryError(err.response?.data?.detail || 'Query failed')
    } finally {
      setQueryLoading(false)
    }
  }

  const loadLineage = async (tableId: string) => {
    const r = await api.get(endpoints.lineage(tableId))
    setLineage(r.data)
    setShowLineage(true)
  }

  const filteredTables = selectedLayer === 'all' ? tables : tables.filter((t) => t.layer === selectedLayer)
  const formatDate = (iso: string) => {
    const d = new Date(iso)
    const diff = (Date.now() - d.getTime()) / 3600000
    if (diff < 1) return `${Math.round(diff * 60)}m ago`
    if (diff < 24) return `${Math.round(diff)}h ago`
    return `${Math.round(diff / 24)}d ago`
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Storage Architecture</h1>
        <p className="text-sm text-gray-400 mt-1">Medallion architecture, table browser, SQL editor, and data lineage</p>
      </div>

      <ExplanationPanel
        title="Data Warehouse & Storage Layers"
        what="The Medallion (Bronze/Silver/Gold) architecture organizes data into progressively refined layers. Bronze = raw landing zone. Silver = cleaned and validated. Gold = business-ready, consumption-optimized tables."
        why="Layered storage enables independent evolution of ingestion (Bronze) and serving (Gold). If transformation logic changes, you can re-process Silver from Bronze without re-ingesting. Gold tables are optimized for BI tools — bad Gold design slows every dashboard query."
        how="Senior engineers partition Gold tables by the most common filter column (usually date), use columnar formats (Parquet/Delta/Iceberg) for 10-100x query speedups, and implement Z-ordering for multi-column filter optimization. They never modify Bronze — it's append-only and immutable."
        tools={['Delta Lake', 'Apache Iceberg', 'Apache Hudi', 'Snowflake', 'BigQuery', 'Databricks']}
        seniorTip="Partition pruning is free performance. A Gold table with 365 daily partitions means a query for 'last 7 days' reads 7/365 = 2% of data. Without partitioning, it reads 100%. Always partition time-series tables by date. Profile query patterns before choosing partition keys."
      />

      {/* Medallion overview */}
      <div className="grid grid-cols-3 gap-4">
        {(Object.entries(LAYER_CONFIG) as [string, typeof LAYER_CONFIG['bronze']][]).map(([layer, cfg]) => {
          const layerTables = tables.filter((t) => t.layer === layer)
          const totalRows = layerTables.reduce((sum, t) => sum + t.row_count, 0)
          const totalMB = layerTables.reduce((sum, t) => sum + t.size_mb, 0)

          return (
            <div
              key={layer}
              className={clsx('card p-5 border cursor-pointer transition-all', cfg.bg, cfg.border, selectedLayer === layer && 'ring-2 ring-blue-500')}
              onClick={() => setSelectedLayer(selectedLayer === layer ? 'all' : layer)}
            >
              <div className={clsx('text-xs font-bold uppercase tracking-widest mb-1', cfg.color)}>{cfg.label}</div>
              <p className="text-xs text-gray-500 mb-4">{cfg.description}</p>
              <div className="grid grid-cols-2 gap-2 text-xs">
                <div>
                  <div className="text-gray-500">Tables</div>
                  <div className="font-mono text-gray-200">{layerTables.length}</div>
                </div>
                <div>
                  <div className="text-gray-500">Total rows</div>
                  <div className="font-mono text-gray-200">{(totalRows / 1e6).toFixed(1)}M</div>
                </div>
                <div>
                  <div className="text-gray-500">Storage</div>
                  <div className="font-mono text-gray-200">{totalMB.toFixed(0)} MB</div>
                </div>
              </div>
            </div>
          )
        })}
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        {/* Table browser */}
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Tables</div>
            <button onClick={() => setSelectedLayer('all')} className="text-xs text-gray-500 hover:text-gray-300">
              {selectedLayer !== 'all' ? 'Show all' : ''}
            </button>
          </div>
          <div className="space-y-2 max-h-96 overflow-y-auto">
            {filteredTables.map((table) => {
              const layerCfg = LAYER_CONFIG[table.layer as keyof typeof LAYER_CONFIG]
              return (
                <button
                  key={table.id}
                  onClick={() => setSelectedTable(table)}
                  className={clsx(
                    'w-full card p-3 text-left hover:border-gray-600 transition-all',
                    selectedTable?.id === table.id && 'ring-2 ring-blue-500'
                  )}
                >
                  <div className="flex items-center gap-2 mb-1">
                    <Database size={12} className={layerCfg?.color} />
                    <span className="text-xs font-semibold text-gray-200 truncate">{table.full_name}</span>
                  </div>
                  <div className="grid grid-cols-2 gap-x-3 text-[10px] text-gray-500">
                    <span>{table.row_count.toLocaleString()} rows</span>
                    <span>{table.size_mb} MB</span>
                    <span>Updated {formatDate(table.last_updated)}</span>
                    {table.partitioned_by && <span>Part: {table.partitioned_by}</span>}
                  </div>
                </button>
              )
            })}
          </div>
        </div>

        {/* Table detail */}
        <div className="xl:col-span-2 space-y-4">
          {selectedTable && (
            <div className="card overflow-hidden">
              <div className="px-5 py-4 border-b border-gray-800 flex items-center justify-between">
                <div>
                  <h3 className="font-semibold text-gray-200">{selectedTable.full_name}</h3>
                  <div className="flex items-center gap-4 mt-1 text-xs text-gray-500">
                    <span>{selectedTable.row_count.toLocaleString()} rows</span>
                    <span>{selectedTable.size_mb} MB</span>
                    {selectedTable.partitioned_by && <span>Partitioned by: {selectedTable.partitioned_by} ({selectedTable.partition_count} partitions)</span>}
                  </div>
                </div>
                <button onClick={() => loadLineage(selectedTable.id)} className="btn-secondary text-xs">
                  <GitBranch size={13} /> Lineage
                </button>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead className="bg-gray-900/50 border-b border-gray-800">
                    <tr>
                      <th className="table-header px-5 py-2">Column</th>
                      <th className="table-header px-4 py-2">Type</th>
                      <th className="table-header px-4 py-2">Nullable</th>
                      <th className="table-header px-4 py-2">Description</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-800/50">
                    {selectedTable.columns.map((col) => (
                      <tr key={col.name} className="hover:bg-gray-800/20">
                        <td className="table-cell px-5 font-mono text-blue-300">{col.name}</td>
                        <td className="table-cell px-4 font-mono text-gray-500">{col.type}</td>
                        <td className="table-cell px-4">{col.nullable ? <span className="text-yellow-500">Yes</span> : <span className="text-green-500">No</span>}</td>
                        <td className="table-cell px-4 text-gray-500 max-w-xs truncate">{col.description}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Upstream/Downstream */}
              <div className="px-5 py-3 border-t border-gray-800 flex items-center gap-6 text-xs">
                <div>
                  <span className="text-gray-500">Upstream: </span>
                  {selectedTable.upstream.length > 0
                    ? selectedTable.upstream.map((u) => <span key={u} className="ml-1 font-mono text-blue-400">{u}</span>)
                    : <span className="text-gray-600">none (source)</span>}
                </div>
                <ArrowRight size={12} className="text-gray-600" />
                <div>
                  <span className="text-gray-500">Downstream: </span>
                  {selectedTable.downstream.map((d) => <span key={d} className="ml-1 font-mono text-green-400">{d}</span>)}
                </div>
              </div>
            </div>
          )}

          {/* Lineage */}
          {showLineage && lineage && (
            <div className="card p-4 animate-slide-in">
              <div className="flex items-center justify-between mb-3">
                <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Data Lineage Graph</div>
                <button onClick={() => setShowLineage(false)} className="text-xs text-gray-500 hover:text-gray-300">Close</button>
              </div>
              <div className="flex flex-wrap items-center gap-3">
                {lineage.nodes.map((node, i) => (
                  <div key={node.id} className="flex items-center gap-2">
                    <div className={clsx(
                      'px-3 py-2 rounded-lg border text-xs',
                      node.layer === 'bronze' ? 'bg-orange-900/20 border-orange-800/40 text-orange-300' :
                      node.layer === 'silver' ? 'bg-gray-800 border-gray-700 text-gray-300' :
                      node.layer === 'gold' ? 'bg-yellow-900/20 border-yellow-800/40 text-yellow-300' :
                      'bg-blue-900/20 border-blue-800/40 text-blue-300'
                    )}>
                      <div className="font-mono font-semibold">{node.label}</div>
                      {node.row_count && <div className="text-[10px] opacity-60 mt-0.5">{node.row_count.toLocaleString()} rows</div>}
                    </div>
                    {i < lineage.nodes.length - 1 && <ArrowRight size={14} className="text-gray-600 flex-shrink-0" />}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* SQL Editor */}
      <div className="card overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-800 flex items-center justify-between flex-wrap gap-3">
          <h3 className="font-semibold text-gray-200">SQL Query Editor</h3>
          <div className="flex items-center gap-3">
            <div className="flex gap-2 flex-wrap">
              {SAMPLE_QUERIES.map((q) => (
                <button key={q.label} onClick={() => setSql(q.sql)} className="btn-secondary text-xs">
                  {q.label}
                </button>
              ))}
            </div>
            <button onClick={runQuery} disabled={queryLoading} className="btn-primary">
              {queryLoading ? <RefreshCw size={14} className="animate-spin" /> : <Play size={14} />}
              Run
            </button>
          </div>
        </div>
        <div className="p-4">
          <textarea
            className="w-full code-block min-h-24 text-xs resize-y outline-none bg-gray-950 text-cyan-300"
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            spellCheck={false}
          />
        </div>

        {queryError && (
          <div className="mx-4 mb-4 p-3 rounded-lg bg-red-900/20 border border-red-800/40 text-red-400 text-xs">
            {queryError}
          </div>
        )}

        {queryResult && !queryError && (
          <div className="border-t border-gray-800 animate-slide-in">
            <div className="flex items-center gap-4 px-4 py-2 bg-gray-900/50 text-xs text-gray-500">
              <span><span className="text-green-400 font-mono">{queryResult.row_count}</span> rows</span>
              <span><span className="text-blue-400 font-mono">{queryResult.execution_time_ms}ms</span></span>
              {queryResult.query_plan.map((step, i) => (
                <span key={i} className="text-gray-600">› {step}</span>
              ))}
            </div>
            <div className="overflow-x-auto max-h-64">
              <table className="w-full text-xs">
                <thead className="bg-gray-900 border-b border-gray-800 sticky top-0">
                  <tr>
                    {queryResult.columns.map((col) => (
                      <th key={col} className="table-header px-4 py-2 whitespace-nowrap">{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-800/30">
                  {queryResult.rows.map((row, i) => (
                    <tr key={i} className="hover:bg-gray-800/20">
                      {queryResult.columns.map((col) => (
                        <td key={col} className="table-cell px-4 font-mono whitespace-nowrap max-w-xs truncate">
                          {String(row[col] ?? '')}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
