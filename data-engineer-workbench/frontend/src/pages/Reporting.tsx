import { useState, useEffect } from 'react'
import { api, endpoints } from '../utils/api'
import ExplanationPanel from '../components/ExplanationPanel'
import {
  LineChart, Line, BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
  AreaChart, Area,
} from 'recharts'
import { TrendingUp, TrendingDown, Play, Plus, Download, RefreshCw } from 'lucide-react'
import clsx from 'clsx'

interface KPIs {
  kpis: Record<string, { value: number; previous: number; change_pct: number; label: string }>
  revenue_trend: Array<{ date: string; revenue: number; orders: number }>
  category_breakdown: Array<{ category: string; revenue: number; pct: number; orders: number; growth_pct: number }>
  region_breakdown: Array<{ region: string; revenue: number; pct: number }>
}

interface AdhocTable {
  name: string
  description: string
  columns: string[]
}

const COLORS = ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#8b5cf6']

function KPICard({ kpi, id }: { kpi: { value: number; previous: number; change_pct: number; label: string }; id: string }) {
  const isPositive = kpi.change_pct >= 0
  const formatValue = (v: number) => {
    if (id === 'gmv') return `$${(v / 1000000).toFixed(2)}M`
    if (id === 'aov') return `$${v.toFixed(2)}`
    if (id === 'dau') return v.toLocaleString()
    if (id === 'orders') return v.toLocaleString()
    return `${v}%`
  }

  return (
    <div className="card p-5">
      <div className="text-xs text-gray-500 mb-2">{kpi.label}</div>
      <div className="text-2xl font-bold text-white mb-1">{formatValue(kpi.value)}</div>
      <div className={clsx('flex items-center gap-1 text-xs', isPositive ? 'text-green-400' : 'text-red-400')}>
        {isPositive ? <TrendingUp size={13} /> : <TrendingDown size={13} />}
        {isPositive ? '+' : ''}{kpi.change_pct.toFixed(1)}% vs prev period
      </div>
    </div>
  )
}

export default function Reporting() {
  const [kpis, setKpis] = useState<KPIs | null>(null)
  const [tables, setTables] = useState<AdhocTable[]>([])
  const [adhoc, setAdhoc] = useState({ table: 'fact_orders', columns: ['order_id', 'revenue', 'status'], aggregation: '', group_by: '', limit: 20 })
  const [adhocResult, setAdhocResult] = useState<{ sql: string; columns: string[]; rows: Record<string, unknown>[]; row_count: number; execution_time_ms: number } | null>(null)
  const [adhocLoading, setAdhocLoading] = useState(false)
  const [reports, setReports] = useState<unknown[]>([])
  const [chartType, setChartType] = useState<'line' | 'bar' | 'area'>('area')
  const [activeTab, setActiveTab] = useState('dashboard')

  useEffect(() => {
    Promise.all([
      api.get(endpoints.kpis),
      api.get(endpoints.availableTables),
      api.get(endpoints.reports),
    ]).then(([kpiRes, tabRes, repRes]) => {
      setKpis(kpiRes.data)
      setTables(tabRes.data.tables)
      setReports(repRes.data.reports)
    })
  }, [])

  const runAdhoc = async () => {
    setAdhocLoading(true)
    try {
      const r = await api.post(endpoints.adhocQuery, adhoc)
      setAdhocResult(r.data)
    } finally {
      setAdhocLoading(false)
    }
  }

  const selectedTable = tables.find((t) => t.name === adhoc.table)

  const formatRevenue = (v: number) => `$${(v / 1000).toFixed(0)}k`

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Reporting & Analytics</h1>
        <p className="text-sm text-gray-400 mt-1">Executive dashboard, ad-hoc queries, and report scheduling</p>
      </div>

      <ExplanationPanel
        title="Reporting & Analytics Serving Layer"
        what="The serving layer is the final layer of the data platform — it exposes Gold data to business users via BI tools, dashboards, and APIs. It includes pre-computed aggregations, semantic metric layers, and self-service query interfaces."
        why="The serving layer is the only layer most business users see. If it's slow, incomplete, or shows different numbers than another dashboard, trust in data collapses. A poor serving layer can negate years of upstream data engineering work."
        how="Senior engineers build semantic layers (dbt Metrics, Cube.js) that define business metrics in a single, authoritative place. They use materialized views and pre-computed aggregations for common queries, implement caching, and design APIs that are BI-tool agnostic."
        tools={['dbt Metrics', 'Cube.js', 'Looker', 'Tableau', 'Metabase', 'Apache Superset']}
        seniorTip="Define metrics as code, not in BI tools. When 'Revenue' is defined in Tableau, Looker, and Power BI separately, they diverge. Use a semantic layer (dbt Metrics or Cube) that provides a single source of truth for all metric definitions, accessible to every BI tool."
      />

      {/* Tabs */}
      <div className="flex gap-1 border-b border-gray-800">
        {['dashboard', 'adhoc', 'reports'].map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={clsx('px-4 py-2 text-sm font-medium border-b-2 -mb-px capitalize transition-colors', activeTab === tab ? 'border-blue-500 text-blue-400' : 'border-transparent text-gray-500 hover:text-gray-300')}
          >
            {tab === 'dashboard' ? 'Executive Dashboard' : tab === 'adhoc' ? 'Ad-hoc Query Builder' : 'Scheduled Reports'}
          </button>
        ))}
      </div>

      {/* Executive Dashboard */}
      {activeTab === 'dashboard' && kpis && (
        <div className="space-y-6 animate-slide-in">
          {/* KPI Cards */}
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
            {Object.entries(kpis.kpis).map(([id, kpi]) => (
              <KPICard key={id} id={id} kpi={kpi} />
            ))}
          </div>

          {/* Revenue trend */}
          <div className="card p-5">
            <div className="flex items-center justify-between mb-4">
              <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Revenue Trend (30 Days)</div>
              <div className="flex gap-2">
                {(['line', 'bar', 'area'] as const).map((t) => (
                  <button
                    key={t}
                    onClick={() => setChartType(t)}
                    className={clsx('text-xs px-2 py-1 rounded border capitalize', chartType === t ? 'bg-blue-600 text-white border-blue-600' : 'bg-gray-800 text-gray-400 border-gray-700')}
                  >
                    {t}
                  </button>
                ))}
              </div>
            </div>
            <ResponsiveContainer width="100%" height={240}>
              {chartType === 'area' ? (
                <AreaChart data={kpis.revenue_trend}>
                  <defs>
                    <linearGradient id="revGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3,3" />
                  <XAxis dataKey="date" tick={{ fontSize: 10 }} interval={4} />
                  <YAxis tick={{ fontSize: 10 }} tickFormatter={formatRevenue} />
                  <Tooltip formatter={(v: number) => [`$${v.toLocaleString()}`, 'Revenue']} />
                  <Area type="monotone" dataKey="revenue" stroke="#3b82f6" fill="url(#revGrad)" strokeWidth={2} />
                </AreaChart>
              ) : chartType === 'bar' ? (
                <BarChart data={kpis.revenue_trend.slice(-14)}>
                  <CartesianGrid strokeDasharray="3,3" />
                  <XAxis dataKey="date" tick={{ fontSize: 10 }} interval={1} />
                  <YAxis tick={{ fontSize: 10 }} tickFormatter={formatRevenue} />
                  <Tooltip formatter={(v: number) => [`$${v.toLocaleString()}`, 'Revenue']} />
                  <Bar dataKey="revenue" fill="#3b82f6" radius={[3, 3, 0, 0]} />
                </BarChart>
              ) : (
                <LineChart data={kpis.revenue_trend}>
                  <CartesianGrid strokeDasharray="3,3" />
                  <XAxis dataKey="date" tick={{ fontSize: 10 }} interval={4} />
                  <YAxis tick={{ fontSize: 10 }} tickFormatter={formatRevenue} />
                  <Tooltip formatter={(v: number) => [`$${v.toLocaleString()}`, 'Revenue']} />
                  <Line type="monotone" dataKey="revenue" stroke="#3b82f6" strokeWidth={2} dot={false} />
                </LineChart>
              )}
            </ResponsiveContainer>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Category breakdown */}
            <div className="card p-5">
              <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-4">Revenue by Category</div>
              <div className="space-y-3">
                {kpis.category_breakdown.map((cat, i) => (
                  <div key={cat.category}>
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-sm text-gray-300">{cat.category}</span>
                      <div className="flex items-center gap-3">
                        <span className={clsx('text-xs', cat.growth_pct >= 0 ? 'text-green-400' : 'text-red-400')}>
                          {cat.growth_pct >= 0 ? '+' : ''}{cat.growth_pct}%
                        </span>
                        <span className="text-sm font-mono text-gray-200">${(cat.revenue / 1000).toFixed(0)}k</span>
                      </div>
                    </div>
                    <div className="bg-gray-800 h-2 rounded-full overflow-hidden">
                      <div
                        className="h-full rounded-full transition-all"
                        style={{ width: `${cat.pct}%`, backgroundColor: COLORS[i % COLORS.length] }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Region breakdown */}
            <div className="card p-5">
              <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-4">Revenue by Region</div>
              <div className="flex items-center gap-6">
                <ResponsiveContainer width={160} height={160}>
                  <PieChart>
                    <Pie data={kpis.region_breakdown} dataKey="revenue" cx="50%" cy="50%" outerRadius={70}>
                      {kpis.region_breakdown.map((_, i) => (
                        <Cell key={i} fill={COLORS[i % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(v: number) => [`$${(v / 1000000).toFixed(2)}M`, 'Revenue']} />
                  </PieChart>
                </ResponsiveContainer>
                <div className="flex-1 space-y-3">
                  {kpis.region_breakdown.map((r, i) => (
                    <div key={r.region} className="flex items-center gap-2">
                      <div className="w-2.5 h-2.5 rounded-sm flex-shrink-0" style={{ backgroundColor: COLORS[i % COLORS.length] }} />
                      <span className="text-xs text-gray-400 flex-1">{r.region}</span>
                      <span className="text-xs font-mono text-gray-200">{r.pct}%</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Ad-hoc Query Builder */}
      {activeTab === 'adhoc' && (
        <div className="space-y-5 animate-slide-in">
          <div className="card p-5">
            <h3 className="font-semibold text-gray-200 mb-4">Query Builder</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
              <div className="space-y-4">
                <div>
                  <label className="text-xs text-gray-400 block mb-1">Table</label>
                  <select className="select" value={adhoc.table} onChange={(e) => setAdhoc((p) => ({ ...p, table: e.target.value, columns: [] }))}>
                    {tables.map((t) => (
                      <option key={t.name} value={t.name}>{t.name} — {t.description}</option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="text-xs text-gray-400 block mb-1">Columns (select multiple)</label>
                  <div className="bg-gray-800 border border-gray-700 rounded-lg p-3 max-h-40 overflow-y-auto">
                    {selectedTable?.columns.map((col) => (
                      <label key={col} className="flex items-center gap-2 py-1 cursor-pointer hover:text-gray-200">
                        <input
                          type="checkbox"
                          className="rounded"
                          checked={adhoc.columns.includes(col)}
                          onChange={(e) => setAdhoc((p) => ({
                            ...p,
                            columns: e.target.checked ? [...p.columns, col] : p.columns.filter((c) => c !== col)
                          }))}
                        />
                        <span className="text-xs font-mono text-gray-400">{col}</span>
                      </label>
                    ))}
                  </div>
                </div>
              </div>

              <div className="space-y-4">
                <div>
                  <label className="text-xs text-gray-400 block mb-1">Aggregation (optional)</label>
                  <select className="select" value={adhoc.aggregation} onChange={(e) => setAdhoc((p) => ({ ...p, aggregation: e.target.value }))}>
                    <option value="">None (raw rows)</option>
                    <option value="SUM">SUM</option>
                    <option value="AVG">AVG</option>
                    <option value="COUNT">COUNT</option>
                    <option value="MAX">MAX</option>
                    <option value="MIN">MIN</option>
                  </select>
                </div>

                {adhoc.aggregation && (
                  <div>
                    <label className="text-xs text-gray-400 block mb-1">Group By</label>
                    <select className="select" value={adhoc.group_by} onChange={(e) => setAdhoc((p) => ({ ...p, group_by: e.target.value }))}>
                      <option value="">Select column...</option>
                      {selectedTable?.columns.map((col) => (
                        <option key={col} value={col}>{col}</option>
                      ))}
                    </select>
                  </div>
                )}

                <div>
                  <label className="text-xs text-gray-400 block mb-1">Row limit</label>
                  <input className="input" type="number" value={adhoc.limit} onChange={(e) => setAdhoc((p) => ({ ...p, limit: parseInt(e.target.value) || 20 }))} />
                </div>

                <button onClick={runAdhoc} disabled={adhocLoading} className="btn-primary w-full">
                  {adhocLoading ? <RefreshCw size={14} className="animate-spin" /> : <Play size={14} />}
                  Run Query
                </button>
              </div>
            </div>

            {adhocResult && (
              <div className="mt-5 space-y-3 animate-slide-in">
                <div className="code-block text-xs">{adhocResult.sql}</div>
                <div className="text-xs text-gray-500">
                  <span className="text-green-400 font-mono">{adhocResult.row_count}</span> rows in
                  <span className="text-blue-400 font-mono ml-1">{adhocResult.execution_time_ms}ms</span>
                </div>
                <div className="overflow-x-auto max-h-64 border border-gray-800 rounded-lg">
                  <table className="w-full text-xs">
                    <thead className="bg-gray-900 border-b border-gray-800 sticky top-0">
                      <tr>
                        {adhocResult.columns.map((col) => (
                          <th key={col} className="table-header px-4 py-2 whitespace-nowrap">{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-800">
                      {adhocResult.rows.map((row, i) => (
                        <tr key={i} className="hover:bg-gray-800/20">
                          {adhocResult.columns.map((col) => (
                            <td key={col} className="table-cell px-4 font-mono whitespace-nowrap">{String(row[col] ?? '')}</td>
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
      )}

      {/* Scheduled Reports */}
      {activeTab === 'reports' && (
        <div className="space-y-4 animate-slide-in">
          {(reports as Array<{id: string; name: string; description: string; schedule_human: string; delivery: string; recipients: string[]; last_sent: string; status: string}>).map((report) => (
            <div key={report.id} className="card p-5">
              <div className="flex items-start justify-between">
                <div>
                  <h3 className="font-semibold text-gray-200">{report.name}</h3>
                  <p className="text-xs text-gray-500 mt-1">{report.description}</p>
                </div>
                <span className={clsx('text-xs px-2 py-0.5 rounded-full border', report.status === 'active' ? 'text-green-400 bg-green-900/20 border-green-800/40' : 'text-gray-400 bg-gray-800 border-gray-700')}>
                  {report.status}
                </span>
              </div>
              <div className="flex items-center gap-4 mt-3 text-xs text-gray-500">
                <span>Schedule: <span className="text-gray-300">{report.schedule_human}</span></span>
                <span>Delivery: <span className="text-gray-300 capitalize">{report.delivery}</span></span>
                <span>Recipients: <span className="text-blue-400">{report.recipients.join(', ')}</span></span>
                <span>Last sent: <span className="text-gray-300">{new Date(report.last_sent).toLocaleDateString()}</span></span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
