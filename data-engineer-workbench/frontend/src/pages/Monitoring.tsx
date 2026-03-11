import { useState, useEffect, useCallback } from 'react'
import { api, endpoints } from '../utils/api'
import ExplanationPanel from '../components/ExplanationPanel'
import {
  LineChart, Line, BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend
} from 'recharts'
import { CheckCircle, XCircle, AlertTriangle, Clock, RefreshCw, Plus } from 'lucide-react'
import clsx from 'clsx'

interface Metrics {
  summary: {
    pipeline_success_rate: number
    avg_duration_seconds: number
    total_rows_ingested_24h: number
    active_pipelines: number
    failed_tasks_24h: number
    open_alerts: number
  }
  success_rate_trend: Array<{ timestamp: string; value: number }>
  duration_trend: Array<{ timestamp: string; value: number }>
  volume_trend: Array<{ timestamp: string; rows: number }>
  pipeline_failures: Array<{ pipeline: string; failures: number }>
  table_freshness: Array<{ table: string; hours_since_update: number; sla_hours: number; status: string; last_updated: string }>
}

interface AlertRule {
  id: string
  rule_name: string
  metric: string
  operator: string
  threshold: number
  severity: string
  enabled: boolean
}

interface AlertEvent {
  id: string
  rule: string
  severity: string
  triggered_at: string
  resolved: boolean
  resolved_at: string | null
  detail: string
}

const FRESHNESS_STATUS = {
  fresh: { color: 'text-green-400', bg: 'bg-green-900/20', dot: 'bg-green-500' },
  stale: { color: 'text-yellow-400', bg: 'bg-yellow-900/20', dot: 'bg-yellow-500' },
  breached: { color: 'text-red-400', bg: 'bg-red-900/20', dot: 'bg-red-500 animate-pulse' },
}

const PIE_COLORS = ['#ef4444', '#eab308', '#3b82f6', '#8b5cf6', '#22c55e']

function MetricCard({ label, value, unit, color, trend }: { label: string; value: string | number; unit?: string; color?: string; trend?: 'up' | 'down' }) {
  return (
    <div className="card p-5">
      <div className="text-xs text-gray-500 font-medium mb-2">{label}</div>
      <div className={clsx('text-2xl font-bold', color || 'text-white')}>
        {value}<span className="text-sm font-normal text-gray-400 ml-1">{unit}</span>
      </div>
    </div>
  )
}

// ── Anomaly Detection (Z-score) ───────────────────────────────────────────────
function AnomalyDetection() {
  // Generate 30-day simulated row count history with planted anomalies
  const series = Array.from({ length: 30 }, (_, i) => {
    const base = 45000 + Math.sin(i * 0.4) * 3000
    const noise = (Math.random() - 0.5) * 2000
    const anomaly = i === 12 ? -18000 : i === 22 ? 25000 : 0
    return { day: `Day ${i + 1}`, rows: Math.round(base + noise + anomaly) }
  })

  const values = series.map(d => d.rows)
  const mean = values.reduce((a, b) => a + b, 0) / values.length
  const std = Math.sqrt(values.map(v => (v - mean) ** 2).reduce((a, b) => a + b, 0) / values.length)

  const enriched = series.map(d => {
    const z = (d.rows - mean) / std
    return { ...d, z: parseFloat(z.toFixed(2)), anomaly: Math.abs(z) > 2, mean: Math.round(mean) }
  })

  const anomalies = enriched.filter(d => d.anomaly)

  return (
    <div className="card p-5">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="font-semibold text-gray-200">Anomaly Detection — Row Count (Z-score)</h3>
          <p className="text-xs text-gray-500 mt-0.5">Statistical anomaly detection: flag rows where |Z| {'>'} 2.0</p>
        </div>
        <div className="flex gap-3 text-xs">
          <span className="text-gray-400">μ = {Math.round(mean).toLocaleString()} rows</span>
          <span className="text-gray-400">σ = {Math.round(std).toLocaleString()}</span>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2">
          <ResponsiveContainer width="100%" height={180}>
            <LineChart data={enriched}>
              <CartesianGrid strokeDasharray="3,3" stroke="#374151" />
              <XAxis dataKey="day" tick={{ fontSize: 9, fill: '#6b7280' }} interval={4} />
              <YAxis tick={{ fontSize: 9, fill: '#6b7280' }} tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                formatter={(v: number, name: string) => name === 'rows' ? [v.toLocaleString(), 'Row count'] : [v.toLocaleString(), 'Baseline']}
                labelStyle={{ color: '#fff' }}
              />
              <Line type="monotone" dataKey="mean" stroke="#374151" strokeDasharray="4 4" dot={false} name="mean" />
              <Line type="monotone" dataKey="rows" stroke="#3b82f6" strokeWidth={2} dot={(props: any) => {
                if (enriched[props.index]?.anomaly) {
                  return <circle key={props.index} cx={props.cx} cy={props.cy} r={5} fill="#ef4444" stroke="#ef4444" />
                }
                return <circle key={props.index} cx={props.cx} cy={props.cy} r={0} />
              }} name="rows" />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="space-y-2">
          <p className="text-xs text-gray-400 font-semibold mb-2">Detected Anomalies</p>
          {anomalies.length === 0 ? (
            <p className="text-xs text-green-400">No anomalies detected</p>
          ) : (
            anomalies.map((a, i) => (
              <div key={i} className="p-2 bg-red-900/20 border border-red-800 rounded">
                <div className="flex justify-between text-xs">
                  <span className="text-red-300 font-mono">{a.day}</span>
                  <span className="text-red-400">Z = {a.z}</span>
                </div>
                <div className="text-xs text-gray-400 mt-0.5">
                  {a.rows.toLocaleString()} rows ({a.rows > mean ? '+' : ''}{Math.round(a.rows - mean).toLocaleString()} from mean)
                </div>
              </div>
            ))
          )}
          <div className="mt-3 p-2 bg-gray-900 rounded text-xs text-gray-500">
            <p className="text-gray-400 font-semibold mb-1">Z-score formula:</p>
            <p className="font-mono">Z = (x − μ) / σ</p>
            <p className="mt-1">|Z| {'>'} 2.0 → warning</p>
            <p>|Z| {'>'} 3.0 → critical alert</p>
          </div>
        </div>
      </div>
    </div>
  )
}

// ── Cost Monitoring ───────────────────────────────────────────────────────────
function CostMonitoring() {
  const months = ['Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb']
  const costData = months.map((m, i) => ({
    month: m,
    compute: Math.round(4200 + i * 180 + (Math.random() - 0.5) * 300),
    storage: Math.round(1100 + i * 45 + (Math.random() - 0.5) * 80),
    transfer: Math.round(380 + i * 20 + (Math.random() - 0.5) * 60),
  }))

  const latest = costData[costData.length - 1]
  const prev = costData[costData.length - 2]
  const totalLatest = latest.compute + latest.storage + latest.transfer
  const totalPrev = prev.compute + prev.storage + prev.transfer
  const pctChange = (((totalLatest - totalPrev) / totalPrev) * 100).toFixed(1)

  const recommendations = [
    { issue: 'Idle Spark clusters detected', saving: '$420/mo', action: 'Enable auto-termination after 30 min idle' },
    { issue: 'Unpartitioned large tables', saving: '$180/mo', action: 'Partition orders by date — reduces scan cost ~40%' },
    { issue: 'Unused dev clusters running 24/7', saving: '$650/mo', action: 'Schedule dev clusters: 08:00–20:00 weekdays only' },
    { issue: 'Redundant pipeline re-runs', saving: '$95/mo', action: 'Fix idempotency bug causing 3× daily re-runs' },
  ]

  return (
    <div className="card p-5">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="font-semibold text-gray-200">Infrastructure Cost Monitoring</h3>
          <p className="text-xs text-gray-500 mt-0.5">Compute + storage + egress costs by month</p>
        </div>
        <div className="text-right">
          <p className="text-xl font-bold text-white">${totalLatest.toLocaleString()}</p>
          <p className={`text-xs ${Number(pctChange) > 0 ? 'text-red-400' : 'text-green-400'}`}>
            {Number(pctChange) > 0 ? '+' : ''}{pctChange}% vs last month
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <ResponsiveContainer width="100%" height={180}>
          <BarChart data={costData}>
            <CartesianGrid strokeDasharray="3,3" stroke="#374151" />
            <XAxis dataKey="month" tick={{ fontSize: 10, fill: '#9ca3af' }} />
            <YAxis tick={{ fontSize: 10, fill: '#9ca3af' }} tickFormatter={(v) => `$${(v / 1000).toFixed(1)}k`} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
              formatter={(v: number, name: string) => [`$${v.toLocaleString()}`, name]}
            />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <Bar dataKey="compute" name="Compute" fill="#3b82f6" stackId="a" />
            <Bar dataKey="storage" name="Storage" fill="#8b5cf6" stackId="a" />
            <Bar dataKey="transfer" name="Transfer" fill="#f59e0b" stackId="a" radius={[3, 3, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>

        <div className="space-y-2">
          <p className="text-xs font-semibold text-gray-400">Cost Optimization Opportunities</p>
          {recommendations.map((rec, i) => (
            <div key={i} className="p-2 bg-gray-900/50 border border-gray-700 rounded">
              <div className="flex items-center justify-between mb-0.5">
                <p className="text-xs text-white">{rec.issue}</p>
                <span className="text-xs text-green-400 font-mono font-semibold">{rec.saving}</span>
              </div>
              <p className="text-xs text-gray-500">{rec.action}</p>
            </div>
          ))}
          <div className="pt-1 flex justify-between text-xs">
            <span className="text-gray-400">Total potential savings:</span>
            <span className="font-mono font-bold text-green-400">
              ${recommendations.reduce((acc, r) => acc + parseInt(r.saving.replace(/\$|\/mo/g, '')), 0).toLocaleString()}/mo
            </span>
          </div>
        </div>
      </div>
    </div>
  )
}

export default function Monitoring() {
  const [metrics, setMetrics] = useState<Metrics | null>(null)
  const [alerts, setAlerts] = useState<{ rules: AlertRule[]; alert_history: AlertEvent[] } | null>(null)
  const [slas, setSlas] = useState<Array<{ pipeline: string; sla_hours: number; compliance_30d: number; breaches_30d: number; last_run: string }>>([])
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [lastRefresh, setLastRefresh] = useState(new Date())
  const [newRule, setNewRule] = useState({ rule_name: '', metric: 'pipeline_failure_rate', operator: '>', threshold: '0.1', severity: 'warning' })
  const [showNewRule, setShowNewRule] = useState(false)

  const fetchMetrics = useCallback(async () => {
    const [metricsRes, alertsRes, slasRes] = await Promise.all([
      api.get(endpoints.metrics),
      api.get(endpoints.alertRules),
      api.get(endpoints.sla),
    ])
    setMetrics(metricsRes.data)
    setAlerts(alertsRes.data)
    setSlas(slasRes.data.slas)
    setLastRefresh(new Date())
  }, [])

  useEffect(() => {
    fetchMetrics()
  }, [fetchMetrics])

  useEffect(() => {
    if (!autoRefresh) return
    const interval = setInterval(fetchMetrics, 5000)
    return () => clearInterval(interval)
  }, [autoRefresh, fetchMetrics])

  const createAlert = async () => {
    await api.post(endpoints.alertRules, {
      ...newRule,
      threshold: parseFloat(newRule.threshold),
    })
    setShowNewRule(false)
    fetchMetrics()
  }

  const formatTime = (iso: string) => {
    const d = new Date(iso)
    const diff = (Date.now() - d.getTime()) / 60000
    if (diff < 60) return `${Math.round(diff)}m ago`
    return `${Math.round(diff / 60)}h ago`
  }

  if (!metrics) return (
    <div className="space-y-6">
      <div className="h-8 w-64 skeleton rounded" />
      <div className="grid grid-cols-3 gap-4">{[1, 2, 3].map((i) => <div key={i} className="card h-28 skeleton" />)}</div>
    </div>
  )

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Monitoring & Observability</h1>
          <p className="text-sm text-gray-400 mt-1">Real-time pipeline metrics, alerts, and SLA tracking</p>
        </div>
        <div className="flex items-center gap-3">
          <span className="text-xs text-gray-500">Refreshed {lastRefresh.toLocaleTimeString()}</span>
          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            className={clsx('btn-secondary text-xs', autoRefresh && 'text-green-400 border-green-800/50')}
          >
            <RefreshCw size={12} className={autoRefresh ? 'animate-spin' : ''} />
            {autoRefresh ? 'Live' : 'Paused'}
          </button>
          <button onClick={fetchMetrics} className="btn-secondary text-xs">Refresh</button>
        </div>
      </div>

      <ExplanationPanel
        title="Data Observability & Monitoring"
        what="Data observability means having real-time visibility into whether your pipelines are running, data is fresh, and data quality metrics are within acceptable bounds. It's the difference between 'I think the data is correct' and 'I know the data is correct'."
        why="Data incidents (stale dashboards, broken pipelines, quality regressions) cause business decisions based on wrong data. The avg time to detect a data incident is 7 hours — by which time analysts have already acted on bad data. Early alerting cuts this to minutes."
        how="Senior engineers implement a three-tier monitoring system: (1) Infrastructure monitoring (is the pipeline running?), (2) Volume monitoring (did the expected data arrive?), (3) Quality monitoring (does the data look right?). Each tier catches different failure modes."
        tools={['Monte Carlo', 'Bigeye', 'Great Expectations', 'dbt tests', 'Grafana', 'Datadog']}
        seniorTip="Volume monitoring is the most powerful and underused technique. If yesterday's orders table had 45,000 rows and today it has 200, something is wrong — even if the data passes schema checks. Train a rolling average model on row counts and alert on statistical anomalies."
      />

      {/* Summary KPIs */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        <MetricCard label="Success Rate (24h)" value={`${metrics.summary.pipeline_success_rate}%`} color={metrics.summary.pipeline_success_rate >= 95 ? 'text-green-400' : 'text-yellow-400'} />
        <MetricCard label="Avg Duration" value={metrics.summary.avg_duration_seconds} unit="s" />
        <MetricCard label="Rows Ingested (24h)" value={`${(metrics.summary.total_rows_ingested_24h / 1000).toFixed(0)}k`} color="text-blue-400" />
        <MetricCard label="Active Pipelines" value={metrics.summary.active_pipelines} color="text-purple-400" />
        <MetricCard label="Failed Tasks (24h)" value={metrics.summary.failed_tasks_24h} color={metrics.summary.failed_tasks_24h > 0 ? 'text-red-400' : 'text-green-400'} />
        <MetricCard label="Open Alerts" value={metrics.summary.open_alerts} color={metrics.summary.open_alerts > 0 ? 'text-red-400' : 'text-green-400'} />
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="card p-5">
          <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-4">Pipeline Success Rate (24h)</div>
          <ResponsiveContainer width="100%" height={180}>
            <LineChart data={metrics.success_rate_trend}>
              <CartesianGrid strokeDasharray="3,3" />
              <XAxis dataKey="timestamp" tick={{ fontSize: 10 }} interval={3} />
              <YAxis domain={[80, 100]} tick={{ fontSize: 10 }} unit="%" />
              <Tooltip formatter={(v: number) => [`${v}%`, 'Success Rate']} />
              <Line type="monotone" dataKey="value" stroke="#22c55e" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="card p-5">
          <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-4">Data Volume Ingested (24h)</div>
          <ResponsiveContainer width="100%" height={180}>
            <BarChart data={metrics.volume_trend.slice(-12)}>
              <CartesianGrid strokeDasharray="3,3" />
              <XAxis dataKey="timestamp" tick={{ fontSize: 10 }} interval={2} />
              <YAxis tick={{ fontSize: 10 }} />
              <Tooltip formatter={(v: number) => [v.toLocaleString(), 'Rows']} />
              <Bar dataKey="rows" fill="#3b82f6" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="card p-5">
          <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-4">Failures by Pipeline (24h)</div>
          <div className="flex items-center gap-6">
            <ResponsiveContainer width={180} height={180}>
              <PieChart>
                <Pie data={metrics.pipeline_failures} dataKey="failures" nameKey="pipeline" cx="50%" cy="50%" innerRadius={50} outerRadius={80}>
                  {metrics.pipeline_failures.map((_, i) => (
                    <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
            <div className="flex-1 space-y-2">
              {metrics.pipeline_failures.map((p, i) => (
                <div key={p.pipeline} className="flex items-center gap-2 text-xs">
                  <div className="w-2 h-2 rounded-full" style={{ backgroundColor: PIE_COLORS[i % PIE_COLORS.length] }} />
                  <span className="flex-1 text-gray-400 truncate">{p.pipeline}</span>
                  <span className="font-mono font-bold" style={{ color: PIE_COLORS[i % PIE_COLORS.length] }}>{p.failures}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Data Freshness */}
        <div className="card p-5">
          <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-4">Data Freshness</div>
          <div className="space-y-3">
            {metrics.table_freshness.map((t) => {
              const cfg = FRESHNESS_STATUS[t.status as keyof typeof FRESHNESS_STATUS] || FRESHNESS_STATUS.fresh
              const pct = Math.min(t.hours_since_update / t.sla_hours * 100, 100)
              return (
                <div key={t.table}>
                  <div className="flex items-center justify-between mb-1">
                    <div className="flex items-center gap-2">
                      <span className={clsx('w-1.5 h-1.5 rounded-full', cfg.dot)} />
                      <span className="text-xs font-mono text-gray-300">{t.table}</span>
                    </div>
                    <div className="flex items-center gap-2 text-xs">
                      <span className="text-gray-500">{t.hours_since_update}h</span>
                      <span className={cfg.color}>{t.status}</span>
                    </div>
                  </div>
                  <div className="bg-gray-800 h-1.5 rounded-full overflow-hidden">
                    <div
                      className={clsx('h-full rounded-full transition-all', pct >= 100 ? 'bg-red-500' : pct >= 70 ? 'bg-yellow-500' : 'bg-green-500')}
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                  <div className="text-[10px] text-gray-600 mt-0.5">SLA: {t.sla_hours}h</div>
                </div>
              )
            })}
          </div>
        </div>
      </div>

      {/* Alert Rules */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="card overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-800 flex items-center justify-between">
            <h3 className="font-semibold text-gray-200">Alert Rules</h3>
            <button onClick={() => setShowNewRule(!showNewRule)} className="btn-primary text-xs">
              <Plus size={12} /> New Rule
            </button>
          </div>

          {showNewRule && (
            <div className="p-4 border-b border-gray-800 bg-gray-950/30 animate-slide-in space-y-3">
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="text-xs text-gray-400 block mb-1">Rule name</label>
                  <input className="input text-xs" value={newRule.rule_name} onChange={(e) => setNewRule((p) => ({ ...p, rule_name: e.target.value }))} />
                </div>
                <div>
                  <label className="text-xs text-gray-400 block mb-1">Metric</label>
                  <select className="select text-xs" value={newRule.metric} onChange={(e) => setNewRule((p) => ({ ...p, metric: e.target.value }))}>
                    <option value="pipeline_failure_rate">Pipeline Failure Rate</option>
                    <option value="null_rate">Null Rate</option>
                    <option value="row_count">Row Count</option>
                    <option value="duration_seconds">Duration (seconds)</option>
                    <option value="hours_since_update">Hours Since Update</option>
                  </select>
                </div>
                <div>
                  <label className="text-xs text-gray-400 block mb-1">Threshold</label>
                  <div className="flex gap-2">
                    <select className="select text-xs w-16" value={newRule.operator} onChange={(e) => setNewRule((p) => ({ ...p, operator: e.target.value }))}>
                      <option>{'>'}</option>
                      <option>{'<'}</option>
                      <option>{'='}</option>
                    </select>
                    <input className="input text-xs flex-1" type="number" value={newRule.threshold} onChange={(e) => setNewRule((p) => ({ ...p, threshold: e.target.value }))} />
                  </div>
                </div>
                <div>
                  <label className="text-xs text-gray-400 block mb-1">Severity</label>
                  <select className="select text-xs" value={newRule.severity} onChange={(e) => setNewRule((p) => ({ ...p, severity: e.target.value }))}>
                    <option value="info">Info</option>
                    <option value="warning">Warning</option>
                    <option value="critical">Critical</option>
                  </select>
                </div>
              </div>
              <button onClick={createAlert} className="btn-primary text-xs">Create Rule</button>
            </div>
          )}

          <div className="divide-y divide-gray-800">
            {alerts?.rules.map((rule) => (
              <div key={rule.id} className="flex items-center gap-3 px-5 py-3">
                <div className={clsx(
                  'w-1.5 h-1.5 rounded-full flex-shrink-0',
                  rule.severity === 'critical' ? 'bg-red-500' : rule.severity === 'warning' ? 'bg-yellow-500' : 'bg-blue-500'
                )} />
                <div className="flex-1 min-w-0">
                  <div className="text-sm text-gray-200 truncate">{rule.rule_name}</div>
                  <div className="text-xs text-gray-500 font-mono">{rule.metric} {rule.operator} {rule.threshold}</div>
                </div>
                <span className={clsx(
                  'text-[10px] px-2 py-0.5 rounded-full border',
                  rule.severity === 'critical' ? 'text-red-400 bg-red-900/20 border-red-800/40' :
                  rule.severity === 'warning' ? 'text-yellow-400 bg-yellow-900/20 border-yellow-800/40' :
                  'text-blue-400 bg-blue-900/20 border-blue-800/40'
                )}>{rule.severity}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Alert history */}
        <div className="card overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-800">
            <h3 className="font-semibold text-gray-200">Alert History</h3>
          </div>
          <div className="divide-y divide-gray-800 max-h-80 overflow-y-auto">
            {alerts?.alert_history.map((evt) => (
              <div key={evt.id} className="px-5 py-3">
                <div className="flex items-center justify-between mb-1">
                  <div className="flex items-center gap-2">
                    {evt.severity === 'critical' ? <XCircle size={13} className="text-red-400" /> : <AlertTriangle size={13} className="text-yellow-400" />}
                    <span className="text-sm text-gray-200">{evt.rule}</span>
                  </div>
                  <span className={clsx(
                    'text-[10px] px-2 py-0.5 rounded-full',
                    evt.resolved ? 'bg-green-900/20 text-green-400' : 'bg-red-900/20 text-red-400'
                  )}>
                    {evt.resolved ? 'Resolved' : 'Active'}
                  </span>
                </div>
                <p className="text-xs text-gray-500">{evt.detail}</p>
                <div className="flex items-center gap-3 mt-1 text-[10px] text-gray-600">
                  <Clock size={10} />
                  <span>Triggered: {formatTime(evt.triggered_at)}</span>
                  {evt.resolved && evt.resolved_at && <span>Resolved: {formatTime(evt.resolved_at)}</span>}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Anomaly Detection */}
      <AnomalyDetection />

      {/* Cost Monitoring */}
      <CostMonitoring />

      {/* SLA Table */}
      <div className="card overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-800">
          <h3 className="font-semibold text-gray-200">SLA Compliance (Last 30 Days)</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-900 border-b border-gray-800">
              <tr>
                <th className="table-header px-5 py-3">Pipeline</th>
                <th className="table-header px-4 py-3">SLA</th>
                <th className="table-header px-4 py-3">Compliance</th>
                <th className="table-header px-4 py-3">Breaches</th>
                <th className="table-header px-4 py-3">Last Run</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-800">
              {slas.map((sla) => (
                <tr key={sla.pipeline} className="hover:bg-gray-800/20">
                  <td className="table-cell px-5 font-mono text-xs">{sla.pipeline}</td>
                  <td className="table-cell px-4 text-xs">{sla.sla_hours}h</td>
                  <td className="table-cell px-4">
                    <div className="flex items-center gap-2">
                      <div className="flex-1 bg-gray-800 rounded-full h-1.5 w-24 overflow-hidden">
                        <div
                          className={clsx('h-full rounded-full', sla.compliance_30d >= 99 ? 'bg-green-500' : sla.compliance_30d >= 95 ? 'bg-yellow-500' : 'bg-red-500')}
                          style={{ width: `${sla.compliance_30d}%` }}
                        />
                      </div>
                      <span className={clsx(
                        'text-xs font-mono',
                        sla.compliance_30d >= 99 ? 'text-green-400' : sla.compliance_30d >= 95 ? 'text-yellow-400' : 'text-red-400'
                      )}>{sla.compliance_30d}%</span>
                    </div>
                  </td>
                  <td className="table-cell px-4">
                    <span className={clsx('font-mono text-xs', sla.breaches_30d > 0 ? 'text-red-400' : 'text-green-400')}>
                      {sla.breaches_30d}
                    </span>
                  </td>
                  <td className="table-cell px-4 text-xs text-gray-500">{formatTime(sla.last_run)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

function formatTime(iso: string) {
  const d = new Date(iso)
  const diff = (Date.now() - d.getTime()) / 60000
  if (diff < 60) return `${Math.round(diff)}m ago`
  if (diff < 1440) return `${Math.round(diff / 60)}h ago`
  return `${Math.round(diff / 1440)}d ago`
}
