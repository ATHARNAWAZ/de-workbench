import { useState, useEffect, useRef, useCallback } from 'react'
import { api, WS_BASE } from '../utils/api'
import ExplanationPanel from '../components/ExplanationPanel'
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import {
  Zap, Server, Layers, Activity, GitBranch, Cpu, Globe,
  Play, Square, RefreshCw, Plus, Clock, ChevronDown, ChevronRight,
  CheckCircle, XCircle, AlertTriangle, Database, Code, BookOpen,
  Network, Settings, TrendingUp, Package, Shield, ArrowRight,
  ChevronUp, Eye, Terminal, Box, Boxes,
} from 'lucide-react'
import clsx from 'clsx'

// ─── Shared helpers ──────────────────────────────────────

function SectionTab({ id, label, icon: Icon, active, onClick }: {
  id: string; label: string; icon: React.ElementType; active: boolean; onClick: () => void
}) {
  return (
    <button
      onClick={onClick}
      className={clsx(
        'flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all whitespace-nowrap',
        active
          ? 'bg-orange-600/15 text-orange-400 border border-orange-600/20'
          : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800'
      )}
    >
      <Icon size={14} />
      <span className="hidden sm:inline">{label}</span>
      <span className="sm:hidden">{id}</span>
    </button>
  )
}

function SubTab({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className={clsx(
        'px-3 py-1.5 text-xs font-medium rounded-md transition-colors',
        active ? 'bg-gray-700 text-white' : 'text-gray-500 hover:text-gray-300 hover:bg-gray-800'
      )}
    >
      {label}
    </button>
  )
}

function StatusBadge({ state }: { state: string }) {
  const map: Record<string, string> = {
    RUNNING: 'badge-success', ACTIVE: 'badge-success', SUCCESS: 'badge-success', SUCCEEDED: 'badge-success',
    TERMINATED: 'badge-error', FAILED: 'badge-error', ERROR: 'badge-error',
    STARTING: 'badge-warning', PAUSED: 'badge-warning', WARNING: 'badge-warning',
  }
  return <span className={clsx('badge', map[state] ?? 'badge-info')}>{state}</span>
}

function MetricCard({ label, value, sub, color = 'blue' }: { label: string; value: string | number; sub?: string; color?: string }) {
  const colors: Record<string, string> = {
    blue: 'text-blue-400', green: 'text-green-400', orange: 'text-orange-400',
    red: 'text-red-400', purple: 'text-purple-400', cyan: 'text-cyan-400',
  }
  return (
    <div className="card p-4">
      <div className="text-xs text-gray-500 mb-1">{label}</div>
      <div className={clsx('text-2xl font-bold font-mono', colors[color] ?? 'text-blue-400')}>{value}</div>
      {sub && <div className="text-xs text-gray-600 mt-0.5">{sub}</div>}
    </div>
  )
}

// ─── SECTION A: Spark Fundamentals ───────────────────────

function SectionSpark() {
  const [sub, setSub] = useState('concepts')
  const [concepts, setConcepts] = useState<any>(null)
  const [code, setCode] = useState('')
  const [datasetSize, setDatasetSize] = useState('medium')
  const [jobResult, setJobResult] = useState<any>(null)
  const [running, setRunning] = useState(false)
  const [templates, setTemplates] = useState<any[]>([])

  useEffect(() => {
    api.get('/api/bigdata/spark/concepts').then(r => setConcepts(r.data))
    api.get('/api/bigdata/spark/templates').then(r => {
      setTemplates(r.data)
      setCode(r.data[0]?.code ?? '')
    })
  }, [])

  const runJob = async () => {
    if (!code.trim()) return
    setRunning(true)
    setJobResult(null)
    try {
      const r = await api.post('/api/bigdata/spark/simulate', { code, dataset_size: datasetSize })
      setJobResult(r.data)
    } finally {
      setRunning(false)
    }
  }

  const typeColor: Record<string, string> = {
    narrow: 'text-green-400 bg-green-900/20 border-green-800/40',
    wide: 'text-red-400 bg-red-900/20 border-red-800/40',
    action: 'text-yellow-400 bg-yellow-900/20 border-yellow-800/40',
    source: 'text-blue-400 bg-blue-900/20 border-blue-800/40',
  }

  return (
    <div className="space-y-4">
      <div className="flex gap-2 flex-wrap">
        {['concepts', 'lazy-eval', 'transformations', 'job-simulator'].map(s => (
          <SubTab key={s} label={s.replace('-', ' ').replace(/\b\w/g, c => c.toUpperCase())} active={sub === s} onClick={() => setSub(s)} />
        ))}
      </div>

      {sub === 'concepts' && concepts && (
        <div className="space-y-6">
          {/* RDD / DataFrame / Dataset */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {concepts.abstractions.map((a: any) => (
              <div key={a.name} className={clsx('card p-5 border-t-2', {
                'border-orange-500': a.color === 'orange',
                'border-blue-500': a.color === 'blue',
                'border-green-500': a.color === 'green',
              })}>
                <div className="flex items-center justify-between mb-3">
                  <span className={clsx('text-xl font-bold font-mono', {
                    'text-orange-400': a.color === 'orange',
                    'text-blue-400': a.color === 'blue',
                    'text-green-400': a.color === 'green',
                  })}>{a.name}</span>
                  <span className="text-[10px] text-gray-600 font-mono">{a.year}</span>
                </div>
                <div className="text-xs text-gray-500 mb-3">{a.full_name}</div>
                <p className="text-sm text-gray-400 mb-3">{a.description}</p>
                <div className="space-y-2">
                  <div className="text-xs">
                    <span className="text-gray-600">Use when: </span>
                    <span className="text-gray-300">{a.use_when}</span>
                  </div>
                  <div className="flex items-center gap-1 flex-wrap">
                    {a.api_langs.map((l: string) => (
                      <span key={l} className="text-[10px] px-1.5 py-0.5 bg-gray-800 border border-gray-700 rounded text-gray-400">{l}</span>
                    ))}
                  </div>
                  <div className="flex items-center gap-1.5">
                    {a.optimized
                      ? <><CheckCircle size={12} className="text-green-400" /><span className="text-xs text-green-400">Catalyst optimized</span></>
                      : <><XCircle size={12} className="text-gray-600" /><span className="text-xs text-gray-600">No optimizer</span></>
                    }
                  </div>
                </div>
                {/* Partition diagram */}
                <div className="mt-4 flex gap-1">
                  {Array.from({ length: a.partition_count }).map((_, i) => (
                    <div key={i} className={clsx('flex-1 h-6 rounded text-[9px] flex items-center justify-center font-mono', {
                      'bg-orange-900/40 text-orange-500': a.color === 'orange',
                      'bg-blue-900/40 text-blue-500': a.color === 'blue',
                      'bg-green-900/40 text-green-500': a.color === 'green',
                    })}>P{i}</div>
                  ))}
                </div>
                <div className="text-[10px] text-gray-600 mt-1 text-center">partitions distributed across executors</div>
              </div>
            ))}
          </div>

          {/* DAG Stages */}
          <div className="card p-5">
            <div className="text-sm font-semibold text-gray-200 mb-4">Spark DAG — Stage Breakdown</div>
            <div className="flex items-stretch gap-2 overflow-x-auto pb-2">
              {concepts.dag_stages.map((stage: any, i: number) => (
                <div key={stage.stage} className="flex items-center gap-2 flex-shrink-0">
                  <div className={clsx('rounded-lg p-3 text-center min-w-[140px]', {
                    'bg-green-900/20 border border-green-800/40': stage.type === 'narrow',
                    'bg-red-900/20 border border-red-800/40': stage.type === 'shuffle_write' || stage.type === 'shuffle_read',
                    'bg-yellow-900/20 border border-yellow-800/40': stage.type === 'action',
                  })}>
                    <div className="text-[10px] font-semibold mb-1"
                      style={{ color: stage.type === 'narrow' ? '#4ade80' : stage.type === 'action' ? '#facc15' : '#f87171' }}>
                      {stage.name}
                    </div>
                    <div className="text-[10px] text-gray-500">{stage.tasks} tasks</div>
                    {stage.shuffle_out_mb > 0 && <div className="text-[10px] text-red-400 mt-1">Shuffle: {stage.shuffle_out_mb}MB</div>}
                  </div>
                  {i < concepts.dag_stages.length - 1 && <ArrowRight size={14} className="text-gray-600 flex-shrink-0" />}
                </div>
              ))}
            </div>
            <div className="flex gap-4 mt-3 text-[10px]">
              <span className="flex items-center gap-1"><span className="w-3 h-3 rounded bg-green-900/40 border border-green-800/40 inline-block" /> Narrow (no shuffle)</span>
              <span className="flex items-center gap-1"><span className="w-3 h-3 rounded bg-red-900/40 border border-red-800/40 inline-block" /> Wide (shuffle boundary)</span>
              <span className="flex items-center gap-1"><span className="w-3 h-3 rounded bg-yellow-900/40 border border-yellow-800/40 inline-block" /> Action (triggers execution)</span>
            </div>
          </div>
        </div>
      )}

      {sub === 'lazy-eval' && concepts && (
        <div className="card p-5 space-y-4">
          <div className="text-sm font-semibold text-gray-200">Lazy Evaluation — Nothing Runs Until an Action</div>
          <p className="text-xs text-gray-500">Each transformation builds a logical plan. Only when an <strong className="text-yellow-400">action</strong> is called does Spark execute the entire optimized plan in one pass.</p>
          <div className="space-y-2">
            {concepts.lazy_evaluation_chain.map((step: any, i: number) => (
              <div key={i} className="flex items-start gap-3">
                <div className="flex flex-col items-center">
                  <div className={clsx('w-6 h-6 rounded-full flex items-center justify-center text-[10px] font-bold flex-shrink-0', {
                    'bg-blue-600/20 text-blue-400': step.type === 'source',
                    'bg-green-600/20 text-green-400': step.type === 'narrow',
                    'bg-red-600/20 text-red-400': step.type === 'wide',
                    'bg-yellow-600/20 text-yellow-400': step.type === 'action',
                  })}>{i + 1}</div>
                  {i < concepts.lazy_evaluation_chain.length - 1 && <div className="w-px h-4 bg-gray-700 mt-1" />}
                </div>
                <div className="flex-1 min-w-0">
                  <code className={clsx('text-xs font-mono px-2 py-1 rounded border block', typeColor[step.type])}>{step.op}</code>
                  <p className="text-[11px] text-gray-500 mt-1">{step.note}</p>
                </div>
                <span className={clsx('text-[9px] px-1.5 py-0.5 rounded uppercase font-bold flex-shrink-0', typeColor[step.type])}>
                  {step.type}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {sub === 'transformations' && concepts && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="card p-5">
            <div className="flex items-center gap-2 mb-4">
              <div className="w-2 h-2 rounded-full bg-green-400" />
              <span className="text-sm font-semibold text-green-400">Narrow Transformations</span>
            </div>
            <p className="text-xs text-gray-500 mb-3">Each partition is processed independently. No network shuffle. Stays in same stage.</p>
            <div className="space-y-2">
              {concepts.transformations.narrow.map((t: any) => (
                <div key={t.name} className="flex items-center gap-3 p-2 rounded bg-green-900/10 border border-green-900/30">
                  <code className="text-xs font-mono text-green-400 w-32 flex-shrink-0">{t.name}</code>
                  <span className="text-xs text-gray-400">{t.description}</span>
                </div>
              ))}
            </div>
            {/* Visual: partitions stay in place */}
            <div className="mt-4 flex gap-2">
              {[0, 1, 2, 3].map(p => (
                <div key={p} className="flex-1 flex flex-col gap-1">
                  <div className="h-8 rounded bg-green-900/20 border border-green-800/40 flex items-center justify-center text-[9px] text-green-500">P{p}</div>
                  <ArrowRight size={12} className="text-green-600 mx-auto rotate-90" />
                  <div className="h-8 rounded bg-green-900/30 border border-green-800/50 flex items-center justify-center text-[9px] text-green-400">P{p}'</div>
                </div>
              ))}
            </div>
            <div className="text-[10px] text-gray-600 text-center mt-1">Partitions stay on same executor</div>
          </div>

          <div className="card p-5">
            <div className="flex items-center gap-2 mb-4">
              <div className="w-2 h-2 rounded-full bg-red-400 animate-pulse" />
              <span className="text-sm font-semibold text-red-400">Wide Transformations (Shuffle)</span>
            </div>
            <p className="text-xs text-gray-500 mb-3">Data must cross network to be co-located by key. Creates a new stage boundary.</p>
            <div className="space-y-2">
              {concepts.transformations.wide.map((t: any) => (
                <div key={t.name} className="flex items-center gap-3 p-2 rounded bg-red-900/10 border border-red-900/30">
                  <code className="text-xs font-mono text-red-400 w-32 flex-shrink-0">{t.name}</code>
                  <span className="text-xs text-gray-400">{t.description}</span>
                </div>
              ))}
            </div>
            {/* Visual: partitions cross */}
            <div className="mt-4 relative">
              <div className="flex gap-2 mb-2">
                {[0, 1, 2, 3].map(p => (
                  <div key={p} className="flex-1 h-8 rounded bg-red-900/20 border border-red-800/40 flex items-center justify-center text-[9px] text-red-500">P{p}</div>
                ))}
              </div>
              <div className="text-[10px] text-red-400 text-center py-1 border-y border-dashed border-red-800/40 my-1">
                ← SHUFFLE (network transfer) →
              </div>
              <div className="flex gap-2 mt-2">
                {[0, 1, 2, 3].map(p => (
                  <div key={p} className="flex-1 h-8 rounded bg-red-900/30 border border-red-800/50 flex items-center justify-center text-[9px] text-red-400">P{p}'</div>
                ))}
              </div>
            </div>
            <div className="text-[10px] text-gray-600 text-center mt-1">Data redistributed across all executors by key</div>
          </div>
        </div>
      )}

      {sub === 'job-simulator' && (
        <div className="space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div className="card p-5 space-y-3">
              <div className="text-sm font-semibold text-gray-200">PySpark Code Editor</div>
              <div className="flex gap-2 flex-wrap">
                {templates.map((t: any) => (
                  <button key={t.id} onClick={() => setCode(t.code)}
                    className="text-xs px-2 py-1 bg-gray-800 hover:bg-gray-700 border border-gray-700 rounded text-gray-300 transition-colors">
                    {t.name}
                  </button>
                ))}
              </div>
              <textarea
                value={code}
                onChange={e => setCode(e.target.value)}
                className="w-full h-64 font-mono text-xs bg-gray-900 border border-gray-700 rounded-lg p-3 text-green-400 resize-none focus:outline-none focus:border-orange-600/50"
                placeholder="# Write PySpark code here..."
                spellCheck={false}
              />
              <div className="flex items-center gap-3">
                <select value={datasetSize} onChange={e => setDatasetSize(e.target.value)} className="select text-sm flex-1">
                  <option value="small">Small dataset (10K rows)</option>
                  <option value="medium">Medium dataset (100K rows)</option>
                  <option value="large">Large dataset (1M rows)</option>
                </select>
                <button onClick={runJob} disabled={running} className="btn-primary flex items-center gap-2 px-4 py-2">
                  {running ? <RefreshCw size={14} className="animate-spin" /> : <Play size={14} />}
                  {running ? 'Simulating...' : 'Run Job'}
                </button>
              </div>
            </div>

            {/* Results */}
            <div className="card p-5 space-y-4">
              <div className="text-sm font-semibold text-gray-200">Spark UI — Job Results</div>
              {!jobResult && !running && (
                <div className="flex flex-col items-center justify-center h-48 text-gray-600 text-sm">
                  <Terminal size={32} className="mb-2 opacity-30" />
                  Run a job to see the execution breakdown
                </div>
              )}
              {running && (
                <div className="flex flex-col items-center justify-center h-48 space-y-3">
                  <RefreshCw size={24} className="animate-spin text-orange-400" />
                  <span className="text-sm text-gray-400">Simulating Spark execution...</span>
                </div>
              )}
              {jobResult && (
                <div className="space-y-4">
                  <div className="grid grid-cols-2 gap-2">
                    <MetricCard label="Status" value={jobResult.status} color="green" />
                    <MetricCard label="Total Duration" value={`${(jobResult.total_duration_ms / 1000).toFixed(1)}s`} color="blue" />
                    <MetricCard label="Input Rows" value={jobResult.input_rows.toLocaleString()} color="orange" />
                    <MetricCard label="Output Rows" value={jobResult.output_rows.toLocaleString()} color="cyan" />
                  </div>
                  <div className="text-xs text-gray-500 font-semibold uppercase tracking-wider">Stage Breakdown</div>
                  {jobResult.stages.map((s: any) => (
                    <div key={s.stage_id} className="space-y-1">
                      <div className="flex justify-between text-xs">
                        <span className="text-gray-300">{s.name}</span>
                        <span className="text-gray-500 font-mono">{(s.duration_ms / 1000).toFixed(1)}s · {s.tasks} tasks</span>
                      </div>
                      <div className="w-full bg-gray-800 rounded-full h-2">
                        <div className="bg-orange-500 h-2 rounded-full" style={{ width: `${Math.min(100, (s.duration_ms / jobResult.total_duration_ms) * 100)}%` }} />
                      </div>
                      {s.shuffle_read_mb > 0 && <div className="text-[10px] text-red-400">Shuffle read: {s.shuffle_read_mb}MB → Shuffle write: {s.shuffle_write_mb}MB</div>}
                    </div>
                  ))}
                  <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider mt-2">Executor Summary</div>
                  <div className="overflow-x-auto">
                    <table className="w-full text-xs">
                      <thead><tr className="border-b border-gray-800">
                        {['Executor', 'Tasks', 'GC Time', 'Peak Mem', 'Shuffle R/W'].map(h => <th key={h} className="text-left py-1 pr-3 text-gray-600 font-medium">{h}</th>)}
                      </tr></thead>
                      <tbody>{jobResult.executors.map((e: any) => (
                        <tr key={e.id} className="border-b border-gray-800/50">
                          <td className="py-1 pr-3 text-gray-400 font-mono">{e.id}</td>
                          <td className="py-1 pr-3 text-gray-300">{e.tasks_completed}</td>
                          <td className="py-1 pr-3 text-gray-400">{e.gc_time_ms}ms</td>
                          <td className="py-1 pr-3 text-gray-400">{e.peak_memory_mb}MB</td>
                          <td className="py-1 pr-3 text-gray-400">{e.shuffle_read_mb}/{e.shuffle_write_mb}MB</td>
                        </tr>
                      ))}</tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

// ─── SECTION B: Databricks ────────────────────────────────

function SectionDatabricks() {
  const [sub, setSub] = useState('notebooks')
  const [notebooks, setNotebooks] = useState<any[]>([])
  const [selectedNb, setSelectedNb] = useState<any>(null)
  const [clusters, setClusters] = useState<any[]>([])
  const [nodeTypes, setNodeTypes] = useState<any[]>([])
  const [runtimes, setRuntimes] = useState<string[]>([])
  const [catalog, setCatalog] = useState<any>(null)
  const [runningCells, setRunningCells] = useState<Set<string>>(new Set())
  const [ranCells, setRanCells] = useState<Set<string>>(new Set())
  const [showCreateCluster, setShowCreateCluster] = useState(false)
  const [newCluster, setNewCluster] = useState({ name: '', node_type: 'i3.xlarge', min_workers: 2, max_workers: 8, autoscaling: true, runtime: '' })
  const [expandedCatalog, setExpandedCatalog] = useState<Record<string, boolean>>({ prod: true, 'prod.silver': true })
  const [selectedTable, setSelectedTable] = useState<string>('')

  useEffect(() => {
    api.get('/api/bigdata/databricks/notebooks').then(r => {
      setNotebooks(r.data)
      if (r.data.length > 0) loadNotebook(r.data[0].id)
    })
    api.get('/api/bigdata/databricks/clusters').then(r => setClusters(r.data))
    api.get('/api/bigdata/databricks/node-types').then(r => {
      setNodeTypes(r.data.node_types)
      setRuntimes(r.data.runtimes)
      setNewCluster(c => ({ ...c, runtime: r.data.runtimes[0] }))
    })
    api.get('/api/bigdata/databricks/unity-catalog').then(r => setCatalog(r.data))
  }, [])

  const loadNotebook = async (id: string) => {
    const r = await api.get(`/api/bigdata/databricks/notebooks/${id}`)
    setSelectedNb(r.data)
    setRanCells(new Set())
  }

  const runCell = async (cellId: string) => {
    setRunningCells(prev => new Set(prev).add(cellId))
    await new Promise(r => setTimeout(r, 1200 + Math.random() * 1000))
    setRunningCells(prev => { const s = new Set(prev); s.delete(cellId); return s })
    setRanCells(prev => new Set(prev).add(cellId))
  }

  const runAll = async () => {
    if (!selectedNb) return
    for (const cell of selectedNb.cells) {
      if (cell.type !== 'markdown') await runCell(cell.id)
    }
  }

  const toggleCluster = async (id: string) => {
    const r = await api.put(`/api/bigdata/databricks/clusters/${id}/toggle`)
    setClusters(prev => prev.map(c => c.id === id ? r.data : c))
  }

  const createCluster = async () => {
    if (!newCluster.name) return
    const r = await api.post('/api/bigdata/databricks/clusters', newCluster)
    setClusters(prev => [...prev, r.data])
    setShowCreateCluster(false)
    setNewCluster({ name: '', node_type: 'i3.xlarge', min_workers: 2, max_workers: 8, autoscaling: true, runtime: runtimes[0] })
  }

  const dbuCost = (cluster: any) => {
    const dbu = cluster.dbu_hour * cluster.current_workers
    return `$${(dbu * 0.40).toFixed(2)}/hr`
  }

  const cellTypeColor: Record<string, string> = {
    python: 'border-blue-800/40 bg-blue-900/10',
    sql: 'border-purple-800/40 bg-purple-900/10',
    markdown: 'border-gray-700 bg-gray-900/30',
  }
  const cellTypeLabel: Record<string, string> = { python: 'Python', sql: 'SQL', markdown: 'Markdown' }

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        {['notebooks', 'clusters', 'unity-catalog'].map(s => (
          <SubTab key={s} label={s.replace('-', ' ').replace(/\b\w/g, c => c.toUpperCase())} active={sub === s} onClick={() => setSub(s)} />
        ))}
      </div>

      {sub === 'notebooks' && (
        <div className="grid grid-cols-12 gap-4">
          {/* Notebook list */}
          <div className="col-span-3 space-y-1">
            <div className="text-[10px] text-gray-600 uppercase tracking-wider font-semibold px-2 mb-2">Notebooks</div>
            {notebooks.map(nb => (
              <button key={nb.id} onClick={() => loadNotebook(nb.id)}
                className={clsx('w-full text-left p-2.5 rounded-lg text-xs transition-colors', selectedNb?.id === nb.id ? 'bg-orange-600/10 border border-orange-600/20 text-orange-300' : 'text-gray-400 hover:bg-gray-800')}>
                <div className="font-medium">{nb.name}</div>
                <div className="text-gray-600 mt-0.5">{nb.description}</div>
              </button>
            ))}
          </div>

          {/* Notebook content */}
          <div className="col-span-9 space-y-3">
            {selectedNb && (
              <>
                <div className="flex items-center justify-between">
                  <div className="text-sm font-semibold text-gray-200">{selectedNb.name}</div>
                  <div className="flex gap-2">
                    <button onClick={runAll} className="btn-primary flex items-center gap-1.5 text-xs px-3 py-1.5">
                      <Play size={12} /> Run All
                    </button>
                  </div>
                </div>
                {selectedNb.cells.map((cell: any) => (
                  <div key={cell.id} className={clsx('rounded-lg border overflow-hidden', cellTypeColor[cell.type])}>
                    <div className="flex items-center justify-between px-3 py-1.5 border-b border-gray-700/50">
                      <span className="text-[10px] font-semibold text-gray-500 uppercase tracking-wider">{cellTypeLabel[cell.type]}</span>
                      {cell.type !== 'markdown' && (
                        <div className="flex items-center gap-2">
                          {cell.duration_ms && ranCells.has(cell.id) && (
                            <span className="text-[10px] text-gray-600 font-mono">{(cell.duration_ms / 1000).toFixed(2)}s</span>
                          )}
                          <button onClick={() => runCell(cell.id)} disabled={runningCells.has(cell.id)}
                            className="text-[10px] px-2 py-0.5 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 flex items-center gap-1">
                            {runningCells.has(cell.id) ? <RefreshCw size={10} className="animate-spin" /> : <Play size={10} />}
                            {runningCells.has(cell.id) ? 'Running' : 'Run'}
                          </button>
                        </div>
                      )}
                    </div>
                    <pre className="px-4 py-3 text-xs font-mono text-gray-300 whitespace-pre-wrap overflow-x-auto">{cell.source}</pre>

                    {/* Output */}
                    {cell.type !== 'markdown' && ranCells.has(cell.id) && cell.output && (
                      <div className="border-t border-gray-700/50 bg-gray-950/50">
                        <pre className="px-4 py-3 text-xs font-mono text-green-400 whitespace-pre-wrap overflow-x-auto">{cell.output}</pre>
                      </div>
                    )}
                    {runningCells.has(cell.id) && (
                      <div className="border-t border-gray-700/50 px-4 py-2 flex items-center gap-2">
                        <RefreshCw size={12} className="animate-spin text-orange-400" />
                        <span className="text-xs text-gray-500">Executing on cluster...</span>
                      </div>
                    )}

                    {/* Annotation */}
                    {cell.annotation && ranCells.has(cell.id) && (
                      <div className="border-t border-amber-800/20 bg-amber-900/5 px-4 py-2">
                        <span className="text-[10px] text-amber-400 font-semibold">Note: </span>
                        <span className="text-[10px] text-gray-500">{cell.annotation}</span>
                      </div>
                    )}
                  </div>
                ))}
              </>
            )}
          </div>
        </div>
      )}

      {sub === 'clusters' && (
        <div className="space-y-4">
          <div className="flex justify-between items-center">
            <div className="text-sm font-semibold text-gray-300">{clusters.length} Clusters</div>
            <button onClick={() => setShowCreateCluster(!showCreateCluster)} className="btn-primary flex items-center gap-2 text-sm px-3 py-1.5">
              <Plus size={14} /> New Cluster
            </button>
          </div>

          {showCreateCluster && (
            <div className="card p-5 border-blue-800/30 border space-y-4">
              <div className="text-sm font-semibold text-gray-200">Create Cluster</div>
              <div className="grid grid-cols-2 gap-4">
                <div><label className="text-xs text-gray-500 mb-1 block">Cluster Name</label>
                  <input value={newCluster.name} onChange={e => setNewCluster(c => ({ ...c, name: e.target.value }))} className="input w-full text-sm" placeholder="my-cluster" /></div>
                <div><label className="text-xs text-gray-500 mb-1 block">Runtime</label>
                  <select value={newCluster.runtime} onChange={e => setNewCluster(c => ({ ...c, runtime: e.target.value }))} className="select w-full text-sm">
                    {runtimes.map(r => <option key={r} value={r}>{r}</option>)}
                  </select></div>
                <div><label className="text-xs text-gray-500 mb-1 block">Node Type</label>
                  <select value={newCluster.node_type} onChange={e => setNewCluster(c => ({ ...c, node_type: e.target.value }))} className="select w-full text-sm">
                    {nodeTypes.map(n => <option key={n.id} value={n.id}>{n.id} ({n.vcpus} vCPU, {n.ram_gb}GB RAM) — {n.dbu_hour} DBU/hr</option>)}
                  </select></div>
                <div className="flex gap-4">
                  <div><label className="text-xs text-gray-500 mb-1 block">Min Workers</label>
                    <input type="number" value={newCluster.min_workers} onChange={e => setNewCluster(c => ({ ...c, min_workers: +e.target.value }))} className="input w-24 text-sm" /></div>
                  <div><label className="text-xs text-gray-500 mb-1 block">Max Workers</label>
                    <input type="number" value={newCluster.max_workers} onChange={e => setNewCluster(c => ({ ...c, max_workers: +e.target.value }))} className="input w-24 text-sm" /></div>
                </div>
              </div>
              <div className="flex items-center gap-3">
                <label className="flex items-center gap-2 text-xs text-gray-400 cursor-pointer">
                  <input type="checkbox" checked={newCluster.autoscaling} onChange={e => setNewCluster(c => ({ ...c, autoscaling: e.target.checked }))} className="rounded" />
                  Enable Autoscaling
                </label>
                <div className="ml-auto flex gap-2">
                  <button onClick={() => setShowCreateCluster(false)} className="btn-secondary text-sm px-3 py-1.5">Cancel</button>
                  <button onClick={createCluster} className="btn-primary text-sm px-3 py-1.5">Create</button>
                </div>
              </div>
            </div>
          )}

          <div className="space-y-3">
            {clusters.map(c => (
              <div key={c.id} className="card p-5">
                <div className="flex items-start justify-between mb-3">
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="font-medium text-gray-200">{c.name}</span>
                      <StatusBadge state={c.state} />
                    </div>
                    <div className="text-xs text-gray-500 mt-0.5">{c.id} · {c.runtime}</div>
                  </div>
                  <div className="flex items-center gap-3">
                    <div className="text-right">
                      <div className="text-xs text-gray-500">DBU/hr</div>
                      <div className="text-sm font-mono text-orange-400">{c.state === 'RUNNING' ? dbuCost(c) : '—'}</div>
                    </div>
                    <button onClick={() => toggleCluster(c.id)}
                      className={clsx('flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg transition-colors',
                        c.state === 'RUNNING' ? 'bg-red-900/20 hover:bg-red-900/30 text-red-400 border border-red-800/30' : 'bg-green-900/20 hover:bg-green-900/30 text-green-400 border border-green-800/30')}>
                      {c.state === 'RUNNING' ? <><Square size={12} /> Terminate</> : <><Play size={12} /> Start</>}
                    </button>
                  </div>
                </div>
                <div className="grid grid-cols-4 gap-3 mb-3 text-xs">
                  <div><span className="text-gray-600">Node Type</span><div className="text-gray-300 font-mono">{c.node_type}</div></div>
                  <div><span className="text-gray-600">Workers</span><div className="text-gray-300">{c.current_workers} / {c.max_workers}</div></div>
                  <div><span className="text-gray-600">Autoscaling</span><div className={c.autoscaling ? 'text-green-400' : 'text-gray-500'}>{c.autoscaling ? 'Enabled' : 'Disabled'}</div></div>
                  <div><span className="text-gray-600">Created</span><div className="text-gray-300">{c.created_at.substring(0, 10)}</div></div>
                </div>
                {/* Events */}
                <div className="space-y-1 border-t border-gray-800 pt-3">
                  {c.events.slice(-3).map((ev: any, i: number) => (
                    <div key={i} className="flex items-center gap-2 text-[11px]">
                      <span className="text-gray-600 font-mono">{ev.time}</span>
                      <span className={clsx('px-1.5 py-0.5 rounded text-[9px] font-semibold', {
                        'bg-green-900/30 text-green-400': ev.type === 'STARTED' || ev.type === 'STARTING',
                        'bg-red-900/30 text-red-400': ev.type === 'TERMINATED',
                        'bg-blue-900/30 text-blue-400': ev.type === 'DRIVER',
                        'bg-yellow-900/30 text-yellow-400': ev.type === 'AUTOSCALE',
                        'bg-gray-800 text-gray-400': !['STARTED', 'STARTING', 'TERMINATED', 'DRIVER', 'AUTOSCALE'].includes(ev.type),
                      })}>{ev.type}</span>
                      <span className="text-gray-500">{ev.message}</span>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {sub === 'unity-catalog' && catalog && (
        <div className="grid grid-cols-12 gap-4">
          <div className="col-span-4 card p-4">
            <div className="text-sm font-semibold text-gray-200 mb-3 flex items-center gap-2">
              <Database size={14} className="text-blue-400" /> Unity Catalog
            </div>
            <div className="text-xs text-gray-500 mb-3">Three-level namespace: Catalog → Schema → Table</div>
            <div className="space-y-1">
              {catalog.catalogs.map((cat: any) => (
                <div key={cat.name}>
                  <button
                    onClick={() => setExpandedCatalog(e => ({ ...e, [cat.name]: !e[cat.name] }))}
                    className="flex items-center gap-1.5 w-full text-left py-1 text-sm text-blue-300 hover:text-blue-200">
                    {expandedCatalog[cat.name] ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                    <Database size={12} /> {cat.name}
                  </button>
                  {expandedCatalog[cat.name] && cat.schemas.map((schema: any) => (
                    <div key={schema.name} className="ml-4">
                      <button
                        onClick={() => setExpandedCatalog(e => ({ ...e, [`${cat.name}.${schema.name}`]: !e[`${cat.name}.${schema.name}`] }))}
                        className="flex items-center gap-1.5 w-full text-left py-1 text-xs text-purple-300 hover:text-purple-200">
                        {expandedCatalog[`${cat.name}.${schema.name}`] ? <ChevronDown size={10} /> : <ChevronRight size={10} />}
                        <Box size={10} /> {schema.name}
                      </button>
                      {expandedCatalog[`${cat.name}.${schema.name}`] && schema.tables.map((table: any) => (
                        <button key={table.name}
                          onClick={() => setSelectedTable(`${cat.name}.${schema.name}.${table.name}`)}
                          className={clsx('flex items-center gap-1.5 w-full text-left py-1 ml-4 text-xs transition-colors', selectedTable === `${cat.name}.${schema.name}.${table.name}` ? 'text-orange-300' : 'text-gray-400 hover:text-gray-200')}>
                          <Boxes size={10} /> {table.name}
                        </button>
                      ))}
                    </div>
                  ))}
                </div>
              ))}
            </div>
          </div>
          <div className="col-span-8 space-y-4">
            <div className="card p-5">
              <div className="text-sm font-semibold text-gray-200 mb-4">What is Unity Catalog?</div>
              <p className="text-sm text-gray-400 leading-relaxed mb-3">Unity Catalog is Databricks' unified governance solution — a <strong className="text-white">single metastore</strong> that governs data across all your workspaces, clouds, and data assets.</p>
              <div className="grid grid-cols-3 gap-3">
                {[
                  { icon: Shield, label: 'Column-Level Security', desc: 'Mask PII columns per role without duplicating tables' },
                  { icon: Eye, label: 'Data Lineage', desc: 'Automatically tracks column-level lineage across notebooks and jobs' },
                  { icon: Network, label: 'Cross-Workspace', desc: 'One catalog visible across all Databricks workspaces in your account' },
                ].map(({ icon: Icon, label, desc }) => (
                  <div key={label} className="p-3 bg-gray-800/50 rounded-lg">
                    <Icon size={14} className="text-blue-400 mb-2" />
                    <div className="text-xs font-semibold text-gray-300 mb-1">{label}</div>
                    <div className="text-[11px] text-gray-500">{desc}</div>
                  </div>
                ))}
              </div>
            </div>
            {/* Table listing */}
            {catalog.catalogs.flatMap((cat: any) => cat.schemas.flatMap((s: any) => s.tables.map((t: any) => ({
              ...t, catalog: cat.name, schema: s.name
            })))).slice(0, 4).map((t: any) => (
              <div key={t.name} className="card p-4">
                <div className="flex items-center justify-between mb-2">
                  <span className="font-mono text-sm text-gray-200">{t.catalog}.{t.schema}.{t.name}</span>
                  <span className="text-xs px-2 py-0.5 bg-blue-900/30 text-blue-400 rounded border border-blue-800/30">{t.format}</span>
                </div>
                <div className="flex gap-4 text-xs text-gray-500 mb-2">
                  <span>{t.rows.toLocaleString()} rows</span>
                  <span>{t.size_mb}MB</span>
                  <span>Owner: {t.owner}</span>
                  {t.pii && <span className="text-red-400 flex items-center gap-1"><AlertTriangle size={10} /> Contains PII</span>}
                </div>
                <div className="flex gap-1 flex-wrap">
                  {t.tags.map((tag: string) => (
                    <span key={tag} className="text-[10px] px-1.5 py-0.5 bg-gray-800 border border-gray-700 rounded text-gray-400">{tag}</span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ─── SECTION C: Delta Lake ────────────────────────────────

function SectionDelta() {
  const [sub, setSub] = useState('operations')
  const [history, setHistory] = useState<any>(null)
  const [loading, setLoading] = useState(false)
  const [lastOp, setLastOp] = useState<any>(null)
  const [timeTravelVersion, setTimeTravelVersion] = useState(0)
  const [snapshot, setSnapshot] = useState<any>(null)
  const [comparison, setComparison] = useState<any>(null)

  useEffect(() => {
    refreshHistory()
    api.get('/api/bigdata/delta/format-comparison').then(r => setComparison(r.data))
  }, [])

  const refreshHistory = async () => {
    const r = await api.get('/api/bigdata/delta/history')
    setHistory(r.data)
  }

  const executeOp = async (op: string) => {
    setLoading(true)
    setLastOp(null)
    try {
      const r = await api.post('/api/bigdata/delta/execute', { operation: op })
      setLastOp(r.data)
      await refreshHistory()
    } finally {
      setLoading(false)
    }
  }

  const loadSnapshot = async (v: number) => {
    setTimeTravelVersion(v)
    const r = await api.get(`/api/bigdata/delta/snapshot/${v}`)
    setSnapshot(r.data)
  }

  const OPS = [
    { id: 'insert', label: 'INSERT', color: 'text-green-400 bg-green-900/20 border-green-800/30 hover:bg-green-900/30', desc: 'Add new row' },
    { id: 'update', label: 'UPDATE', color: 'text-blue-400 bg-blue-900/20 border-blue-800/30 hover:bg-blue-900/30', desc: 'Pending → Shipped' },
    { id: 'delete', label: 'DELETE', color: 'text-red-400 bg-red-900/20 border-red-800/30 hover:bg-red-900/30', desc: 'Remove low-value rows' },
    { id: 'merge', label: 'MERGE', color: 'text-purple-400 bg-purple-900/20 border-purple-800/30 hover:bg-purple-900/30', desc: 'UPSERT from source' },
    { id: 'optimize', label: 'OPTIMIZE', color: 'text-orange-400 bg-orange-900/20 border-orange-800/30 hover:bg-orange-900/30', desc: 'Compact small files + Z-ORDER' },
    { id: 'vacuum', label: 'VACUUM', color: 'text-yellow-400 bg-yellow-900/20 border-yellow-800/30 hover:bg-yellow-900/30', desc: 'Delete old file versions' },
  ]

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        {['operations', 'time-travel', 'delta-log', 'format-comparison'].map(s => (
          <SubTab key={s} label={s.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase())} active={sub === s} onClick={() => setSub(s)} />
        ))}
      </div>

      {sub === 'operations' && history && (
        <div className="grid grid-cols-12 gap-4">
          <div className="col-span-5 space-y-4">
            <div className="card p-4">
              <div className="text-sm font-semibold text-gray-200 mb-3">Delta Table Operations</div>
              <div className="grid grid-cols-2 gap-2">
                {OPS.map(op => (
                  <button key={op.id} onClick={() => executeOp(op.id)} disabled={loading}
                    className={clsx('flex flex-col p-3 rounded-lg border text-left transition-colors', op.color)}>
                    <span className="text-xs font-bold font-mono">{op.label}</span>
                    <span className="text-[11px] mt-0.5 opacity-70">{op.desc}</span>
                  </button>
                ))}
              </div>
              {loading && (
                <div className="mt-3 flex items-center gap-2 text-xs text-gray-500">
                  <RefreshCw size={12} className="animate-spin" /> Executing operation...
                </div>
              )}
              {lastOp && (
                <div className="mt-3 p-3 bg-green-900/10 border border-green-800/30 rounded-lg">
                  <div className="text-xs font-semibold text-green-400 mb-1">✓ Operation Complete</div>
                  <div className="text-xs text-gray-400">{lastOp.message}</div>
                  <div className="text-[10px] text-gray-600 mt-1">Version: {lastOp.version} · {lastOp.log_entry.timestamp?.substring(0, 19)}</div>
                </div>
              )}
            </div>

            <div className="card p-4">
              <div className="text-xs font-semibold text-gray-400 mb-2 flex items-center gap-2">
                <Layers size={12} className="text-blue-400" /> Table State
                <span className="ml-auto text-gray-600">v{history.current_version} · {history.row_count} rows</span>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead><tr className="border-b border-gray-800">
                    {['order_id', 'status', 'revenue', 'region'].map(h => (
                      <th key={h} className="text-left py-1 pr-3 text-gray-600 font-medium">{h}</th>
                    ))}
                  </tr></thead>
                  <tbody>
                    {history.current_data?.slice(0, 8).map((row: any) => (
                      <tr key={row.order_id} className="border-b border-gray-800/50 hover:bg-gray-800/30">
                        <td className="py-1 pr-3 text-gray-300 font-mono">{row.order_id}</td>
                        <td className="py-1 pr-3">
                          <span className={clsx('text-[10px] px-1.5 py-0.5 rounded', {
                            'text-green-400 bg-green-900/20': row.status === 'completed',
                            'text-blue-400 bg-blue-900/20': row.status === 'shipped',
                            'text-yellow-400 bg-yellow-900/20': row.status === 'pending',
                          })}>{row.status}</span>
                        </td>
                        <td className="py-1 pr-3 text-gray-400 font-mono">${row.revenue}</td>
                        <td className="py-1 pr-3 text-gray-400">{row.region}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div className="col-span-7 card p-4">
            <div className="text-sm font-semibold text-gray-200 mb-3">_delta_log/ Transaction Log</div>
            <div className="space-y-2 max-h-[500px] overflow-y-auto">
              {history.log.slice().reverse().map((entry: any) => (
                <div key={entry.version} className="border border-gray-700 rounded-lg overflow-hidden">
                  <div className="flex items-center gap-3 px-3 py-2 bg-gray-800/50">
                    <span className="text-[10px] font-mono text-gray-600">v{entry.version}</span>
                    <span className={clsx('text-[10px] font-bold px-1.5 py-0.5 rounded', {
                      'text-green-400 bg-green-900/30': ['WRITE', 'CREATE TABLE', 'INSERT'].includes(entry.operation),
                      'text-blue-400 bg-blue-900/30': entry.operation === 'UPDATE',
                      'text-red-400 bg-red-900/30': entry.operation === 'DELETE',
                      'text-purple-400 bg-purple-900/30': entry.operation === 'MERGE',
                      'text-orange-400 bg-orange-900/30': entry.operation === 'OPTIMIZE',
                      'text-yellow-400 bg-yellow-900/30': entry.operation.startsWith('VACUUM'),
                    })}>{entry.operation}</span>
                    <span className="text-[10px] text-gray-600 ml-auto">{entry.timestamp?.substring(0, 19)}</span>
                  </div>
                  <div className="px-3 py-2 font-mono text-[10px] text-gray-500 bg-gray-900/50 space-y-0.5">
                    <div>{JSON.stringify(entry.commit_info, null, 0)}</div>
                    {entry.add_files.length > 0 && <div className="text-green-600">add: [{entry.add_files.slice(0, 2).join(', ')}{entry.add_files.length > 2 ? `, +${entry.add_files.length - 2} more` : ''}]</div>}
                    {entry.remove_files.length > 0 && <div className="text-red-600">remove: [{entry.remove_files.slice(0, 2).join(', ')}{entry.remove_files.length > 2 ? `, +${entry.remove_files.length - 2} more` : ''}]</div>}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {sub === 'time-travel' && history && (
        <div className="space-y-4">
          <div className="card p-5">
            <div className="text-sm font-semibold text-gray-200 mb-2">Time Travel — Query Any Historical Version</div>
            <p className="text-xs text-gray-500 mb-4">Delta keeps all previous file versions. You can query any past state without duplicating data — Delta reads the correct Parquet files for that version.</p>
            <div className="flex items-center gap-4 mb-4">
              <span className="text-xs text-gray-500">Version:</span>
              <input type="range" min="0" max={Math.max(0, history.current_version)}
                value={timeTravelVersion} onChange={e => { setTimeTravelVersion(+e.target.value); loadSnapshot(+e.target.value) }}
                className="flex-1 accent-orange-500" />
              <span className="text-sm font-mono text-orange-400 w-16 text-right">v{timeTravelVersion}</span>
              <button onClick={() => loadSnapshot(timeTravelVersion)} className="btn-secondary text-xs px-3 py-1.5">Load</button>
            </div>
            {/* PySpark code preview */}
            <pre className="code-block text-xs mb-4">{`# PySpark time travel
df = spark.read.format("delta") \\
    .option("versionAsOf", ${timeTravelVersion}) \\
    .load("s3://datalake/silver/orders/")`}</pre>
            {snapshot ? (
              <div>
                <div className="flex items-center gap-3 mb-3">
                  <span className="text-xs text-gray-500">Operation at v{snapshot.version}:</span>
                  <span className="text-xs font-semibold text-orange-400">{snapshot.operation}</span>
                  <span className="text-xs text-gray-600">{snapshot.timestamp?.substring(0, 19)}</span>
                  <span className="ml-auto text-xs text-gray-500">{snapshot.row_count} rows</span>
                </div>
                <table className="w-full text-xs">
                  <thead><tr className="border-b border-gray-800">
                    {['order_id', 'status', 'revenue', 'region'].map(h => (
                      <th key={h} className="text-left py-1.5 pr-4 text-gray-600 font-medium">{h}</th>
                    ))}
                  </tr></thead>
                  <tbody>
                    {snapshot.data.map((row: any) => (
                      <tr key={row.order_id} className="border-b border-gray-800/50">
                        <td className="py-1.5 pr-4 text-gray-300 font-mono">{row.order_id}</td>
                        <td className="py-1.5 pr-4 text-gray-400">{row.status}</td>
                        <td className="py-1.5 pr-4 text-gray-400 font-mono">${row.revenue}</td>
                        <td className="py-1.5 pr-4 text-gray-400">{row.region}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-center text-gray-600 text-xs py-4">Adjust the slider and click Load to see a historical snapshot</div>
            )}
          </div>

          {/* History table */}
          <div className="card p-4">
            <div className="text-sm font-semibold text-gray-200 mb-3">Version History</div>
            <table className="w-full text-xs">
              <thead><tr className="border-b border-gray-800">
                {['Version', 'Timestamp', 'Operation', 'Rows Added', 'Rows Removed', 'Files Added', 'Files Removed'].map(h => (
                  <th key={h} className="text-left py-1.5 pr-4 text-gray-600 font-medium">{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {history.log.slice().reverse().map((entry: any) => (
                  <tr key={entry.version} className="border-b border-gray-800/50 hover:bg-gray-800/20">
                    <td className="py-1.5 pr-4 text-orange-400 font-mono">v{entry.version}</td>
                    <td className="py-1.5 pr-4 text-gray-400">{entry.timestamp?.substring(0, 19)}</td>
                    <td className="py-1.5 pr-4 text-gray-300 font-semibold">{entry.operation}</td>
                    <td className="py-1.5 pr-4 text-green-400">{entry.rows_added > 0 ? `+${entry.rows_added}` : '—'}</td>
                    <td className="py-1.5 pr-4 text-red-400">{entry.rows_removed > 0 ? `-${entry.rows_removed}` : '—'}</td>
                    <td className="py-1.5 pr-4 text-gray-400">{entry.add_files.length}</td>
                    <td className="py-1.5 pr-4 text-gray-400">{entry.remove_files.length}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {sub === 'delta-log' && (
        <div className="space-y-4">
          <div className="card p-5">
            <div className="text-sm font-semibold text-gray-200 mb-3">How Delta Achieves ACID on Object Storage</div>
            <div className="grid grid-cols-3 gap-4 mb-4">
              {[
                { label: 'Atomicity', desc: 'Each commit writes a new JSON log file atomically. Either the JSON appears (commit succeeds) or it doesn\'t (rollback).', color: 'blue' },
                { label: 'Consistency', desc: 'Schema enforcement prevents writing incompatible data. Schema evolution is explicit and logged.', color: 'green' },
                { label: 'Isolation', desc: 'Optimistic concurrency: multiple writers use conflict detection on the log to prevent lost updates.', color: 'purple' },
              ].map(({ label, desc, color }) => (
                <div key={label} className="p-3 bg-gray-800/50 rounded-lg">
                  <div className={clsx('text-xs font-bold mb-1', {
                    'text-blue-400': color === 'blue', 'text-green-400': color === 'green', 'text-purple-400': color === 'purple'
                  })}>{label}</div>
                  <p className="text-[11px] text-gray-500 leading-relaxed">{desc}</p>
                </div>
              ))}
            </div>
            <div className="text-xs text-gray-500 mb-2 font-semibold">Sample _delta_log/00000000000000000001.json</div>
            <pre className="code-block text-xs overflow-x-auto">{`{
  "commitInfo": {
    "timestamp": 1706400000000,
    "operation": "WRITE",
    "operationParameters": {"mode": "Append", "partitionBy": "[]"},
    "readVersion": 0,
    "isolationLevel": "WriteSerializable",
    "numOutputRows": 9891,
    "numFiles": 48
  },
  "add": {
    "path": "part-00000-abc123.snappy.parquet",
    "size": 1342508,
    "modificationTime": 1706400001000,
    "dataChange": true,
    "stats": "{\"numRecords\": 206, \"minValues\": {\"revenue\": 10.5}, \"maxValues\": {\"revenue\": 499.99}}"
  }
}`}</pre>
          </div>
        </div>
      )}

      {sub === 'format-comparison' && comparison && (
        <div className="card overflow-hidden">
          <div className="p-4 border-b border-gray-800">
            <div className="text-sm font-semibold text-gray-200">Open Table Format Comparison: Delta Lake vs Iceberg vs Hudi</div>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead><tr className="border-b border-gray-800 bg-gray-800/30">
                <th className="text-left px-4 py-2.5 text-gray-500 font-medium w-40">Criterion</th>
                {comparison.formats.map((f: string) => (
                  <th key={f} className="text-left px-4 py-2.5 font-semibold" style={{ color: f === 'Delta Lake' ? '#fb923c' : f === 'Apache Iceberg' ? '#22d3ee' : '#4ade80' }}>{f}</th>
                ))}
              </tr></thead>
              <tbody>
                {comparison.criteria.map((row: any, i: number) => (
                  <tr key={row.name} className={clsx('border-b border-gray-800/50', i % 2 === 0 && 'bg-gray-800/10')}>
                    <td className="px-4 py-2.5 text-gray-400 font-medium">{row.name}</td>
                    {row.values.map((v: string, j: number) => (
                      <td key={j} className="px-4 py-2.5 text-gray-400">{v}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

// ─── SECTION D: Kafka ─────────────────────────────────────

function SectionKafka() {
  const [sub, setSub] = useState('architecture')
  const [topics, setTopics] = useState<any[]>([])
  const [selectedTopic, setSelectedTopic] = useState('orders')
  const [messages, setMessages] = useState<any[]>([])
  const [connectors, setConnectors] = useState<any[]>([])
  const [streamMetrics, setStreamMetrics] = useState<any>(null)
  const [liveMessages, setLiveMessages] = useState<any[]>([])
  const [streaming, setStreaming] = useState(false)
  const wsRef = useRef<WebSocket | null>(null)

  useEffect(() => {
    api.get('/api/bigdata/kafka/topics').then(r => setTopics(r.data))
    api.get('/api/bigdata/kafka/connectors').then(r => setConnectors(r.data))
    api.get('/api/bigdata/kafka/streaming-metrics').then(r => setStreamMetrics(r.data))
    loadMessages('orders')
    return () => wsRef.current?.close()
  }, [])

  const loadMessages = async (topic: string) => {
    setSelectedTopic(topic)
    const r = await api.get(`/api/bigdata/kafka/messages/${topic}`)
    setMessages(r.data.messages)
  }

  const toggleStream = useCallback(() => {
    if (streaming) {
      wsRef.current?.close()
      wsRef.current = null
      setStreaming(false)
    } else {
      const ws = new WebSocket(`${WS_BASE}/ws/kafka-stream`)
      ws.onmessage = (e) => {
        const msg = JSON.parse(e.data)
        setLiveMessages(prev => [msg, ...prev].slice(0, 20))
      }
      ws.onerror = () => setStreaming(false)
      wsRef.current = ws
      setStreaming(true)
    }
  }, [streaming])

  return (
    <div className="space-y-4">
      <div className="flex gap-2 flex-wrap">
        {['architecture', 'topics', 'live-stream', 'connectors', 'streaming-metrics'].map(s => (
          <SubTab key={s} label={s.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase())} active={sub === s} onClick={() => setSub(s)} />
        ))}
      </div>

      {sub === 'architecture' && (
        <div className="space-y-4">
          <div className="card p-6">
            <div className="text-sm font-semibold text-gray-200 mb-6">Kafka Architecture — Animated Data Flow</div>
            {/* Architecture SVG */}
            <div className="overflow-x-auto">
              <div className="flex items-stretch gap-4 min-w-[700px]">
                {/* Producers */}
                <div className="flex flex-col gap-2 w-36">
                  <div className="text-[10px] text-gray-600 uppercase tracking-wider text-center mb-1">Producers</div>
                  {['Order Service', 'Payment API', 'Sensor IoT'].map((p, i) => (
                    <div key={p} className="p-2.5 bg-blue-900/20 border border-blue-800/40 rounded-lg text-center">
                      <div className="text-xs text-blue-400 font-medium">{p}</div>
                    </div>
                  ))}
                </div>

                {/* Arrow */}
                <div className="flex flex-col justify-center items-center gap-1 w-8">
                  <div className="w-px h-full bg-blue-800/40 relative">
                    <div className="absolute top-1/4 left-1/2 -translate-x-1/2 w-2 h-2 border-t-2 border-r-2 border-blue-600 rotate-45 animate-bounce" />
                  </div>
                </div>

                {/* Broker / Topics */}
                <div className="flex-1">
                  <div className="text-[10px] text-gray-600 uppercase tracking-wider text-center mb-1">Kafka Broker (Topics + Partitions)</div>
                  <div className="border border-gray-700 rounded-lg p-3 bg-gray-800/20 space-y-2">
                    {topics.slice(0, 3).map(topic => (
                      <div key={topic.name} className="bg-gray-900 border border-gray-700 rounded p-2">
                        <div className="flex items-center justify-between mb-2">
                          <span className="text-xs font-mono text-gray-300">{topic.name}</span>
                          <span className="text-[10px] text-gray-600">{topic.partitions}p × {topic.replication_factor}r</span>
                        </div>
                        <div className="flex gap-1">
                          {Array.from({ length: Math.min(topic.partitions, 6) }).map((_, p) => (
                            <div key={p} className="flex-1 h-5 rounded bg-gray-800 border border-gray-700 flex items-center justify-center text-[8px] text-gray-500">P{p}</div>
                          ))}
                          {topic.partitions > 6 && <div className="text-[9px] text-gray-600 self-center">+{topic.partitions - 6}</div>}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Arrow */}
                <div className="flex flex-col justify-center w-8">
                  <div className="w-px h-full bg-green-800/40 relative mx-auto">
                    <div className="absolute top-3/4 left-1/2 -translate-x-1/2 w-2 h-2 border-t-2 border-r-2 border-green-600 rotate-45 animate-bounce" />
                  </div>
                </div>

                {/* Consumer Groups */}
                <div className="w-44">
                  <div className="text-[10px] text-gray-600 uppercase tracking-wider text-center mb-1">Consumer Groups</div>
                  <div className="space-y-2">
                    {['spark-streaming (lag: 0)', 'analytics-job (lag: 120)', 'delta-sink (lag: 450)'].map((c, i) => (
                      <div key={c} className="p-2.5 bg-green-900/20 border border-green-800/40 rounded-lg">
                        <div className="text-[11px] text-green-400">{c.split(' (')[0]}</div>
                        <div className={clsx('text-[10px]', c.includes('lag: 0') ? 'text-green-600' : c.includes('120') ? 'text-yellow-500' : 'text-red-400')}>
                          {c.split(' (')[1].replace(')', '')}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </div>

            {/* Replication diagram */}
            <div className="mt-6 border-t border-gray-800 pt-4">
              <div className="text-xs font-semibold text-gray-400 mb-3">Replication Factor = 3 (Partition P0)</div>
              <div className="flex gap-2">
                {['broker-1 (Leader)', 'broker-2 (Follower)', 'broker-3 (Follower)'].map((b, i) => (
                  <div key={b} className={clsx('flex-1 p-3 rounded-lg border text-center', i === 0 ? 'bg-blue-900/20 border-blue-800/40' : 'bg-gray-800/30 border-gray-700')}>
                    <div className={clsx('text-xs font-semibold', i === 0 ? 'text-blue-400' : 'text-gray-500')}>{b}</div>
                    <div className="text-[10px] text-gray-600 mt-1">{i === 0 ? 'Reads + Writes' : 'Replicates from leader'}</div>
                    <div className="w-full h-3 bg-gray-900 rounded mt-2 flex gap-px overflow-hidden">
                      {Array.from({ length: 8 }).map((_, j) => (
                        <div key={j} className={clsx('flex-1 h-full', i === 0 ? 'bg-blue-600/60' : 'bg-gray-600/40')} />
                      ))}
                    </div>
                    <div className="text-[9px] text-gray-600 mt-1">offset: 487,231</div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {sub === 'topics' && (
        <div className="space-y-3">
          {topics.map(topic => (
            <div key={topic.name} className="card p-5">
              <div className="flex items-center justify-between mb-3">
                <div>
                  <span className="font-mono text-sm font-semibold text-gray-200">{topic.name}</span>
                  <span className="ml-2 text-xs text-gray-600">{topic.partitions} partitions · {topic.replication_factor}× replicated · {topic.retention_hours}h retention</span>
                </div>
                <div className="text-right">
                  <div className="text-xs text-gray-500">Throughput</div>
                  <div className="text-sm font-mono text-orange-400">{topic.msgs_per_sec.toLocaleString()} msg/s</div>
                </div>
              </div>
              <div className="flex items-center gap-2 mb-2">
                <span className={clsx('text-xs', topic.total_lag > 1000 ? 'text-red-400' : topic.total_lag > 100 ? 'text-yellow-400' : 'text-green-400')}>
                  Total lag: {topic.total_lag.toLocaleString()}
                </span>
                <button onClick={() => { setSub('topics'); loadMessages(topic.name) }}
                  className="ml-auto text-xs text-blue-400 hover:text-blue-300">View messages →</button>
              </div>
              <div className="flex gap-1 overflow-x-auto pb-1">
                {topic.partitions_detail?.slice(0, 8).map((p: any) => (
                  <div key={p.partition} className="flex-shrink-0 w-20 p-2 bg-gray-800 rounded text-[10px]">
                    <div className="text-gray-400 font-semibold">P{p.partition}</div>
                    <div className="text-gray-600">@{p.leader}</div>
                    <div className={clsx('mt-1', p.consumer_lag > 1000 ? 'text-red-400' : p.consumer_lag > 100 ? 'text-yellow-400' : 'text-green-400')}>
                      lag: {p.consumer_lag}
                    </div>
                  </div>
                ))}
                {topic.partitions > 8 && <div className="text-[10px] text-gray-600 self-center px-2">+{topic.partitions - 8} more</div>}
              </div>
            </div>
          ))}
          {/* Messages viewer */}
          <div className="card p-4">
            <div className="flex items-center gap-3 mb-3">
              <select value={selectedTopic} onChange={e => loadMessages(e.target.value)} className="select text-xs">
                {topics.map(t => <option key={t.name} value={t.name}>{t.name}</option>)}
              </select>
              <button onClick={() => loadMessages(selectedTopic)} className="btn-secondary text-xs px-2 py-1 flex items-center gap-1"><RefreshCw size={10} /> Refresh</button>
              <span className="text-xs text-gray-600">{messages.length} messages</span>
            </div>
            <div className="space-y-1 max-h-72 overflow-y-auto">
              {messages.map(m => (
                <div key={m.offset} className="flex items-start gap-3 p-2 bg-gray-800/30 rounded hover:bg-gray-800/50 text-[11px]">
                  <span className="text-gray-600 font-mono flex-shrink-0 w-20">off:{m.offset}</span>
                  <span className="text-gray-600 flex-shrink-0 w-6">P{m.partition}</span>
                  <span className="text-gray-400 flex-shrink-0 w-24">{m.timestamp?.substring(11, 19)}</span>
                  <span className="text-green-400 font-mono truncate">{m.value}</span>
                  <span className="text-gray-700 flex-shrink-0">{m.size_bytes}B</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {sub === 'live-stream' && (
        <div className="space-y-4">
          <div className="card p-5">
            <div className="flex items-center justify-between mb-4">
              <div>
                <div className="text-sm font-semibold text-gray-200">Live Kafka Message Stream</div>
                <div className="text-xs text-gray-500 mt-0.5">Real-time messages from all topics via WebSocket</div>
              </div>
              <button onClick={toggleStream} className={clsx('flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors', streaming ? 'bg-red-900/20 border border-red-800/30 text-red-400 hover:bg-red-900/30' : 'btn-primary')}>
                {streaming ? <><Square size={14} /> Stop Stream</> : <><Play size={14} /> Start Stream</>}
              </button>
            </div>
            {streaming && (
              <div className="flex items-center gap-2 mb-3">
                <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
                <span className="text-xs text-green-400">Streaming live...</span>
              </div>
            )}
            <div className="bg-gray-950 rounded-lg border border-gray-800 h-80 overflow-y-auto p-3 font-mono text-xs space-y-1">
              {liveMessages.length === 0 && (
                <div className="text-gray-700 text-center py-10">Press "Start Stream" to see live Kafka messages</div>
              )}
              {liveMessages.map((m, i) => (
                <div key={i} className={clsx('flex gap-3 items-start', i === 0 && 'animate-slide-in')}>
                  <span className="text-gray-700 flex-shrink-0 w-8">{i + 1}</span>
                  <span className={clsx('flex-shrink-0 w-28 px-1.5 rounded text-[10px]', {
                    'text-blue-400 bg-blue-900/20': m.topic === 'orders',
                    'text-purple-400 bg-purple-900/20': m.topic === 'user-events',
                    'text-orange-400 bg-orange-900/20': m.topic === 'sensor-readings',
                    'text-green-400 bg-green-900/20': m.topic === 'payment-events',
                  })}>{m.topic}</span>
                  <span className="text-gray-700 flex-shrink-0">P{m.partition}@{m.offset}</span>
                  <span className="text-green-400 truncate">{m.value}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {sub === 'connectors' && (
        <div className="space-y-3">
          <div className="text-sm text-gray-500 mb-2">Kafka Connect — pre-built connectors for moving data in/out of Kafka without code</div>
          {connectors.map(c => (
            <div key={c.id} className="card p-5">
              <div className="flex items-start justify-between mb-3">
                <div>
                  <div className="flex items-center gap-2">
                    <span className="font-semibold text-gray-200">{c.name}</span>
                    <span className={clsx('text-[10px] px-1.5 py-0.5 rounded font-bold', c.type === 'SOURCE' ? 'bg-blue-900/30 text-blue-400' : 'bg-green-900/30 text-green-400')}>{c.type}</span>
                    <StatusBadge state={c.status} />
                  </div>
                  <div className="text-xs text-gray-600 mt-0.5 font-mono">{c.class}</div>
                </div>
                <div className="text-right text-xs">
                  <div className="text-gray-500">Tasks: {c.tasks}</div>
                  {c.errors_last_hour > 0 && <div className="text-red-400">{c.errors_last_hour} errors/hr</div>}
                </div>
              </div>
              <div className="flex items-center gap-2 text-xs text-gray-500">
                <span>Topics:</span>
                {c.topics.map((t: string) => <span key={t} className="px-1.5 py-0.5 bg-gray-800 rounded font-mono text-gray-400">{t}</span>)}
              </div>
            </div>
          ))}
        </div>
      )}

      {sub === 'streaming-metrics' && streamMetrics && (
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            {streamMetrics.queries.map((q: any) => (
              <div key={q.name} className="card p-4">
                <div className="flex items-center justify-between mb-3">
                  <span className="text-xs font-mono text-gray-300">{q.name}</span>
                  <StatusBadge state={q.status} />
                </div>
                <div className="grid grid-cols-3 gap-2">
                  <MetricCard label="Records/sec" value={q.records_per_sec.toLocaleString()} color="green" />
                  <MetricCard label="Batches" value={q.batches.toLocaleString()} color="blue" />
                  <MetricCard label="Avg Batch" value={`${q.avg_batch_ms}ms`} color="orange" />
                </div>
                <div className="text-[10px] text-gray-600 mt-2">Watermark: {q.watermark}</div>
              </div>
            ))}
          </div>
          <div className="card p-4">
            <div className="text-sm font-semibold text-gray-200 mb-4">Message Throughput (last 30 min)</div>
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={streamMetrics.throughput_series}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="minute" tick={{ fill: '#6b7280', fontSize: 11 }} />
                <YAxis tick={{ fill: '#6b7280', fontSize: 11 }} />
                <Tooltip contentStyle={{ background: '#1f2937', border: '1px solid #374151', fontSize: 11 }} />
                <Legend />
                <Line type="monotone" dataKey="orders" stroke="#fb923c" dot={false} strokeWidth={2} />
                <Line type="monotone" dataKey="user_events" stroke="#60a5fa" dot={false} strokeWidth={2} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}
    </div>
  )
}

// ─── SECTION E: Airflow + Spark ──────────────────────────

function SectionAirflow() {
  const [templates, setTemplates] = useState<any[]>([])
  const [selectedTemplate, setSelectedTemplate] = useState<any>(null)
  const [costData, setCostData] = useState<any>(null)

  useEffect(() => {
    api.get('/api/bigdata/airflow/dag-templates').then(r => { setTemplates(r.data); setSelectedTemplate(r.data[0]) })
    api.get('/api/bigdata/airflow/cost-calculator').then(r => setCostData(r.data))
  }, [])

  const taskTypeColor: Record<string, string> = {
    S3KeySensor: 'border-yellow-700 bg-yellow-900/20 text-yellow-400',
    ExternalTaskSensor: 'border-yellow-700 bg-yellow-900/20 text-yellow-400',
    SparkSubmitOperator: 'border-orange-700 bg-orange-900/20 text-orange-400',
    DatabricksRunNowOperator: 'border-orange-700 bg-orange-900/20 text-orange-400',
    DatabricksSubmitRunOperator: 'border-orange-700 bg-orange-900/20 text-orange-400',
    PythonOperator: 'border-blue-700 bg-blue-900/20 text-blue-400',
    BranchPythonOperator: 'border-purple-700 bg-purple-900/20 text-purple-400',
    TriggerDagRunOperator: 'border-green-700 bg-green-900/20 text-green-400',
    SlackWebhookOperator: 'border-green-700 bg-green-900/20 text-green-400',
    PagerDutyEventsOperator: 'border-red-700 bg-red-900/20 text-red-400',
  }

  return (
    <div className="space-y-4">
      {/* DAG templates selector */}
      <div className="flex gap-2">
        {templates.map(t => (
          <button key={t.id} onClick={() => setSelectedTemplate(t)}
            className={clsx('flex-1 text-left p-3 rounded-lg border text-xs transition-colors', selectedTemplate?.id === t.id ? 'border-orange-700 bg-orange-900/10 text-orange-300' : 'border-gray-700 bg-gray-800/30 text-gray-400 hover:border-gray-600')}>
            <div className="font-semibold mb-0.5">{t.name}</div>
            <div className="text-gray-600">{t.schedule_human}</div>
          </button>
        ))}
      </div>

      {selectedTemplate && (
        <div className="card p-5">
          <div className="flex items-center justify-between mb-4">
            <div>
              <div className="text-sm font-semibold text-gray-200">{selectedTemplate.name}</div>
              <div className="text-xs text-gray-500 mt-0.5">{selectedTemplate.description}</div>
            </div>
            <div className="text-xs text-gray-600 font-mono bg-gray-800 px-2 py-1 rounded">{selectedTemplate.schedule}</div>
          </div>

          {/* DAG visualization */}
          <div className="overflow-x-auto pb-2">
            <div className="flex items-start gap-3 min-w-max">
              {selectedTemplate.tasks.map((task: any, i: number) => (
                <div key={task.id} className="flex items-center gap-3">
                  {i > 0 && task.upstream.length > 0 && <ArrowRight size={14} className="text-gray-600 flex-shrink-0" />}
                  <div className={clsx('p-3 rounded-lg border min-w-[140px] max-w-[180px]', taskTypeColor[task.type] ?? 'border-gray-700 bg-gray-800/30 text-gray-400')}>
                    <div className="text-[10px] font-semibold opacity-70 mb-1">{task.type}</div>
                    <div className="text-xs font-mono font-bold">{task.id}</div>
                    <div className="text-[10px] mt-1 opacity-70 leading-tight">{task.description}</div>
                    {task.upstream.length > 1 && (
                      <div className="text-[9px] mt-1 opacity-50">after: {task.upstream.join(', ')}</div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Cost calculator */}
      {costData && (
        <div className="grid grid-cols-2 gap-4">
          {Object.entries(costData).map(([key, c]: [string, any]) => (
            <div key={key} className={clsx('card p-5 border-t-2', key === 'job_cluster' ? 'border-green-500' : 'border-orange-500')}>
              <div className={clsx('text-sm font-bold mb-1', key === 'job_cluster' ? 'text-green-400' : 'text-orange-400')}>{c.label}</div>
              <p className="text-xs text-gray-500 mb-4">{c.description}</p>
              <div className="grid grid-cols-2 gap-2 mb-3">
                <MetricCard label="Daily Cost" value={`$${c.daily_cost_usd}`} color={key === 'job_cluster' ? 'green' : 'orange'} />
                <MetricCard label="Monthly Cost" value={`$${c.monthly_cost_usd}`} color={key === 'job_cluster' ? 'green' : 'orange'} />
              </div>
              <div className="text-xs">
                <span className="text-gray-600">Idle time: </span>
                <span className={clsx(c.idle_pct > 0 ? 'text-red-400' : 'text-green-400')}>{c.idle_pct}%</span>
              </div>
              <div className="text-xs mt-1">
                <span className="text-gray-600">Best for: </span>
                <span className="text-gray-400">{c.recommended_for}</span>
              </div>
            </div>
          ))}
          <div className="col-span-2 p-4 bg-green-900/10 border border-green-800/30 rounded-lg text-xs text-gray-400 leading-relaxed">
            <span className="text-green-400 font-semibold">Senior Tip: </span>
            Always use <strong className="text-white">job clusters</strong> for production ETL. All-purpose clusters are for interactive development only. A 16-node all-purpose cluster left running overnight can cost $1,200+ vs $80 for the equivalent job cluster running 3 hours.
          </div>
        </div>
      )}
    </div>
  )
}

// ─── SECTION F: Performance Tuning ───────────────────────

function SectionPerformance() {
  const [scenarios, setScenarios] = useState<any[]>([])
  const [catalyst, setCatalyst] = useState<any>(null)
  const [selectedScenario, setSelectedScenario] = useState<any>(null)
  const [sub, setSub] = useState('scenarios')

  useEffect(() => {
    api.get('/api/bigdata/performance/scenarios').then(r => { setScenarios(r.data); setSelectedScenario(r.data[0]) })
    api.get('/api/bigdata/performance/catalyst').then(r => setCatalyst(r.data))
  }, [])

  const iconMap: Record<string, React.ElementType> = {
    skew: AlertTriangle, files: Package, join: Network, partition: Cpu,
  }

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        <SubTab label="Performance Scenarios" active={sub === 'scenarios'} onClick={() => setSub('scenarios')} />
        <SubTab label="Catalyst Query Optimizer" active={sub === 'catalyst'} onClick={() => setSub('catalyst')} />
      </div>

      {sub === 'scenarios' && (
        <div className="grid grid-cols-12 gap-4">
          <div className="col-span-4 space-y-2">
            {scenarios.map(s => {
              const Icon = iconMap[s.icon] ?? Cpu
              return (
                <button key={s.id} onClick={() => setSelectedScenario(s)}
                  className={clsx('w-full text-left p-3 rounded-lg border transition-colors', selectedScenario?.id === s.id ? 'border-orange-700 bg-orange-900/10' : 'border-gray-700 bg-gray-800/20 hover:border-gray-600')}>
                  <div className="flex items-center gap-2 mb-1">
                    <Icon size={14} className={selectedScenario?.id === s.id ? 'text-orange-400' : 'text-gray-500'} />
                    <span className={clsx('text-xs font-semibold', selectedScenario?.id === s.id ? 'text-orange-300' : 'text-gray-300')}>{s.title}</span>
                  </div>
                  <div className="text-[11px] text-gray-600 leading-tight">{s.symptom.substring(0, 60)}...</div>
                </button>
              )
            })}
          </div>

          {selectedScenario && (
            <div className="col-span-8 space-y-4">
              <div className="card p-5">
                <div className="text-sm font-bold text-orange-400 mb-4">{selectedScenario.title}</div>

                <div className="space-y-3 mb-4">
                  <div>
                    <span className="text-[10px] text-red-400 font-semibold uppercase tracking-wider">Symptom</span>
                    <p className="text-xs text-gray-400 mt-1">{selectedScenario.symptom}</p>
                  </div>
                  <div>
                    <span className="text-[10px] text-yellow-400 font-semibold uppercase tracking-wider">Root Cause</span>
                    <p className="text-xs text-gray-400 mt-1">{selectedScenario.cause}</p>
                  </div>
                </div>

                {/* Before/After metrics */}
                <div className="grid grid-cols-2 gap-4 mb-4">
                  <div>
                    <div className="text-[10px] font-semibold text-red-400 mb-2 uppercase tracking-wider">BEFORE (broken)</div>
                    <div className="space-y-2">
                      {Object.entries(selectedScenario.before).map(([k, v]) => (
                        <div key={k} className="flex justify-between text-xs">
                          <span className="text-gray-600">{k.replace(/_/g, ' ')}</span>
                          <span className="text-red-400 font-mono">{String(v)}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div>
                    <div className="text-[10px] font-semibold text-green-400 mb-2 uppercase tracking-wider">AFTER (fixed)</div>
                    <div className="space-y-2">
                      {Object.entries(selectedScenario.after).map(([k, v]) => (
                        <div key={k} className="flex justify-between text-xs">
                          <span className="text-gray-600">{k.replace(/_/g, ' ')}</span>
                          <span className="text-green-400 font-mono">{String(v)}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>

                <div>
                  <span className="text-[10px] text-green-400 font-semibold uppercase tracking-wider">Fix</span>
                  <p className="text-xs text-gray-400 mt-1 mb-2">{selectedScenario.fix}</p>
                  <pre className="code-block text-xs overflow-x-auto">{selectedScenario.fix_code}</pre>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {sub === 'catalyst' && catalyst && (
        <div className="space-y-4">
          <div className="card p-5">
            <div className="text-sm font-semibold text-gray-200 mb-2">Catalyst Query Optimizer</div>
            <p className="text-xs text-gray-500 mb-4">Every Spark SQL query goes through 4 phases. Catalyst applies rule-based and cost-based optimizations automatically.</p>
            <pre className="code-block text-xs mb-6">{catalyst.query}</pre>
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
              {catalyst.phases.map((phase: any) => (
                <div key={phase.name} className={clsx('rounded-lg border p-4', {
                  'border-gray-700 bg-gray-800/20': phase.color === 'gray',
                  'border-blue-700 bg-blue-900/10': phase.color === 'blue',
                  'border-orange-700 bg-orange-900/10': phase.color === 'orange',
                  'border-green-700 bg-green-900/10': phase.color === 'green',
                })}>
                  <div className={clsx('text-xs font-bold mb-2', {
                    'text-gray-400': phase.color === 'gray', 'text-blue-400': phase.color === 'blue',
                    'text-orange-400': phase.color === 'orange', 'text-green-400': phase.color === 'green',
                  })}>{phase.name}</div>
                  <p className="text-[10px] text-gray-500 mb-3 leading-relaxed">{phase.description}</p>
                  <div className="space-y-1">
                    {phase.nodes.map((node: string, i: number) => (
                      <div key={i} className="text-[10px] font-mono text-gray-400 truncate">{node}</div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
            <div className="mt-4 border-t border-gray-800 pt-4">
              <div className="text-[10px] text-gray-500 font-semibold uppercase tracking-wider mb-2">Optimizations Applied</div>
              <div className="flex flex-wrap gap-2">
                {catalyst.optimizations.map((opt: string) => (
                  <span key={opt} className="text-[10px] px-2 py-1 bg-green-900/20 border border-green-800/30 rounded text-green-400">{opt}</span>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

// ─── SECTION G: Ecosystem Map ─────────────────────────────

function SectionEcosystem() {
  const [tools, setTools] = useState<any[]>([])
  const [timeline, setTimeline] = useState<any[]>([])
  const [comparison, setComparison] = useState<any>(null)
  const [selectedTool, setSelectedTool] = useState<any>(null)
  const [sub, setSub] = useState('ecosystem')

  useEffect(() => {
    api.get('/api/bigdata/ecosystem/tools').then(r => setTools(r.data))
    api.get('/api/bigdata/ecosystem/timeline').then(r => setTimeline(r.data))
    api.get('/api/bigdata/ecosystem/comparison').then(r => setComparison(r.data))
  }, [])

  const categoryColor: Record<string, string> = {
    Processing: 'border-orange-700 bg-orange-900/10',
    Streaming: 'border-blue-700 bg-blue-900/10',
    'Table Format': 'border-cyan-700 bg-cyan-900/10',
    Query: 'border-yellow-700 bg-yellow-900/10',
    Storage: 'border-gray-700 bg-gray-800/20',
    'Resource Mgmt': 'border-gray-700 bg-gray-800/20',
    Orchestration: 'border-teal-700 bg-teal-900/10',
    Security: 'border-red-700 bg-red-900/10',
    BI: 'border-purple-700 bg-purple-900/10',
    Coordination: 'border-gray-700 bg-gray-800/20',
  }

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        {['ecosystem', 'timeline', 'comparison'].map(s => (
          <SubTab key={s} label={s.charAt(0).toUpperCase() + s.slice(1)} active={sub === s} onClick={() => setSub(s)} />
        ))}
      </div>

      {sub === 'ecosystem' && (
        <div className="grid grid-cols-12 gap-4">
          <div className="col-span-8">
            <div className="grid grid-cols-3 gap-2">
              {tools.map(tool => (
                <button key={tool.id} onClick={() => setSelectedTool(tool === selectedTool ? null : tool)}
                  className={clsx('p-3 rounded-lg border text-left transition-all', tool === selectedTool ? 'ring-2 ring-orange-500/50 border-orange-700 bg-orange-900/10' : categoryColor[tool.category] ?? 'border-gray-700 bg-gray-800/20', 'hover:border-orange-700/40')}>
                  <div className="flex items-start justify-between mb-1">
                    <span className="text-xs font-semibold text-gray-200">{tool.name}</span>
                    {tool.databricks_replaces
                      ? <span className="text-[9px] px-1 bg-red-900/30 border border-red-800/30 rounded text-red-400">Replaced</span>
                      : <span className="text-[9px] px-1 bg-green-900/30 border border-green-800/30 rounded text-green-400">Integrates</span>
                    }
                  </div>
                  <div className="text-[10px] text-gray-600">{tool.category} · {tool.year}</div>
                </button>
              ))}
            </div>
          </div>

          <div className="col-span-4">
            {selectedTool ? (
              <div className="card p-5 sticky top-4">
                <div className="text-sm font-bold text-gray-200 mb-1">{selectedTool.name}</div>
                <div className="text-[10px] text-gray-600 mb-3">{selectedTool.category} · {selectedTool.year}</div>
                <p className="text-xs text-gray-400 leading-relaxed mb-3">{selectedTool.description}</p>
                <div className="mb-3">
                  <span className="text-[10px] text-blue-400 font-semibold">Use when: </span>
                  <span className="text-[10px] text-gray-400">{selectedTool.use_when}</span>
                </div>
                <div className={clsx('p-3 rounded-lg border text-[11px] leading-relaxed', selectedTool.databricks_replaces ? 'border-red-800/30 bg-red-900/10 text-red-300' : 'border-green-800/30 bg-green-900/10 text-green-300')}>
                  <span className="font-semibold">{selectedTool.databricks_replaces ? '⚠ Databricks replaces: ' : '✓ Databricks integration: '}</span>
                  {selectedTool.databricks_note}
                </div>
              </div>
            ) : (
              <div className="card p-5 text-center text-gray-600 text-xs">
                <Globe size={24} className="mx-auto mb-2 opacity-30" />
                Click any tool to see details
              </div>
            )}
          </div>
        </div>
      )}

      {sub === 'timeline' && (
        <div className="card p-6">
          <div className="text-sm font-semibold text-gray-200 mb-6">Big Data Technology Evolution Timeline</div>
          <div className="relative space-y-6">
            <div className="absolute left-16 top-0 bottom-0 w-px bg-gray-700" />
            {timeline.map((event: any) => (
              <div key={event.year} className="flex items-start gap-4 relative">
                <div className={clsx('w-12 h-12 rounded-lg flex flex-col items-center justify-center flex-shrink-0 text-[10px] font-bold text-white z-10', {
                  'bg-gray-700': event.color === 'gray',
                  'bg-blue-700': event.color === 'blue',
                  'bg-orange-600': event.color === 'orange',
                  'bg-cyan-700': event.color === 'cyan',
                  'bg-green-700': event.color === 'green',
                  'bg-purple-700': event.color === 'purple',
                })}>
                  <span>{event.year}</span>
                </div>
                <div className="absolute left-16 top-6 w-4 h-px bg-gray-700" />
                <div className="flex-1 pt-1">
                  <div className={clsx('text-[10px] font-semibold mb-0.5', {
                    'text-gray-500': event.color === 'gray', 'text-blue-400': event.color === 'blue',
                    'text-orange-400': event.color === 'orange', 'text-cyan-400': event.color === 'cyan',
                    'text-green-400': event.color === 'green', 'text-purple-400': event.color === 'purple',
                  })}>{event.era}</div>
                  <div className="text-sm text-gray-300">{event.event}</div>
                  <div className="flex gap-1 flex-wrap mt-1">
                    {event.tools.map((t: string) => <span key={t} className="text-[10px] px-1.5 py-0.5 bg-gray-800 border border-gray-700 rounded text-gray-400">{t}</span>)}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {sub === 'comparison' && comparison && (
        <div className="card overflow-hidden">
          <div className="p-4 border-b border-gray-800">
            <div className="text-sm font-semibold text-gray-200">Platform Comparison: Databricks vs Snowflake vs BigQuery vs Redshift vs Synapse</div>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-gray-800 bg-gray-800/30">
                  <th className="text-left px-4 py-3 text-gray-500 font-medium w-36">Criterion</th>
                  {comparison.platforms.map((p: string, i: number) => (
                    <th key={p} className={clsx('text-left px-4 py-3 font-semibold', {
                      'text-orange-400': i === 0, 'text-cyan-400': i === 1, 'text-blue-400': i === 2,
                      'text-red-400': i === 3, 'text-purple-400': i === 4,
                    })}>{p}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {comparison.rows.map((row: any, i: number) => (
                  <tr key={row.criterion} className={clsx('border-b border-gray-800/50', i % 2 === 0 && 'bg-gray-800/10')}>
                    <td className="px-4 py-2.5 text-gray-400 font-medium">{row.criterion}</td>
                    {row.values.map((v: string, j: number) => (
                      <td key={j} className={clsx('px-4 py-2.5', j === 0 ? 'text-orange-300 font-medium' : 'text-gray-400')}>{v}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

// ─── Main Component ───────────────────────────────────────

const SECTIONS = [
  { id: 'A', label: 'Spark Fundamentals', icon: Zap },
  { id: 'B', label: 'Databricks Platform', icon: Server },
  { id: 'C', label: 'Delta Lake', icon: Layers },
  { id: 'D', label: 'Kafka & Streaming', icon: Activity },
  { id: 'E', label: 'Airflow + Spark', icon: GitBranch },
  { id: 'F', label: 'Performance Tuning', icon: Cpu },
  { id: 'G', label: 'Ecosystem Map', icon: Globe },
]

export default function BigData() {
  const [activeSection, setActiveSection] = useState('A')

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <div className="flex items-center gap-3 mb-1">
            <div className="p-2 bg-orange-600/20 rounded-lg"><Zap size={18} className="text-orange-400" /></div>
            <h1 className="text-xl font-bold text-white">Big Data & Databricks Workbench</h1>
            <span className="text-[10px] font-mono bg-orange-900/30 text-orange-400 border border-orange-800/30 px-2 py-0.5 rounded">MODULE 12</span>
          </div>
          <p className="text-sm text-gray-500 ml-11">Apache Spark · Delta Lake · Kafka · Databricks · Performance Tuning · Ecosystem Map</p>
        </div>
      </div>

      <ExplanationPanel
        title="Big Data & Databricks"
        what="The Apache ecosystem (Spark, Kafka, Delta Lake) + Databricks platform form the backbone of modern large-scale data engineering. Spark provides distributed in-memory processing, Kafka handles real-time event streaming, and Delta Lake adds ACID transactions to cheap object storage."
        why="You cannot be a senior data engineer without understanding distributed systems. When data exceeds a single machine's memory, you need Spark. When you need real-time pipelines, you need Kafka. When you need reliable lakehouse tables, you need Delta Lake or Iceberg."
        how="Senior engineers pick the right tool per use case, tune Spark configs for the actual cluster size, avoid common anti-patterns (data skew, small files, eager evaluation of lazy DAGs), and understand when Databricks simplifies vs when it adds cost."
        tools={['Apache Spark', 'Databricks', 'Delta Lake', 'Apache Kafka', 'Apache Iceberg', 'Apache Hudi', 'Apache Flink', 'Apache Airflow']}
        seniorTip="The #1 mistake junior engineers make: writing Spark code that looks correct but creates data skew, thousands of small files, or forces full shuffles when broadcast joins would be 20× faster. Always check the Spark UI before declaring a job 'done'."
      />

      {/* Section tabs */}
      <div className="flex gap-1.5 flex-wrap">
        {SECTIONS.map(s => (
          <SectionTab key={s.id} id={s.id} label={s.label} icon={s.icon} active={activeSection === s.id} onClick={() => setActiveSection(s.id)} />
        ))}
      </div>

      {/* Section content */}
      {activeSection === 'A' && <SectionSpark />}
      {activeSection === 'B' && <SectionDatabricks />}
      {activeSection === 'C' && <SectionDelta />}
      {activeSection === 'D' && <SectionKafka />}
      {activeSection === 'E' && <SectionAirflow />}
      {activeSection === 'F' && <SectionPerformance />}
      {activeSection === 'G' && <SectionEcosystem />}
    </div>
  )
}