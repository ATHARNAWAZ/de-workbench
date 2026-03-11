import { useState, useEffect, useRef } from 'react'
import { api, endpoints, WS_BASE } from '../utils/api'
import ExplanationPanel from '../components/ExplanationPanel'
import { Play, RefreshCw, CheckCircle, XCircle, Clock } from 'lucide-react'
import clsx from 'clsx'

interface DAGTask {
  task_id: string
  type: string
  upstream: string[]
  retries: number
  retry_delay_minutes: number
}

interface DAG {
  dag_id: string
  name: string
  description: string
  schedule: string
  schedule_human: string
  owner: string
  active: boolean
  tags: string[]
  last_run: string
  next_run: string
  avg_duration_minutes: number
  success_rate_30d: number
  tasks: DAGTask[]
}

interface Run {
  run_id: string
  dag_id: string
  dag_name: string
  status: string
  started_at: string
  completed_at: string | null
  duration_seconds: number | null
  trigger: string
}


const STATUS_CONFIG: Record<string, { color: string; bg: string; icon: React.ElementType }> = {
  success: { color: 'text-green-400', bg: 'bg-green-900/30', icon: CheckCircle },
  failed: { color: 'text-red-400', bg: 'bg-red-900/30', icon: XCircle },
  running: { color: 'text-blue-400', bg: 'bg-blue-900/30', icon: RefreshCw },
  pending: { color: 'text-gray-400', bg: 'bg-gray-800', icon: Clock },
}

export default function Orchestration() {
  const [dags, setDags] = useState<DAG[]>([])
  const [runs, setRuns] = useState<Run[]>([])
  const [selectedDag, setSelectedDag] = useState<DAG | null>(null)
  const [taskStatuses, setTaskStatuses] = useState<Record<string, string>>({})
  const [taskMessages, setTaskMessages] = useState<Record<string, string>>({})
  const [dagRunning, setDagRunning] = useState(false)
  const [cronExpr, setCronExpr] = useState('0 2 * * *')
  const [cronPreview, setCronPreview] = useState<{ human_readable: string; next_5_runs: string[] } | null>(null)
  const wsRef = useRef<WebSocket | null>(null)

  useEffect(() => {
    Promise.all([
      api.get(endpoints.dags),
      api.get(endpoints.runs),
    ]).then(([dagsRes, runsRes]) => {
      setDags(dagsRes.data.dags)
      setRuns(runsRes.data.runs.slice(0, 30))
      setSelectedDag(dagsRes.data.dags[0])
    })
  }, [])

  useEffect(() => {
    const t = setTimeout(async () => {
      if (cronExpr.trim()) {
        const r = await api.get(`${endpoints.cronPreview}?expression=${encodeURIComponent(cronExpr)}`)
        setCronPreview(r.data)
      }
    }, 400)
    return () => clearTimeout(t)
  }, [cronExpr])

  const triggerDAG = (dagId: string) => {
    setDagRunning(true)
    setTaskStatuses({})
    setTaskMessages({})

    if (wsRef.current) wsRef.current.close()
    const ws = new WebSocket(`${WS_BASE}/ws/dag-run`)
    wsRef.current = ws

    ws.onmessage = (e) => {
      const data = JSON.parse(e.data)
      if (data.task_id === '__done__') {
        setDagRunning(false)
        return
      }
      setTaskStatuses((prev) => ({ ...prev, [data.task_id]: data.status }))
      setTaskMessages((prev) => ({ ...prev, [data.task_id]: data.message }))
    }

    ws.onerror = () => setDagRunning(false)
    ws.onclose = () => setDagRunning(false)
  }

  const formatTime = (iso: string) => {
    const d = new Date(iso)
    const diff = Math.floor((Date.now() - d.getTime()) / 60000)
    if (diff < 60) return `${diff}m ago`
    if (diff < 1440) return `${Math.floor(diff / 60)}h ago`
    return `${Math.floor(diff / 1440)}d ago`
  }

  const getTaskLayout = (dag: DAG) => {
    const levels: Record<string, number> = {}
    const tasks = dag.tasks

    const getLevel = (taskId: string): number => {
      if (levels[taskId] !== undefined) return levels[taskId]
      const task = tasks.find((t) => t.task_id === taskId)
      if (!task || task.upstream.length === 0) {
        levels[taskId] = 0
        return 0
      }
      const maxUpstreamLevel = Math.max(...task.upstream.map(getLevel))
      levels[taskId] = maxUpstreamLevel + 1
      return levels[taskId]
    }

    tasks.forEach((t) => getLevel(t.task_id))
    return levels
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Pipeline Orchestration</h1>
          <p className="text-sm text-gray-400 mt-1">DAG management, scheduling, execution, and monitoring</p>
        </div>
      </div>

      <ExplanationPanel
        title="Pipeline Orchestration & Scheduling"
        what="Orchestration is the coordination of pipeline tasks with defined dependencies, schedules, retry logic, and monitoring. A DAG (Directed Acyclic Graph) defines tasks and their dependencies — ensuring correct execution order."
        why="Without orchestration, pipelines run manually or via cron with no dependency management, retry logic, or alerting. One failed upstream task silently corrupts all downstream outputs. Orchestration makes failures visible and recoverable."
        how="Senior engineers design idempotent DAGs — running a DAG twice for the same date produces the same result. They implement backfill strategies (catching up historical data), SLA monitoring, and task-level alerting. They separate orchestration concerns from transformation code."
        tools={['Apache Airflow', 'Prefect', 'Dagster', 'Temporal', 'AWS Step Functions', 'Mage']}
        seniorTip="Design DAGs for the 'unhappy path' first. Ask: What happens when task 3 fails on day 200? Can you re-run just that task? Does re-running task 3 corrupt data written by task 4? Idempotency and atomicity are non-negotiable for production pipelines."
      />

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        {/* DAG list */}
        <div className="space-y-3">
          <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider">DAGs ({dags.length})</div>
          {dags.map((dag) => (
            <button
              key={dag.dag_id}
              onClick={() => setSelectedDag(dag)}
              className={clsx(
                'w-full card p-4 text-left transition-all',
                selectedDag?.dag_id === dag.dag_id ? 'ring-2 ring-blue-500' : 'hover:border-gray-700'
              )}
            >
              <div className="flex items-center justify-between mb-1">
                <span className="text-sm font-semibold text-gray-200 truncate">{dag.name}</span>
                <span className={clsx('w-2 h-2 rounded-full flex-shrink-0', dag.active ? 'bg-green-500' : 'bg-gray-600')} />
              </div>
              <div className="text-xs text-gray-500 mb-2">{dag.schedule_human}</div>
              <div className="flex items-center gap-3 text-xs">
                <span className={clsx(
                  dag.success_rate_30d >= 95 ? 'text-green-400' : dag.success_rate_30d >= 85 ? 'text-yellow-400' : 'text-red-400'
                )}>
                  {dag.success_rate_30d}% success
                </span>
                <span className="text-gray-600">•</span>
                <span className="text-gray-500">{dag.avg_duration_minutes}min avg</span>
              </div>
            </button>
          ))}
        </div>

        {/* DAG detail */}
        <div className="xl:col-span-2 space-y-5">
          {selectedDag && (
            <>
              <div className="card p-5">
                <div className="flex items-start justify-between mb-4">
                  <div>
                    <h3 className="font-bold text-gray-100">{selectedDag.name}</h3>
                    <p className="text-sm text-gray-400 mt-1">{selectedDag.description}</p>
                  </div>
                  <button
                    onClick={() => triggerDAG(selectedDag.dag_id)}
                    disabled={dagRunning}
                    className="btn-primary flex-shrink-0"
                  >
                    {dagRunning ? <RefreshCw size={14} className="animate-spin" /> : <Play size={14} />}
                    Trigger Run
                  </button>
                </div>

                <div className="grid grid-cols-4 gap-3 text-sm">
                  <div>
                    <div className="text-xs text-gray-500">Schedule</div>
                    <div className="font-mono text-gray-300 text-xs mt-0.5">{selectedDag.schedule}</div>
                  </div>
                  <div>
                    <div className="text-xs text-gray-500">Last run</div>
                    <div className="text-gray-300 text-xs mt-0.5">{formatTime(selectedDag.last_run)}</div>
                  </div>
                  <div>
                    <div className="text-xs text-gray-500">Success rate</div>
                    <div className={clsx('font-bold text-sm mt-0.5', selectedDag.success_rate_30d >= 95 ? 'text-green-400' : 'text-yellow-400')}>
                      {selectedDag.success_rate_30d}%
                    </div>
                  </div>
                  <div>
                    <div className="text-xs text-gray-500">Owner</div>
                    <div className="text-gray-400 text-xs mt-0.5 truncate">{selectedDag.owner}</div>
                  </div>
                </div>
              </div>

              {/* DAG Visualizer */}
              <div className="card p-4">
                <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">
                  DAG Visualization
                  {dagRunning && <span className="ml-2 text-blue-400 animate-pulse">● Live</span>}
                </div>
                <div className="overflow-x-auto">
                  <div className="flex items-start gap-6 min-w-max p-2">
                    {(() => {
                      const levels = getTaskLayout(selectedDag)
                      const maxLevel = Math.max(...Object.values(levels))
                      return Array.from({ length: maxLevel + 1 }, (_, l) => (
                        <div key={l} className="flex flex-col gap-3">
                          {selectedDag.tasks.filter((t) => levels[t.task_id] === l).map((task) => {
                            const status = taskStatuses[task.task_id] || 'pending'
                            const msg = taskMessages[task.task_id]
                            const cfg = STATUS_CONFIG[status] || STATUS_CONFIG.pending
                            const Icon = cfg.icon
                            return (
                              <div
                                key={task.task_id}
                                className={clsx(
                                  'w-44 rounded-xl border p-3 transition-all',
                                  status === 'pending' ? 'border-gray-700 bg-gray-800/40' :
                                  status === 'running' ? 'border-blue-600 bg-blue-900/20 shadow-lg shadow-blue-900/20' :
                                  status === 'success' ? 'border-green-600 bg-green-900/20' :
                                  'border-red-600 bg-red-900/20'
                                )}
                              >
                                <div className="flex items-center gap-2 mb-1">
                                  <Icon size={12} className={clsx(cfg.color, status === 'running' && 'animate-spin')} />
                                  <span className="text-xs font-semibold text-gray-200 leading-tight">{task.task_id}</span>
                                </div>
                                <div className="text-[10px] text-gray-500">{task.type}</div>
                                {msg && <div className="text-[10px] text-gray-400 mt-1 leading-tight">{msg}</div>}
                                <div className="text-[10px] text-gray-600 mt-1">Retries: {task.retries}</div>
                              </div>
                            )
                          })}
                        </div>
                      ))
                    })()}
                  </div>
                </div>
              </div>
            </>
          )}

          {/* Cron Builder */}
          <div className="card p-5">
            <h3 className="font-semibold text-gray-200 mb-4">Cron Expression Builder</h3>
            <div className="flex items-center gap-3 mb-3">
              <input
                className="input font-mono w-48"
                value={cronExpr}
                onChange={(e) => setCronExpr(e.target.value)}
                placeholder="0 2 * * *"
              />
              <div className="flex gap-2 flex-wrap">
                {['*/15 * * * *', '0 2 * * *', '0 */6 * * *', '0 4 * * 1', '0 0 1 * *'].map((expr) => (
                  <button key={expr} onClick={() => setCronExpr(expr)} className="btn-secondary text-xs font-mono">
                    {expr}
                  </button>
                ))}
              </div>
            </div>
            {cronPreview && (
              <div className="bg-gray-800/50 rounded-lg p-4 space-y-2">
                <div className="text-sm font-medium text-blue-300">{cronPreview.human_readable}</div>
                <div className="text-xs text-gray-500">Next 5 runs:</div>
                <div className="space-y-0.5">
                  {cronPreview.next_5_runs.map((run, i) => (
                    <div key={i} className="text-xs font-mono text-gray-400">{run}</div>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Run history table */}
          <div className="card overflow-hidden">
            <div className="px-5 py-4 border-b border-gray-800">
              <h3 className="font-semibold text-gray-200">Run History</h3>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-900 border-b border-gray-800">
                  <tr>
                    <th className="table-header px-5 py-3">Pipeline</th>
                    <th className="table-header px-4 py-3">Status</th>
                    <th className="table-header px-4 py-3">Started</th>
                    <th className="table-header px-4 py-3">Duration</th>
                    <th className="table-header px-4 py-3">Trigger</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-800">
                  {runs.slice(0, 15).map((run) => {
                    const cfg = STATUS_CONFIG[run.status] || STATUS_CONFIG.pending
                    const Icon = cfg.icon
                    return (
                      <tr key={run.run_id} className="hover:bg-gray-800/30 transition-colors">
                        <td className="table-cell px-5 font-mono text-xs">{run.dag_name}</td>
                        <td className="table-cell px-4">
                          <span className={clsx('flex items-center gap-1.5 text-xs', cfg.color)}>
                            <Icon size={12} className={run.status === 'running' ? 'animate-spin' : ''} />
                            {run.status}
                          </span>
                        </td>
                        <td className="table-cell px-4 text-xs text-gray-500">{formatTime(run.started_at)}</td>
                        <td className="table-cell px-4 font-mono text-xs">
                          {run.duration_seconds ? `${Math.round(run.duration_seconds)}s` : '—'}
                        </td>
                        <td className="table-cell px-4">
                          <span className={clsx('badge-info text-[10px]', run.trigger === 'manual' && 'badge-purple')}>
                            {run.trigger}
                          </span>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
