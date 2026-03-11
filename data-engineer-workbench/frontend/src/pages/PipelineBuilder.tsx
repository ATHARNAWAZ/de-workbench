import { useState, useRef } from 'react'
import { api, endpoints } from '../utils/api'
import ExplanationPanel from '../components/ExplanationPanel'
import { Play, Code, Download, Plus, Trash2, ChevronRight, CheckCircle, Clock, RefreshCw } from 'lucide-react'
import clsx from 'clsx'

type NodeType = 'source' | 'filter' | 'join' | 'aggregate' | 'rename' | 'sink'

interface PipelineNode {
  id: string
  type: NodeType
  config: Record<string, string>
  position: { x: number; y: number }
}

interface PipelineEdge {
  source: string
  target: string
}

interface StepResult {
  node_id: string
  node_type: string
  input_rows: number
  output_rows: number
  duration_seconds: number
  logs: string[]
  status: string
  data_preview: Record<string, unknown>[]
}

const NODE_TEMPLATES: Record<NodeType, { label: string; color: string; border: string; defaultConfig: Record<string, string> }> = {
  source: { label: 'Source', color: 'bg-blue-900/30', border: 'border-blue-700', defaultConfig: { table: 'fact_orders' } },
  filter: { label: 'Filter', color: 'bg-yellow-900/30', border: 'border-yellow-700', defaultConfig: { condition: "status = 'completed'", pandas_condition: "df['status'] == 'completed'" } },
  join: { label: 'Join', color: 'bg-purple-900/30', border: 'border-purple-700', defaultConfig: { right_table: 'dim_customer', on: 'customer_id', join_type: 'LEFT' } },
  aggregate: { label: 'Aggregate', color: 'bg-green-900/30', border: 'border-green-700', defaultConfig: { group_by: 'category', agg_column: 'revenue', agg_function: 'sum' } },
  rename: { label: 'Rename', color: 'bg-orange-900/30', border: 'border-orange-700', defaultConfig: { from: 'revenue', to: 'total_revenue' } },
  sink: { label: 'Sink', color: 'bg-red-900/30', border: 'border-red-700', defaultConfig: { destination: 'gold_daily_revenue', write_mode: 'overwrite', partition_by: 'date' } },
}

const DEFAULT_PIPELINE: { nodes: PipelineNode[]; edges: PipelineEdge[] } = {
  nodes: [
    { id: 'n1', type: 'source', config: { table: 'fact_orders' }, position: { x: 40, y: 160 } },
    { id: 'n2', type: 'filter', config: { condition: "status = 'completed'", pandas_condition: "df['status'] == 'completed'" }, position: { x: 220, y: 160 } },
    { id: 'n3', type: 'join', config: { right_table: 'dim_customer', on: 'customer_id', join_type: 'LEFT' }, position: { x: 400, y: 160 } },
    { id: 'n4', type: 'aggregate', config: { group_by: 'category', agg_column: 'revenue', agg_function: 'sum' }, position: { x: 580, y: 160 } },
    { id: 'n5', type: 'sink', config: { destination: 'gold_daily_revenue', write_mode: 'overwrite', partition_by: 'date' }, position: { x: 760, y: 160 } },
  ],
  edges: [
    { source: 'n1', target: 'n2' },
    { source: 'n2', target: 'n3' },
    { source: 'n3', target: 'n4' },
    { source: 'n4', target: 'n5' },
  ],
}

export default function PipelineBuilder() {
  const [nodes, setNodes] = useState<PipelineNode[]>(DEFAULT_PIPELINE.nodes)
  const [edges, setEdges] = useState<PipelineEdge[]>(DEFAULT_PIPELINE.edges)
  const [running, setRunning] = useState(false)
  const [steps, setSteps] = useState<StepResult[]>([])
  const [generatedCode, setGeneratedCode] = useState('')
  const [showCode, setShowCode] = useState(false)
  const [selectedNode, setSelectedNode] = useState<string | null>(null)
  const canvasRef = useRef<HTMLDivElement>(null)

  const handleRun = async () => {
    setRunning(true)
    setSteps([])
    try {
      const r = await api.post(endpoints.pipelineRun, { nodes, edges })
      setSteps(r.data.steps)
    } finally {
      setRunning(false)
    }
  }

  const handleGenerateCode = async () => {
    const r = await api.post(endpoints.pipelineCode, { nodes, edges })
    setGeneratedCode(r.data.code)
    setShowCode(true)
  }

  const addNode = (type: NodeType) => {
    const template = NODE_TEMPLATES[type]
    const newNode: PipelineNode = {
      id: `n${Date.now()}`,
      type,
      config: { ...template.defaultConfig },
      position: { x: 100 + nodes.length * 180, y: 160 },
    }
    setNodes([...nodes, newNode])
  }

  const removeNode = (id: string) => {
    setNodes(nodes.filter((n) => n.id !== id))
    setEdges(edges.filter((e) => e.source !== id && e.target !== id))
    if (selectedNode === id) setSelectedNode(null)
  }

  const updateNodeConfig = (id: string, key: string, value: string) => {
    setNodes(nodes.map((n) => n.id === id ? { ...n, config: { ...n.config, [key]: value } } : n))
  }

  const selectedNodeData = nodes.find((n) => n.id === selectedNode)

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Pipeline Builder</h1>
          <p className="text-sm text-gray-400 mt-1">Visual drag-and-drop data transformation pipeline designer</p>
        </div>
        <div className="flex items-center gap-3">
          <button onClick={handleGenerateCode} className="btn-secondary">
            <Code size={14} /> Generate Code
          </button>
          <button onClick={handleRun} disabled={running} className="btn-primary">
            {running ? <RefreshCw size={14} className="animate-spin" /> : <Play size={14} />}
            Execute Pipeline
          </button>
        </div>
      </div>

      <ExplanationPanel
        title="Data Transformation Pipelines"
        what="A transformation pipeline is a series of operations applied to data to convert it from raw format into a clean, structured, analysis-ready form. Each step is a discrete, testable unit of work."
        why="Transformation is where 60% of data engineering effort lives. Poorly designed transformations are brittle (break on schema changes), slow (no pushdown optimization), and untestable. Bad transformations are the #1 source of data quality incidents."
        how="Senior engineers design pipelines to be idempotent (running twice = same result), modular (each transformation is independently testable), and observable (each step logs row counts, timing, and schema). They use column lineage to understand the impact of changes."
        tools={['dbt', 'Apache Spark', 'Apache Flink', 'Pandas', 'Polars', 'dlt']}
        seniorTip="Always checkpoint row counts between steps. If you start with 1M rows and end with 10 rows, that's either correct or a silent bug. Add assertions: assert len(df) > 0, 'Unexpected empty output'. Log input/output schemas to catch silent type changes."
      />

      {/* Node palette */}
      <div className="card p-4">
        <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Add Node</div>
        <div className="flex flex-wrap gap-2">
          {(Object.keys(NODE_TEMPLATES) as NodeType[]).map((type) => {
            const t = NODE_TEMPLATES[type]
            return (
              <button
                key={type}
                onClick={() => addNode(type)}
                className={clsx('flex items-center gap-2 px-3 py-1.5 rounded-lg border text-xs font-medium transition-all hover:opacity-80', t.color, t.border)}
              >
                <Plus size={12} />
                {t.label}
              </button>
            )
          })}
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        {/* Canvas */}
        <div className="xl:col-span-2">
          <div className="card p-4">
            <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Pipeline Canvas</div>
            <div ref={canvasRef} className="relative overflow-x-auto" style={{ minHeight: 320 }}>
              <svg className="absolute inset-0 w-full h-full pointer-events-none" style={{ minWidth: nodes.length * 190 + 60 }}>
                {edges.map((edge, i) => {
                  const src = nodes.find((n) => n.id === edge.source)
                  const tgt = nodes.find((n) => n.id === edge.target)
                  if (!src || !tgt) return null
                  const x1 = src.position.x + 140
                  const y1 = src.position.y + 30
                  const x2 = tgt.position.x
                  const y2 = tgt.position.y + 30
                  const mx = (x1 + x2) / 2
                  return (
                    <g key={i}>
                      <path d={`M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}`} fill="none" stroke="#374151" strokeWidth="2" />
                      <polygon points={`${x2},${y2} ${x2 - 8},${y2 - 4} ${x2 - 8},${y2 + 4}`} fill="#374151" />
                    </g>
                  )
                })}
              </svg>

              <div className="relative" style={{ minWidth: nodes.length * 190 + 60, height: 320 }}>
                {nodes.map((node) => {
                  const t = NODE_TEMPLATES[node.type]
                  const step = steps.find((s) => s.node_id === node.id)
                  const isSelected = selectedNode === node.id
                  return (
                    <div
                      key={node.id}
                      onClick={() => setSelectedNode(isSelected ? null : node.id)}
                      className={clsx(
                        'absolute w-36 rounded-xl border-2 cursor-pointer transition-all select-none',
                        t.color, t.border,
                        isSelected && 'ring-2 ring-blue-500 ring-offset-2 ring-offset-gray-900',
                        step?.status === 'success' && 'ring-2 ring-green-500 ring-offset-2 ring-offset-gray-900',
                      )}
                      style={{ left: node.position.x, top: node.position.y }}
                    >
                      <div className="px-3 py-2">
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-xs font-semibold text-gray-200">{t.label}</span>
                          <button
                            onClick={(e) => { e.stopPropagation(); removeNode(node.id) }}
                            className="text-gray-600 hover:text-red-400 transition-colors"
                          >
                            <Trash2 size={10} />
                          </button>
                        </div>
                        <div className="text-[10px] text-gray-400 truncate">
                          {Object.values(node.config)[0] || node.type}
                        </div>
                        {step && (
                          <div className="mt-1.5 flex items-center gap-1.5">
                            <CheckCircle size={10} className="text-green-400" />
                            <span className="text-[10px] text-green-400">{step.output_rows.toLocaleString()} rows</span>
                          </div>
                        )}
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>
          </div>

          {/* Node config */}
          {selectedNodeData && (
            <div className="card p-4 mt-4 animate-slide-in">
              <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">
                Configure: {NODE_TEMPLATES[selectedNodeData.type].label}
              </div>
              <div className="grid grid-cols-2 gap-3">
                {Object.entries(selectedNodeData.config).map(([key, val]) => (
                  <div key={key}>
                    <label className="text-xs text-gray-500 block mb-1">{key}</label>
                    <input
                      className="input text-xs"
                      value={val}
                      onChange={(e) => updateNodeConfig(selectedNodeData.id, key, e.target.value)}
                    />
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Execution log */}
        <div className="card overflow-hidden">
          <div className="px-4 py-3 border-b border-gray-800 flex items-center justify-between">
            <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Execution Log</div>
            {running && <RefreshCw size={12} className="animate-spin text-blue-400" />}
          </div>
          <div className="divide-y divide-gray-800 overflow-y-auto max-h-96">
            {steps.length === 0 && !running && (
              <div className="flex flex-col items-center justify-center h-48 text-gray-600 text-sm">
                <Play size={32} className="mb-2 opacity-30" />
                Execute pipeline to see logs
              </div>
            )}
            {steps.map((step) => (
              <div key={step.node_id} className="p-4 space-y-2 animate-slide-in">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <CheckCircle size={14} className="text-green-400" />
                    <span className="text-xs font-semibold text-gray-300 capitalize">{step.node_type}</span>
                  </div>
                  <div className="flex items-center gap-3 text-xs text-gray-500">
                    <span><Clock size={10} className="inline mr-1" />{step.duration_seconds}s</span>
                  </div>
                </div>
                <div className="flex gap-3 text-xs">
                  <span className="text-gray-500">{step.input_rows.toLocaleString()} in</span>
                  <ChevronRight size={12} className="text-gray-600" />
                  <span className={clsx(step.output_rows < step.input_rows ? 'text-yellow-400' : 'text-green-400')}>
                    {step.output_rows.toLocaleString()} out
                  </span>
                </div>
                <div className="space-y-0.5">
                  {step.logs.map((log, i) => (
                    <div key={i} className="text-[10px] text-gray-500 font-mono">{log}</div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Code output */}
      {showCode && generatedCode && (
        <div className="card overflow-hidden animate-slide-in">
          <div className="px-5 py-4 border-b border-gray-800 flex items-center justify-between">
            <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Generated Python / Pandas Code</div>
            <div className="flex gap-2">
              <button
                onClick={() => navigator.clipboard.writeText(generatedCode)}
                className="btn-secondary text-xs"
              >
                <Download size={12} /> Copy
              </button>
              <button onClick={() => setShowCode(false)} className="text-gray-500 hover:text-gray-300 text-xs">Close</button>
            </div>
          </div>
          <pre className="code-block m-4 text-xs leading-relaxed overflow-x-auto max-h-96">{generatedCode}</pre>
        </div>
      )}
    </div>
  )
}
