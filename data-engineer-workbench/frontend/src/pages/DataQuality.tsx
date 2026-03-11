import { useState, useEffect } from 'react'
import { api, endpoints } from '../utils/api'
import ExplanationPanel from '../components/ExplanationPanel'
import { RadialBarChart, RadialBar, ResponsiveContainer, Tooltip } from 'recharts'
import { ShieldCheck, AlertTriangle, XCircle, Info, ChevronDown, ChevronUp, Play } from 'lucide-react'
import clsx from 'clsx'

interface ColumnResult {
  column: string
  type: string
  null_pct: number
  null_count: number
  unique_count: number
  cardinality_pct: number
  score: number
  issues: Array<{ type: string; severity: string; detail: string }>
  sample_values: (string | number)[]
  suggestions: string[]
}

interface QualityReport {
  dataset_id: string
  dataset_name: string
  row_count: number
  column_count: number
  overall_score: number
  grade: string
  duplicate_rows: number
  referential_integrity_issues: number
  columns: ColumnResult[]
  summary: { critical_issues: number; warnings: number; info: number }
  checked_at: string
}

interface Dataset {
  id: string
  name: string
  layer: string
  row_count: number
  column_count: number
}

const SEV_CONFIG = {
  critical: { color: 'text-red-400', bg: 'bg-red-900/20 border-red-800/40', icon: XCircle },
  warning: { color: 'text-yellow-400', bg: 'bg-yellow-900/20 border-yellow-800/40', icon: AlertTriangle },
  info: { color: 'text-blue-400', bg: 'bg-blue-900/20 border-blue-800/40', icon: Info },
}

function ScoreGauge({ score, grade }: { score: number; grade: string }) {
  const color = score >= 90 ? '#22c55e' : score >= 70 ? '#eab308' : '#ef4444'
  const data = [{ value: score, fill: color }]

  return (
    <div className="relative w-40 h-40 flex items-center justify-center">
      <ResponsiveContainer width="100%" height="100%">
        <RadialBarChart cx="50%" cy="50%" innerRadius="60%" outerRadius="90%" startAngle={90} endAngle={-270} data={data}>
          <RadialBar dataKey="value" cornerRadius={8} background={{ fill: '#1f2937' }} />
        </RadialBarChart>
      </ResponsiveContainer>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-3xl font-bold text-white">{score}</span>
        <span className="text-sm font-mono" style={{ color }}>{grade}</span>
      </div>
    </div>
  )
}

function NullBar({ pct }: { pct: number }) {
  const color = pct > 0.1 ? 'bg-red-500' : pct > 0.02 ? 'bg-yellow-500' : 'bg-green-500'
  return (
    <div className="flex items-center gap-2 w-full">
      <div className="flex-1 bg-gray-800 rounded-full h-1.5 overflow-hidden">
        <div className={clsx('h-full rounded-full transition-all', color)} style={{ width: `${Math.min(pct * 100, 100)}%` }} />
      </div>
      <span className="text-xs font-mono text-gray-400 w-10 text-right">{(pct * 100).toFixed(1)}%</span>
    </div>
  )
}

export default function DataQuality() {
  const [datasets, setDatasets] = useState<Dataset[]>([])
  const [selected, setSelected] = useState<string>('')
  const [report, setReport] = useState<QualityReport | null>(null)
  const [loading, setLoading] = useState(false)
  const [expandedCol, setExpandedCol] = useState<string | null>(null)
  const [customRule, setCustomRule] = useState({ column: '', rule: 'NOT NULL', value: '' })
  const [ruleResult, setRuleResult] = useState<{ pass_rate: number; violations: number; status: string } | null>(null)

  useEffect(() => {
    api.get(endpoints.qualityDatasets).then((r) => {
      setDatasets(r.data.datasets)
      setSelected(r.data.datasets[0]?.id || '')
    })
  }, [])

  const runChecks = async () => {
    if (!selected) return
    setLoading(true)
    setReport(null)
    try {
      const r = await api.get(endpoints.quality(selected))
      setReport(r.data)
    } finally {
      setLoading(false)
    }
  }

  const runCustomRule = async () => {
    const r = await api.post(endpoints.qualityRuleCheck, {
      dataset_id: selected,
      column: customRule.column,
      rule: customRule.rule,
      value: customRule.value,
    })
    setRuleResult(r.data)
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Data Quality</h1>
          <p className="text-sm text-gray-400 mt-1">Automated quality checks, scoring, and fix suggestions</p>
        </div>
      </div>

      <ExplanationPanel
        title="Data Quality & Validation"
        what="Data quality encompasses completeness (no missing values), uniqueness (no duplicates), validity (values match expected types/ranges), consistency (referential integrity), and timeliness (data is fresh)."
        why="Bad data quality costs enterprises an average of $12.9M annually (Gartner). A 5% null rate in a revenue column can mean millions in incorrect reporting. Data quality issues found in production are 10-100x more expensive to fix than at ingestion time."
        how="Senior engineers implement multi-layer validation: schema checks at ingestion, Great Expectations suites on raw data, dbt tests on transformed models, and Soda Core monitors on serving layer. They define data contracts — SLAs that upstream teams must meet — and automate alerts when contracts are violated."
        tools={['Great Expectations', 'dbt tests', 'Soda Core', 'Monte Carlo', 'Bigeye', 'Apache Griffin']}
        seniorTip="Treat data quality like test coverage. Aim for >95% coverage of critical columns with quality assertions. Every new pipeline should ship with quality tests — otherwise you're flying blind. Tag columns as 'tier-1' (business-critical) and require 100% quality score for those."
      />

      {/* Dataset selector */}
      <div className="card p-5 flex items-end gap-4 flex-wrap">
        <div className="flex-1 min-w-48">
          <label className="text-xs text-gray-400 font-medium block mb-1.5">Select Dataset</label>
          <select className="select" value={selected} onChange={(e) => setSelected(e.target.value)}>
            {datasets.map((d) => (
              <option key={d.id} value={d.id}>{d.name} ({d.layer}) — {d.row_count.toLocaleString()} rows</option>
            ))}
          </select>
        </div>
        <button onClick={runChecks} disabled={loading} className="btn-primary">
          {loading ? (
            <svg className="animate-spin w-4 h-4" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
          ) : <Play size={14} />}
          Run Quality Checks
        </button>
      </div>

      {/* Loading skeleton */}
      {loading && (
        <div className="grid grid-cols-4 gap-4">
          {[1, 2, 3, 4].map((i) => <div key={i} className="card p-5 h-28 skeleton" />)}
        </div>
      )}

      {/* Report */}
      {report && (
        <div className="space-y-5 animate-slide-in">
          {/* Summary cards */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="card p-5 flex flex-col items-center justify-center">
              <ScoreGauge score={report.overall_score} grade={report.grade} />
              <div className="text-xs text-gray-400 mt-2 text-center">Overall Quality Score</div>
            </div>

            <div className="card p-5 space-y-4">
              <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Issue Summary</div>
              <div className="space-y-3">
                <div className="flex justify-between items-center">
                  <span className="flex items-center gap-2 text-sm text-red-400"><XCircle size={14} /> Critical</span>
                  <span className="font-mono font-bold text-red-400">{report.summary.critical_issues}</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="flex items-center gap-2 text-sm text-yellow-400"><AlertTriangle size={14} /> Warnings</span>
                  <span className="font-mono font-bold text-yellow-400">{report.summary.warnings}</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="flex items-center gap-2 text-sm text-blue-400"><Info size={14} /> Info</span>
                  <span className="font-mono font-bold text-blue-400">{report.summary.info}</span>
                </div>
              </div>
            </div>

            <div className="card p-5 space-y-4">
              <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Dataset Stats</div>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-gray-400">Rows</span>
                  <span className="font-mono text-gray-200">{report.row_count.toLocaleString()}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Columns</span>
                  <span className="font-mono text-gray-200">{report.column_count}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Duplicate rows</span>
                  <span className={clsx('font-mono', report.duplicate_rows > 0 ? 'text-yellow-400' : 'text-green-400')}>
                    {report.duplicate_rows}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Ref. integrity issues</span>
                  <span className={clsx('font-mono', report.referential_integrity_issues > 0 ? 'text-red-400' : 'text-green-400')}>
                    {report.referential_integrity_issues}
                  </span>
                </div>
              </div>
            </div>

            <div className="card p-5">
              <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Column Health Heatmap</div>
              <div className="grid grid-cols-4 gap-1">
                {report.columns.map((col) => (
                  <div
                    key={col.column}
                    title={`${col.column}: ${col.score}/100`}
                    className="w-full aspect-square rounded"
                    style={{
                      backgroundColor: col.score >= 90 ? '#16a34a44' : col.score >= 70 ? '#ca8a0444' : '#dc262644'
                    }}
                  />
                ))}
              </div>
              <div className="flex justify-between mt-2 text-[10px] text-gray-600">
                <span>Good</span><span>Warn</span><span>Bad</span>
              </div>
            </div>
          </div>

          {/* Column details */}
          <div className="card overflow-hidden">
            <div className="px-5 py-4 border-b border-gray-800">
              <h3 className="font-semibold text-gray-200">Column Analysis</h3>
            </div>
            <div className="divide-y divide-gray-800">
              {report.columns.map((col) => {
                const isExp = expandedCol === col.column
                return (
                  <div key={col.column}>
                    <button
                      onClick={() => setExpandedCol(isExp ? null : col.column)}
                      className="w-full flex items-center gap-4 px-5 py-3 hover:bg-gray-800/30 text-left transition-colors"
                    >
                      <div className="w-36 font-mono text-sm text-blue-300 truncate">{col.column}</div>
                      <div className="w-20 text-xs text-gray-500">{col.type}</div>
                      <div className="flex-1">
                        <NullBar pct={col.null_pct} />
                      </div>
                      <div className="w-16 text-right">
                        <span className={clsx(
                          'text-sm font-bold',
                          col.score >= 90 ? 'text-green-400' : col.score >= 70 ? 'text-yellow-400' : 'text-red-400'
                        )}>{col.score}</span>
                      </div>
                      <div className="w-20 flex flex-wrap gap-1 justify-end">
                        {col.issues.map((issue, i) => (
                          <span key={i} className={clsx(
                            'w-2 h-2 rounded-full',
                            issue.severity === 'critical' ? 'bg-red-500' : issue.severity === 'warning' ? 'bg-yellow-500' : 'bg-blue-500'
                          )} title={issue.detail} />
                        ))}
                      </div>
                      {isExp ? <ChevronUp size={14} className="text-gray-500" /> : <ChevronDown size={14} className="text-gray-500" />}
                    </button>

                    {isExp && (
                      <div className="px-5 pb-4 bg-gray-950/40 animate-slide-in">
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4 pt-3">
                          <div className="bg-gray-800/50 rounded-lg p-3">
                            <div className="text-xs text-gray-500">Null count</div>
                            <div className="text-sm font-mono text-gray-200 mt-0.5">{col.null_count.toLocaleString()}</div>
                          </div>
                          <div className="bg-gray-800/50 rounded-lg p-3">
                            <div className="text-xs text-gray-500">Unique values</div>
                            <div className="text-sm font-mono text-gray-200 mt-0.5">{col.unique_count.toLocaleString()}</div>
                          </div>
                          <div className="bg-gray-800/50 rounded-lg p-3">
                            <div className="text-xs text-gray-500">Cardinality</div>
                            <div className="text-sm font-mono text-gray-200 mt-0.5">{col.cardinality_pct}%</div>
                          </div>
                          <div className="bg-gray-800/50 rounded-lg p-3">
                            <div className="text-xs text-gray-500">Sample values</div>
                            <div className="text-sm font-mono text-gray-200 mt-0.5 truncate">{col.sample_values.slice(0, 2).join(', ')}</div>
                          </div>
                        </div>

                        {col.issues.length > 0 && (
                          <div className="space-y-2 mb-4">
                            {col.issues.map((issue, i) => {
                              const cfg = SEV_CONFIG[issue.severity as keyof typeof SEV_CONFIG] || SEV_CONFIG.info
                              const IssueIcon = cfg.icon
                              return (
                                <div key={i} className={clsx('flex items-start gap-2 px-3 py-2 rounded-lg border text-xs', cfg.bg)}>
                                  <IssueIcon size={13} className={clsx(cfg.color, 'flex-shrink-0 mt-0.5')} />
                                  <span className="text-gray-300">{issue.detail}</span>
                                </div>
                              )
                            })}
                          </div>
                        )}

                        {col.suggestions.length > 0 && (
                          <div>
                            <div className="text-xs font-semibold text-green-400 uppercase tracking-wider mb-2">Fix Suggestions</div>
                            <ul className="space-y-1">
                              {col.suggestions.map((s, i) => (
                                <li key={i} className="flex items-start gap-2 text-xs text-gray-400">
                                  <ShieldCheck size={12} className="text-green-400 flex-shrink-0 mt-0.5" />
                                  {s}
                                </li>
                              ))}
                            </ul>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          </div>

          {/* Custom rule builder */}
          <div className="card p-5">
            <h3 className="font-semibold text-gray-200 mb-4">Custom Rule Builder</h3>
            <div className="flex items-end gap-3 flex-wrap">
              <div>
                <label className="text-xs text-gray-400 block mb-1">Column</label>
                <input
                  className="input w-40"
                  placeholder="e.g. age"
                  value={customRule.column}
                  onChange={(e) => setCustomRule((p) => ({ ...p, column: e.target.value }))}
                />
              </div>
              <div>
                <label className="text-xs text-gray-400 block mb-1">Rule</label>
                <select
                  className="select w-48"
                  value={customRule.rule}
                  onChange={(e) => setCustomRule((p) => ({ ...p, rule: e.target.value }))}
                >
                  <option>NOT NULL</option>
                  <option>BETWEEN</option>
                  <option>REGEX MATCH</option>
                  <option>IN LIST</option>
                  <option>UNIQUE</option>
                  <option>MIN LENGTH</option>
                </select>
              </div>
              <div>
                <label className="text-xs text-gray-400 block mb-1">Value (optional)</label>
                <input
                  className="input w-40"
                  placeholder="e.g. 0,120"
                  value={customRule.value}
                  onChange={(e) => setCustomRule((p) => ({ ...p, value: e.target.value }))}
                />
              </div>
              <button onClick={runCustomRule} className="btn-primary">Run Rule</button>
            </div>

            {ruleResult && (
              <div className={clsx(
                'mt-4 p-4 rounded-lg border flex items-center gap-4',
                ruleResult.status === 'pass' ? 'bg-green-900/20 border-green-800/40' : 'bg-red-900/20 border-red-800/40'
              )}>
                <div className="text-sm">
                  <span className={clsx('font-semibold', ruleResult.status === 'pass' ? 'text-green-400' : 'text-red-400')}>
                    {ruleResult.status.toUpperCase()}
                  </span>
                  <span className="text-gray-400 ml-3">Pass rate: <span className="font-mono">{ruleResult.pass_rate}%</span></span>
                  <span className="text-gray-400 ml-3">Violations: <span className="font-mono text-red-400">{ruleResult.violations}</span></span>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
