import { useState, useEffect } from 'react'
import { api } from '../utils/api'

const ep = {
  features: '/api/featurestore/features',
  dataset: '/api/featurestore/dataset',
  monitoring: '/api/featurestore/monitoring',
  pipeline: '/api/featurestore/pipeline',
  serving: '/api/featurestore/online-serving',
}

function SectionTab({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return <button onClick={onClick} className={`px-4 py-2 text-sm font-medium rounded-lg transition-all ${active ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white hover:bg-gray-700'}`}>{label}</button>
}

function FeatureRegistry() {
  const [features, setFeatures] = useState<any[]>([])
  const [serving, setServing] = useState<any>(null)
  const [selected, setSelected] = useState<Set<string>>(new Set())

  useEffect(() => {
    api.get(ep.features).then(r => setFeatures(r.data.features))
    api.get(ep.serving).then(r => setServing(r.data))
  }, [])

  const toggleSelect = (id: string) => setSelected(p => { const s = new Set(p); s.has(id) ? s.delete(id) : s.add(id); return s })

  const statusColor: Record<string, string> = { fresh: 'text-green-400', stale: 'text-red-400', pending: 'text-gray-400' }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
        {features.map(f => (
          <div key={f.id} onClick={() => toggleSelect(f.id)} className={`bg-gray-800 rounded-xl border cursor-pointer transition-all p-4 ${selected.has(f.id) ? 'border-blue-500 ring-1 ring-blue-500/30' : 'border-gray-700 hover:border-gray-600'}`}>
            <div className="flex items-start justify-between mb-2">
              <div>
                <div className="font-mono text-sm font-bold text-blue-300">{f.name}</div>
                <div className="text-xs text-gray-400 mt-0.5">{f.entity} · {f.dtype}</div>
              </div>
              <span className={`text-xs ${statusColor[f.status]}`}>{f.status}</span>
            </div>
            <div className="text-xs text-gray-300 mb-2">{f.description}</div>
            <div className="flex items-center justify-between text-xs">
              <span className="text-gray-500">SLA: {f.freshness_sla_hours}h · Owner: {f.owner}</span>
              <div className="flex gap-1">
                {f.online_store && <span className="px-1.5 py-0.5 bg-green-500/20 text-green-400 rounded">online</span>}
                {f.offline_store && <span className="px-1.5 py-0.5 bg-blue-500/20 text-blue-400 rounded">offline</span>}
              </div>
            </div>
            {f.tags?.length > 0 && (
              <div className="flex gap-1 mt-2 flex-wrap">
                {f.tags.map((t: string) => <span key={t} className="text-xs px-1.5 py-0.5 bg-gray-700 text-gray-400 rounded">{t}</span>)}
              </div>
            )}
          </div>
        ))}
      </div>
      {serving && (
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <div className="text-sm font-semibold text-white mb-3">Online Serving Demo — Sub-Millisecond Feature Retrieval</div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <div className="text-xs text-gray-400 mb-1">Request</div>
              <pre className="text-xs text-green-300 font-mono bg-gray-900 rounded p-2">{JSON.stringify(serving.demo.request, null, 2)}</pre>
            </div>
            <div>
              <div className="text-xs text-gray-400 mb-1">Response — <span className="text-green-400">{serving.demo.latency_ms}ms</span> from Redis</div>
              <pre className="text-xs text-blue-300 font-mono bg-gray-900 rounded p-2">{JSON.stringify(serving.demo.response, null, 2)}</pre>
            </div>
          </div>
          <div className="mt-3 grid grid-cols-2 gap-3">
            <div className="bg-gray-700/50 rounded p-3 text-center">
              <div className="text-green-400 text-xl font-bold">{serving.vs_offline.online_latency_ms}ms</div>
              <div className="text-xs text-gray-400">Online Store (Redis) — real-time serving</div>
            </div>
            <div className="bg-gray-700/50 rounded p-3 text-center">
              <div className="text-yellow-400 text-xl font-bold">{serving.vs_offline.offline_latency_ms}ms</div>
              <div className="text-xs text-gray-400">Offline Store (S3/Parquet) — model training</div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function DatasetGenerator() {
  const [features, setFeatures] = useState<any[]>([])
  const [selected, setSelected] = useState<string[]>([])
  const [window, setWindow] = useState(180)
  const [result, setResult] = useState<any>(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    api.get(ep.features).then(r => setFeatures(r.data.features))
  }, [])

  const toggle = (id: string) => setSelected(p => p.includes(id) ? p.filter(x => x !== id) : [...p, id])

  const generate = async () => {
    if (selected.length === 0) return
    setLoading(true)
    try {
      const res = await api.post(ep.dataset, { feature_ids: selected, training_window_days: window })
      setResult(res.data)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-6">
      <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
        <div className="text-sm font-semibold text-white mb-3">Select Features for Training Dataset</div>
        <div className="flex flex-wrap gap-2 mb-4">
          {features.map(f => (
            <button key={f.id} onClick={() => toggle(f.id)} className={`px-3 py-1.5 text-xs rounded-lg font-mono transition-all ${selected.includes(f.id) ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300 hover:bg-gray-600'}`}>{f.name}</button>
          ))}
        </div>
        <div className="flex items-center gap-4 mb-4">
          <label className="text-sm text-gray-400">Training window (days):</label>
          {[30, 90, 180, 365].map(d => (
            <button key={d} onClick={() => setWindow(d)} className={`px-3 py-1 text-sm rounded ${window === d ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300'}`}>{d}d</button>
          ))}
        </div>
        <button onClick={generate} disabled={loading || selected.length === 0} className="px-4 py-2 bg-green-600 text-white rounded-lg text-sm hover:bg-green-700 disabled:opacity-50">
          {loading ? 'Generating...' : `Generate Dataset (${selected.length} features)`}
        </button>
      </div>
      {result && (
        <div className="space-y-4">
          <div className="bg-gray-800 rounded-xl border border-green-700/40 p-4">
            <div className="text-sm font-semibold text-green-400 mb-3">Dataset Generated — Point-In-Time Correct</div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              {[['Rows', result.dataset.rows.toLocaleString()], ['Features', result.dataset.features.length], ['Window', `${result.dataset.training_window_days}d`], ['Export', result.dataset.export_formats.join(', ')]].map(([k, v]) => (
                <div key={k} className="bg-gray-700/50 rounded p-2 text-center">
                  <div className="text-gray-500 text-xs">{k}</div>
                  <div className="text-white font-semibold text-sm">{String(v)}</div>
                </div>
              ))}
            </div>
          </div>
          <div className="bg-red-900/10 border border-red-700/40 rounded-xl p-4">
            <div className="text-sm font-semibold text-red-400 mb-2">Data Leakage Warning — What We Prevent</div>
            <div className="text-xs text-gray-300 mb-2">{result.point_in_time_explanation}</div>
            <div className="space-y-2">
              <div className="bg-red-900/20 rounded p-2">
                <div className="text-xs text-red-400 font-semibold">WRONG (Naive JOIN):</div>
                <pre className="text-xs font-mono text-gray-300">{result.leakage_example.naive_join}</pre>
                <div className="text-xs text-red-300 mt-1">{result.leakage_example.problem}</div>
              </div>
              <div className="bg-green-900/20 rounded p-2">
                <div className="text-xs text-green-400 font-semibold">CORRECT (As-of JOIN):</div>
                <pre className="text-xs font-mono text-gray-300">{result.leakage_example.correct_approach}</pre>
              </div>
            </div>
          </div>
          <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
            <div className="text-sm font-semibold text-white mb-3">Feature Statistics</div>
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead><tr className="text-gray-500 border-b border-gray-700">{['Feature', 'Mean', 'Std', 'Null %', 'Min', 'Max'].map(h => <th key={h} className="text-left py-1 pr-3">{h}</th>)}</tr></thead>
                <tbody>
                  {Object.entries(result.feature_statistics).map(([name, stats]: any) => (
                    <tr key={name} className="border-b border-gray-800">
                      <td className="py-1.5 pr-3 font-mono text-blue-300">{name}</td>
                      <td className="py-1.5 pr-3 text-gray-300">{stats.mean}</td>
                      <td className="py-1.5 pr-3 text-gray-300">{stats.std}</td>
                      <td className={`py-1.5 pr-3 ${stats.null_pct > 3 ? 'text-red-400' : 'text-gray-300'}`}>{stats.null_pct}%</td>
                      <td className="py-1.5 pr-3 text-gray-300">{stats.min}</td>
                      <td className="py-1.5 text-gray-300">{stats.max}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function FeatureMonitoring() {
  const [monitoring, setMonitoring] = useState<any[]>([])
  const [pipeline, setPipeline] = useState<any>(null)

  useEffect(() => {
    api.get(ep.monitoring).then(r => setMonitoring(r.data.monitoring))
    api.get(ep.pipeline).then(r => setPipeline(r.data))
  }, [])

  const statusColor: Record<string, string> = { healthy: 'text-green-400', stale: 'text-red-400', drift_detected: 'text-yellow-400' }

  return (
    <div className="space-y-6">
      <div className="text-sm font-semibold text-white mb-3">Feature Health Dashboard</div>
      <div className="space-y-2">
        {monitoring.map(m => (
          <div key={m.feature_id} className={`p-3 rounded-lg border ${m.status === 'healthy' ? 'border-gray-700 bg-gray-800' : m.status === 'stale' ? 'border-red-700/50 bg-red-900/10' : 'border-yellow-700/50 bg-yellow-900/10'}`}>
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <span className={`text-sm font-mono font-semibold ${statusColor[m.status]}`}>{m.feature_name}</span>
                <span className={`text-xs ${statusColor[m.status]}`}>{m.status}</span>
              </div>
              <div className="flex gap-4 text-xs">
                <span className="text-gray-400">Drift: <span className={m.drift_detected ? 'text-yellow-400' : 'text-gray-300'}>{m.drift_score}</span></span>
                <span className="text-gray-400">Age: <span className={m.freshness_hours_ago > 24 ? 'text-red-400' : 'text-gray-300'}>{m.freshness_hours_ago}h ago</span></span>
                <span className="text-gray-400">Nulls: <span className="text-gray-300">{m.null_pct_today}%</span></span>
              </div>
            </div>
          </div>
        ))}
      </div>
      {pipeline && (
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <div className="text-sm font-semibold text-white mb-3">ML Pipeline — Where DE Responsibility Ends</div>
          <div className="flex overflow-x-auto gap-0 pb-2">
            {pipeline.stages.map((s: any, i: number) => (
              <div key={s.id} className="flex items-center flex-shrink-0">
                <div className="w-36 p-3 bg-gray-700 rounded-lg text-center">
                  <div className="text-xs font-semibold text-white mb-1">{s.name}</div>
                  <div className="text-xs text-gray-400">{s.description}</div>
                  <div className={`text-xs mt-1 font-medium ${s.owner.includes('data-engineering') ? 'text-blue-400' : 'text-purple-400'}`}>{s.owner}</div>
                </div>
                {i < pipeline.stages.length - 1 && <span className="text-gray-600 px-1">→</span>}
              </div>
            ))}
          </div>
          <div className="mt-3 p-2 bg-blue-900/20 rounded text-xs text-blue-300">{pipeline.de_boundary}</div>
        </div>
      )}
    </div>
  )
}

const SECTIONS = ['Feature Registry', 'Dataset Generator', 'Feature Monitoring']

export default function FeatureStore() {
  const [activeSection, setActiveSection] = useState(0)
  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-800 flex-shrink-0">
        <div className="flex items-center gap-3 mb-1">
          <span className="text-xs font-mono text-gray-500 bg-gray-800 px-2 py-0.5 rounded">MODULE 17</span>
          <h1 className="text-xl font-bold text-white">Feature Store & MLOps Data Layer</h1>
        </div>
        <p className="text-sm text-gray-400">Feast/Tecton-style feature registry, point-in-time correct training datasets, and feature drift monitoring</p>
      </div>
      <div className="flex gap-2 px-6 py-3 border-b border-gray-800 flex-shrink-0">
        {SECTIONS.map((s, i) => <SectionTab key={s} label={s} active={activeSection === i} onClick={() => setActiveSection(i)} />)}
      </div>
      <div className="flex-1 overflow-y-auto p-6">
        {activeSection === 0 && <FeatureRegistry />}
        {activeSection === 1 && <DatasetGenerator />}
        {activeSection === 2 && <FeatureMonitoring />}
        <div className="mt-8 bg-blue-900/20 border border-blue-700/30 rounded-xl p-5">
          <div className="text-sm font-semibold text-blue-400 mb-2">Training-Serving Skew — The Most Dangerous ML Bug</div>
          <div className="text-sm text-gray-300 space-y-2">
            <p><strong className="text-white">Training-serving skew</strong> happens when the feature values used at training time differ from those available at serving time. The model learns patterns that don't exist in production. Accuracy metrics look great in evaluation but fail in production.</p>
            <p><strong className="text-white">How feature stores prevent it:</strong> Both training and serving retrieve features from the same registry using the same computation logic. The offline store (S3/Parquet) stores historical values for training. The online store (Redis) stores current values for serving. The computation is identical — only the time horizon differs.</p>
            <p><strong className="text-white">Data Engineer's role:</strong> Own stages 1-3 (raw data → feature engineering → feature store). The ML engineer plugs into the feature store API and never touches raw data directly.</p>
          </div>
        </div>
      </div>
    </div>
  )
}
