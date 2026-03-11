import { useState } from 'react'
import { api, endpoints } from '../utils/api'

const ep = {
  stages: '/api/cicd/pipeline-stages',
  runStage: '/api/cicd/run-stage',
  testSuites: '/api/cicd/test-suites',
  runTests: '/api/cicd/run-tests',
  branching: '/api/cicd/branching-strategy',
  generateYaml: '/api/cicd/generate-yaml',
}

function SectionTab({ id, label, active, onClick }: { id: string; label: string; active: boolean; onClick: () => void }) {
  return (
    <button onClick={onClick} className={`px-4 py-2 text-sm font-medium rounded-lg transition-all ${active ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white hover:bg-gray-700'}`}>
      {label}
    </button>
  )
}

function StatusBadge({ status }: { status: string }) {
  const map: Record<string, string> = { passed: 'bg-green-500/20 text-green-400', failed: 'bg-red-500/20 text-red-400', pending: 'bg-gray-500/20 text-gray-400', running: 'bg-yellow-500/20 text-yellow-400' }
  return <span className={`px-2 py-0.5 rounded text-xs font-medium ${map[status] || map.pending}`}>{status.toUpperCase()}</span>
}

function PipelineVisualizer() {
  const [stages, setStages] = useState<any[]>([])
  const [stageResults, setStageResults] = useState<Record<string, any>>({})
  const [running, setRunning] = useState<string | null>(null)

  const load = async () => {
    const res = await api.get(ep.stages)
    setStages(res.data.stages)
  }

  const runStage = async (stageId: string, fail = false) => {
    setRunning(stageId)
    try {
      const res = await api.post(ep.runStage, { stage_id: stageId, force_fail: fail })
      setStageResults(p => ({ ...p, [stageId]: res.data }))
    } finally {
      setRunning(null)
    }
  }

  if (!stages.length) return (
    <div className="text-center py-12">
      <button onClick={load} className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700">Load CI/CD Pipeline Stages</button>
    </div>
  )

  return (
    <div className="space-y-4">
      <p className="text-sm text-gray-400">Click each stage to simulate running it. Watch logs appear in real time.</p>
      <div className="flex items-start gap-2 overflow-x-auto pb-2">
        {stages.map((stage, i) => {
          const result = stageResults[stage.id]
          const status = result ? result.status : 'pending'
          return (
            <div key={stage.id} className="flex items-center gap-2 flex-shrink-0">
              <div className={`w-40 bg-gray-800 border rounded-lg p-3 transition-all ${status === 'passed' ? 'border-green-600' : status === 'failed' ? 'border-red-600' : 'border-gray-700'}`}>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-xs font-semibold text-white">{stage.name}</span>
                  <StatusBadge status={status} />
                </div>
                <p className="text-xs text-gray-500 mb-2">{stage.duration_s}s</p>
                <div className="flex gap-1">
                  <button onClick={() => runStage(stage.id)} disabled={running === stage.id} className="flex-1 text-xs px-2 py-1 bg-blue-600/20 text-blue-400 rounded hover:bg-blue-600/40 disabled:opacity-50">Run</button>
                  <button onClick={() => runStage(stage.id, true)} disabled={running === stage.id} className="flex-1 text-xs px-2 py-1 bg-red-600/20 text-red-400 rounded hover:bg-red-600/40 disabled:opacity-50">Fail</button>
                </div>
              </div>
              {i < stages.length - 1 && <span className="text-gray-600 text-lg mt-4">→</span>}
            </div>
          )
        })}
      </div>
      {Object.entries(stageResults).map(([stageId, result]) => (
        <div key={stageId} className={`rounded-lg border p-3 ${result.status === 'passed' ? 'border-green-700 bg-green-900/10' : 'border-red-700 bg-red-900/10'}`}>
          <div className="text-sm font-semibold text-white mb-2">{stageId} — {result.status.toUpperCase()} ({result.duration_s}s)</div>
          <pre className="text-xs text-gray-300 font-mono whitespace-pre-wrap">{result.logs?.join('\n')}</pre>
        </div>
      ))}
    </div>
  )
}

function TestSuiteBuilder() {
  const [suites, setSuites] = useState<any>(null)
  const [activesSuite, setActiveSuite] = useState('unit')
  const [results, setResults] = useState<any>(null)
  const [running, setRunning] = useState(false)

  const loadSuites = async () => {
    const res = await api.get(ep.testSuites)
    setSuites(res.data.suites)
  }

  const runTests = async (suite: string) => {
    setRunning(true)
    setResults(null)
    try {
      const res = await api.post(ep.runTests, { suite })
      setResults(res.data)
    } finally {
      setRunning(false)
    }
  }

  if (!suites) return (
    <div className="text-center py-12">
      <button onClick={loadSuites} className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700">Load Test Suites</button>
    </div>
  )

  const SUITE_TABS = ['unit', 'integration', 'contract', 'regression']
  const suite = suites[activesSuite]

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        {SUITE_TABS.map(s => (
          <button key={s} onClick={() => { setActiveSuite(s); setResults(null) }} className={`px-3 py-1.5 text-sm rounded-lg ${activesSuite === s ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300 hover:bg-gray-600'}`}>{s}</button>
        ))}
      </div>
      {suite && (
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <div className="flex items-center justify-between mb-3">
            <div>
              <div className="font-semibold text-white">{suite.name}</div>
              <div className="text-sm text-gray-400">{suite.description}</div>
            </div>
            <button onClick={() => runTests(activesSuite)} disabled={running} className="px-4 py-2 bg-green-600 text-white rounded-lg text-sm hover:bg-green-700 disabled:opacity-50">
              {running ? 'Running...' : 'Run All Tests'}
            </button>
          </div>
          <div className="space-y-2">
            {suite.tests.map((t: any) => {
              const r = results?.results?.find((x: any) => x.id === t.id)
              return (
                <div key={t.id} className={`flex items-start gap-3 p-2 rounded border ${r?.status === 'passed' ? 'border-green-700 bg-green-900/10' : r?.status === 'failed' ? 'border-red-700 bg-red-900/10' : 'border-gray-700'}`}>
                  <StatusBadge status={r?.status || 'pending'} />
                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-mono text-white">{t.name}</div>
                    <div className="text-xs text-gray-400">{r?.error || t.description}</div>
                  </div>
                  {r && <span className="text-xs text-gray-500">{r.duration_ms}ms</span>}
                </div>
              )
            })}
          </div>
          {results && (
            <div className={`mt-3 p-3 rounded-lg text-sm ${results.failed === 0 ? 'bg-green-900/20 border border-green-700 text-green-400' : 'bg-red-900/20 border border-red-700 text-red-400'}`}>
              {results.passed}/{results.total} passed · Coverage: {results.coverage} · Duration: {results.duration_ms}ms
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function BranchingVisualizer() {
  const [data, setData] = useState<any>(null)

  const load = async () => {
    const res = await api.get(ep.branching)
    setData(res.data)
  }

  if (!data) return (
    <div className="text-center py-12">
      <button onClick={load} className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700">Load Branching Strategy</button>
    </div>
  )

  const branchColors: Record<string, string> = { feature: 'border-purple-500 bg-purple-500/10', dev: 'border-blue-500 bg-blue-500/10', staging: 'border-yellow-500 bg-yellow-500/10', production: 'border-green-500 bg-green-500/10' }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {data.branches.map((b: any) => (
          <div key={b.name} className={`rounded-lg border p-4 ${branchColors[b.type] || 'border-gray-700'}`}>
            <div className="font-mono text-sm font-bold text-white mb-1">{b.name}</div>
            <div className="text-xs text-gray-400 mb-2">{b.description || `${b.commits} commits ahead of dev`}</div>
            {b.protection && <div className="text-xs text-yellow-400">Protected: {b.protection}</div>}
          </div>
        ))}
      </div>
      <div>
        <div className="text-sm font-semibold text-white mb-3">Environment Config per Branch</div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          {Object.entries(data.env_configs).map(([env, cfg]: any) => (
            <div key={env} className="bg-gray-800 rounded-lg border border-gray-700 p-3">
              <div className="text-xs font-bold text-blue-400 uppercase mb-2">{env}</div>
              {Object.entries(cfg).map(([k, v]: any) => (
                <div key={k} className="flex justify-between text-xs mb-1">
                  <span className="text-gray-500">{k}</span>
                  <span className="text-gray-200 font-mono">{String(v)}</span>
                </div>
              ))}
            </div>
          ))}
        </div>
      </div>
      <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
        <div className="text-sm font-semibold text-white mb-2">Blue/Green Deployment</div>
        <div className="text-sm text-gray-400 mb-3">{data.blue_green.description}</div>
        <ol className="space-y-1">
          {data.blue_green.steps.map((step: string, i: number) => (
            <li key={i} className="text-sm text-gray-300 flex gap-2"><span className="text-blue-400 font-mono">{i + 1}.</span>{step}</li>
          ))}
        </ol>
      </div>
    </div>
  )
}

function YamlGenerator() {
  const [steps, setSteps] = useState<string[]>(['lint', 'unit', 'integration'])
  const [dbt, setDbt] = useState(false)
  const [ge, setGe] = useState(true)
  const [python, setPython] = useState('3.11')
  const [yaml, setYaml] = useState('')

  const toggle = (s: string) => setSteps(p => p.includes(s) ? p.filter(x => x !== s) : [...p, s])

  const generate = async () => {
    const res = await api.post(ep.generateYaml, { steps, use_dbt: dbt, use_great_expectations: ge, python_version: python, trigger_branch: 'main' })
    setYaml(res.data.yaml)
  }

  return (
    <div className="space-y-4">
      <div className="bg-gray-800 rounded-lg border border-gray-700 p-4 space-y-3">
        <div className="text-sm font-semibold text-white">Configure CI Steps</div>
        <div className="flex flex-wrap gap-3">
          {['lint', 'unit', 'integration', 'quality_gate'].map(s => (
            <label key={s} className="flex items-center gap-2 cursor-pointer">
              <input type="checkbox" checked={steps.includes(s)} onChange={() => toggle(s)} className="rounded" />
              <span className="text-sm text-gray-300 capitalize">{s.replace('_', ' ')}</span>
            </label>
          ))}
          <label className="flex items-center gap-2 cursor-pointer">
            <input type="checkbox" checked={dbt} onChange={e => setDbt(e.target.checked)} className="rounded" />
            <span className="text-sm text-gray-300">dbt tests</span>
          </label>
          <label className="flex items-center gap-2 cursor-pointer">
            <input type="checkbox" checked={ge} onChange={e => setGe(e.target.checked)} className="rounded" />
            <span className="text-sm text-gray-300">Great Expectations</span>
          </label>
        </div>
        <div className="flex items-center gap-3">
          <span className="text-sm text-gray-400">Python version:</span>
          {['3.10', '3.11', '3.12'].map(v => (
            <button key={v} onClick={() => setPython(v)} className={`px-3 py-1 text-sm rounded ${python === v ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300'}`}>{v}</button>
          ))}
        </div>
        <button onClick={generate} className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">Generate YAML</button>
      </div>
      {yaml && (
        <div className="bg-gray-900 rounded-lg border border-gray-700 p-4">
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs text-gray-500 font-mono">.github/workflows/pipeline.yml</span>
          </div>
          <pre className="text-xs text-green-300 font-mono whitespace-pre overflow-x-auto max-h-96">{yaml}</pre>
        </div>
      )}
    </div>
  )
}

const SECTIONS = ['Pipeline Visualizer', 'Test Suite Builder', 'Branching Strategy', 'YAML Generator']

export default function CICD() {
  const [activeSection, setActiveSection] = useState(0)

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-800 flex-shrink-0">
        <div className="flex items-center gap-3 mb-1">
          <span className="text-xs font-mono text-gray-500 bg-gray-800 px-2 py-0.5 rounded">MODULE 13</span>
          <h1 className="text-xl font-bold text-white">CI/CD for Data Pipelines</h1>
        </div>
        <p className="text-sm text-gray-400">Automated testing, branching strategies, and GitHub Actions workflows for data pipelines</p>
      </div>
      <div className="flex gap-2 px-6 py-3 border-b border-gray-800 flex-shrink-0">
        {SECTIONS.map((s, i) => <SectionTab key={s} id={String(i)} label={s} active={activeSection === i} onClick={() => setActiveSection(i)} />)}
      </div>
      <div className="flex-1 overflow-y-auto p-6">
        {activeSection === 0 && <PipelineVisualizer />}
        {activeSection === 1 && <TestSuiteBuilder />}
        {activeSection === 2 && <BranchingVisualizer />}
        {activeSection === 3 && <YamlGenerator />}
        <div className="mt-8 bg-blue-900/20 border border-blue-700/30 rounded-xl p-5">
          <div className="text-sm font-semibold text-blue-400 mb-2">Why CI/CD for Data is Harder Than for Apps</div>
          <div className="text-sm text-gray-300 space-y-2">
            <p>Software CI/CD validates behavior. Data CI/CD must also validate <em>correctness of data</em> — a test can pass and yet produce wrong aggregations.</p>
            <p><strong className="text-white">Testing hierarchy:</strong> Unit tests (mock data → fast, cheap) → Integration tests (real DB, 1K rows → slower) → Data contract tests (schema validation → critical) → Regression tests (golden dataset comparison → most thorough).</p>
            <p><strong className="text-white">Senior engineer pattern:</strong> Never deploy to production without a data quality gate — at minimum, row count variance check and schema contract validation. Blue/green deployments catch incorrect business logic that unit tests miss.</p>
          </div>
        </div>
      </div>
    </div>
  )
}
