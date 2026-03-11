import { useState, useEffect } from 'react'
import { api } from '../utils/api'

const ep = {
  scenarios: '/api/incidents/scenarios',
  trigger: '/api/incidents/trigger',
  runbooks: '/api/incidents/runbooks',
  runbook: (id: string) => `/api/incidents/runbooks/${id}`,
  postmortem: '/api/incidents/postmortem',
  oncall: '/api/incidents/oncall',
}

function SectionTab({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return <button onClick={onClick} className={`px-4 py-2 text-sm font-medium rounded-lg transition-all ${active ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white hover:bg-gray-700'}`}>{label}</button>
}

function SeverityBadge({ severity }: { severity: string }) {
  const map: Record<string, string> = { P0: 'bg-red-600 text-white', P1: 'bg-red-500/20 text-red-400', P2: 'bg-yellow-500/20 text-yellow-400', P3: 'bg-gray-500/20 text-gray-400' }
  return <span className={`px-2 py-0.5 rounded text-xs font-bold ${map[severity] || map.P3}`}>{severity}</span>
}

function IncidentSimulator() {
  const [scenarios, setScenarios] = useState<any[]>([])
  const [active, setActive] = useState<any>(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => { api.get(ep.scenarios).then(r => setScenarios(r.data.scenarios)) }, [])

  const trigger = async (scenarioId?: string) => {
    setLoading(true)
    try {
      const url = scenarioId ? `${ep.trigger}?scenario_id=${scenarioId}` : ep.trigger
      const res = await api.post(url)
      setActive(res.data.incident)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="text-sm text-gray-400">10 realistic incident scenarios. Each shows alert, investigation, root cause, and prevention.</div>
        <button onClick={() => trigger()} disabled={loading} className="px-4 py-2 bg-red-600 text-white rounded-lg text-sm hover:bg-red-700 disabled:opacity-50">{loading ? 'Triggering...' : 'Random Incident'}</button>
      </div>
      <div className="grid grid-cols-2 md:grid-cols-5 gap-2">
        {scenarios.map(s => (
          <button key={s.id} onClick={() => trigger(s.id)} className="p-2 bg-gray-800 border border-gray-700 rounded-lg text-xs text-left hover:border-gray-600 transition-all">
            <SeverityBadge severity={s.severity} />
            <div className="text-gray-300 mt-1 leading-tight">{s.title.slice(0, 40)}...</div>
          </button>
        ))}
      </div>
      {active && (
        <div className="bg-gray-800 rounded-xl border border-red-700/40 overflow-hidden">
          <div className="bg-red-900/20 px-4 py-3 flex items-center gap-3 border-b border-red-700/30">
            <SeverityBadge severity={active.severity} />
            <div className="font-semibold text-white">{active.title}</div>
            <span className="text-xs text-gray-400 ml-auto">{active.category}</span>
          </div>
          <div className="p-4 space-y-4">
            <div>
              <div className="text-xs font-semibold text-red-400 mb-1">ALERT</div>
              <div className="text-sm text-gray-300 font-mono bg-red-900/10 rounded p-2">{active.alert}</div>
            </div>
            <div>
              <div className="text-xs font-semibold text-yellow-400 mb-1">SYMPTOMS</div>
              {active.symptoms.map((s: string, i: number) => <div key={i} className="text-sm text-gray-300">• {s}</div>)}
            </div>
            <div>
              <div className="text-xs font-semibold text-blue-400 mb-1">INVESTIGATION STEPS</div>
              {active.investigation_steps.map((s: string, i: number) => (
                <div key={i} className="text-sm text-gray-300 mb-1 flex gap-2">
                  <span className="text-blue-400 flex-shrink-0">{i + 1}.</span>
                  <pre className="font-mono text-xs text-gray-200 whitespace-pre-wrap">{s}</pre>
                </div>
              ))}
            </div>
            <div>
              <div className="text-xs font-semibold text-orange-400 mb-1">ROOT CAUSE</div>
              <div className="text-sm text-gray-300 bg-orange-900/10 rounded p-2">{active.root_cause}</div>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <div className="text-xs font-semibold text-green-400 mb-1">RESOLUTION</div>
                {active.resolution.map((s: string, i: number) => <div key={i} className="text-sm text-gray-300">• {s}</div>)}
              </div>
              <div>
                <div className="text-xs font-semibold text-purple-400 mb-1">PREVENTION</div>
                {active.prevention.map((s: string, i: number) => <div key={i} className="text-sm text-gray-300">• {s}</div>)}
              </div>
            </div>
            <div className="flex gap-4 text-xs text-gray-500">
              <span>Detection delay: <span className="text-yellow-400">{active.detection_delay_min} min</span></span>
              <span>Resolution time: <span className="text-green-400">{active.resolution_time_min} min</span></span>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function RunbookViewer() {
  const [runbooks, setRunbooks] = useState<any[]>([])
  const [selected, setSelected] = useState<any>(null)

  useEffect(() => { api.get(ep.runbooks).then(r => setRunbooks(r.data.runbooks)) }, [])

  const load = async (id: string) => {
    const res = await api.get(ep.runbook(id))
    setSelected(res.data)
  }

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <div className="space-y-2">
        {runbooks.map(r => (
          <div key={r.id} onClick={() => load(r.id)} className={`p-3 rounded-lg border cursor-pointer transition-all ${selected?.id === r.id ? 'border-blue-500 bg-blue-900/20' : 'border-gray-700 bg-gray-800 hover:border-gray-600'}`}>
            <div className="text-sm font-semibold text-white">{r.title}</div>
            <div className="text-xs text-gray-400 mt-1">{r.category}</div>
            <div className="text-xs text-gray-500 mt-1">{r.overview}</div>
          </div>
        ))}
      </div>
      <div className="lg:col-span-2">
        {selected ? (
          <div className="bg-gray-800 rounded-xl border border-gray-700 p-4 space-y-4">
            <div className="text-lg font-bold text-white">{selected.title}</div>
            <div>
              <div className="text-xs font-semibold text-red-400 mb-1">ALERT CONDITIONS</div>
              {selected.alert_conditions?.map((a: string, i: number) => <div key={i} className="text-sm text-gray-300 font-mono text-xs mb-1">{a}</div>)}
            </div>
            <div>
              <div className="text-xs font-semibold text-blue-400 mb-2">INVESTIGATION STEPS</div>
              {selected.investigation_steps?.map((s: any) => (
                <div key={s.step} className="mb-3 p-3 bg-gray-700/50 rounded-lg">
                  <div className="text-sm font-semibold text-white mb-1">Step {s.step}: {s.description}</div>
                  {s.command && <pre className="text-xs text-green-300 font-mono bg-gray-900 rounded p-2 mb-1">{s.command}</pre>}
                  {s.expected && <div className="text-xs text-gray-400">Expected: {s.expected}</div>}
                </div>
              ))}
            </div>
            <div>
              <div className="text-xs font-semibold text-green-400 mb-2">RESOLUTION STEPS</div>
              {selected.resolution_steps?.map((s: any) => (
                <div key={s.step} className="mb-2 text-sm text-gray-300">
                  <span className="text-green-400">{s.step}.</span> {s.description}
                </div>
              ))}
            </div>
            <div>
              <div className="text-xs font-semibold text-orange-400 mb-2">ESCALATION PATH</div>
              <div className="flex gap-2 flex-wrap">
                {selected.escalation?.map((e: any) => (
                  <div key={e.level} className="bg-gray-700 rounded p-2 text-xs">
                    <div className="font-bold text-orange-400">{e.level}</div>
                    <div className="text-gray-300">{e.contact}</div>
                    <div className="text-gray-500">After {e.timeout_min}min</div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        ) : <div className="flex items-center justify-center h-40 text-gray-500">Select a runbook</div>}
      </div>
    </div>
  )
}

function PostmortemGenerator() {
  const [form, setForm] = useState({ incident_title: '', timeline: '', root_cause: '', impact: '', contributing_factors: [''], action_items: [''] })
  const [result, setResult] = useState<any>(null)

  const updateList = (field: 'contributing_factors' | 'action_items', i: number, val: string) => {
    setForm(p => { const arr = [...p[field]]; arr[i] = val; return { ...p, [field]: arr } })
  }
  const addItem = (field: 'contributing_factors' | 'action_items') => setForm(p => ({ ...p, [field]: [...p[field], ''] }))

  const generate = async () => {
    const res = await api.post(ep.postmortem, { ...form, contributing_factors: form.contributing_factors.filter(Boolean), action_items: form.action_items.filter(Boolean) })
    setResult(res.data)
  }

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <input value={form.incident_title} onChange={e => setForm(p => ({ ...p, incident_title: e.target.value }))} placeholder="Incident title" className="col-span-2 bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white" />
        <textarea value={form.timeline} onChange={e => setForm(p => ({ ...p, timeline: e.target.value }))} placeholder="Timeline: 14:00 - Alert fired. 14:15 - On-call acknowledged. 14:45 - Root cause identified..." rows={3} className="bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white" />
        <textarea value={form.root_cause} onChange={e => setForm(p => ({ ...p, root_cause: e.target.value }))} placeholder="Root cause (be specific)" rows={3} className="bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white" />
        <textarea value={form.impact} onChange={e => setForm(p => ({ ...p, impact: e.target.value }))} placeholder="Business impact: N users affected, X hours of stale data..." rows={2} className="bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white" />
      </div>
      <div className="grid grid-cols-2 gap-4">
        <div>
          <div className="text-xs font-semibold text-gray-400 mb-2">Contributing Factors</div>
          {form.contributing_factors.map((f, i) => (
            <input key={i} value={f} onChange={e => updateList('contributing_factors', i, e.target.value)} placeholder={`Factor ${i + 1}`} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-1.5 text-sm text-white mb-1" />
          ))}
          <button onClick={() => addItem('contributing_factors')} className="text-xs text-blue-400">+ Add factor</button>
        </div>
        <div>
          <div className="text-xs font-semibold text-gray-400 mb-2">Action Items</div>
          {form.action_items.map((a, i) => (
            <input key={i} value={a} onChange={e => updateList('action_items', i, e.target.value)} placeholder={`Action ${i + 1}`} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-1.5 text-sm text-white mb-1" />
          ))}
          <button onClick={() => addItem('action_items')} className="text-xs text-blue-400">+ Add action</button>
        </div>
      </div>
      <button onClick={generate} className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">Generate Postmortem</button>
      {result && (
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <div className="text-sm font-semibold text-white mb-3">5-Why Analysis</div>
          <div className="space-y-2 mb-4">
            {result.five_whys.map((w: string, i: number) => (
              <div key={i} className="flex gap-2 text-sm"><span className="text-blue-400 flex-shrink-0">{i + 1}.</span><span className="text-gray-300">{w}</span></div>
            ))}
          </div>
          <div className="text-xs text-gray-400 mb-2">Generated postmortem document:</div>
          <pre className="text-xs text-gray-300 whitespace-pre-wrap font-mono bg-gray-900 rounded p-3 overflow-y-auto max-h-80">{result.postmortem_markdown}</pre>
        </div>
      )}
    </div>
  )
}

function OnCallSchedule() {
  const [data, setData] = useState<any>(null)

  useEffect(() => { api.get(ep.oncall).then(r => setData(r.data)) }, [])

  if (!data) return <div className="text-gray-500 py-8 text-center">Loading...</div>

  return (
    <div className="space-y-6">
      <div className="bg-blue-900/20 border border-blue-700/40 rounded-xl p-4">
        <div className="text-sm font-semibold text-blue-400 mb-2">Current On-Call</div>
        <div className="grid grid-cols-3 gap-3">
          {[['Primary', data.current_week.primary, 'text-green-400'], ['Secondary', data.current_week.secondary, 'text-blue-400'], ['Escalation', data.current_week.escalation, 'text-orange-400']].map(([role, person, color]) => (
            <div key={String(role)} className="bg-gray-800 rounded-lg p-3 text-center">
              <div className={`text-xs font-semibold ${color} mb-1`}>{role}</div>
              <div className="text-sm text-white">{person}</div>
            </div>
          ))}
        </div>
      </div>
      <div>
        <div className="text-sm font-semibold text-white mb-3">Alert Routing Rules</div>
        <div className="space-y-2">
          {data.alert_routing.map((r: any, i: number) => (
            <div key={i} className="flex items-start gap-3 p-3 bg-gray-800 rounded-lg border border-gray-700 text-sm">
              <span className="text-gray-400 flex-shrink-0 w-40">{r.alert}</span>
              <span className="text-gray-300 flex-1">{r.routing}</span>
              <span className="text-yellow-400 text-xs">Escalate after {r.escalation_timeout_min}min</span>
            </div>
          ))}
        </div>
      </div>
      <div>
        <div className="text-sm font-semibold text-white mb-3">Rotation Schedule</div>
        <div className="space-y-2">
          {data.schedule.map((w: any) => (
            <div key={w.week} className={`p-3 rounded-lg border text-sm ${w.week === data.current_week.week ? 'border-blue-500 bg-blue-900/20' : 'border-gray-700 bg-gray-800'}`}>
              <div className="flex items-center justify-between">
                <span className="font-mono text-blue-300">{w.week}</span>
                {w.week === data.current_week.week && <span className="text-xs text-blue-400">CURRENT</span>}
              </div>
              <div className="text-gray-400 text-xs mt-1">Primary: {w.primary} · Secondary: {w.secondary}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

const SECTIONS = ['Incident Simulator', 'Runbooks', 'Postmortem Generator', 'On-Call']

export default function IncidentsPage() {
  const [activeSection, setActiveSection] = useState(0)
  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-800 flex-shrink-0">
        <div className="flex items-center gap-3 mb-1">
          <span className="text-xs font-mono text-gray-500 bg-gray-800 px-2 py-0.5 rounded">MODULE 20</span>
          <h1 className="text-xl font-bold text-white">Incident Management & On-Call</h1>
        </div>
        <p className="text-sm text-gray-400">Simulate real data incidents, navigate runbooks, generate blameless postmortems, and manage on-call rotations</p>
      </div>
      <div className="flex gap-2 px-6 py-3 border-b border-gray-800 flex-shrink-0">
        {SECTIONS.map((s, i) => <SectionTab key={s} label={s} active={activeSection === i} onClick={() => setActiveSection(i)} />)}
      </div>
      <div className="flex-1 overflow-y-auto p-6">
        {activeSection === 0 && <IncidentSimulator />}
        {activeSection === 1 && <RunbookViewer />}
        {activeSection === 2 && <PostmortemGenerator />}
        {activeSection === 3 && <OnCallSchedule />}
        <div className="mt-8 bg-blue-900/20 border border-blue-700/30 rounded-xl p-5">
          <div className="text-sm font-semibold text-blue-400 mb-2">On-Call Culture in Data Engineering</div>
          <div className="text-sm text-gray-300 space-y-2">
            <p><strong className="text-white">Blameless postmortems:</strong> The goal is to improve systems, not assign blame. Humans make mistakes in complex systems — the question is why the system allowed the mistake to propagate to production.</p>
            <p><strong className="text-white">Runbooks at 3am:</strong> A good runbook is written for someone who has never seen this system before, woken up at 3am. Every investigation step should include the exact command to run, not just "check the logs."</p>
            <p><strong className="text-white">SLI/SLO/SLA:</strong> SLI = the metric (data freshness). SLO = your internal target (99.5% of hours, data is less than 2h old). SLA = the contractual commitment to customers (often looser than your SLO to give engineering buffer).</p>
          </div>
        </div>
      </div>
    </div>
  )
}
