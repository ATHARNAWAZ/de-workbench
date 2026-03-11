import { useState, useEffect } from 'react'
import { api } from '../utils/api'

const ep = {
  pipelines: '/api/reversetl/pipelines',
  destinations: '/api/reversetl/destinations',
  tables: '/api/reversetl/tables',
  sync: '/api/reversetl/sync',
  log: '/api/reversetl/sync-log',
  transforms: '/api/reversetl/field-transforms',
}

function SectionTab({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return <button onClick={onClick} className={`px-4 py-2 text-sm font-medium rounded-lg transition-all ${active ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white hover:bg-gray-700'}`}>{label}</button>
}

function PipelineList() {
  const [pipelines, setPipelines] = useState<any[]>([])
  const [syncing, setSyncing] = useState<string | null>(null)
  const [syncResults, setSyncResults] = useState<Record<string, any>>({})

  useEffect(() => {
    api.get(ep.pipelines).then(r => setPipelines(r.data.pipelines))
  }, [])

  const runSync = async (id: string) => {
    setSyncing(id)
    try {
      const res = await api.post(ep.sync, { pipeline_id: id })
      setSyncResults(p => ({ ...p, [id]: res.data.sync_result }))
      setPipelines(p => p.map(pl => pl.id === id ? res.data.pipeline : pl))
    } finally {
      setSyncing(null)
    }
  }

  const syncModeColor: Record<string, string> = { incremental: 'text-blue-400', mirror: 'text-purple-400', full: 'text-orange-400' }

  return (
    <div className="space-y-4">
      {pipelines.map(p => (
        <div key={p.id} className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <div className="flex items-start justify-between mb-3">
            <div>
              <div className="font-semibold text-white">{p.name}</div>
              <div className="text-xs text-gray-400">{p.source_table}{p.source_filter ? ` WHERE ${p.source_filter}` : ''} → {p.destination}</div>
            </div>
            <div className="flex items-center gap-3">
              <span className={`text-xs font-medium ${syncModeColor[p.sync_mode] || 'text-gray-400'}`}>{p.sync_mode}</span>
              <span className="text-xs text-gray-500">{p.schedule}</span>
              <button onClick={() => runSync(p.id)} disabled={syncing === p.id} className="px-3 py-1.5 bg-blue-600 text-white rounded-lg text-xs hover:bg-blue-700 disabled:opacity-50">{syncing === p.id ? 'Syncing...' : 'Run Sync'}</button>
            </div>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-2 mb-3">
            {[['Records Synced', p.records_synced?.toLocaleString()], ['Last Sync', p.last_sync ? new Date(p.last_sync).toLocaleTimeString() : 'Never'], ['Errors', p.errors], ['Status', p.status]].map(([k, v]) => (
              <div key={k} className="bg-gray-700/50 rounded p-2 text-xs">
                <div className="text-gray-500">{k}</div>
                <div className={`font-semibold ${k === 'Errors' && Number(v) > 0 ? 'text-red-400' : 'text-white'}`}>{String(v)}</div>
              </div>
            ))}
          </div>
          <div className="text-xs font-semibold text-gray-400 mb-2">Field Mappings</div>
          <div className="flex flex-wrap gap-2">
            {p.field_mappings.map((m: any, i: number) => (
              <div key={i} className="flex items-center gap-1 text-xs bg-gray-700 rounded px-2 py-1">
                <span className="text-blue-300 font-mono">{m.source}</span>
                <span className="text-gray-500">→</span>
                <span className="text-green-300 font-mono">{m.destination}</span>
                {m.transform && <span className="text-yellow-400 ml-1">({m.transform})</span>}
              </div>
            ))}
          </div>
          {syncResults[p.id] && (
            <div className={`mt-3 p-2 rounded text-xs ${syncResults[p.id].status === 'success' ? 'bg-green-900/20 text-green-400' : 'bg-yellow-900/20 text-yellow-400'}`}>
              Sync complete: +{syncResults[p.id].records_added} added, ~{syncResults[p.id].records_updated} updated, {syncResults[p.id].errors} errors
            </div>
          )}
        </div>
      ))}
    </div>
  )
}

function PipelineBuilder() {
  const [destinations, setDestinations] = useState<any[]>([])
  const [tables, setTables] = useState<any[]>([])
  const [transforms, setTransforms] = useState<any[]>([])
  const [form, setForm] = useState({ name: '', source_table: '', source_filter: '', destination: '', sync_mode: 'incremental', schedule: 'hourly' })
  const [mappings, setMappings] = useState([{ source: '', destination: '', transform: '' }])
  const [created, setCreated] = useState<any>(null)

  useEffect(() => {
    api.get(ep.destinations).then(r => setDestinations(r.data.destinations))
    api.get(ep.tables).then(r => setTables(r.data.tables))
    api.get(ep.transforms).then(r => setTransforms(r.data.transforms))
  }, [])

  const selectedTable = tables.find(t => t.id === form.source_table)

  const addMapping = () => setMappings(p => [...p, { source: '', destination: '', transform: '' }])
  const updateMapping = (i: number, field: string, val: string) => setMappings(p => p.map((m, idx) => idx === i ? { ...m, [field]: val } : m))

  const create = async () => {
    const res = await api.post(ep.pipelines, { ...form, field_mappings: mappings.filter(m => m.source && m.destination) })
    setCreated(res.data)
  }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="space-y-4">
          <div className="text-sm font-semibold text-white">Pipeline Configuration</div>
          <input value={form.name} onChange={e => setForm(p => ({ ...p, name: e.target.value }))} placeholder="Pipeline name" className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white" />
          <div>
            <label className="text-xs text-gray-400 mb-1 block">Source Table</label>
            <select value={form.source_table} onChange={e => setForm(p => ({ ...p, source_table: e.target.value }))} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white">
              <option value="">Select table</option>
              {tables.map(t => <option key={t.id} value={t.id}>{t.name} ({t.rows.toLocaleString()} rows)</option>)}
            </select>
          </div>
          <input value={form.source_filter} onChange={e => setForm(p => ({ ...p, source_filter: e.target.value }))} placeholder="Filter (e.g. ltv_score > 0.8)" className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white" />
          <div>
            <label className="text-xs text-gray-400 mb-1 block">Destination</label>
            <select value={form.destination} onChange={e => setForm(p => ({ ...p, destination: e.target.value }))} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white">
              <option value="">Select destination</option>
              {destinations.map(d => <option key={d.id} value={d.id}>{d.name} ({d.type})</option>)}
            </select>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-xs text-gray-400 mb-1 block">Sync Mode</label>
              <select value={form.sync_mode} onChange={e => setForm(p => ({ ...p, sync_mode: e.target.value }))} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white">
                {['full', 'incremental', 'mirror'].map(m => <option key={m}>{m}</option>)}
              </select>
            </div>
            <div>
              <label className="text-xs text-gray-400 mb-1 block">Schedule</label>
              <select value={form.schedule} onChange={e => setForm(p => ({ ...p, schedule: e.target.value }))} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white">
                {['real-time', 'hourly', 'daily', 'weekly'].map(s => <option key={s}>{s}</option>)}
              </select>
            </div>
          </div>
        </div>
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <div className="text-sm font-semibold text-white">Field Mappings</div>
            <button onClick={addMapping} className="text-xs text-blue-400 hover:text-blue-300">+ Add mapping</button>
          </div>
          {selectedTable && <div className="text-xs text-gray-400">Available columns: {selectedTable.columns.join(', ')}</div>}
          {mappings.map((m, i) => (
            <div key={i} className="flex gap-2 items-center">
              <input value={m.source} onChange={e => updateMapping(i, 'source', e.target.value)} placeholder="Source column" className="flex-1 bg-gray-700 border border-gray-600 rounded px-2 py-1.5 text-xs text-white" />
              <span className="text-gray-500">→</span>
              <input value={m.destination} onChange={e => updateMapping(i, 'destination', e.target.value)} placeholder="Destination field" className="flex-1 bg-gray-700 border border-gray-600 rounded px-2 py-1.5 text-xs text-white" />
              <select value={m.transform} onChange={e => updateMapping(i, 'transform', e.target.value)} className="bg-gray-700 border border-gray-600 rounded px-2 py-1.5 text-xs text-white">
                <option value="">No transform</option>
                {transforms.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
              </select>
            </div>
          ))}
          <button onClick={create} className="w-full py-2 bg-green-600 text-white rounded-lg text-sm hover:bg-green-700">Create Pipeline</button>
        </div>
      </div>
      {created && (
        <div className="bg-green-900/20 border border-green-700 rounded-xl p-4">
          <div className="text-green-400 font-semibold mb-1">Pipeline Created: {created.name}</div>
          <div className="text-sm text-gray-300">ID: {created.id} · Status: {created.status} · Schedule: {created.schedule}</div>
        </div>
      )}
    </div>
  )
}

function SyncLog() {
  const [logs, setLogs] = useState<any[]>([])

  useEffect(() => {
    api.get(ep.log).then(r => setLogs(r.data.logs))
  }, [])

  return (
    <div className="space-y-2">
      {logs.map(l => (
        <div key={l.id} className={`p-3 rounded-lg border text-sm ${l.status === 'success' ? 'border-green-700/40 bg-green-900/10' : 'border-yellow-700/40 bg-yellow-900/10'}`}>
          <div className="flex items-center justify-between mb-1">
            <span className="font-semibold text-white">{l.pipeline_id}</span>
            <span className={`text-xs px-2 py-0.5 rounded ${l.status === 'success' ? 'bg-green-500/20 text-green-400' : 'bg-yellow-500/20 text-yellow-400'}`}>{l.status}</span>
          </div>
          <div className="text-xs text-gray-400">{l.started_at} → {l.finished_at}</div>
          <div className="flex gap-4 text-xs mt-1">
            <span className="text-green-400">+{l.records_added} added</span>
            <span className="text-blue-400">~{l.records_updated} updated</span>
            {l.errors > 0 && <span className="text-red-400">{l.errors} errors</span>}
          </div>
        </div>
      ))}
    </div>
  )
}

const SECTIONS = ['Active Pipelines', 'Build Pipeline', 'Sync Log']

export default function ReverseETL() {
  const [activeSection, setActiveSection] = useState(0)
  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-800 flex-shrink-0">
        <div className="flex items-center gap-3 mb-1">
          <span className="text-xs font-mono text-gray-500 bg-gray-800 px-2 py-0.5 rounded">MODULE 16</span>
          <h1 className="text-xl font-bold text-white">Reverse ETL & Data Activation</h1>
        </div>
        <p className="text-sm text-gray-400">Sync warehouse insights back to operational tools — Salesforce, HubSpot, Slack, and REST APIs</p>
      </div>
      <div className="flex gap-2 px-6 py-3 border-b border-gray-800 flex-shrink-0">
        {SECTIONS.map((s, i) => <SectionTab key={s} label={s} active={activeSection === i} onClick={() => setActiveSection(i)} />)}
      </div>
      <div className="flex-1 overflow-y-auto p-6">
        {activeSection === 0 && <PipelineList />}
        {activeSection === 1 && <PipelineBuilder />}
        {activeSection === 2 && <SyncLog />}
        <div className="mt-8 bg-blue-900/20 border border-blue-700/30 rounded-xl p-5">
          <div className="text-sm font-semibold text-blue-400 mb-2">Operational Analytics — The Composable CDP Pattern</div>
          <div className="text-sm text-gray-300 space-y-2">
            <p><strong className="text-white">What is Reverse ETL?</strong> Traditional ETL moves data into the warehouse. Reverse ETL moves warehouse insights back to operational systems where business teams work (CRM, marketing tools, support platforms).</p>
            <p><strong className="text-white">Use cases:</strong> Sync churn predictions to Salesforce so sales reps see risk scores on customer records. Push LTV-weighted segments to HubSpot for campaign targeting. Activate ML model outputs in real business workflows.</p>
            <p><strong className="text-white">Sync modes matter:</strong> Incremental only syncs new/changed rows (efficient). Mirror also syncs deletions (dangerous — test carefully). Full sync is safest for small tables but expensive at scale.</p>
          </div>
        </div>
      </div>
    </div>
  )
}
