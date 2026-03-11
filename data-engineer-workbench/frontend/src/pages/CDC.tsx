import { useState, useEffect } from 'react'
import { api } from '../utils/api'

const ep = {
  connectors: '/api/cdc/connectors',
  events: '/api/cdc/events',
  simulate: '/api/cdc/simulate',
  comparison: '/api/cdc/comparison',
  wal: '/api/cdc/wal-explainer',
}

function SectionTab({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return <button onClick={onClick} className={`px-4 py-2 text-sm font-medium rounded-lg transition-all ${active ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white hover:bg-gray-700'}`}>{label}</button>
}

function StatusBadge({ status }: { status: string }) {
  const map: Record<string, string> = { RUNNING: 'bg-green-500/20 text-green-400', DEGRADED: 'bg-yellow-500/20 text-yellow-400', STOPPED: 'bg-gray-500/20 text-gray-400', STARTING: 'bg-blue-500/20 text-blue-400' }
  return <span className={`px-2 py-0.5 rounded text-xs font-medium ${map[status] || map.STOPPED}`}>{status}</span>
}

function OpBadge({ op }: { op: string }) {
  const map: Record<string, string> = { INSERT: 'bg-green-500/20 text-green-400', UPDATE: 'bg-blue-500/20 text-blue-400', DELETE: 'bg-red-500/20 text-red-400', 'READ (snapshot)': 'bg-purple-500/20 text-purple-400' }
  return <span className={`px-2 py-0.5 rounded text-xs font-bold ${map[op] || 'bg-gray-500/20 text-gray-400'}`}>{op}</span>
}

function ConnectorManager() {
  const [connectors, setConnectors] = useState<any[]>([])
  const [form, setForm] = useState({ name: 'postgres-orders', type: 'debezium-postgres', host: 'prod-postgres.internal', port: 5432, database: 'orders_db', table_whitelist: 'public.orders', snapshot_mode: 'initial' })
  const [showForm, setShowForm] = useState(false)

  useEffect(() => {
    api.get(ep.connectors).then(r => setConnectors(r.data.connectors))
  }, [])

  const create = async () => {
    const res = await api.post(ep.connectors, form)
    setConnectors(p => [...p, res.data])
    setShowForm(false)
  }

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <div className="text-sm text-gray-400">{connectors.length} connectors active</div>
        <button onClick={() => setShowForm(!showForm)} className="px-3 py-1.5 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">+ New Connector</button>
      </div>
      {showForm && (
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4 space-y-3">
          <div className="text-sm font-semibold text-white">Configure Debezium Connector</div>
          <div className="grid grid-cols-2 gap-3">
            {[['name', 'Connector Name'], ['host', 'Source Host'], ['database', 'Database'], ['table_whitelist', 'Table Whitelist']].map(([k, label]) => (
              <div key={k}>
                <label className="text-xs text-gray-400 mb-1 block">{label}</label>
                <input value={(form as any)[k]} onChange={e => setForm(p => ({ ...p, [k]: e.target.value }))} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white" />
              </div>
            ))}
            <div>
              <label className="text-xs text-gray-400 mb-1 block">Connector Type</label>
              <select value={form.type} onChange={e => setForm(p => ({ ...p, type: e.target.value }))} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white">
                {['debezium-postgres', 'debezium-mysql', 'debezium-oracle', 'debezium-sqlserver'].map(t => <option key={t}>{t}</option>)}
              </select>
            </div>
            <div>
              <label className="text-xs text-gray-400 mb-1 block">Snapshot Mode</label>
              <select value={form.snapshot_mode} onChange={e => setForm(p => ({ ...p, snapshot_mode: e.target.value }))} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white">
                {['initial', 'initial_only', 'schema_only', 'never'].map(m => <option key={m}>{m}</option>)}
              </select>
            </div>
          </div>
          <div className="flex gap-2">
            <button onClick={create} className="px-4 py-2 bg-green-600 text-white rounded-lg text-sm hover:bg-green-700">Create Connector</button>
            <button onClick={() => setShowForm(false)} className="px-4 py-2 bg-gray-700 text-gray-300 rounded-lg text-sm hover:bg-gray-600">Cancel</button>
          </div>
        </div>
      )}
      <div className="space-y-3">
        {connectors.map(c => (
          <div key={c.id} className="bg-gray-800 rounded-xl border border-gray-700 p-4">
            <div className="flex items-center justify-between mb-3">
              <div>
                <div className="font-semibold text-white">{c.name}</div>
                <div className="text-xs text-gray-400">{c.type} · {c.host}:{c.port} · {c.database}</div>
              </div>
              <StatusBadge status={c.status} />
            </div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
              {[['Tables', c.table_whitelist], ['Events/sec', c.events_per_second], ['Lag', `${c.lag_ms}ms`], ['Errors', c.errors]].map(([k, v]) => (
                <div key={k} className="bg-gray-700/50 rounded p-2">
                  <div className="text-gray-500">{k}</div>
                  <div className={`font-semibold ${k === 'Errors' && Number(v) > 0 ? 'text-red-400' : k === 'Lag' && Number(String(v).replace('ms','')) > 1000 ? 'text-yellow-400' : 'text-white'}`}>{String(v)}</div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function EventFeed() {
  const [events, setEvents] = useState<any[]>([])
  const [simulateForm, setSimulateForm] = useState({ operation: 'INSERT', table: 'orders', record_id: 'ORD-9999' })
  const [simResult, setSimResult] = useState<any>(null)

  useEffect(() => {
    api.get(ep.events).then(r => setEvents(r.data.events))
  }, [])

  const simulate = async () => {
    const res = await api.post(ep.simulate, simulateForm)
    setSimResult(res.data)
    setEvents(p => [res.data.event, ...p].slice(0, 10))
  }

  return (
    <div className="space-y-4">
      <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
        <div className="text-sm font-semibold text-white mb-3">Simulate CDC Operation</div>
        <div className="flex gap-3">
          <select value={simulateForm.operation} onChange={e => setSimulateForm(p => ({ ...p, operation: e.target.value }))} className="bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white">
            {['INSERT', 'UPDATE', 'DELETE'].map(o => <option key={o}>{o}</option>)}
          </select>
          <input value={simulateForm.record_id} onChange={e => setSimulateForm(p => ({ ...p, record_id: e.target.value }))} className="flex-1 bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white" placeholder="Record ID" />
          <button onClick={simulate} className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">Trigger</button>
        </div>
        {simResult && (
          <div className="mt-3 p-3 bg-gray-900 rounded-lg border border-gray-700">
            <div className="text-xs text-gray-400 mb-1">Kafka Topic: <span className="text-blue-400">{simResult.kafka_topic}</span> · {simResult.serialization}</div>
            <div className="text-xs text-yellow-400 mb-1">Latency: {simResult.event.latency_ms}ms</div>
            <pre className="text-xs text-green-300 font-mono overflow-x-auto">{JSON.stringify(simResult.event, null, 2)}</pre>
          </div>
        )}
      </div>
      <div className="text-sm font-semibold text-white">Live CDC Event Stream</div>
      <div className="space-y-2 max-h-96 overflow-y-auto">
        {events.map((e, i) => (
          <div key={i} className="bg-gray-800 rounded-lg border border-gray-700 p-3">
            <div className="flex items-center gap-3 mb-2">
              <OpBadge op={e.op_name} />
              <span className="text-xs font-mono text-gray-400">{e.topic}</span>
              <span className="text-xs text-gray-500">LSN: {e.lsn}</span>
            </div>
            <div className="grid grid-cols-2 gap-2 text-xs">
              {e.before && <div><div className="text-gray-500 mb-1">BEFORE</div><pre className="text-red-300 font-mono overflow-hidden">{JSON.stringify(e.before, null, 2).slice(0, 150)}</pre></div>}
              {e.after && <div><div className="text-gray-500 mb-1">AFTER</div><pre className="text-green-300 font-mono overflow-hidden">{JSON.stringify(e.after, null, 2).slice(0, 150)}</pre></div>}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function ComparisonView() {
  const [data, setData] = useState<any>(null)
  const [wal, setWal] = useState<any>(null)

  useEffect(() => {
    api.get(ep.comparison).then(r => setData(r.data))
    api.get(ep.wal).then(r => setWal(r.data))
  }, [])

  if (!data) return <div className="text-gray-500 py-8 text-center">Loading...</div>

  const { cdc, polling } = data.comparison
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {[{ label: 'CDC (Debezium)', data: cdc, color: 'green' }, { label: 'Polling', data: polling, color: 'orange' }].map(({ label, data: d, color }) => (
          <div key={label} className={`bg-gray-800 rounded-xl border ${color === 'green' ? 'border-green-700/40' : 'border-orange-700/40'} p-4`}>
            <div className={`text-sm font-bold mb-3 ${color === 'green' ? 'text-green-400' : 'text-orange-400'}`}>{label}</div>
            <div className="space-y-2 text-sm">
              {[['Latency', d.latency_ms + 'ms'], ['Source DB Load', d.source_db_load], ['Data Loss Risk', d.data_loss_risk], ['Setup Complexity', d.setup_complexity], ['Data Captured', d.data_captured]].map(([k, v]) => (
                <div key={k} className="flex gap-2">
                  <span className="text-gray-500 w-32 flex-shrink-0">{k}:</span>
                  <span className="text-gray-200">{v}</span>
                </div>
              ))}
            </div>
            <div className="mt-3">
              <div className="text-xs text-green-400 mb-1">PROS:</div>
              {d.pros.map((p: string, i: number) => <div key={i} className="text-xs text-gray-300">✓ {p}</div>)}
              <div className="text-xs text-red-400 mt-2 mb-1">CONS:</div>
              {d.cons.map((c: string, i: number) => <div key={i} className="text-xs text-gray-300">✗ {c}</div>)}
            </div>
          </div>
        ))}
      </div>
      {wal && (
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <div className="text-sm font-semibold text-white mb-3">How the WAL / Transaction Log Powers CDC</div>
          <p className="text-sm text-gray-300 mb-4">{wal.explanation}</p>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
            {Object.entries(wal.databases).map(([db, info]: any) => (
              <div key={db} className="bg-gray-700/50 rounded-lg p-3 text-xs">
                <div className="font-semibold text-blue-400 mb-2">{db}</div>
                <div className="text-gray-400">Log: <span className="text-white">{info.log_name}</span></div>
                <div className="text-gray-400 mt-1">Method: <span className="text-gray-200">{info.debezium_method}</span></div>
                <div className="text-gray-400 mt-1">Permissions: <span className="text-yellow-400">{info.permission}</span></div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

const SECTIONS = ['Connector Manager', 'Live Event Feed', 'CDC vs Polling']

export default function CDCPage() {
  const [activeSection, setActiveSection] = useState(0)
  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-800 flex-shrink-0">
        <div className="flex items-center gap-3 mb-1">
          <span className="text-xs font-mono text-gray-500 bg-gray-800 px-2 py-0.5 rounded">MODULE 15</span>
          <h1 className="text-xl font-bold text-white">Change Data Capture (CDC)</h1>
        </div>
        <p className="text-sm text-gray-400">Real-time data sync using Debezium and the database WAL — the gold standard for sub-second data replication</p>
      </div>
      <div className="flex gap-2 px-6 py-3 border-b border-gray-800 flex-shrink-0">
        {SECTIONS.map((s, i) => <SectionTab key={s} label={s} active={activeSection === i} onClick={() => setActiveSection(i)} />)}
      </div>
      <div className="flex-1 overflow-y-auto p-6">
        {activeSection === 0 && <ConnectorManager />}
        {activeSection === 1 && <EventFeed />}
        {activeSection === 2 && <ComparisonView />}
        <div className="mt-8 bg-blue-900/20 border border-blue-700/30 rounded-xl p-5">
          <div className="text-sm font-semibold text-blue-400 mb-2">CDC in Production — Senior Engineer Perspective</div>
          <div className="text-sm text-gray-300 space-y-2">
            <p><strong className="text-white">Why CDC beats polling:</strong> Polling every 5 minutes means 5-minute latency minimum, can't capture DELETEs, puts load on the source DB with table scans. CDC reads the transaction log (WAL/binlog/redo log) with near-zero DB impact and sub-second latency.</p>
            <p><strong className="text-white">LSN (Log Sequence Number)</strong> is your checkpoint. Debezium stores the last processed LSN — on restart, it resumes from exactly that point. No duplicates, no data loss.</p>
            <p><strong className="text-white">Production gotchas:</strong> Schema changes in the source break CDC — enforce schema registry. LSN resets (log rotation) can cause Debezium to lose its position — set pg_wal_keep_size high enough. Monitor consumer lag: if lag grows, your CDC pipeline is falling behind real-time.</p>
          </div>
        </div>
      </div>
    </div>
  )
}
