import { useState, useEffect } from 'react'
import { api } from '../utils/api'

const ep = {
  contracts: '/api/contracts',
  schemas: '/api/contracts/schemas/list',
  registerSchema: '/api/contracts/schemas/register',
  compatibilityCheck: '/api/contracts/compatibility-check',
  compatibilityRules: '/api/contracts/compatibility-rules',
  impact: (id: string) => `/api/contracts/impact/${id}`,
  diff: (id: string) => `/api/contracts/schemas/${id}/diff`,
}

function SectionTab({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return <button onClick={onClick} className={`px-4 py-2 text-sm font-medium rounded-lg transition-all ${active ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white hover:bg-gray-700'}`}>{label}</button>
}

function ContractStudio() {
  const [contracts, setContracts] = useState<any[]>([])
  const [selected, setSelected] = useState<any>(null)

  useEffect(() => {
    api.get(ep.contracts).then(r => setContracts(r.data.contracts))
  }, [])

  const qualityColor = (score: number) => score >= 99 ? 'text-green-400' : score >= 95 ? 'text-yellow-400' : 'text-red-400'

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <div className="space-y-2">
        <div className="text-sm font-semibold text-white mb-3">Active Contracts</div>
        {contracts.map(c => (
          <div key={c.id} onClick={() => setSelected(c)} className={`p-3 rounded-lg border cursor-pointer transition-all ${selected?.id === c.id ? 'border-blue-500 bg-blue-900/20' : 'border-gray-700 bg-gray-800 hover:border-gray-600'}`}>
            <div className="flex items-center justify-between">
              <span className="text-sm font-semibold text-white">{c.name}</span>
              <span className={`text-sm font-bold ${qualityColor(c.quality_score)}`}>{c.quality_score}%</span>
            </div>
            <div className="text-xs text-gray-400 mt-1">{c.dataset} · v{c.version}</div>
            <div className="text-xs text-gray-500">{c.producer} → {c.consumer}</div>
          </div>
        ))}
      </div>
      <div className="lg:col-span-2">
        {selected ? (
          <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
            <div className="flex items-center justify-between mb-4">
              <div>
                <div className="text-lg font-bold text-white">{selected.name}</div>
                <div className="text-sm text-gray-400">v{selected.version} · Owner: {selected.owner}</div>
              </div>
              <div className={`px-3 py-1 rounded-full text-sm font-bold ${qualityColor(selected.quality_score)} bg-gray-700`}>{selected.quality_score}% quality</div>
            </div>
            <div className="grid grid-cols-2 gap-3 mb-4">
              {[['Producer', selected.producer], ['Consumer', selected.consumer], ['Dataset', selected.dataset], ['SLA Freshness', `${selected.sla_freshness_hours}h`], ['SLA Quality', `${selected.sla_quality_pct}%`], ['Status', selected.status]].map(([k, v]) => (
                <div key={k} className="bg-gray-700/50 rounded-lg p-2">
                  <div className="text-xs text-gray-500">{k}</div>
                  <div className="text-sm text-white font-medium">{v}</div>
                </div>
              ))}
            </div>
            <div className="text-sm font-semibold text-white mb-2">Schema Fields ({selected.fields?.length})</div>
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead><tr className="text-gray-500 border-b border-gray-700">{['Field', 'Type', 'Nullable', 'PII', 'Validation'].map(h => <th key={h} className="text-left py-1 pr-3">{h}</th>)}</tr></thead>
                <tbody>
                  {selected.fields?.map((f: any) => (
                    <tr key={f.name} className="border-b border-gray-800 hover:bg-gray-700/30">
                      <td className="py-1.5 pr-3 font-mono text-blue-300">{f.name}</td>
                      <td className="py-1.5 pr-3 text-gray-300">{f.type}</td>
                      <td className="py-1.5 pr-3">{f.nullable ? <span className="text-gray-500">yes</span> : <span className="text-red-400">NO</span>}</td>
                      <td className="py-1.5 pr-3">{f.pii ? <span className="text-orange-400">PII</span> : <span className="text-gray-600">-</span>}</td>
                      <td className="py-1.5 text-gray-400">{f.validation}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ) : (
          <div className="flex items-center justify-center h-64 text-gray-500">Select a contract to view details</div>
        )}
      </div>
    </div>
  )
}

function SchemaRegistry() {
  const [schemas, setSchemas] = useState<any>(null)
  const [selectedSubject, setSelectedSubject] = useState<string | null>(null)
  const [diffResult, setDiffResult] = useState<any>(null)
  const [compatForm, setCompatForm] = useState({ subject: 'orders-value', compatibility_mode: 'BACKWARD', proposed_change: '' })
  const [compatResult, setCompatResult] = useState<any>(null)
  const [rules, setRules] = useState<any>(null)

  useEffect(() => {
    api.get(ep.schemas).then(r => setSchemas(r.data))
    api.get(ep.compatibilityRules).then(r => setRules(r.data.rules))
  }, [])

  const checkDiff = async (schemaId: string) => {
    const res = await api.get(ep.diff(schemaId))
    setDiffResult(res.data)
  }

  const checkCompat = async () => {
    const res = await api.post(ep.compatibilityCheck, compatForm)
    setCompatResult(res.data)
  }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div>
          <div className="text-sm font-semibold text-white mb-3">Registered Schemas by Subject</div>
          {schemas && Object.entries(schemas.subjects).map(([subject, versions]: any) => (
            <div key={subject} className="bg-gray-800 rounded-lg border border-gray-700 p-3 mb-2">
              <div className="flex items-center justify-between mb-2">
                <span className="font-mono text-sm text-blue-300">{subject}</span>
                <span className="text-xs text-gray-500">{versions.length} version(s)</span>
              </div>
              <div className="flex gap-2 flex-wrap">
                {versions.map((v: any) => (
                  <div key={v.version} className="flex gap-1">
                    <span className="px-2 py-0.5 bg-gray-700 rounded text-xs text-gray-300">v{v.version} ({v.format})</span>
                    {v.version > 1 && (
                      <button onClick={() => checkDiff(v.id)} className="px-2 py-0.5 bg-blue-600/20 text-blue-400 rounded text-xs hover:bg-blue-600/40">diff</button>
                    )}
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
        <div>
          <div className="text-sm font-semibold text-white mb-3">Schema Diff Viewer</div>
          {diffResult ? (
            <div className="bg-gray-800 rounded-lg border border-gray-700 p-4 text-sm">
              <div className="text-gray-400 mb-2">v{diffResult.from_version} → v{diffResult.to_version} · Mode: {diffResult.compatibility}</div>
              <div className={`px-3 py-2 rounded-lg mb-3 text-sm font-semibold ${diffResult.compatibility_verdict === 'COMPATIBLE' ? 'bg-green-900/30 text-green-400' : 'bg-red-900/30 text-red-400'}`}>{diffResult.compatibility_verdict}</div>
              {diffResult.changes?.map((c: any, i: number) => (
                <div key={i} className="p-2 bg-gray-700 rounded text-xs mb-1">
                  <span className="text-yellow-400">{c.change_type}</span>
                  <span className="text-white ml-2">{c.field}</span>
                  <span className="text-gray-400 ml-2">— {c.detail}</span>
                </div>
              ))}
            </div>
          ) : <div className="text-gray-500 text-sm">Click "diff" on a schema version above</div>}
        </div>
      </div>
      <div>
        <div className="text-sm font-semibold text-white mb-3">Compatibility Mode Rules</div>
        {rules && (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
            {Object.entries(rules).map(([mode, rule]: any) => (
              <div key={mode} className="bg-gray-800 rounded-lg border border-gray-700 p-3">
                <div className="font-semibold text-blue-400 mb-1">{mode}</div>
                <div className="text-xs text-gray-400 mb-2">{rule.description}</div>
                <div className="text-xs text-green-400 mb-1">ALLOWED:</div>
                {rule.allowed.slice(0, 2).map((a: string, i: number) => <div key={i} className="text-xs text-gray-300 ml-2">✓ {a}</div>)}
                {rule.rejected.length > 0 && <>
                  <div className="text-xs text-red-400 mt-1 mb-1">REJECTED:</div>
                  {rule.rejected.slice(0, 2).map((r: string, i: number) => <div key={i} className="text-xs text-gray-300 ml-2">✗ {r}</div>)}
                </>}
              </div>
            ))}
          </div>
        )}
      </div>
      <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
        <div className="text-sm font-semibold text-white mb-3">Compatibility Checker</div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-3">
          <input value={compatForm.subject} onChange={e => setCompatForm(p => ({ ...p, subject: e.target.value }))} placeholder="Subject" className="bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white" />
          <select value={compatForm.compatibility_mode} onChange={e => setCompatForm(p => ({ ...p, compatibility_mode: e.target.value }))} className="bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white">
            {['BACKWARD', 'FORWARD', 'FULL', 'NONE'].map(m => <option key={m}>{m}</option>)}
          </select>
          <input value={compatForm.proposed_change} onChange={e => setCompatForm(p => ({ ...p, proposed_change: e.target.value }))} placeholder="e.g., remove required field customer_id" className="bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white" />
        </div>
        <button onClick={checkCompat} className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">Check Compatibility</button>
        {compatResult && (
          <div className={`mt-3 p-3 rounded-lg text-sm ${compatResult.breaking ? 'bg-red-900/20 border border-red-700 text-red-400' : 'bg-green-900/20 border border-green-700 text-green-400'}`}>
            <div className="font-semibold mb-1">{compatResult.verdict}</div>
            <div className="text-gray-300">{compatResult.recommendation}</div>
          </div>
        )}
      </div>
    </div>
  )
}

function ImpactAnalyzer() {
  const [schemaId, setSchemaId] = useState('sch-002')
  const [impact, setImpact] = useState<any>(null)

  const analyze = async () => {
    const res = await api.get(ep.impact(schemaId))
    setImpact(res.data)
  }

  const severityColor: Record<string, string> = { HIGH: 'text-red-400', MEDIUM: 'text-yellow-400', LOW: 'text-green-400' }

  return (
    <div className="space-y-4">
      <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
        <div className="text-sm font-semibold text-white mb-3">Breaking Change Impact Analyzer</div>
        <div className="flex gap-3">
          <input value={schemaId} onChange={e => setSchemaId(e.target.value)} placeholder="Schema ID (e.g. sch-002)" className="flex-1 bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white" />
          <button onClick={analyze} className="px-4 py-2 bg-orange-600 text-white rounded-lg text-sm hover:bg-orange-700">Analyze Impact</button>
        </div>
      </div>
      {impact && (
        <div className="space-y-4">
          <div className="bg-gray-800 rounded-xl border border-orange-700/30 p-4">
            <div className="text-sm font-semibold text-white mb-3">Consumers Affected: {impact.affected_consumers.length}</div>
            <div className="space-y-2">
              {impact.affected_consumers.map((c: any, i: number) => (
                <div key={i} className="flex items-start gap-3 p-3 bg-gray-700 rounded-lg">
                  <span className={`text-xs font-bold px-2 py-0.5 rounded ${severityColor[c.impact]} bg-gray-800`}>{c.impact}</span>
                  <div>
                    <div className="text-sm font-semibold text-white">{c.name}</div>
                    <div className="text-xs text-gray-400">{c.team} · {c.action_required}</div>
                  </div>
                </div>
              ))}
            </div>
          </div>
          <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
            <div className="text-sm font-semibold text-white mb-3">Safe Migration Path</div>
            <ol className="space-y-2">
              {impact.migration_path.map((step: string, i: number) => (
                <li key={i} className="flex gap-2 text-sm text-gray-300">
                  <span className="text-blue-400 font-mono flex-shrink-0">{i + 1}.</span>{step}
                </li>
              ))}
            </ol>
          </div>
        </div>
      )}
    </div>
  )
}

const SECTIONS = ['Contract Studio', 'Schema Registry', 'Impact Analyzer']

export default function DataContracts() {
  const [activeSection, setActiveSection] = useState(0)
  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-800 flex-shrink-0">
        <div className="flex items-center gap-3 mb-1">
          <span className="text-xs font-mono text-gray-500 bg-gray-800 px-2 py-0.5 rounded">MODULE 14</span>
          <h1 className="text-xl font-bold text-white">Data Contracts & Schema Registry</h1>
        </div>
        <p className="text-sm text-gray-400">Define producer-consumer agreements, enforce schema evolution rules, and detect breaking changes</p>
      </div>
      <div className="flex gap-2 px-6 py-3 border-b border-gray-800 flex-shrink-0">
        {SECTIONS.map((s, i) => <SectionTab key={s} label={s} active={activeSection === i} onClick={() => setActiveSection(i)} />)}
      </div>
      <div className="flex-1 overflow-y-auto p-6">
        {activeSection === 0 && <ContractStudio />}
        {activeSection === 1 && <SchemaRegistry />}
        {activeSection === 2 && <ImpactAnalyzer />}
        <div className="mt-8 bg-blue-900/20 border border-blue-700/30 rounded-xl p-5">
          <div className="text-sm font-semibold text-blue-400 mb-2">The Data Contract Revolution</div>
          <div className="text-sm text-gray-300 space-y-2">
            <p>A data contract is a formal agreement between a data producer and consumer specifying schema, SLAs, and quality rules. Violations trigger alerts, not broken dashboards discovered days later.</p>
            <p><strong className="text-white">Schema Registry:</strong> Without it, Kafka consumers break silently when producers change message format. The registry enforces BACKWARD/FORWARD/FULL compatibility before any schema change is accepted.</p>
            <p><strong className="text-white">BACKWARD compatibility</strong> (most common): new schema can read data written with old schema. Adding an optional field with a default is always BACKWARD compatible. Removing or renaming a field is BREAKING.</p>
          </div>
        </div>
      </div>
    </div>
  )
}
