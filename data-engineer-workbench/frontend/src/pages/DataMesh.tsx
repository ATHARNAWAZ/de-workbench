import { useState, useEffect } from 'react'
import { api } from '../utils/api'

const ep = {
  domains: '/api/datamesh/domains',
  products: '/api/datamesh/products',
  principles: '/api/datamesh/principles',
  subscribe: '/api/datamesh/subscribe',
  apiBuilder: '/api/datamesh/api-builder',
}

function SectionTab({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return <button onClick={onClick} className={`px-4 py-2 text-sm font-medium rounded-lg transition-all ${active ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white hover:bg-gray-700'}`}>{label}</button>
}

function ArchitectureVisualizer() {
  const [domains, setDomains] = useState<any[]>([])
  const [products, setProducts] = useState<any[]>([])

  useEffect(() => {
    api.get(ep.domains).then(r => setDomains(r.data.domains))
    api.get(ep.products).then(r => setProducts(r.data.products))
  }, [])

  const domainColors = ['border-blue-500/40 bg-blue-900/10', 'border-green-500/40 bg-green-900/10', 'border-purple-500/40 bg-purple-900/10', 'border-orange-500/40 bg-orange-900/10']

  return (
    <div className="space-y-6">
      <div className="text-sm font-semibold text-white mb-3">Federated Data Mesh — Domain Architecture</div>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {domains.map((d, i) => (
          <div key={d.id} className={`rounded-xl border p-4 ${domainColors[i % domainColors.length]}`}>
            <div className="text-sm font-bold text-white mb-1">{d.name}</div>
            <div className="text-xs text-gray-400 mb-3">{d.description}</div>
            <div className="text-xs text-gray-500">Team: {d.team} ({d.team_size})</div>
            <div className="text-xs text-gray-500 mt-1">{d.product_count} data products</div>
            <div className="mt-3 space-y-1">
              {d.data_products.map((p: string) => {
                const product = products.find(x => x.id === p)
                return product ? (
                  <div key={p} className="text-xs bg-gray-800/60 rounded px-2 py-1 text-gray-300">📦 {product.name}</div>
                ) : null
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function DataProductRegistry() {
  const [products, setProducts] = useState<any[]>([])
  const [selected, setSelected] = useState<any>(null)
  const [subscribeResult, setSubscribeResult] = useState<any>(null)

  useEffect(() => { api.get(ep.products).then(r => setProducts(r.data.products)) }, [])

  const subscribe = async (productId: string) => {
    const res = await api.post(ep.subscribe, { product_id: productId, consumer_name: 'my-pipeline', use_case: 'Analytics' })
    setSubscribeResult(res.data)
  }

  const qualityColor = (score: number) => score >= 99 ? 'text-green-400' : score >= 95 ? 'text-yellow-400' : 'text-red-400'

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <div className="space-y-2">
        {products.map(p => (
          <div key={p.id} onClick={() => { setSelected(p); setSubscribeResult(null) }} className={`p-3 rounded-lg border cursor-pointer transition-all ${selected?.id === p.id ? 'border-blue-500 bg-blue-900/20' : 'border-gray-700 bg-gray-800 hover:border-gray-600'}`}>
            <div className="flex items-center justify-between">
              <span className="text-sm font-semibold text-white">{p.name}</span>
              <span className={`text-xs font-bold ${qualityColor(p.quality_score)}`}>{p.quality_score}%</span>
            </div>
            <div className="text-xs text-gray-400 mt-1">{p.domain} · v{p.version}</div>
            <div className="flex gap-1 mt-1 flex-wrap">
              {p.output_ports.map((port: any, i: number) => (
                <span key={i} className="text-xs px-1.5 py-0.5 bg-gray-700 rounded text-gray-400">{port.type}</span>
              ))}
            </div>
          </div>
        ))}
      </div>
      <div className="lg:col-span-2">
        {selected ? (
          <div className="bg-gray-800 rounded-xl border border-gray-700 p-4 space-y-4">
            <div className="flex items-start justify-between">
              <div>
                <div className="text-lg font-bold text-white">{selected.name}</div>
                <div className="text-sm text-gray-400">{selected.description}</div>
              </div>
              <button onClick={() => subscribe(selected.id)} className="px-3 py-1.5 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">Subscribe</button>
            </div>
            {subscribeResult && (
              <div className={`p-3 rounded-lg text-sm ${subscribeResult.status === 'approved' ? 'bg-green-900/20 border border-green-700 text-green-400' : 'bg-yellow-900/20 border border-yellow-700 text-yellow-400'}`}>
                {subscribeResult.message}
              </div>
            )}
            <div className="grid grid-cols-2 gap-2">
              {[['Owner', selected.owner], ['SLA Freshness', `${selected.sla_freshness_hours}h`], ['SLA Quality', `${selected.sla_quality_pct}%`], ['Consumers', selected.consumers?.length || 0], ['Version', selected.version], ['Status', selected.status]].map(([k, v]) => (
                <div key={k} className="bg-gray-700/50 rounded p-2 text-xs">
                  <div className="text-gray-500">{k}</div>
                  <div className="text-white font-medium">{String(v)}</div>
                </div>
              ))}
            </div>
            <div>
              <div className="text-xs font-semibold text-white mb-2">Output Ports</div>
              {selected.output_ports?.map((port: any, i: number) => (
                <div key={i} className="flex items-center gap-3 p-2 bg-gray-700/50 rounded mb-1 text-xs">
                  <span className="px-1.5 py-0.5 bg-blue-500/20 text-blue-400 rounded">{port.type}</span>
                  <span className="font-mono text-gray-300">{port.address}</span>
                  <span className="text-gray-500">{port.format}</span>
                  <span className="text-yellow-400 ml-auto">SLA: {port.sla_freshness_hours}h</span>
                </div>
              ))}
            </div>
            {selected.schema?.length > 0 && (
              <div>
                <div className="text-xs font-semibold text-white mb-2">Schema ({selected.schema.length} fields)</div>
                <div className="space-y-1">
                  {selected.schema.slice(0, 6).map((f: any) => (
                    <div key={f.name} className="flex gap-2 text-xs">
                      <span className="font-mono text-blue-300 w-40">{f.name}</span>
                      <span className="text-gray-400">{f.type}</span>
                      {f.description && <span className="text-gray-500">— {f.description}</span>}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        ) : <div className="flex items-center justify-center h-40 text-gray-500">Select a data product</div>}
      </div>
    </div>
  )
}

function PrinciplesExplorer() {
  const [principles, setPrinciples] = useState<any[]>([])
  const [active, setActive] = useState(0)

  useEffect(() => { api.get(ep.principles).then(r => setPrinciples(r.data.principles)) }, [])

  if (!principles.length) return <div className="text-gray-500 py-8 text-center">Loading...</div>

  const p = principles[active]

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        {principles.map((p, i) => (
          <button key={p.id} onClick={() => setActive(i)} className={`flex-1 px-3 py-2 text-sm rounded-lg transition-all ${active === i ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-300 hover:bg-gray-700'}`}>
            {p.id}. {p.name}
          </button>
        ))}
      </div>
      <div className="bg-gray-800 rounded-xl border border-blue-700/30 p-5">
        <div className="text-lg font-bold text-blue-400 mb-1">{p.id}. {p.name}</div>
        <div className="text-sm text-gray-300 mb-4">{p.description}</div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="bg-red-900/10 border border-red-700/40 rounded-lg p-3">
            <div className="text-xs font-semibold text-red-400 mb-1">TRADITIONAL PROBLEM</div>
            <div className="text-sm text-gray-300">{p.traditional_problem}</div>
          </div>
          <div className="bg-green-900/10 border border-green-700/40 rounded-lg p-3">
            <div className="text-xs font-semibold text-green-400 mb-1">MESH SOLUTION</div>
            <div className="text-sm text-gray-300">{p.mesh_solution}</div>
          </div>
          <div className="bg-blue-900/10 border border-blue-700/40 rounded-lg p-3">
            <div className="text-xs font-semibold text-blue-400 mb-1">HOW TO APPLY</div>
            <ul className="space-y-1">
              {p.how_to_apply.map((h: string, i: number) => <li key={i} className="text-sm text-gray-300">• {h}</li>)}
            </ul>
          </div>
        </div>
      </div>
    </div>
  )
}

function APIBuilder() {
  const [form, setForm] = useState({ table: 'gold.daily_revenue', endpoint_path: '/v1/revenue', filters: ['date', 'region'], auth: 'bearer_token', rate_limit_per_min: 100 })
  const [result, setResult] = useState<any>(null)
  const [filterInput, setFilterInput] = useState('date, region')

  const build = async () => {
    const filters = filterInput.split(',').map(s => s.trim()).filter(Boolean)
    const res = await api.post(ep.apiBuilder, { ...form, filters })
    setResult(res.data)
  }

  return (
    <div className="space-y-4">
      <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
        <div className="text-sm font-semibold text-white mb-3">Design a Data API Over Your Warehouse</div>
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="text-xs text-gray-400 mb-1 block">Source Table</label>
            <input value={form.table} onChange={e => setForm(p => ({ ...p, table: e.target.value }))} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white font-mono" />
          </div>
          <div>
            <label className="text-xs text-gray-400 mb-1 block">Endpoint Path</label>
            <input value={form.endpoint_path} onChange={e => setForm(p => ({ ...p, endpoint_path: e.target.value }))} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white font-mono" />
          </div>
          <div>
            <label className="text-xs text-gray-400 mb-1 block">Filter Columns (comma-separated)</label>
            <input value={filterInput} onChange={e => setFilterInput(e.target.value)} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white" />
          </div>
          <div className="grid grid-cols-2 gap-2">
            <div>
              <label className="text-xs text-gray-400 mb-1 block">Auth</label>
              <select value={form.auth} onChange={e => setForm(p => ({ ...p, auth: e.target.value }))} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white">
                {['bearer_token', 'api_key', 'oauth2', 'none'].map(a => <option key={a}>{a}</option>)}
              </select>
            </div>
            <div>
              <label className="text-xs text-gray-400 mb-1 block">Rate Limit/min</label>
              <input type="number" value={form.rate_limit_per_min} onChange={e => setForm(p => ({ ...p, rate_limit_per_min: Number(e.target.value) }))} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white" />
            </div>
          </div>
        </div>
        <button onClick={build} className="mt-3 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">Generate API Code</button>
      </div>
      {result && (
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <div className="text-sm font-semibold text-white mb-2">Generated FastAPI Code</div>
          <pre className="text-xs text-green-300 font-mono bg-gray-900 rounded p-3 overflow-x-auto whitespace-pre max-h-80">{result.code}</pre>
        </div>
      )}
    </div>
  )
}

const SECTIONS = ['Architecture', 'Product Registry', '4 Principles', 'API Builder']

export default function DataMesh() {
  const [activeSection, setActiveSection] = useState(0)
  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-800 flex-shrink-0">
        <div className="flex items-center gap-3 mb-1">
          <span className="text-xs font-mono text-gray-500 bg-gray-800 px-2 py-0.5 rounded">MODULE 21</span>
          <h1 className="text-xl font-bold text-white">Data Mesh & Data Products</h1>
        </div>
        <p className="text-sm text-gray-400">Federated domain ownership, data-as-a-product, self-serve infrastructure, and federated governance</p>
      </div>
      <div className="flex gap-2 px-6 py-3 border-b border-gray-800 flex-shrink-0">
        {SECTIONS.map((s, i) => <SectionTab key={s} label={s} active={activeSection === i} onClick={() => setActiveSection(i)} />)}
      </div>
      <div className="flex-1 overflow-y-auto p-6">
        {activeSection === 0 && <ArchitectureVisualizer />}
        {activeSection === 1 && <DataProductRegistry />}
        {activeSection === 2 && <PrinciplesExplorer />}
        {activeSection === 3 && <APIBuilder />}
        <div className="mt-8 bg-blue-900/20 border border-blue-700/30 rounded-xl p-5">
          <div className="text-sm font-semibold text-blue-400 mb-2">Data Mesh is an Operating Model, Not a Technology</div>
          <div className="text-sm text-gray-300 space-y-2">
            <p><strong className="text-white">When to use Data Mesh:</strong> Large organizations (&gt;50 engineers) with multiple business domains, where a central data team has become a bottleneck. Not appropriate for startups or small data teams.</p>
            <p><strong className="text-white">The hardest part is organizational:</strong> Domain teams need to accept accountability for data quality and freshness. This requires incentive alignment — if the Orders team doesn't own orders data quality, nothing changes.</p>
            <p><strong className="text-white">Federated governance:</strong> Platform enforces global rules (PII, security). Domains own domain rules. Without this balance, you get either chaos (no governance) or a bottleneck (central governance team reviews everything).</p>
          </div>
        </div>
      </div>
    </div>
  )
}
