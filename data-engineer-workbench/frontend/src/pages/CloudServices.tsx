import { useState, useEffect } from 'react'
import { api } from '../utils/api'

const ep = {
  services: '/api/cloud/services',
  aws: '/api/cloud/aws',
  gcp: '/api/cloud/gcp',
  azure: '/api/cloud/azure',
  costCalc: '/api/cloud/cost-calculator',
}

function SectionTab({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return <button onClick={onClick} className={`px-4 py-2 text-sm font-medium rounded-lg transition-all ${active ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white hover:bg-gray-700'}`}>{label}</button>
}

function ServiceMap() {
  const [services, setServices] = useState<any[]>([])

  useEffect(() => { api.get(ep.services).then(r => setServices(r.data.services)) }, [])

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="border-b border-gray-700">
            <th className="text-left py-2 pr-4 text-gray-400 font-semibold w-40">Function</th>
            <th className="text-left py-2 pr-4 text-orange-400 font-semibold">AWS</th>
            <th className="text-left py-2 pr-4 text-blue-400 font-semibold">Azure</th>
            <th className="text-left py-2 text-green-400 font-semibold">GCP</th>
          </tr>
        </thead>
        <tbody>
          {services.map((s, i) => (
            <tr key={i} className="border-b border-gray-800 hover:bg-gray-800/50">
              <td className="py-3 pr-4 font-semibold text-white">{s.function}</td>
              {['aws', 'azure', 'gcp'].map(cloud => (
                <td key={cloud} className="py-3 pr-4 align-top">
                  <div className="font-medium text-white">{(s as any)[cloud]?.name}</div>
                  <div className="text-xs text-gray-400 mt-0.5">{(s as any)[cloud]?.pricing}</div>
                  <div className="text-xs text-gray-500 mt-0.5 max-w-xs">{(s as any)[cloud]?.notes?.slice(0, 80)}...</div>
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function AWSLab() {
  const [services, setServices] = useState<any[]>([])
  const [selected, setSelected] = useState(0)

  useEffect(() => { api.get(ep.aws).then(r => setServices(r.data.services)) }, [])

  if (!services.length) return <div className="text-gray-500 py-8 text-center">Loading...</div>
  const svc = services[selected]

  return (
    <div className="grid grid-cols-1 lg:grid-cols-4 gap-4">
      <div className="space-y-2">
        {services.map((s, i) => (
          <button key={i} onClick={() => setSelected(i)} className={`w-full text-left p-3 rounded-lg text-sm transition-all ${selected === i ? 'bg-orange-600/20 border border-orange-500/40 text-orange-300' : 'bg-gray-800 border border-gray-700 text-gray-300 hover:bg-gray-700'}`}>
            <div className="font-semibold">{s.name}</div>
            <div className="text-xs text-gray-500">{s.category}</div>
          </button>
        ))}
      </div>
      <div className="lg:col-span-3 bg-gray-800 rounded-xl border border-gray-700 p-4 space-y-4">
        <div>
          <div className="text-lg font-bold text-orange-400">{svc.name}</div>
          <div className="text-sm text-gray-400">{svc.description}</div>
          <div className="text-xs text-gray-500 mt-1">{svc.pricing}</div>
        </div>
        {svc.storage_classes && (
          <div>
            <div className="text-sm font-semibold text-white mb-2">Storage Classes</div>
            <div className="grid grid-cols-2 gap-2">
              {svc.storage_classes.map((c: any) => (
                <div key={c.name} className="bg-gray-700/50 rounded p-3 text-xs">
                  <div className="font-semibold text-white">{c.name}</div>
                  <div className="text-gray-400">${c.cost_gb}/GB/month · {c.retrieval}</div>
                  <div className="text-gray-500">{c.use_case}</div>
                </div>
              ))}
            </div>
          </div>
        )}
        {svc.partitioning_impact && (
          <div>
            <div className="text-sm font-semibold text-white mb-2">Partitioning Impact on Athena Cost</div>
            <div className="grid grid-cols-2 gap-2">
              {[['Without Partitioning', svc.partitioning_impact.without_partition, 'red'], ['With Partitioning', svc.partitioning_impact.with_partition, 'green']].map(([label, d, color]: any) => (
                <div key={label} className={`bg-gray-700/50 rounded p-3 text-xs border ${color === 'green' ? 'border-green-700/40' : 'border-red-700/40'}`}>
                  <div className="font-semibold text-white mb-1">{label}</div>
                  <div className="text-gray-400 font-mono">{d.query}</div>
                  <div className={`mt-1 font-bold ${color === 'green' ? 'text-green-400' : 'text-red-400'}`}>{d.data_scanned_gb}GB scanned = {d.cost}</div>
                </div>
              ))}
            </div>
          </div>
        )}
        {svc.benchmark && (
          <div>
            <div className="text-sm font-semibold text-white mb-2">Query Benchmark</div>
            <pre className="text-xs text-gray-300 bg-gray-900 rounded p-2 mb-2 font-mono">{svc.benchmark.query}</pre>
            <div className="grid grid-cols-2 gap-2">
              {[['Unoptimized', svc.benchmark.without_optimization, 'red'], ['Parquet + Partitions', svc.benchmark.with_parquet_partitioning, 'green']].map(([label, d, color]: any) => (
                <div key={label} className={`rounded p-3 text-xs border ${color === 'green' ? 'bg-green-900/10 border-green-700/40' : 'bg-red-900/10 border-red-700/40'}`}>
                  <div className="font-semibold text-white">{label}</div>
                  <div className="text-gray-400">{d.data_scanned_gb}GB · {d.duration_s}s · {d.cost}</div>
                </div>
              ))}
            </div>
            <div className="text-xs text-green-400 mt-1">{svc.benchmark.improvement}</div>
          </div>
        )}
        {svc.shards && (
          <div>
            <div className="text-sm font-semibold text-white mb-2">Kinesis Shards (Live Simulation)</div>
            <div className="space-y-2">
              {svc.shards.map((sh: any) => (
                <div key={sh.id} className="flex items-center gap-3">
                  <span className="text-xs text-gray-400 w-16">Shard {sh.id}</span>
                  <div className="flex-1 bg-gray-700 rounded-full h-2">
                    <div className="bg-orange-500 h-2 rounded-full" style={{ width: `${(sh.records_per_sec / 1000) * 100}%` }} />
                  </div>
                  <span className="text-xs text-gray-300">{sh.records_per_sec} rec/s</span>
                </div>
              ))}
            </div>
          </div>
        )}
        {svc.glue_catalog && (
          <div className="grid grid-cols-4 gap-2">
            {[['Databases', svc.glue_catalog.databases.length], ['Tables', svc.glue_catalog.total_tables], ['Crawlers Active', svc.glue_catalog.crawlers_active], ['Last Crawl', svc.glue_catalog.last_crawl?.split('T')[0]]].map(([k, v]) => (
              <div key={k} className="bg-gray-700/50 rounded p-2 text-center text-xs">
                <div className="text-white font-bold text-base">{String(v)}</div>
                <div className="text-gray-500">{k}</div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function CostCalculator() {
  const [cloud, setCloud] = useState('aws')
  const [form, setForm] = useState({ storage_gb: 500, queries_per_day: 200, query_avg_gb_scanned: 5, spark_hours_per_day: 4, streaming_events_per_day: 1000000 })
  const [result, setResult] = useState<any>(null)

  const calculate = async () => {
    const res = await api.post(ep.costCalc, { ...form, cloud })
    setResult(res.data)
  }

  return (
    <div className="space-y-6">
      <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
        <div className="text-sm font-semibold text-white mb-4">Cloud Cost Calculator</div>
        <div className="flex gap-2 mb-4">
          {['aws', 'gcp', 'azure'].map(c => (
            <button key={c} onClick={() => setCloud(c)} className={`px-4 py-2 text-sm rounded-lg font-semibold ${cloud === c ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300'}`}>{c.toUpperCase()}</button>
          ))}
        </div>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
          {[['Storage (GB)', 'storage_gb', 1, 100000], ['Queries/day', 'queries_per_day', 1, 10000], ['Avg GB/query', 'query_avg_gb_scanned', 0.1, 1000], ['Spark hours/day', 'spark_hours_per_day', 0, 24], ['Streaming events/day', 'streaming_events_per_day', 1000, 100000000]].map(([label, key, min, max]) => (
            <div key={String(key)}>
              <label className="text-xs text-gray-400 mb-1 block">{label}</label>
              <input type="number" min={Number(min)} max={Number(max)} value={(form as any)[String(key)]} onChange={e => setForm(p => ({ ...p, [String(key)]: Number(e.target.value) }))} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white" />
            </div>
          ))}
        </div>
        <button onClick={calculate} className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">Calculate Monthly Cost</button>
      </div>
      {result && (
        <div className="space-y-4">
          <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
            <div className="text-sm font-semibold text-white mb-3">Monthly Cost Breakdown — {result.cloud.toUpperCase()}</div>
            <div className="space-y-2">
              {Object.entries(result.monthly_cost_usd).map(([k, v]) => (
                <div key={k} className={`flex items-center justify-between p-2 rounded ${k === 'TOTAL' ? 'bg-blue-900/20 border border-blue-700/40' : 'bg-gray-700/50'}`}>
                  <span className={`text-sm ${k === 'TOTAL' ? 'font-bold text-blue-400' : 'text-gray-300'}`}>{k}</span>
                  <span className={`font-semibold ${k === 'TOTAL' ? 'text-blue-400 text-lg' : 'text-white'}`}>${Number(v).toLocaleString()}</span>
                </div>
              ))}
            </div>
          </div>
          <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
            <div className="text-sm font-semibold text-white mb-3">Top Cost Optimization Tips</div>
            {result.optimization_tips.map((t: any) => (
              <div key={t.id} className="flex items-start gap-3 p-3 bg-gray-700/50 rounded-lg mb-2">
                <span className="text-green-400 font-bold text-lg">-{t.savings_pct}%</span>
                <div>
                  <div className="text-sm font-semibold text-white">{t.title}</div>
                  <div className="text-xs text-gray-400">{t.detail}</div>
                  <span className={`text-xs px-2 py-0.5 rounded mt-1 inline-block ${t.effort === 'Low' ? 'bg-green-500/20 text-green-400' : t.effort === 'Medium' ? 'bg-yellow-500/20 text-yellow-400' : 'bg-red-500/20 text-red-400'}`}>{t.effort} effort</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

const SECTIONS = ['Multi-Cloud Map', 'AWS Lab', 'Cost Calculator']

export default function CloudServices() {
  const [activeSection, setActiveSection] = useState(0)
  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-800 flex-shrink-0">
        <div className="flex items-center gap-3 mb-1">
          <span className="text-xs font-mono text-gray-500 bg-gray-800 px-2 py-0.5 rounded">MODULE 18</span>
          <h1 className="text-xl font-bold text-white">Cloud-Native Data Services</h1>
        </div>
        <p className="text-sm text-gray-400">AWS, Azure, and GCP data service comparison — storage, compute, streaming, warehousing, and cost optimization</p>
      </div>
      <div className="flex gap-2 px-6 py-3 border-b border-gray-800 flex-shrink-0">
        {SECTIONS.map((s, i) => <SectionTab key={s} label={s} active={activeSection === i} onClick={() => setActiveSection(i)} />)}
      </div>
      <div className="flex-1 overflow-y-auto p-6">
        {activeSection === 0 && <ServiceMap />}
        {activeSection === 1 && <AWSLab />}
        {activeSection === 2 && <CostCalculator />}
        <div className="mt-8 bg-blue-900/20 border border-blue-700/30 rounded-xl p-5">
          <div className="text-sm font-semibold text-blue-400 mb-2">Cloud-Agnostic Thinking</div>
          <div className="text-sm text-gray-300 space-y-2">
            <p><strong className="text-white">The 80/20 rule:</strong> All three major clouds have equivalent services for data engineering. The differences are in pricing models, ecosystem integrations, and managed service quality — not fundamental capabilities.</p>
            <p><strong className="text-white">Avoid lock-in where it matters:</strong> Open formats (Parquet, Delta Lake, Iceberg) and open protocols (Kafka, JDBC) keep your options open. Avoid proprietary transformation logic in cloud-native ETL services.</p>
            <p><strong className="text-white">Cost optimization first principle:</strong> Compression + partitioning typically reduces query cost by 90%+. Reserved instances save 60% on predictable workloads. Spot/preemptible instances for batch jobs save another 70%.</p>
          </div>
        </div>
      </div>
    </div>
  )
}
