import { useState, useEffect } from 'react'
import { api } from '../utils/api'

const ep = {
  comparison: '/api/olap/comparison',
  query: '/api/olap/query',
  clickhouse: '/api/olap/clickhouse',
  druid: '/api/olap/druid',
  trino: '/api/olap/trino',
  benchmark: '/api/olap/benchmark',
}

function SectionTab({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return <button onClick={onClick} className={`px-4 py-2 text-sm font-medium rounded-lg transition-all ${active ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white hover:bg-gray-700'}`}>{label}</button>
}

function EngineComparison() {
  const [engines, setEngines] = useState<any[]>([])
  const [benchResult, setBenchResult] = useState<any>(null)

  useEffect(() => { api.get(ep.comparison).then(r => setEngines(r.data.engines)) }, [])

  const runBenchmark = async (engine: string) => {
    const res = await api.post(ep.benchmark, { rows_millions: 100, engine })
    setBenchResult(res.data)
  }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
        {engines.map(e => (
          <div key={e.engine} className="bg-gray-800 rounded-xl border border-gray-700 p-4">
            <div className="flex items-start justify-between mb-2">
              <div>
                <div className="font-bold text-white">{e.engine}</div>
                <div className="text-xs text-gray-500">{e.by}</div>
              </div>
              <div className="text-right">
                <div className="text-lg font-bold text-green-400">{e.query_latency_ms}ms</div>
                <div className="text-xs text-gray-500">typical query</div>
              </div>
            </div>
            <div className="text-xs text-gray-400 mb-3">{e.use_case}</div>
            <div className="space-y-1 mb-3">
              {e.strengths.slice(0, 3).map((s: string, i: number) => <div key={i} className="text-xs text-green-400">✓ {s}</div>)}
            </div>
            <div className="text-xs text-gray-500 mb-2 italic">{e.best_for.slice(0, 80)}...</div>
            <button onClick={() => runBenchmark(e.engine.toLowerCase())} className="w-full py-1.5 bg-blue-600/20 text-blue-400 rounded text-xs hover:bg-blue-600/40">Benchmark</button>
          </div>
        ))}
      </div>
      {benchResult && (
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <div className="text-sm font-semibold text-white mb-3">Benchmark: {(benchResult.rows_scanned / 1_000_000).toFixed(0)}M rows</div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {Object.entries(benchResult.results).map(([engine, res]: any) => (
              <div key={engine} className={`rounded-lg p-3 text-center border ${engine === benchResult.winner ? 'border-green-500 bg-green-900/10' : 'border-gray-700 bg-gray-700/50'}`}>
                <div className={`text-sm font-bold capitalize ${engine === benchResult.winner ? 'text-green-400' : 'text-white'}`}>{engine}</div>
                <div className={`text-xl font-bold mt-1 ${engine === benchResult.winner ? 'text-green-400' : 'text-gray-300'}`}>{res.latency_ms}ms</div>
                <div className="text-xs text-gray-500">{(res.rows_per_ms / 1000).toFixed(0)}K rows/ms</div>
                {engine === benchResult.winner && <div className="text-xs text-green-400 mt-1">WINNER</div>}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function ClickHouseLab() {
  const [data, setData] = useState<any>(null)
  const [queryResult, setQueryResult] = useState<any>(null)
  const [tab, setTab] = useState<'mergetree' | 'mv'>('mergetree')

  useEffect(() => { api.get(ep.clickhouse).then(r => setData(r.data)) }, [])

  const runQuery = async () => {
    const res = await api.post(ep.query, { sql: "SELECT region, SUM(revenue) FROM orders GROUP BY region ORDER BY 2 DESC", engine: 'clickhouse', rows_in_table: 1000000 })
    setQueryResult(res.data)
  }

  if (!data) return <div className="text-gray-500 py-8 text-center">Loading...</div>

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        <button onClick={() => setTab('mergetree')} className={`px-3 py-1.5 text-sm rounded-lg ${tab === 'mergetree' ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300'}`}>MergeTree Engine</button>
        <button onClick={() => setTab('mv')} className={`px-3 py-1.5 text-sm rounded-lg ${tab === 'mv' ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300'}`}>Materialized Views</button>
      </div>
      {tab === 'mergetree' && (
        <div className="space-y-4">
          <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
            <div className="text-sm font-semibold text-white mb-2">{data.mergetree_demo.description}</div>
            <pre className="text-xs text-green-300 font-mono bg-gray-900 rounded p-3 overflow-x-auto whitespace-pre mb-3">{data.mergetree_demo.ddl}</pre>
            <div className="text-xs font-semibold text-white mb-2">What happens on INSERT:</div>
            <ol className="space-y-1">
              {data.mergetree_demo.what_happens_on_insert.map((s: string, i: number) => (
                <li key={i} className="text-xs text-gray-300 flex gap-2"><span className="text-blue-400">{i + 1}.</span>{s}</li>
              ))}
            </ol>
          </div>
          <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
            <div className="text-sm font-semibold text-white mb-2">Live Query Demo</div>
            <pre className="text-xs text-gray-300 font-mono bg-gray-900 rounded p-2 mb-3">{data.mergetree_demo.benchmark.query}</pre>
            <button onClick={runQuery} className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700 mb-3">Run Query</button>
            {queryResult && (
              <div className="grid grid-cols-2 gap-3">
                <div className="bg-green-900/10 border border-green-700/40 rounded-lg p-3 text-center">
                  <div className="text-2xl font-bold text-green-400">{queryResult.latency_ms}ms</div>
                  <div className="text-xs text-gray-400">ClickHouse</div>
                </div>
                <div className="bg-red-900/10 border border-red-700/40 rounded-lg p-3 text-center">
                  <div className="text-2xl font-bold text-red-400">{queryResult.vs_postgres_ms.toLocaleString()}ms</div>
                  <div className="text-xs text-gray-400">PostgreSQL (estimated)</div>
                </div>
                <div className="col-span-2 text-center text-green-400 font-semibold">{queryResult.speedup}</div>
              </div>
            )}
          </div>
        </div>
      )}
      {tab === 'mv' && (
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <div className="text-sm font-semibold text-white mb-2">{data.materialized_views.description}</div>
          <pre className="text-xs text-green-300 font-mono bg-gray-900 rounded p-3 overflow-x-auto whitespace-pre mb-3">{data.materialized_views.sql}</pre>
          <div className="p-3 bg-blue-900/20 rounded text-sm text-blue-300">{data.materialized_views.benefit}</div>
        </div>
      )}
    </div>
  )
}

function DruidDrilldown() {
  const [data, setData] = useState<any>(null)

  useEffect(() => { api.get(ep.druid).then(r => setData(r.data)) }, [])

  if (!data) return <div className="text-gray-500 py-8 text-center">Loading...</div>

  return (
    <div className="space-y-4">
      <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
        <div className="text-sm font-semibold text-white mb-2">Druid Ingestion Spec (Kafka)</div>
        <pre className="text-xs text-green-300 font-mono bg-gray-900 rounded p-3 overflow-x-auto whitespace-pre max-h-80">{JSON.stringify(data.ingestion_spec, null, 2)}</pre>
        <div className="mt-3 p-3 bg-yellow-900/10 border border-yellow-700/40 rounded text-sm text-yellow-300">{data.rollup_explanation}</div>
      </div>
      <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
        <div className="text-sm font-semibold text-white mb-3">Approximate Algorithms</div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          {Object.entries(data.approximate_algorithms).map(([name, info]: any) => (
            <div key={name} className="bg-gray-700/50 rounded-lg p-3">
              <div className="font-semibold text-blue-400 mb-1">{name}</div>
              <div className="text-xs text-gray-300 mb-1">{info.purpose}</div>
              <div className="text-xs text-gray-500">Error: {info.error_rate}</div>
              {info.storage && <div className="text-xs text-gray-500">Storage: {info.storage}</div>}
              <div className="text-xs text-gray-400 mt-1 italic">{info.use_case}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function TrinoFederation() {
  const [data, setData] = useState<any>(null)

  useEffect(() => { api.get(ep.trino).then(r => setData(r.data)) }, [])

  if (!data) return <div className="text-gray-500 py-8 text-center">Loading...</div>

  return (
    <div className="space-y-4">
      <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
        <div className="text-sm font-semibold text-white mb-3">Connector Map — Query Any Source</div>
        <div className="grid grid-cols-2 gap-2">
          {data.connectors.map((c: any) => (
            <div key={c.name} className="bg-gray-700/50 rounded p-3 text-xs">
              <div className="font-semibold text-white">{c.name}</div>
              <div className="font-mono text-blue-300 mt-0.5">{c.catalog}.</div>
              <pre className="text-gray-400 mt-1 text-xs font-mono">{c.query_example.slice(0, 60)}...</pre>
              <div className="text-gray-500 mt-1">{c.pushdown}</div>
            </div>
          ))}
        </div>
      </div>
      <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
        <div className="text-sm font-semibold text-white mb-2">Federated Query — Multi-Source Join</div>
        <pre className="text-xs text-green-300 font-mono bg-gray-900 rounded p-3 overflow-x-auto whitespace-pre">{data.federated_query.sql}</pre>
        <div className="mt-3">
          <div className="text-xs font-semibold text-blue-400 mb-2">How It Works</div>
          <ol className="space-y-1">
            {data.federated_query.how_it_works.map((s: string, i: number) => (
              <li key={i} className="text-xs text-gray-300 flex gap-2"><span className="text-blue-400">{i + 1}.</span>{s}</li>
            ))}
          </ol>
        </div>
        <div className="mt-3">
          <div className="text-xs font-semibold text-yellow-400 mb-2">Performance Gotchas</div>
          {data.federated_query.gotchas.map((g: string, i: number) => (
            <div key={i} className="text-xs text-yellow-300">⚠ {g}</div>
          ))}
        </div>
      </div>
    </div>
  )
}

const SECTIONS = ['Engine Comparison', 'ClickHouse Lab', 'Apache Druid', 'Trino/Presto Federation']

export default function RealTimeOLAP() {
  const [activeSection, setActiveSection] = useState(0)
  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-800 flex-shrink-0">
        <div className="flex items-center gap-3 mb-1">
          <span className="text-xs font-mono text-gray-500 bg-gray-800 px-2 py-0.5 rounded">MODULE 22</span>
          <h1 className="text-xl font-bold text-white">Real-Time OLAP & Query Engines</h1>
        </div>
        <p className="text-sm text-gray-400">ClickHouse, Druid, Pinot, StarRocks — sub-second analytics at billion-row scale</p>
      </div>
      <div className="flex gap-2 px-6 py-3 border-b border-gray-800 flex-shrink-0">
        {SECTIONS.map((s, i) => <SectionTab key={s} label={s} active={activeSection === i} onClick={() => setActiveSection(i)} />)}
      </div>
      <div className="flex-1 overflow-y-auto p-6">
        {activeSection === 0 && <EngineComparison />}
        {activeSection === 1 && <ClickHouseLab />}
        {activeSection === 2 && <DruidDrilldown />}
        {activeSection === 3 && <TrinoFederation />}
        <div className="mt-8 bg-blue-900/20 border border-blue-700/30 rounded-xl p-5">
          <div className="text-sm font-semibold text-blue-400 mb-2">Why Traditional OLAP Isn't Always Enough</div>
          <div className="text-sm text-gray-300 space-y-2">
            <p><strong className="text-white">Sub-second analytics at scale:</strong> Redshift/BigQuery/Snowflake are designed for internal BI workloads with 10-100 concurrent users. User-facing analytics serving 10,000+ concurrent users with &lt;100ms SLA requirements need purpose-built OLAP engines.</p>
            <p><strong className="text-white">ClickHouse for most use cases:</strong> If you need fast OLAP and your data fits on a few servers, ClickHouse is the simplest option with the best single-node performance. MergeTree engine + materialized views cover 90% of real-time analytics needs.</p>
            <p><strong className="text-white">Choose Druid/Pinot at extreme scale:</strong> When you have &gt;1B events/day and need real-time ingestion + query, or when approximate algorithms (HLL for unique counts) are acceptable. The operational complexity is significant.</p>
          </div>
        </div>
      </div>
    </div>
  )
}
