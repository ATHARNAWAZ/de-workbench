import { useState, useEffect } from 'react'
import { api } from '../utils/api'

const ep = {
  decisionTree: '/api/nosql/decision-tree',
  profiles: '/api/nosql/profiles',
  redis: '/api/nosql/redis/demo',
  cassandra: '/api/nosql/cassandra/demo',
  elasticsearch: '/api/nosql/elasticsearch/demo',
}

function SectionTab({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return <button onClick={onClick} className={`px-4 py-2 text-sm font-medium rounded-lg transition-all ${active ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white hover:bg-gray-700'}`}>{label}</button>
}

function DBSelector() {
  const [data, setData] = useState<any>(null)
  const [answers, setAnswers] = useState<Record<string, string>>({})
  const [current, setCurrent] = useState('q1')
  const [recommendation, setRecommendation] = useState<any>(null)
  const [profiles, setProfiles] = useState<any[]>([])

  useEffect(() => {
    api.get(ep.decisionTree).then(r => setData(r.data))
    api.get(ep.profiles).then(r => setProfiles(r.data.databases))
  }, [])

  const answer = (questionId: string, optionId: string, next: string) => {
    setAnswers(p => ({ ...p, [questionId]: optionId }))
    if (next.startsWith('recommend_')) {
      setRecommendation(data.recommendations[next])
    } else {
      setCurrent(next)
    }
  }

  const reset = () => { setAnswers({}); setCurrent('q1'); setRecommendation(null) }

  if (!data) return <div className="text-gray-500 py-8 text-center">Loading...</div>

  const currentQ = data.questions.find((q: any) => q.id === current)
  const profile = recommendation ? profiles.find(p => p.name.toLowerCase().includes(recommendation.db)) : null

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <div>
        <div className="text-sm font-semibold text-white mb-3">Database Decision Tree</div>
        {!recommendation && currentQ && (
          <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
            <div className="text-sm font-semibold text-white mb-3">{currentQ.question}</div>
            <div className="space-y-2">
              {currentQ.options.map((opt: any) => (
                <button key={opt.id} onClick={() => answer(currentQ.id, opt.id, opt.next)} className="w-full text-left p-3 bg-gray-700 rounded-lg text-sm text-gray-300 hover:bg-gray-600 hover:text-white transition-all">{opt.text}</button>
              ))}
            </div>
          </div>
        )}
        {recommendation && (
          <div className="bg-green-900/20 border border-green-700 rounded-xl p-4">
            <div className="text-sm font-semibold text-green-400 mb-2">Recommendation: {recommendation.db.toUpperCase()}</div>
            <div className="text-sm text-gray-300 mb-3">{recommendation.reason}</div>
            <button onClick={reset} className="px-3 py-1.5 bg-gray-700 text-gray-300 rounded text-sm hover:bg-gray-600">Start Over</button>
          </div>
        )}
      </div>
      <div>
        <div className="text-sm font-semibold text-white mb-3">Database Profiles</div>
        <div className="space-y-3 max-h-96 overflow-y-auto">
          {profiles.map(p => (
            <div key={p.name} className={`bg-gray-800 rounded-xl border p-3 transition-all ${recommendation?.db && p.name.toLowerCase().includes(recommendation.db) ? 'border-green-500 ring-1 ring-green-500/30' : 'border-gray-700'}`}>
              <div className="flex items-center justify-between mb-1">
                <div>
                  <span className="font-semibold text-white">{p.name}</span>
                  <span className="text-xs text-gray-500 ml-2">{p.type}</span>
                </div>
                <div className="text-xs text-green-400">&lt;{p.query_latency_ms}ms</div>
              </div>
              <div className="text-xs text-gray-400 mb-1">{p.description}</div>
              <div className="text-xs text-gray-500">{p.best_for}</div>
              {p.anti_patterns?.length > 0 && <div className="text-xs text-red-400 mt-1">Anti-pattern: {p.anti_patterns[0]}</div>}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function RedisLab() {
  const [operation, setOperation] = useState('STRING')
  const [result, setResult] = useState<any>(null)

  const ops = ['STRING', 'HASH', 'LIST', 'SET', 'SORTED_SET', 'STREAM']

  const load = async (op: string) => {
    setOperation(op)
    const res = await api.post(ep.redis, { operation: op })
    setResult(res.data.demo)
  }

  useEffect(() => { load('STRING') }, [])

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap gap-2">
        {ops.map(op => (
          <button key={op} onClick={() => load(op)} className={`px-3 py-1.5 text-xs rounded-lg font-mono ${operation === op ? 'bg-red-600 text-white' : 'bg-gray-700 text-gray-300 hover:bg-gray-600'}`}>{op}</button>
        ))}
      </div>
      {result && (
        <div className="space-y-4">
          <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
            <div className="text-sm font-semibold text-white mb-1">{result.description}</div>
            {result.use_case_highlight && <div className="text-xs text-blue-300 mb-3">{result.use_case_highlight}</div>}
            <div className="space-y-3">
              {result.commands?.map((c: any, i: number) => (
                <div key={i} className="bg-gray-900 rounded-lg p-3">
                  <pre className="text-xs text-green-300 font-mono mb-1">{c.cmd}</pre>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-gray-500">Result:</span>
                    <span className="text-xs text-yellow-300 font-mono">{c.result}</span>
                  </div>
                  {c.explanation && <div className="text-xs text-gray-500 mt-1">{c.explanation}</div>}
                </div>
              ))}
            </div>
          </div>
          {result.cache_comparison && (
            <div className="grid grid-cols-2 gap-3">
              <div className="bg-red-900/10 border border-red-700/40 rounded-lg p-3 text-center">
                <div className="text-2xl font-bold text-red-400">{result.cache_comparison.without_cache_ms}ms</div>
                <div className="text-xs text-gray-400">Without cache (warehouse)</div>
              </div>
              <div className="bg-green-900/10 border border-green-700/40 rounded-lg p-3 text-center">
                <div className="text-2xl font-bold text-green-400">{result.cache_comparison.with_cache_ms}ms</div>
                <div className="text-xs text-gray-400">With Redis cache</div>
              </div>
              <div className="col-span-2 text-center text-green-400 font-semibold">{result.cache_comparison.speedup}</div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function CassandraLab() {
  const [scenario, setScenario] = useState('partition_design')
  const [result, setResult] = useState<any>(null)

  const load = async (s: string) => {
    setScenario(s)
    const res = await api.post(ep.cassandra, { scenario: s })
    setResult(res.data)
  }

  useEffect(() => { load('partition_design') }, [])

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        {[['partition_design', 'Partition Key Design'], ['replication', 'Replication + CL'], ['ttl', 'TTL & Data Expiry']].map(([s, label]) => (
          <button key={s} onClick={() => load(s)} className={`px-3 py-1.5 text-sm rounded-lg ${scenario === s ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300 hover:bg-gray-600'}`}>{label}</button>
        ))}
      </div>
      {result && (
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <div className="text-sm font-semibold text-white mb-3">{result.title}</div>
          {result.bad_example && (
            <div className="space-y-3">
              <div className="bg-red-900/10 border border-red-700/40 rounded-lg p-3">
                <div className="text-xs text-red-400 font-semibold mb-1">WRONG — Hot Partition</div>
                <pre className="text-xs text-gray-300 font-mono mb-2">{result.bad_example.table}</pre>
                <div className="text-xs text-red-300">{result.bad_example.problem}</div>
              </div>
              <div className="bg-green-900/10 border border-green-700/40 rounded-lg p-3">
                <div className="text-xs text-green-400 font-semibold mb-1">CORRECT — Even Distribution</div>
                <pre className="text-xs text-gray-300 font-mono mb-2">{result.good_example.table}</pre>
                <div className="text-xs text-green-300 mb-1">{result.good_example.benefit}</div>
                <pre className="text-xs text-blue-300 font-mono">{result.good_example.query}</pre>
              </div>
            </div>
          )}
          {result.matrix && (
            <div className="overflow-x-auto">
              <table className="w-full text-xs border-collapse">
                <thead><tr className="border-b border-gray-700">{['Write CL', 'Read CL', 'Consistency', 'Availability', 'Use Case'].map(h => <th key={h} className="text-left py-2 pr-3 text-gray-500">{h}</th>)}</tr></thead>
                <tbody>
                  {result.matrix.map((r: any, i: number) => (
                    <tr key={i} className="border-b border-gray-800">
                      {[r.write_cl, r.read_cl, r.consistency, r.availability, r.use_case].map((v, j) => (
                        <td key={j} className="py-2 pr-3 text-gray-300">{v}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
              {result.quorum_math && <div className="mt-3 p-2 bg-blue-900/20 rounded text-xs text-blue-300">{result.quorum_math}</div>}
            </div>
          )}
          {result.insert_with_ttl && (
            <div className="space-y-3">
              <div className="text-xs text-gray-300">{result.use_case}</div>
              <pre className="text-xs text-green-300 font-mono bg-gray-900 rounded p-3">{result.insert_with_ttl}</pre>
              <div className="text-xs text-gray-300">{result.explanation}</div>
              <div className="p-2 bg-yellow-900/10 border border-yellow-700/40 rounded text-xs text-yellow-300">{result.tombstones_warning}</div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function ElasticsearchLab() {
  const [operation, setOperation] = useState('search')
  const [result, setResult] = useState<any>(null)

  const load = async (op: string) => {
    setOperation(op)
    const res = await api.post(ep.elasticsearch, { operation: op })
    setResult(res.data)
  }

  useEffect(() => { load('search') }, [])

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        {[['search', 'Full-Text Search'], ['aggregation', 'Aggregations'], ['mapping', 'Index Mapping']].map(([s, label]) => (
          <button key={s} onClick={() => load(s)} className={`px-3 py-1.5 text-sm rounded-lg ${operation === s ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300 hover:bg-gray-600'}`}>{label}</button>
        ))}
      </div>
      {result && (
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4 space-y-3">
          <div className="text-sm font-semibold text-white">{result.title}</div>
          {result.search_request && (
            <div className="grid grid-cols-2 gap-3">
              <div>
                <div className="text-xs text-gray-500 mb-1">Query</div>
                <pre className="text-xs text-yellow-300 font-mono bg-gray-900 rounded p-2 overflow-x-auto">{result.search_request}</pre>
              </div>
              <div>
                <div className="text-xs text-gray-500 mb-1">Results</div>
                <div className="space-y-1">
                  {result.results?.slice(0, 3).map((r: any, i: number) => (
                    <div key={i} className="bg-gray-700/50 rounded p-2 text-xs">
                      <span className="text-green-400 font-bold">{r._score}</span>
                      <span className="text-white ml-2">{r._source.title}</span>
                      <span className="text-gray-400 ml-2">${r._source.price}</span>
                    </div>
                  ))}
                </div>
                {result.explanation && <div className="text-xs text-gray-400 mt-2">{result.explanation}</div>}
              </div>
            </div>
          )}
          {result.request && !result.search_request && (
            <pre className="text-xs text-green-300 font-mono bg-gray-900 rounded p-3 overflow-x-auto whitespace-pre">{result.request}</pre>
          )}
          {result.mapping && (
            <pre className="text-xs text-blue-300 font-mono bg-gray-900 rounded p-3 overflow-x-auto whitespace-pre">{JSON.stringify(result.mapping, null, 2)}</pre>
          )}
          {result.keyword_vs_text && <div className="p-2 bg-yellow-900/10 border border-yellow-700/40 rounded text-xs text-yellow-300">{result.keyword_vs_text}</div>}
          {result.use_case && <div className="text-xs text-blue-300">{result.use_case}</div>}
        </div>
      )}
    </div>
  )
}

const SECTIONS = ['DB Decision Tree', 'Redis Lab', 'Cassandra Lab', 'Elasticsearch Lab']

export default function NoSQL() {
  const [activeSection, setActiveSection] = useState(0)
  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-800 flex-shrink-0">
        <div className="flex items-center gap-3 mb-1">
          <span className="text-xs font-mono text-gray-500 bg-gray-800 px-2 py-0.5 rounded">MODULE 23</span>
          <h1 className="text-xl font-bold text-white">NoSQL & Polyglot Persistence</h1>
        </div>
        <p className="text-sm text-gray-400">Redis, Cassandra, Elasticsearch, MongoDB — right tool for the right job</p>
      </div>
      <div className="flex gap-2 px-6 py-3 border-b border-gray-800 flex-shrink-0">
        {SECTIONS.map((s, i) => <SectionTab key={s} label={s} active={activeSection === i} onClick={() => setActiveSection(i)} />)}
      </div>
      <div className="flex-1 overflow-y-auto p-6">
        {activeSection === 0 && <DBSelector />}
        {activeSection === 1 && <RedisLab />}
        {activeSection === 2 && <CassandraLab />}
        {activeSection === 3 && <ElasticsearchLab />}
        <div className="mt-8 bg-blue-900/20 border border-blue-700/30 rounded-xl p-5">
          <div className="text-sm font-semibold text-blue-400 mb-2">Polyglot Persistence — Right Tool for the Right Job</div>
          <div className="text-sm text-gray-300 space-y-2">
            <p><strong className="text-white">CAP theorem in practice:</strong> You can only guarantee 2 of 3 — Consistency, Availability, Partition tolerance. Redis (single-node): CP. Cassandra: AP (tunable consistency). Elasticsearch: AP.</p>
            <p><strong className="text-white">Anti-patterns to avoid:</strong> Using Cassandra like a relational DB (no ad-hoc queries without partition key). Using Elasticsearch as primary datastore (no transactions, eventual consistency). Using Redis as permanent storage without AOF persistence.</p>
            <p><strong className="text-white">Composition pattern:</strong> In a modern data stack, you use multiple databases together: PostgreSQL (operational source), Kafka (streaming), Redis (feature serving), ClickHouse (analytics), Elasticsearch (search). Each does what it does best.</p>
          </div>
        </div>
      </div>
    </div>
  )
}
