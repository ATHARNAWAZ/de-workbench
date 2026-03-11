import { useState, useEffect } from 'react'
import { api, endpoints } from '../utils/api'
import ExplanationPanel from '../components/ExplanationPanel'
import { Search, Tag, ChevronDown, ChevronUp, Shield, BookOpen, Database } from 'lucide-react'
import clsx from 'clsx'

interface Column {
  name: string
  type: string
  nullable: boolean
  pii: boolean
  description: string
  example_values: (string | number)[]
  min: string | null
  max: string | null
  cardinality: number | null
  mean: number | null
}

interface CatalogEntry {
  id: string
  table_name: string
  full_name: string
  layer: string
  description: string
  owner: string
  tags: string[]
  last_profiled: string
  row_count: number
  size_mb: number
  upstream: string[]
  downstream: string[]
  columns: Column[]
  glossary_links: string[]
}

interface GlossaryTerm {
  term: string
  definition: string
  owner: string
  linked_columns: string[]
}

const LAYER_BADGES: Record<string, string> = {
  bronze: 'text-orange-400 bg-orange-900/20 border-orange-800/40',
  silver: 'text-gray-300 bg-gray-800 border-gray-700',
  gold: 'text-yellow-400 bg-yellow-900/20 border-yellow-800/40',
}

const TAG_COLORS: Record<string, string> = {
  PII: 'text-red-400 bg-red-900/20 border-red-800/40',
  Financial: 'text-green-400 bg-green-900/20 border-green-800/40',
  Revenue: 'text-green-400 bg-green-900/20 border-green-800/40',
  Core: 'text-blue-400 bg-blue-900/20 border-blue-800/40',
  Customer: 'text-purple-400 bg-purple-900/20 border-purple-800/40',
  Marketing: 'text-pink-400 bg-pink-900/20 border-pink-800/40',
  Aggregated: 'text-orange-400 bg-orange-900/20 border-orange-800/40',
  Executive: 'text-yellow-400 bg-yellow-900/20 border-yellow-800/40',
  Product: 'text-cyan-400 bg-cyan-900/20 border-cyan-800/40',
  Catalog: 'text-teal-400 bg-teal-900/20 border-teal-800/40',
}

export default function DataCatalog() {
  const [catalog, setCatalog] = useState<CatalogEntry[]>([])
  const [glossary, setGlossary] = useState<GlossaryTerm[]>([])
  const [allTags, setAllTags] = useState<string[]>([])
  const [query, setQuery] = useState('')
  const [activeTag, setActiveTag] = useState('')
  const [activeLayer, setActiveLayer] = useState('')
  const [selected, setSelected] = useState<CatalogEntry | null>(null)
  const [expandedColumn, setExpandedColumn] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'tables' | 'glossary'>('tables')

  useEffect(() => {
    Promise.all([
      api.get(endpoints.catalogTables),
      api.get(endpoints.glossary),
      api.get(endpoints.catalogTags),
    ]).then(([catRes, glosRes, tagsRes]) => {
      setCatalog(catRes.data.tables)
      setGlossary(glosRes.data.terms)
      setAllTags(tagsRes.data.tags)
      setSelected(catRes.data.tables[0])
    })
  }, [])

  const search = async () => {
    const r = await api.get(`${endpoints.catalogSearch}?q=${query}&tag=${activeTag}&layer=${activeLayer}`)
    setCatalog(r.data.results)
  }

  useEffect(() => {
    const t = setTimeout(search, 300)
    return () => clearTimeout(t)
  }, [query, activeTag, activeLayer])

  const formatDate = (iso: string) => new Date(iso).toLocaleDateString()

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Data Catalog</h1>
        <p className="text-sm text-gray-400 mt-1">Searchable metadata, column-level documentation, and business glossary</p>
      </div>

      <ExplanationPanel
        title="Data Catalog & Documentation"
        what="A data catalog is a metadata management system that makes your data assets discoverable, understandable, and trustworthy. It answers: 'What data exists?', 'What does this column mean?', 'Is this data PII?', 'Who owns it?'"
        why="Without a catalog, every analyst re-discovers the same tables, re-asks the same questions about columns, and makes conflicting definitions of 'revenue' or 'active user'. Inconsistent definitions lead to conflicting dashboards and loss of trust in data."
        how="Senior engineers treat the catalog like code — documentation is maintained alongside the schema, updated as part of the PR process, and automatically profiled (min/max/cardinality) on every pipeline run. They enforce column-level tagging (PII, Financial) for governance and row-level security."
        tools={['DataHub', 'Amundsen', 'Alation', 'Collibra', 'dbt docs', 'Apache Atlas']}
        seniorTip="Business glossary is the most impactful part of a catalog. If Finance defines 'Revenue' as gross and Engineering defines it as net, every dashboard is wrong. Document the exact business definition, formula, and owner of every key metric — and link it to the actual column."
      />

      {/* Search and filters */}
      <div className="card p-4 space-y-3">
        <div className="relative">
          <Search size={15} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" />
          <input
            className="input pl-9"
            placeholder="Search tables, columns, descriptions..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </div>
        <div className="flex flex-wrap gap-2 items-center">
          <span className="text-xs text-gray-500">Filter by tag:</span>
          <button
            onClick={() => setActiveTag('')}
            className={clsx('text-xs px-2 py-1 rounded-full border transition-colors', !activeTag ? 'bg-blue-600 text-white border-blue-600' : 'bg-gray-800 text-gray-400 border-gray-700')}
          >
            All
          </button>
          {allTags.map((tag) => (
            <button
              key={tag}
              onClick={() => setActiveTag(activeTag === tag ? '' : tag)}
              className={clsx(
                'text-xs px-2 py-1 rounded-full border transition-colors',
                activeTag === tag
                  ? 'ring-2 ring-blue-500'
                  : '',
                TAG_COLORS[tag] || 'text-gray-400 bg-gray-800 border-gray-700'
              )}
            >
              {tag}
            </button>
          ))}
          <div className="ml-auto flex gap-2">
            {['', 'bronze', 'silver', 'gold'].map((l) => (
              <button
                key={l}
                onClick={() => setActiveLayer(l)}
                className={clsx('text-xs px-2 py-1 rounded border transition-colors', activeLayer === l ? 'bg-blue-600 text-white border-blue-600' : 'bg-gray-800 text-gray-400 border-gray-700')}
              >
                {l || 'All Layers'}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 border-b border-gray-800">
        <button
          onClick={() => setActiveTab('tables')}
          className={clsx('px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors', activeTab === 'tables' ? 'border-blue-500 text-blue-400' : 'border-transparent text-gray-500 hover:text-gray-300')}
        >
          <Database size={14} className="inline mr-2" />Tables ({catalog.length})
        </button>
        <button
          onClick={() => setActiveTab('glossary')}
          className={clsx('px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors', activeTab === 'glossary' ? 'border-blue-500 text-blue-400' : 'border-transparent text-gray-500 hover:text-gray-300')}
        >
          <BookOpen size={14} className="inline mr-2" />Business Glossary ({glossary.length})
        </button>
      </div>

      {activeTab === 'tables' && (
        <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
          {/* Table list */}
          <div className="space-y-3">
            {catalog.map((entry) => (
              <button
                key={entry.id}
                onClick={() => setSelected(entry)}
                className={clsx('w-full card p-4 text-left hover:border-gray-600 transition-all', selected?.id === entry.id && 'ring-2 ring-blue-500')}
              >
                <div className="flex items-start justify-between gap-2 mb-2">
                  <div>
                    <div className="text-sm font-semibold text-gray-200">{entry.table_name}</div>
                    <div className="text-[10px] text-gray-500 font-mono">{entry.full_name}</div>
                  </div>
                  <span className={clsx('text-[10px] px-2 py-0.5 rounded border font-medium flex-shrink-0', LAYER_BADGES[entry.layer])}>
                    {entry.layer}
                  </span>
                </div>
                <p className="text-xs text-gray-500 line-clamp-2 mb-2">{entry.description}</p>
                <div className="flex flex-wrap gap-1">
                  {entry.tags.map((tag) => (
                    <span key={tag} className={clsx('text-[10px] px-1.5 py-0.5 rounded border', TAG_COLORS[tag] || 'text-gray-400 bg-gray-800 border-gray-700')}>
                      {tag}
                    </span>
                  ))}
                </div>
                <div className="flex items-center gap-3 mt-2 text-[10px] text-gray-600">
                  <span>{entry.row_count.toLocaleString()} rows</span>
                  <span>•</span>
                  <span>Profiled {formatDate(entry.last_profiled)}</span>
                </div>
              </button>
            ))}
          </div>

          {/* Entry detail */}
          {selected && (
            <div className="xl:col-span-2 space-y-4">
              <div className="card p-5">
                <div className="flex items-start justify-between mb-4">
                  <div>
                    <h2 className="text-lg font-bold text-white">{selected.table_name}</h2>
                    <div className="text-xs font-mono text-gray-500 mt-0.5">{selected.full_name}</div>
                  </div>
                  <span className={clsx('text-xs px-2 py-1 rounded border', LAYER_BADGES[selected.layer])}>{selected.layer}</span>
                </div>

                <p className="text-sm text-gray-400 mb-4">{selected.description}</p>

                <div className="grid grid-cols-3 gap-3 mb-4">
                  <div className="bg-gray-800/50 rounded-lg p-3 text-center">
                    <div className="text-lg font-bold text-white">{selected.row_count.toLocaleString()}</div>
                    <div className="text-xs text-gray-500">Rows</div>
                  </div>
                  <div className="bg-gray-800/50 rounded-lg p-3 text-center">
                    <div className="text-lg font-bold text-white">{selected.columns.length}</div>
                    <div className="text-xs text-gray-500">Columns</div>
                  </div>
                  <div className="bg-gray-800/50 rounded-lg p-3 text-center">
                    <div className="text-lg font-bold text-white">{selected.size_mb} MB</div>
                    <div className="text-xs text-gray-500">Size</div>
                  </div>
                </div>

                <div className="flex flex-wrap gap-2 mb-4">
                  {selected.tags.map((tag) => (
                    <span key={tag} className={clsx('text-xs px-2 py-0.5 rounded border flex items-center gap-1', TAG_COLORS[tag] || 'text-gray-400 bg-gray-800 border-gray-700')}>
                      <Tag size={10} />
                      {tag}
                    </span>
                  ))}
                </div>

                <div className="text-xs text-gray-500">
                  <span className="font-medium">Owner: </span>
                  <span className="text-blue-400">{selected.owner}</span>
                  <span className="mx-3">•</span>
                  <span className="font-medium">Last profiled: </span>
                  <span>{formatDate(selected.last_profiled)}</span>
                </div>
              </div>

              {/* Columns */}
              <div className="card overflow-hidden">
                <div className="px-5 py-4 border-b border-gray-800 font-semibold text-gray-200">Columns</div>
                <div className="divide-y divide-gray-800">
                  {selected.columns.map((col) => {
                    const isExp = expandedColumn === col.name
                    return (
                      <div key={col.name}>
                        <button
                          onClick={() => setExpandedColumn(isExp ? null : col.name)}
                          className="w-full flex items-center gap-4 px-5 py-3 hover:bg-gray-800/30 text-left transition-colors"
                        >
                          <div className="flex items-center gap-2 w-48">
                            {col.pii && <Shield size={12} className="text-red-400 flex-shrink-0" />}
                            <span className="font-mono text-sm text-blue-300 truncate">{col.name}</span>
                          </div>
                          <span className="text-xs text-gray-500 w-28 font-mono">{col.type}</span>
                          <span className="flex-1 text-xs text-gray-500 truncate">{col.description}</span>
                          {col.pii && <span className="badge-error text-[10px]">PII</span>}
                          {isExp ? <ChevronUp size={14} className="text-gray-600" /> : <ChevronDown size={14} className="text-gray-600" />}
                        </button>

                        {isExp && (
                          <div className="px-5 pb-4 bg-gray-950/30 grid grid-cols-2 md:grid-cols-4 gap-3 pt-3 animate-slide-in">
                            {col.min && <div className="bg-gray-800/50 rounded p-2"><div className="text-[10px] text-gray-500">Min</div><div className="text-xs font-mono text-gray-300">{col.min}</div></div>}
                            {col.max && <div className="bg-gray-800/50 rounded p-2"><div className="text-[10px] text-gray-500">Max</div><div className="text-xs font-mono text-gray-300">{col.max}</div></div>}
                            {col.mean && <div className="bg-gray-800/50 rounded p-2"><div className="text-[10px] text-gray-500">Mean</div><div className="text-xs font-mono text-gray-300">{col.mean}</div></div>}
                            {col.cardinality && <div className="bg-gray-800/50 rounded p-2"><div className="text-[10px] text-gray-500">Cardinality</div><div className="text-xs font-mono text-gray-300">{col.cardinality.toLocaleString()}</div></div>}
                            {col.example_values.length > 0 && (
                              <div className="col-span-2 bg-gray-800/50 rounded p-2">
                                <div className="text-[10px] text-gray-500 mb-1">Sample values</div>
                                <div className="flex flex-wrap gap-1">
                                  {col.example_values.map((v, i) => (
                                    <span key={i} className="text-[10px] font-mono bg-gray-700 px-1.5 py-0.5 rounded text-gray-300">{String(v)}</span>
                                  ))}
                                </div>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Glossary */}
      {activeTab === 'glossary' && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {glossary.map((term) => (
            <div key={term.term} className="card p-5">
              <div className="flex items-center justify-between mb-2">
                <h3 className="font-bold text-gray-100">{term.term}</h3>
                <span className="text-xs text-gray-500">{term.owner}</span>
              </div>
              <p className="text-sm text-gray-400 mb-3">{term.definition}</p>
              {term.linked_columns.length > 0 && (
                <div>
                  <div className="text-xs text-gray-500 mb-1">Linked to:</div>
                  <div className="flex flex-wrap gap-1">
                    {term.linked_columns.map((col) => (
                      <span key={col} className="text-xs font-mono px-2 py-0.5 bg-gray-800 border border-gray-700 rounded text-blue-300">{col}</span>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
