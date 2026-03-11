import { useState } from 'react'
import ExplanationPanel from '../components/ExplanationPanel'
import { Copy, Star, Link } from 'lucide-react'
import clsx from 'clsx'

type SchemaType = 'star' | '3nf' | 'snowflake'
type SCDType = 1 | 2 | 3

interface Column {
  name: string
  type: string
  pk?: boolean
  fk?: string
  nullable?: boolean
  scd_tracked?: boolean
}

interface ERTable {
  id: string
  name: string
  columns: Column[]
  x: number
  y: number
  tableType: 'fact' | 'dimension' | 'bridge' | 'lookup'
}

const ECOMMERCE_STAR_SCHEMA: ERTable[] = [
  {
    id: 'fact_orders', name: 'fact_orders', tableType: 'fact', x: 340, y: 220,
    columns: [
      { name: 'order_id', type: 'VARCHAR(50)', pk: true },
      { name: 'customer_id', type: 'VARCHAR(50)', fk: 'dim_customer' },
      { name: 'product_id', type: 'VARCHAR(50)', fk: 'dim_product' },
      { name: 'date_id', type: 'VARCHAR(8)', fk: 'dim_date' },
      { name: 'quantity', type: 'INTEGER', nullable: false },
      { name: 'unit_price', type: 'DECIMAL(10,2)', nullable: false },
      { name: 'discount', type: 'DECIMAL(5,2)', nullable: true },
      { name: 'revenue', type: 'DECIMAL(12,2)', nullable: false },
      { name: 'status', type: 'VARCHAR(20)', nullable: true },
      { name: 'channel', type: 'VARCHAR(20)', nullable: true },
    ],
  },
  {
    id: 'dim_customer', name: 'dim_customer', tableType: 'dimension', x: 60, y: 80,
    columns: [
      { name: 'customer_id', type: 'VARCHAR(50)', pk: true },
      { name: 'first_name', type: 'VARCHAR(100)', scd_tracked: true },
      { name: 'last_name', type: 'VARCHAR(100)', scd_tracked: true },
      { name: 'email', type: 'VARCHAR(255)', scd_tracked: true },
      { name: 'segment', type: 'VARCHAR(30)', scd_tracked: true },
      { name: 'country', type: 'VARCHAR(50)' },
      { name: 'is_active', type: 'BOOLEAN' },
      { name: 'created_at', type: 'TIMESTAMP' },
    ],
  },
  {
    id: 'dim_product', name: 'dim_product', tableType: 'dimension', x: 640, y: 80,
    columns: [
      { name: 'product_id', type: 'VARCHAR(50)', pk: true },
      { name: 'product_name', type: 'VARCHAR(200)', scd_tracked: true },
      { name: 'category', type: 'VARCHAR(50)', scd_tracked: true },
      { name: 'brand', type: 'VARCHAR(100)' },
      { name: 'unit_price', type: 'DECIMAL(10,2)', scd_tracked: true },
      { name: 'cost', type: 'DECIMAL(10,2)' },
      { name: 'sku', type: 'VARCHAR(50)' },
    ],
  },
  {
    id: 'dim_date', name: 'dim_date', tableType: 'dimension', x: 340, y: 460,
    columns: [
      { name: 'date_id', type: 'VARCHAR(8)', pk: true },
      { name: 'full_date', type: 'DATE' },
      { name: 'year', type: 'INTEGER' },
      { name: 'quarter', type: 'INTEGER' },
      { name: 'month', type: 'INTEGER' },
      { name: 'month_name', type: 'VARCHAR(10)' },
      { name: 'is_weekend', type: 'BOOLEAN' },
    ],
  },
]

const SCD_INFO: Record<number, { title: string; description: string; pros: string; cons: string; example: string }> = {
  1: {
    title: 'SCD Type 1 — Overwrite',
    description: 'Simply overwrite the old value with the new value. No history is kept.',
    pros: 'Simple, no extra storage',
    cons: 'Loses historical context — you cannot analyze "as of" past dates',
    example: 'UPDATE dim_customer SET email = \'new@email.com\' WHERE customer_id = \'CUST-001\'',
  },
  2: {
    title: 'SCD Type 2 — Full History',
    description: 'Add a new row for every change, with effective_from, effective_to, and is_current columns. The most common and powerful SCD type.',
    pros: 'Full history preserved, enables point-in-time analysis',
    cons: 'Dimension table grows over time, queries need to filter is_current = TRUE or specific dates',
    example: 'INSERT new row with effective_from = today, effective_to = NULL, is_current = TRUE\nUPDATE old row SET effective_to = today-1, is_current = FALSE',
  },
  3: {
    title: 'SCD Type 3 — Previous Value',
    description: 'Add "previous_" columns to track exactly one prior value alongside the current value.',
    pros: 'Retains one version of history without multiplying rows',
    cons: 'Only 2 versions max — loses older history beyond the previous value',
    example: 'ALTER TABLE dim_customer ADD COLUMN previous_segment VARCHAR(30);\nUPDATE dim_customer SET previous_segment = segment, segment = \'Enterprise\' WHERE customer_id = \'CUST-001\'',
  },
}

function generateDDL(tables: ERTable[], schemaType: SchemaType, scdType: SCDType): string {
  const lines: string[] = [
    `-- Generated DDL: ${schemaType.toUpperCase()} Schema`,
    `-- SCD Strategy: Type ${scdType}`,
    `-- Generated at ${new Date().toISOString()}`,
    '',
  ]

  for (const table of tables) {
    lines.push(`CREATE TABLE ${table.name} (`)
    const colDefs: string[] = []
    for (const col of table.columns) {
      let def = `  ${col.name.padEnd(30)} ${col.type}`
      if (col.pk) def += ' NOT NULL'
      else if (col.nullable) def += ''
      else def += ' NOT NULL'
      colDefs.push(def)
    }
    if (scdType === 2 && table.tableType === 'dimension') {
      colDefs.push(`  ${'surrogate_key'.padEnd(30)} BIGINT GENERATED ALWAYS AS IDENTITY`)
      colDefs.push(`  ${'effective_from'.padEnd(30)} DATE NOT NULL`)
      colDefs.push(`  ${'effective_to'.padEnd(30)} DATE`)
      colDefs.push(`  ${'is_current'.padEnd(30)} BOOLEAN DEFAULT TRUE`)
    }
    if (scdType === 3 && table.tableType === 'dimension') {
      const tracked = table.columns.filter((c) => c.scd_tracked)
      for (const col of tracked) {
        colDefs.push(`  ${'previous_' + col.name.padEnd(22)} ${col.type}`)
      }
    }
    const pks = table.columns.filter((c) => c.pk)
    if (pks.length > 0) {
      const pkCols = scdType === 2 && table.tableType === 'dimension' ? 'surrogate_key' : pks.map((c) => c.name).join(', ')
      colDefs.push(`  PRIMARY KEY (${pkCols})`)
    }
    const fks = table.columns.filter((c) => c.fk)
    for (const col of fks) {
      colDefs.push(`  FOREIGN KEY (${col.name}) REFERENCES ${col.fk}(${col.fk}_id)`)
    }
    lines.push(colDefs.join(',\n'))
    lines.push(');')
    lines.push('')
  }

  return lines.join('\n')
}

export default function DataModeling() {
  const [schemaType, setSchemaType] = useState<SchemaType>('star')
  const [scdType, setScdType] = useState<SCDType>(2)
  const [selectedTable, setSelectedTable] = useState<string | null>(null)
  const [showDDL, setShowDDL] = useState(false)

  const ddl = generateDDL(ECOMMERCE_STAR_SCHEMA, schemaType, scdType)
  const selectedTableData = ECOMMERCE_STAR_SCHEMA.find((t) => t.id === selectedTable)

  const getRelationLines = () => {
    const lines: Array<{ x1: number; y1: number; x2: number; y2: number; label: string }> = []
    for (const table of ECOMMERCE_STAR_SCHEMA) {
      for (const col of table.columns) {
        if (col.fk) {
          const target = ECOMMERCE_STAR_SCHEMA.find((t) => t.id === col.fk)
          if (target) {
            lines.push({
              x1: table.x + 160,
              y1: table.y + 40,
              x2: target.x + 160,
              y2: target.y + 40,
              label: col.name,
            })
          }
        }
      }
    }
    return lines
  }

  const TABLE_COLORS: Record<string, string> = {
    fact: 'border-blue-600 bg-blue-900/20',
    dimension: 'border-purple-600 bg-purple-900/20',
    bridge: 'border-orange-600 bg-orange-900/20',
    lookup: 'border-gray-600 bg-gray-800/20',
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Data Modeling</h1>
          <p className="text-sm text-gray-400 mt-1">ER diagram designer, schema visualization, DDL generation</p>
        </div>
        <div className="flex items-center gap-3">
          <button onClick={() => setShowDDL(!showDDL)} className="btn-secondary">
            <Copy size={14} /> {showDDL ? 'Hide' : 'Show'} DDL
          </button>
        </div>
      </div>

      <ExplanationPanel
        title="Data Modeling & Schema Design"
        what="Data modeling is the process of defining how data is organized, stored, and related. It determines query performance, storage efficiency, and how easily the data can be understood and used."
        why="A poor data model can make queries 100x slower, confuse business users, and create technical debt that takes years to unwind. Schema decisions made today will constrain your architecture for 5-10 years."
        how="Senior engineers choose between OLTP (3NF, optimized for writes) and OLAP (star/snowflake schema, optimized for reads). For analytics, the Kimball star schema is the industry standard: one fact table surrounded by denormalized dimension tables. Slowly Changing Dimensions (SCD Type 2) enable time-travel queries."
        tools={['dbt', 'Kimball methodology', 'Inmon approach', 'Data Vault 2.0', 'erDiagram (Mermaid)']}
        seniorTip="Always build dim_date first. Every fact table should have a date FK. Date dimensions enable rich time intelligence (week-over-week, quarter-to-date) without any complex SQL. Store date_id as YYYYMMDD integer for fast joins and partition pruning."
      />

      {/* Controls */}
      <div className="card p-5 flex items-center gap-6 flex-wrap">
        <div>
          <div className="text-xs text-gray-400 font-medium mb-2">Schema Type</div>
          <div className="flex rounded-lg overflow-hidden border border-gray-700">
            {(['star', '3nf', 'snowflake'] as const).map((t) => (
              <button
                key={t}
                onClick={() => setSchemaType(t)}
                className={clsx(
                  'px-4 py-1.5 text-xs font-medium capitalize transition-colors',
                  schemaType === t ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-gray-200'
                )}
              >
                {t === '3nf' ? '3NF (OLTP)' : t === 'star' ? 'Star (OLAP)' : 'Snowflake'}
              </button>
            ))}
          </div>
        </div>

        <div>
          <div className="text-xs text-gray-400 font-medium mb-2">SCD Strategy</div>
          <div className="flex rounded-lg overflow-hidden border border-gray-700">
            {([1, 2, 3] as const).map((t) => (
              <button
                key={t}
                onClick={() => setScdType(t)}
                className={clsx(
                  'px-4 py-1.5 text-xs font-medium transition-colors',
                  scdType === t ? 'bg-purple-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-gray-200'
                )}
              >
                Type {t}
              </button>
            ))}
          </div>
        </div>

        <div className="flex items-center gap-4 text-xs">
          <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded border-2 border-blue-600 bg-blue-900/20" /> Fact Table</span>
          <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded border-2 border-purple-600 bg-purple-900/20" /> Dimension Table</span>
        </div>
      </div>

      {/* SCD Info */}
      <div className="card p-4 bg-purple-900/10 border-purple-800/30">
        <div className="font-semibold text-sm text-purple-300 mb-1">{SCD_INFO[scdType].title}</div>
        <p className="text-xs text-gray-400 mb-2">{SCD_INFO[scdType].description}</p>
        <div className="grid grid-cols-2 gap-4 mb-2">
          <div><span className="text-xs text-green-400 font-medium">Pros: </span><span className="text-xs text-gray-400">{SCD_INFO[scdType].pros}</span></div>
          <div><span className="text-xs text-red-400 font-medium">Cons: </span><span className="text-xs text-gray-400">{SCD_INFO[scdType].cons}</span></div>
        </div>
        <pre className="code-block text-[10px] mt-2">{SCD_INFO[scdType].example}</pre>
      </div>

      {/* ER Diagram */}
      <div className="card overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-800">
          <h3 className="font-semibold text-gray-200">E-Commerce Data Warehouse — {schemaType === 'star' ? 'Star Schema' : schemaType === '3nf' ? '3NF (OLTP)' : 'Snowflake Schema'}</h3>
        </div>
        <div className="overflow-auto p-4">
          <div className="relative" style={{ width: 900, height: 640 }}>
            <svg className="absolute inset-0 w-full h-full pointer-events-none">
              {getRelationLines().map((line, i) => (
                <g key={i}>
                  <line x1={line.x1} y1={line.y1} x2={line.x2} y2={line.y2} stroke="#374151" strokeWidth="2" strokeDasharray="6,3" />
                  <text x={(line.x1 + line.x2) / 2} y={(line.y1 + line.y2) / 2 - 5} className="fill-gray-600 text-[9px]" textAnchor="middle" fontSize="9">{line.label}</text>
                </g>
              ))}
            </svg>

            {ECOMMERCE_STAR_SCHEMA.map((table) => (
              <div
                key={table.id}
                onClick={() => setSelectedTable(selectedTable === table.id ? null : table.id)}
                className={clsx(
                  'absolute w-48 rounded-xl border-2 cursor-pointer transition-all',
                  TABLE_COLORS[table.tableType],
                  selectedTable === table.id && 'ring-2 ring-blue-500 ring-offset-2 ring-offset-gray-900',
                )}
                style={{ left: table.x, top: table.y }}
              >
                <div className={clsx(
                  'px-3 py-2 border-b border-opacity-30 flex items-center gap-2',
                  table.tableType === 'fact' ? 'border-blue-600' : 'border-purple-600'
                )}>
                  {table.tableType === 'fact' ? (
                    <Star size={12} className="text-blue-400" />
                  ) : (
                    <Link size={12} className="text-purple-400" />
                  )}
                  <span className="text-xs font-bold text-gray-100">{table.name}</span>
                </div>
                <div className="px-3 py-2 space-y-1 max-h-48 overflow-y-auto">
                  {table.columns.map((col) => (
                    <div key={col.name} className="flex items-center gap-1.5 text-[10px]">
                      {col.pk && <span className="text-yellow-400 font-bold">PK</span>}
                      {col.fk && <span className="text-blue-400 font-bold">FK</span>}
                      {!col.pk && !col.fk && <span className="w-4" />}
                      <span className={clsx('font-mono', col.pk ? 'text-yellow-300' : col.fk ? 'text-blue-300' : 'text-gray-400')}>{col.name}</span>
                      {scdType === 2 && col.scd_tracked && <span className="text-purple-400 text-[8px]">SCD</span>}
                    </div>
                  ))}
                  {scdType === 2 && table.tableType === 'dimension' && (
                    <>
                      <div className="text-[10px] text-purple-400 font-mono border-t border-gray-700 pt-1 mt-1">+ effective_from</div>
                      <div className="text-[10px] text-purple-400 font-mono">+ effective_to</div>
                      <div className="text-[10px] text-purple-400 font-mono">+ is_current</div>
                    </>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* DDL */}
      {showDDL && (
        <div className="card overflow-hidden animate-slide-in">
          <div className="px-5 py-4 border-b border-gray-800 flex items-center justify-between">
            <h3 className="font-semibold text-gray-200">Generated DDL SQL</h3>
            <button onClick={() => navigator.clipboard.writeText(ddl)} className="btn-secondary text-xs">
              <Copy size={12} /> Copy SQL
            </button>
          </div>
          <pre className="code-block m-4 text-xs leading-relaxed overflow-x-auto max-h-96 text-cyan-300">{ddl}</pre>
        </div>
      )}
    </div>
  )
}
