import { useState, useEffect } from 'react'
import { api, endpoints } from '../utils/api'
import ExplanationPanel from '../components/ExplanationPanel'
import { Shield, Lock, Eye, EyeOff, CheckCircle, XCircle, AlertTriangle, User } from 'lucide-react'
import clsx from 'clsx'

interface PIIColumn {
  column: string
  pii_detected: boolean
  pii_type: string | null
  confidence: number | null
  sample_values: (string | number)[]
  recommendation: string | null
}

interface PIIScanResult {
  dataset_id: string
  total_columns: number
  pii_columns: number
  risk_level: string
  columns: PIIColumn[]
  scanned_at: string
}

interface AccessRow {
  table: string
  Analyst: string
  'Senior Analyst': string
  'Data Engineer': string
  Admin: string
}

interface AccessMatrix {
  roles: string[]
  permissions: AccessRow[]
}

interface ComplianceTable {
  table: string
  score: number
  items: Array<{ table: string; item: string; status: boolean; detail: string }>
}

interface AuditEntry {
  log_id: string
  user_email: string
  action: string
  resource: string
  ip_address: string
  timestamp: string
}

const RISK_CONFIG = {
  HIGH: { color: 'text-red-400', bg: 'bg-red-900/20 border-red-800/40', label: 'HIGH RISK' },
  MEDIUM: { color: 'text-yellow-400', bg: 'bg-yellow-900/20 border-yellow-800/40', label: 'MEDIUM RISK' },
  LOW: { color: 'text-green-400', bg: 'bg-green-900/20 border-green-800/40', label: 'LOW RISK' },
}

const PERM_COLORS: Record<string, string> = {
  'ALL': 'text-red-400 bg-red-900/20',
  'DENY': 'text-gray-600 bg-gray-800',
  'SELECT': 'text-green-400 bg-green-900/20',
  'SELECT (masked)': 'text-yellow-400 bg-yellow-900/20',
  'SELECT, INSERT': 'text-blue-400 bg-blue-900/20',
  'SELECT, INSERT, UPDATE': 'text-purple-400 bg-purple-900/20',
}

const DEMO_MASK = {
  email: ['alice@example.com', 'bob@company.org', 'carol@gmail.com'],
  phone: ['+1-555-0100', '555-987-6543', '(212) 555-0199'],
  name: ['Alice Johnson', 'Bob Smith', 'Carol White'],
}

// ── RBAC Manager ─────────────────────────────────────────────────────────────
const ROLES = [
  { name: 'Admin', color: 'text-red-400', bg: 'bg-red-900/20 border-red-800', perms: ['ALL'] },
  { name: 'Data Engineer', color: 'text-purple-400', bg: 'bg-purple-900/20 border-purple-800', perms: ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE'] },
  { name: 'Senior Analyst', color: 'text-blue-400', bg: 'bg-blue-900/20 border-blue-800', perms: ['SELECT', 'INSERT'] },
  { name: 'Analyst', color: 'text-green-400', bg: 'bg-green-900/20 border-green-800', perms: ['SELECT (masked)'] },
  { name: 'ML Engineer', color: 'text-yellow-400', bg: 'bg-yellow-900/20 border-yellow-800', perms: ['SELECT', 'INSERT'] },
  { name: 'Viewer', color: 'text-gray-400', bg: 'bg-gray-700/30 border-gray-700', perms: ['SELECT (masked)'] },
]

const USERS = [
  { name: 'alice@company.com', role: 'Admin', lastLogin: '2h ago', mfa: true },
  { name: 'bob@company.com', role: 'Data Engineer', lastLogin: '4h ago', mfa: true },
  { name: 'carol@company.com', role: 'Senior Analyst', lastLogin: '1d ago', mfa: false },
  { name: 'dave@company.com', role: 'Analyst', lastLogin: '3h ago', mfa: true },
  { name: 'eve@company.com', role: 'ML Engineer', lastLogin: '6h ago', mfa: true },
  { name: 'frank@company.com', role: 'Viewer', lastLogin: '5d ago', mfa: false },
]

function RBACManager() {
  const [selectedRole, setSelectedRole] = useState(ROLES[1])
  const [userRoles, setUserRoles] = useState<Record<string, string>>(
    Object.fromEntries(USERS.map(u => [u.name, u.role]))
  )

  const LAYER_PERMS: Record<string, { layer: string; tables: string[]; granted: string[] }[]> = {
    Admin: [
      { layer: 'Gold', tables: ['fact_orders', 'dim_customer', 'dim_product'], granted: ['SELECT', 'INSERT', 'UPDATE', 'DELETE'] },
      { layer: 'Silver', tables: ['stg_orders', 'stg_customers'], granted: ['ALL'] },
      { layer: 'Bronze', tables: ['raw_*'], granted: ['ALL'] },
    ],
    'Data Engineer': [
      { layer: 'Gold', tables: ['fact_orders', 'dim_customer'], granted: ['SELECT', 'INSERT', 'UPDATE'] },
      { layer: 'Silver', tables: ['stg_orders', 'stg_customers'], granted: ['SELECT', 'INSERT', 'UPDATE', 'DELETE'] },
      { layer: 'Bronze', tables: ['raw_*'], granted: ['SELECT', 'INSERT'] },
    ],
    'Senior Analyst': [
      { layer: 'Gold', tables: ['fact_orders', 'dim_customer', 'dim_product'], granted: ['SELECT'] },
      { layer: 'Silver', tables: ['stg_orders'], granted: ['SELECT'] },
      { layer: 'Bronze', tables: [], granted: [] },
    ],
    Analyst: [
      { layer: 'Gold', tables: ['fact_orders (masked PII)', 'dim_product'], granted: ['SELECT'] },
      { layer: 'Silver', tables: [], granted: [] },
      { layer: 'Bronze', tables: [], granted: [] },
    ],
    'ML Engineer': [
      { layer: 'Gold', tables: ['fact_orders', 'dim_customer'], granted: ['SELECT'] },
      { layer: 'Silver', tables: ['stg_orders', 'stg_customers'], granted: ['SELECT', 'INSERT'] },
      { layer: 'Bronze', tables: [], granted: [] },
    ],
    Viewer: [
      { layer: 'Gold', tables: ['fact_orders (masked PII)'], granted: ['SELECT'] },
      { layer: 'Silver', tables: [], granted: [] },
      { layer: 'Bronze', tables: [], granted: [] },
    ],
  }

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
      {/* Role Definitions */}
      <div className="space-y-2">
        <h3 className="text-sm font-semibold text-gray-200 mb-2">Role Definitions</h3>
        {ROLES.map(role => (
          <div
            key={role.name}
            onClick={() => setSelectedRole(role)}
            className={clsx('p-3 rounded border cursor-pointer transition-all', role.bg, selectedRole.name === role.name ? 'ring-1 ring-blue-500' : '')}
          >
            <div className="flex items-center justify-between">
              <span className={clsx('text-sm font-medium', role.color)}>{role.name}</span>
              <span className="text-xs text-gray-500">{USERS.filter(u => u.role === role.name).length} users</span>
            </div>
            <div className="flex flex-wrap gap-1 mt-1">
              {role.perms.map(p => (
                <span key={p} className="text-[10px] font-mono bg-gray-900 text-gray-300 px-1.5 py-0.5 rounded">{p}</span>
              ))}
            </div>
          </div>
        ))}
      </div>

      {/* Layer Permissions */}
      <div className="space-y-2">
        <h3 className="text-sm font-semibold text-gray-200 mb-2">{selectedRole.name} — Layer Access</h3>
        {(LAYER_PERMS[selectedRole.name] || []).map(lp => (
          <div key={lp.layer} className="card p-3">
            <div className="flex items-center gap-2 mb-2">
              <span className={clsx('w-2 h-2 rounded-full', lp.layer === 'Gold' ? 'bg-yellow-500' : lp.layer === 'Silver' ? 'bg-gray-400' : 'bg-orange-700')} />
              <span className="text-xs font-semibold text-gray-300">{lp.layer} Layer</span>
            </div>
            {lp.tables.length === 0 ? (
              <p className="text-xs text-gray-600">No access</p>
            ) : (
              <>
                <div className="flex flex-wrap gap-1 mb-1">
                  {lp.granted.map(g => (
                    <span key={g} className={clsx('text-[10px] font-mono px-1.5 py-0.5 rounded', PERM_COLORS[g] || 'text-blue-400 bg-blue-900/20')}>{g}</span>
                  ))}
                </div>
                <div className="text-[10px] text-gray-500 font-mono">{lp.tables.join(', ')}</div>
              </>
            )}
          </div>
        ))}
      </div>

      {/* Users */}
      <div className="space-y-2">
        <h3 className="text-sm font-semibold text-gray-200 mb-2">User Assignments</h3>
        {USERS.map(user => (
          <div key={user.name} className="card p-3 flex items-center gap-3">
            <div className="w-7 h-7 rounded-full bg-blue-900/40 flex items-center justify-center flex-shrink-0">
              <User size={12} className="text-blue-400" />
            </div>
            <div className="flex-1 min-w-0">
              <p className="text-xs text-gray-200 truncate">{user.name}</p>
              <div className="flex items-center gap-2 mt-0.5">
                <select
                  value={userRoles[user.name]}
                  onChange={e => setUserRoles(prev => ({ ...prev, [user.name]: e.target.value }))}
                  className="text-[10px] bg-gray-900 border border-gray-700 rounded text-gray-300 px-1 py-0.5"
                >
                  {ROLES.map(r => <option key={r.name}>{r.name}</option>)}
                </select>
                {user.mfa
                  ? <span className="text-[10px] text-green-400">MFA ✓</span>
                  : <span className="text-[10px] text-red-400">No MFA</span>}
              </div>
            </div>
            <span className="text-[10px] text-gray-600">{user.lastLogin}</span>
          </div>
        ))}
        <div className="p-2 bg-yellow-900/20 border border-yellow-800 rounded text-xs text-yellow-300">
          {USERS.filter(u => !u.mfa).length} users without MFA — review required
        </div>
      </div>
    </div>
  )
}

// ── Retention Policies ────────────────────────────────────────────────────────
const RETENTION_POLICIES = [
  { table: 'raw.orders', layer: 'Bronze', retention_days: 90, regulation: 'Internal', action: 'DELETE', row_count: 12_400_000, size_gb: 28.4, next_purge: '2026-04-15', status: 'active' },
  { table: 'stg.customers', layer: 'Silver', retention_days: 365, regulation: 'GDPR', action: 'ANONYMIZE', row_count: 890_000, size_gb: 4.2, next_purge: '2026-06-01', status: 'active' },
  { table: 'gold.fact_orders', layer: 'Gold', retention_days: 2555, regulation: 'SOX', action: 'ARCHIVE', row_count: 45_200_000, size_gb: 112.8, next_purge: '2033-01-01', status: 'active' },
  { table: 'raw.events', layer: 'Bronze', retention_days: 30, regulation: 'Internal', action: 'DELETE', row_count: 340_000_000, size_gb: 820.0, next_purge: '2026-03-30', status: 'warning' },
  { table: 'stg.payments', layer: 'Silver', retention_days: 2555, regulation: 'PCI-DSS', action: 'ENCRYPT+ARCHIVE', row_count: 7_600_000, size_gb: 18.9, next_purge: '2033-01-01', status: 'active' },
  { table: 'raw.logs', layer: 'Bronze', retention_days: 14, regulation: 'Internal', action: 'DELETE', row_count: 1_800_000_000, size_gb: 2100.0, next_purge: '2026-03-20', status: 'overdue' },
]

function RetentionPolicies() {
  const [policies, setPolicies] = useState(RETENTION_POLICIES)
  const [showForm, setShowForm] = useState(false)
  const [newPolicy, setNewPolicy] = useState({ table: '', layer: 'Bronze', retention_days: 90, regulation: 'Internal', action: 'DELETE' })

  const statusConfig = {
    active: { color: 'text-green-400', bg: 'bg-green-900/20 border-green-800' },
    warning: { color: 'text-yellow-400', bg: 'bg-yellow-900/20 border-yellow-800' },
    overdue: { color: 'text-red-400', bg: 'bg-red-900/20 border-red-800' },
  } as Record<string, { color: string; bg: string }>

  const layerColor = { Bronze: 'text-orange-400', Silver: 'text-gray-300', Gold: 'text-yellow-400' } as Record<string, string>

  const totalReclaimable = policies
    .filter(p => p.status === 'overdue' || p.status === 'warning')
    .reduce((acc, p) => acc + p.size_gb, 0)

  const addPolicy = () => {
    if (!newPolicy.table) return
    setPolicies(prev => [...prev, { ...newPolicy, row_count: 0, size_gb: 0, next_purge: 'TBD', status: 'active' }])
    setShowForm(false)
    setNewPolicy({ table: '', layer: 'Bronze', retention_days: 90, regulation: 'Internal', action: 'DELETE' })
  }

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-3 gap-3">
        <div className="card p-4 text-center">
          <p className="text-2xl font-bold text-white">{policies.length}</p>
          <p className="text-xs text-gray-400 mt-1">Total Policies</p>
        </div>
        <div className="card p-4 text-center">
          <p className="text-2xl font-bold text-red-400">{policies.filter(p => p.status === 'overdue').length}</p>
          <p className="text-xs text-gray-400 mt-1">Overdue Purges</p>
        </div>
        <div className="card p-4 text-center">
          <p className="text-2xl font-bold text-yellow-400">{totalReclaimable.toFixed(0)} GB</p>
          <p className="text-xs text-gray-400 mt-1">Reclaimable Storage</p>
        </div>
      </div>

      <div className="flex justify-between items-center">
        <h3 className="text-sm font-semibold text-gray-200">Retention Policy Registry</h3>
        <button onClick={() => setShowForm(!showForm)} className="btn-primary text-xs">
          + Add Policy
        </button>
      </div>

      {showForm && (
        <div className="card p-4 space-y-3 border-blue-800">
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            <div className="col-span-2">
              <label className="text-xs text-gray-400">Table name</label>
              <input value={newPolicy.table} onChange={e => setNewPolicy(p => ({ ...p, table: e.target.value }))}
                className="input text-xs mt-1 w-full" placeholder="schema.table_name" />
            </div>
            <div>
              <label className="text-xs text-gray-400">Layer</label>
              <select value={newPolicy.layer} onChange={e => setNewPolicy(p => ({ ...p, layer: e.target.value }))}
                className="select text-xs mt-1 w-full">
                {['Bronze', 'Silver', 'Gold'].map(l => <option key={l}>{l}</option>)}
              </select>
            </div>
            <div>
              <label className="text-xs text-gray-400">Retention (days)</label>
              <input type="number" value={newPolicy.retention_days}
                onChange={e => setNewPolicy(p => ({ ...p, retention_days: Number(e.target.value) }))}
                className="input text-xs mt-1 w-full" />
            </div>
            <div>
              <label className="text-xs text-gray-400">Regulation</label>
              <select value={newPolicy.regulation} onChange={e => setNewPolicy(p => ({ ...p, regulation: e.target.value }))}
                className="select text-xs mt-1 w-full">
                {['Internal', 'GDPR', 'CCPA', 'SOX', 'PCI-DSS', 'HIPAA'].map(r => <option key={r}>{r}</option>)}
              </select>
            </div>
          </div>
          <div className="flex gap-2">
            <button onClick={addPolicy} className="btn-primary text-xs">Save Policy</button>
            <button onClick={() => setShowForm(false)} className="btn-secondary text-xs">Cancel</button>
          </div>
        </div>
      )}

      <div className="overflow-x-auto card">
        <table className="w-full text-sm">
          <thead className="bg-gray-900 border-b border-gray-800">
            <tr>
              {['Table', 'Layer', 'Retention', 'Regulation', 'Action', 'Size', 'Next Purge', 'Status'].map(h => (
                <th key={h} className="table-header px-4 py-3">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-800">
            {policies.map((p, i) => {
              const cfg = statusConfig[p.status] || statusConfig.active
              return (
                <tr key={i} className="hover:bg-gray-800/20">
                  <td className="table-cell px-4 font-mono text-xs text-gray-200">{p.table}</td>
                  <td className="table-cell px-4">
                    <span className={clsx('text-xs font-medium', layerColor[p.layer])}>{p.layer}</span>
                  </td>
                  <td className="table-cell px-4 text-xs text-gray-300">{p.retention_days}d</td>
                  <td className="table-cell px-4">
                    <span className="text-[10px] font-mono bg-gray-800 text-blue-300 px-1.5 py-0.5 rounded">{p.regulation}</span>
                  </td>
                  <td className="table-cell px-4 text-xs text-gray-400 font-mono">{p.action}</td>
                  <td className="table-cell px-4 text-xs font-mono text-gray-300">{p.size_gb.toFixed(1)} GB</td>
                  <td className="table-cell px-4 text-xs text-gray-500">{p.next_purge}</td>
                  <td className="table-cell px-4">
                    <span className={clsx('text-[10px] px-2 py-0.5 rounded border font-medium', cfg.bg, cfg.color)}>
                      {p.status}
                    </span>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default function Governance() {
  const [scanDataset, setScanDataset] = useState('ds-customers')
  const [scanResult, setScanResult] = useState<PIIScanResult | null>(null)
  const [scanning, setScanning] = useState(false)
  const [accessMatrix, setAccessMatrix] = useState<AccessMatrix | null>(null)
  const [compliance, setCompliance] = useState<ComplianceTable[]>([])
  const [auditLog, setAuditLog] = useState<AuditEntry[]>([])
  const [maskDemo, setMaskDemo] = useState<Record<string, unknown> | null>(null)
  const [maskType, setMaskType] = useState<'email' | 'phone' | 'name'>('email')
  const [showOriginal, setShowOriginal] = useState(false)
  const [activeTab, setActiveTab] = useState('pii')

  useEffect(() => {
    Promise.all([
      api.get(endpoints.accessMatrix),
      api.get(endpoints.compliance),
      api.get(endpoints.auditLog + '?limit=30'),
    ]).then(([accRes, compRes, auditRes]) => {
      setAccessMatrix(accRes.data)
      setCompliance(compRes.data.compliance)
      setAuditLog(auditRes.data.logs)
    })
  }, [])

  const runScan = async () => {
    setScanning(true)
    try {
      const r = await api.post(endpoints.scanPII, { dataset_id: scanDataset })
      setScanResult(r.data)
    } finally {
      setScanning(false)
    }
  }

  const runMaskDemo = async () => {
    const values = DEMO_MASK[maskType]
    const r = await api.post(endpoints.maskDemo, {
      column_name: maskType,
      pii_type: maskType,
      values,
    })
    setMaskDemo(r.data)
  }

  const formatTime = (iso: string) => new Date(iso).toLocaleString()

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Data Governance & Security</h1>
        <p className="text-sm text-gray-400 mt-1">PII detection, data masking, access control, and compliance</p>
      </div>

      <ExplanationPanel
        title="Data Governance & Security"
        what="Data governance ensures data is used appropriately, securely, and in compliance with regulations (GDPR, CCPA, HIPAA). It encompasses PII detection, access controls, data masking, audit logging, and retention policies."
        why="A single GDPR violation can cost up to €20M or 4% of global annual turnover. In 2023, the average cost of a data breach was $4.45M. For data engineers, governance is not optional — it's a core engineering responsibility, not a compliance afterthought."
        how="Senior engineers implement privacy-by-design: PII columns are automatically detected at ingestion, tagged in the catalog, masked in non-production environments, and access-controlled by role. They build automated GDPR workflows for right-to-delete requests and maintain immutable audit logs."
        tools={['Apache Ranger', 'AWS Lake Formation', 'Privacera', 'BigID', 'Immuta']}
        seniorTip="Implement column-level encryption for PII and use tokenization for foreign keys. This means analyst-facing tables never contain raw email or SSN — only tokens that can be resolved by the tokenization service. Analysts can still join on customer_token, but they can't see the actual email."
      />

      {/* Tabs */}
      <div className="flex gap-1 border-b border-gray-800">
        {['pii', 'masking', 'access', 'compliance', 'audit', 'rbac', 'retention'].map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={clsx(
              'px-4 py-2 text-sm font-medium border-b-2 -mb-px capitalize transition-colors',
              activeTab === tab ? 'border-blue-500 text-blue-400' : 'border-transparent text-gray-500 hover:text-gray-300'
            )}
          >
            {tab === 'pii' ? 'PII Scanner' : tab === 'masking' ? 'Data Masking' : tab === 'access' ? 'Access Control' : tab === 'compliance' ? 'Compliance' : tab === 'audit' ? 'Audit Log' : tab === 'rbac' ? 'RBAC Manager' : 'Retention Policies'}
          </button>
        ))}
      </div>

      {/* PII Scanner */}
      {activeTab === 'pii' && (
        <div className="space-y-5 animate-slide-in">
          <div className="card p-5 flex items-end gap-4 flex-wrap">
            <div>
              <label className="text-xs text-gray-400 block mb-1">Dataset to Scan</label>
              <select className="select w-48" value={scanDataset} onChange={(e) => setScanDataset(e.target.value)}>
                <option value="ds-customers">dim_customer (silver)</option>
                <option value="ds-orders">fact_orders (gold)</option>
              </select>
            </div>
            <button onClick={runScan} disabled={scanning} className="btn-primary">
              {scanning ? <span className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" /> : <Shield size={14} />}
              Scan for PII
            </button>
          </div>

          {scanResult && (
            <div className="space-y-4 animate-slide-in">
              <div className="grid grid-cols-4 gap-4">
                <div className={clsx('card p-4 border', RISK_CONFIG[scanResult.risk_level as keyof typeof RISK_CONFIG].bg)}>
                  <div className="text-xs text-gray-500 mb-1">Risk Level</div>
                  <div className={clsx('text-xl font-bold', RISK_CONFIG[scanResult.risk_level as keyof typeof RISK_CONFIG].color)}>
                    {RISK_CONFIG[scanResult.risk_level as keyof typeof RISK_CONFIG].label}
                  </div>
                </div>
                <div className="card p-4">
                  <div className="text-xs text-gray-500 mb-1">Total Columns</div>
                  <div className="text-xl font-bold text-white">{scanResult.total_columns}</div>
                </div>
                <div className="card p-4">
                  <div className="text-xs text-gray-500 mb-1">PII Columns</div>
                  <div className="text-xl font-bold text-red-400">{scanResult.pii_columns}</div>
                </div>
                <div className="card p-4">
                  <div className="text-xs text-gray-500 mb-1">Clean Columns</div>
                  <div className="text-xl font-bold text-green-400">{scanResult.total_columns - scanResult.pii_columns}</div>
                </div>
              </div>

              <div className="card overflow-hidden">
                <div className="px-5 py-4 border-b border-gray-800 font-semibold text-gray-200">Column Analysis</div>
                <div className="divide-y divide-gray-800">
                  {scanResult.columns.map((col) => (
                    <div key={col.column} className={clsx('px-5 py-4', col.pii_detected ? 'bg-red-900/5' : '')}>
                      <div className="flex items-start gap-4">
                        <div className="flex-shrink-0 mt-0.5">
                          {col.pii_detected
                            ? <AlertTriangle size={16} className="text-red-400" />
                            : <CheckCircle size={16} className="text-green-400" />}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-3 flex-wrap mb-1">
                            <span className="font-mono text-sm text-gray-200">{col.column}</span>
                            {col.pii_detected && (
                              <>
                                <span className="badge-error text-[10px]">{col.pii_type?.toUpperCase()}</span>
                                <span className="text-xs text-gray-500">Confidence: {((col.confidence || 0) * 100).toFixed(0)}%</span>
                              </>
                            )}
                          </div>
                          {col.pii_detected && col.recommendation && (
                            <p className="text-xs text-gray-400 mt-1">{col.recommendation}</p>
                          )}
                          <div className="flex flex-wrap gap-1 mt-2">
                            {col.sample_values.slice(0, 3).map((v, i) => (
                              <span key={i} className="text-[10px] font-mono bg-gray-800 border border-gray-700 px-1.5 py-0.5 rounded text-gray-400">
                                {String(v)}
                              </span>
                            ))}
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Data Masking */}
      {activeTab === 'masking' && (
        <div className="space-y-5 animate-slide-in">
          <div className="card p-5">
            <h3 className="font-semibold text-gray-200 mb-4">Data Masking Demo</h3>
            <div className="flex items-end gap-4 mb-4">
              <div>
                <label className="text-xs text-gray-400 block mb-1">PII Type</label>
                <select className="select w-32" value={maskType} onChange={(e) => setMaskType(e.target.value as 'email' | 'phone' | 'name')}>
                  <option value="email">Email</option>
                  <option value="phone">Phone</option>
                  <option value="name">Name</option>
                </select>
              </div>
              <button onClick={runMaskDemo} className="btn-primary">
                <Shield size={14} /> Run Masking Demo
              </button>
              <button onClick={() => setShowOriginal(!showOriginal)} className="btn-secondary">
                {showOriginal ? <EyeOff size={14} /> : <Eye size={14} />}
                {showOriginal ? 'Hide' : 'Show'} Original
              </button>
            </div>

            {maskDemo && (
              <div className="overflow-x-auto animate-slide-in">
                <table className="w-full text-sm">
                  <thead className="bg-gray-900 border-b border-gray-800">
                    <tr>
                      {showOriginal && <th className="table-header px-4 py-3">Original (PII)</th>}
                      <th className="table-header px-4 py-3">Masked</th>
                      <th className="table-header px-4 py-3">Tokenized</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-800">
                    {(maskDemo.rows as Array<{ original: string; masked: string; tokenized: string }>).map((row, i) => (
                      <tr key={i} className="hover:bg-gray-800/20">
                        {showOriginal && (
                          <td className="table-cell px-4 font-mono text-red-300">{row.original}</td>
                        )}
                        <td className="table-cell px-4 font-mono text-yellow-300">{row.masked}</td>
                        <td className="table-cell px-4 font-mono text-blue-300 text-xs">{row.tokenized}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            <div className="mt-4 p-4 bg-gray-800/30 rounded-lg">
              <div className="text-xs font-semibold text-gray-400 mb-2">Masking Strategies Explained</div>
              <div className="grid grid-cols-3 gap-3 text-xs text-gray-500">
                <div><span className="text-yellow-400 font-medium">Masking:</span> Partial redaction. Shows structure but hides sensitive chars. Good for debugging.</div>
                <div><span className="text-blue-400 font-medium">Tokenization:</span> Replace with random opaque token. Reversible by authorized systems only. Best for analytics.</div>
                <div><span className="text-green-400 font-medium">Hashing:</span> One-way transformation. Irreversible. Good for deduplication without exposing PII.</div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Access Matrix */}
      {activeTab === 'access' && accessMatrix && (
        <div className="animate-slide-in">
          <div className="card overflow-hidden">
            <div className="px-5 py-4 border-b border-gray-800">
              <h3 className="font-semibold text-gray-200">Role-Based Access Control Matrix</h3>
              <p className="text-xs text-gray-500 mt-1">Permissions by role × table. ALL = full admin access.</p>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-900 border-b border-gray-800">
                  <tr>
                    <th className="table-header px-5 py-3">Table</th>
                    {accessMatrix.roles.map((role) => (
                      <th key={role} className="table-header px-4 py-3 text-center">{role}</th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-800">
                  {accessMatrix.permissions.map((row) => (
                    <tr key={row.table} className="hover:bg-gray-800/20">
                      <td className="table-cell px-5 font-mono text-xs">{row.table}</td>
                      {accessMatrix.roles.map((role) => {
                        const perm = row[role as keyof AccessRow]
                        const colorClass = PERM_COLORS[perm] || 'text-gray-500'
                        return (
                          <td key={role} className="table-cell px-4 text-center">
                            <span className={clsx('text-xs px-2 py-0.5 rounded font-medium', colorClass)}>
                              {perm}
                            </span>
                          </td>
                        )
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* Compliance */}
      {activeTab === 'compliance' && (
        <div className="space-y-4 animate-slide-in">
          {compliance.map((tbl) => (
            <div key={tbl.table} className="card overflow-hidden">
              <div className="px-5 py-4 border-b border-gray-800 flex items-center justify-between">
                <div>
                  <h3 className="font-semibold text-gray-200">{tbl.table}</h3>
                  <p className="text-xs text-gray-500 mt-0.5">GDPR / CCPA Compliance Checklist</p>
                </div>
                <div className="text-right">
                  <div className={clsx('text-xl font-bold', tbl.score >= 80 ? 'text-green-400' : tbl.score >= 60 ? 'text-yellow-400' : 'text-red-400')}>
                    {tbl.score}%
                  </div>
                  <div className="text-xs text-gray-500">compliance</div>
                </div>
              </div>
              <div className="divide-y divide-gray-800">
                {tbl.items.map((item) => (
                  <div key={item.item} className="flex items-start gap-4 px-5 py-3">
                    {item.status
                      ? <CheckCircle size={16} className="text-green-400 flex-shrink-0 mt-0.5" />
                      : <XCircle size={16} className="text-red-400 flex-shrink-0 mt-0.5" />}
                    <div className="flex-1">
                      <div className={clsx('text-sm font-medium', item.status ? 'text-gray-200' : 'text-red-300')}>{item.item}</div>
                      <div className="text-xs text-gray-500 mt-0.5">{item.detail}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* RBAC Manager */}
      {activeTab === 'rbac' && (
        <div className="animate-slide-in space-y-4">
          <RBACManager />
        </div>
      )}

      {/* Retention Policies */}
      {activeTab === 'retention' && (
        <div className="animate-slide-in space-y-4">
          <RetentionPolicies />
        </div>
      )}

      {/* Audit Log */}
      {activeTab === 'audit' && (
        <div className="animate-slide-in">
          <div className="card overflow-hidden">
            <div className="px-5 py-4 border-b border-gray-800">
              <h3 className="font-semibold text-gray-200">Audit Log</h3>
              <p className="text-xs text-gray-500 mt-1">Immutable record of all data access events</p>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-900 border-b border-gray-800">
                  <tr>
                    <th className="table-header px-5 py-3">User</th>
                    <th className="table-header px-4 py-3">Action</th>
                    <th className="table-header px-4 py-3">Resource</th>
                    <th className="table-header px-4 py-3">IP Address</th>
                    <th className="table-header px-4 py-3">Timestamp</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-800">
                  {auditLog.map((entry) => (
                    <tr key={entry.log_id} className="hover:bg-gray-800/20">
                      <td className="table-cell px-5">
                        <div className="flex items-center gap-2">
                          <div className="w-6 h-6 rounded-full bg-blue-900/30 flex items-center justify-center">
                            <User size={12} className="text-blue-400" />
                          </div>
                          <span className="text-xs">{entry.user_email}</span>
                        </div>
                      </td>
                      <td className="table-cell px-4">
                        <span className={clsx(
                          'text-xs font-mono px-2 py-0.5 rounded',
                          entry.action === 'SELECT' ? 'text-green-400 bg-green-900/20' :
                          entry.action === 'DELETE' ? 'text-red-400 bg-red-900/20' :
                          entry.action === 'EXPORT' ? 'text-yellow-400 bg-yellow-900/20' :
                          'text-blue-400 bg-blue-900/20'
                        )}>{entry.action}</span>
                      </td>
                      <td className="table-cell px-4 font-mono text-xs text-gray-300">{entry.resource}</td>
                      <td className="table-cell px-4 font-mono text-xs text-gray-500">{entry.ip_address}</td>
                      <td className="table-cell px-4 text-xs text-gray-500">{formatTime(entry.timestamp)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
