import { useState, useEffect } from 'react'
import { api } from '../utils/api'

const ep = {
  modules: '/api/iac/modules',
  generate: '/api/iac/generate',
  plan: '/api/iac/plan',
  gitops: '/api/iac/gitops-flow',
  secrets: '/api/iac/secrets',
}

function SectionTab({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return <button onClick={onClick} className={`px-4 py-2 text-sm font-medium rounded-lg transition-all ${active ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white hover:bg-gray-700'}`}>{label}</button>
}

function TerraformPlayground() {
  const [modules, setModules] = useState<any[]>([])
  const [selectedModule, setSelectedModule] = useState<any>(null)
  const [vars, setVars] = useState<Record<string, string>>({})
  const [hcl, setHcl] = useState('')
  const [plan, setPlan] = useState('')

  useEffect(() => { api.get(ep.modules).then(r => setModules(r.data.modules)) }, [])

  const selectModule = (mod: any) => {
    setSelectedModule(mod)
    const defaults: Record<string, string> = {}
    mod.variables.forEach((v: any) => { defaults[v.name] = String(v.default) })
    setVars(defaults)
    setHcl('')
    setPlan('')
  }

  const generate = async () => {
    const res = await api.post(ep.generate, { module_id: selectedModule.id, variables: vars })
    setHcl(res.data.hcl)
  }

  const showPlan = async () => {
    const res = await api.get(`${ep.plan}?module_id=${selectedModule.id}`)
    setPlan(res.data.plan_output)
  }

  return (
    <div className="space-y-4">
      <div className="text-sm text-gray-400 mb-3">Select a pre-built Terraform module, configure variables, and generate production-ready HCL.</div>
      <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
        {modules.map(m => (
          <div key={m.id} onClick={() => selectModule(m)} className={`p-3 rounded-lg border cursor-pointer transition-all ${selectedModule?.id === m.id ? 'border-blue-500 bg-blue-900/20' : 'border-gray-700 bg-gray-800 hover:border-gray-600'}`}>
            <div className="text-sm font-semibold text-white mb-1">{m.name}</div>
            <div className="text-xs text-gray-400 mb-2">{m.description}</div>
            <div className="text-xs text-orange-400">{m.provider}</div>
          </div>
        ))}
      </div>
      {selectedModule && (
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4 space-y-4">
          <div className="text-sm font-semibold text-white">Configure: {selectedModule.name}</div>
          <div className="grid grid-cols-2 gap-3">
            {selectedModule.variables.map((v: any) => (
              <div key={v.name}>
                <label className="text-xs text-gray-400 mb-1 block">{v.name} — <span className="text-gray-500">{v.description}</span></label>
                <input value={vars[v.name] || ''} onChange={e => setVars(p => ({ ...p, [v.name]: e.target.value }))} className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-sm text-white font-mono" />
              </div>
            ))}
          </div>
          <div className="text-xs text-gray-500">Resources to create: {selectedModule.resources_created.join(', ')}</div>
          <div className="flex gap-2">
            <button onClick={generate} className="px-4 py-2 bg-orange-600 text-white rounded-lg text-sm hover:bg-orange-700">Generate HCL</button>
            {hcl && <button onClick={showPlan} className="px-4 py-2 bg-gray-700 text-gray-300 rounded-lg text-sm hover:bg-gray-600">terraform plan</button>}
          </div>
          {hcl && (
            <div>
              <div className="text-xs text-gray-500 mb-1 font-mono">modules/{selectedModule.id.replace('mod-', '')}/main.tf</div>
              <pre className="text-xs text-green-300 font-mono bg-gray-900 rounded p-3 overflow-x-auto max-h-80 whitespace-pre">{hcl}</pre>
            </div>
          )}
          {plan && (
            <div>
              <div className="text-xs text-gray-500 mb-1">terraform plan output</div>
              <pre className="text-xs text-yellow-300 font-mono bg-gray-900 rounded p-3 overflow-x-auto whitespace-pre">{plan}</pre>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function GitOpsWorkflow() {
  const [data, setData] = useState<any>(null)

  useEffect(() => { api.get(ep.gitops).then(r => setData(r.data)) }, [])

  if (!data) return <div className="text-gray-500 py-8 text-center">Loading...</div>

  return (
    <div className="space-y-6">
      <div className="text-sm font-semibold text-white mb-3">GitOps Flow for Infrastructure Changes</div>
      <div className="space-y-2">
        {data.steps.map((s: any) => (
          <div key={s.order} className="flex gap-3 items-start p-3 bg-gray-800 rounded-lg border border-gray-700">
            <span className="w-6 h-6 rounded-full bg-blue-600 text-white text-xs flex items-center justify-center flex-shrink-0 font-bold">{s.order}</span>
            <div>
              <div className="text-sm font-semibold text-white">{s.name}</div>
              <div className="text-xs text-gray-400">{s.description}</div>
            </div>
          </div>
        ))}
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="bg-red-900/10 border border-red-700/40 rounded-xl p-4">
          <div className="text-sm font-semibold text-red-400 mb-2">Local State (Anti-Pattern)</div>
          <div className="text-xs text-gray-300">{data.state_management.local.problem}</div>
        </div>
        <div className="bg-green-900/10 border border-green-700/40 rounded-xl p-4">
          <div className="text-sm font-semibold text-green-400 mb-2">Remote State (S3 + DynamoDB)</div>
          <div className="space-y-1 text-xs text-gray-300 mb-2">
            {data.state_management.remote_s3.benefits.map((b: string, i: number) => <div key={i}>✓ {b}</div>)}
          </div>
          <pre className="text-xs text-blue-300 font-mono overflow-x-auto whitespace-pre">{data.state_management.remote_s3.config}</pre>
        </div>
      </div>
    </div>
  )
}

function SecretsManagement() {
  const [data, setData] = useState<any>(null)
  const [tab, setTab] = useState<'vault' | 'aws'>('vault')

  useEffect(() => { api.get(ep.secrets).then(r => setData(r.data)) }, [])

  if (!data) return <div className="text-gray-500 py-8 text-center">Loading...</div>

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        <button onClick={() => setTab('vault')} className={`px-3 py-1.5 text-sm rounded-lg ${tab === 'vault' ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300'}`}>HashiCorp Vault</button>
        <button onClick={() => setTab('aws')} className={`px-3 py-1.5 text-sm rounded-lg ${tab === 'aws' ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300'}`}>AWS Secrets Manager</button>
      </div>
      {tab === 'vault' && (
        <div className="space-y-4">
          <div className="text-sm text-gray-300">{data.vault.description}</div>
          {data.vault.patterns.map((p: any, i: number) => (
            <div key={i} className="bg-gray-800 rounded-xl border border-gray-700 p-4">
              <div className="text-sm font-semibold text-white mb-1">{p.name}</div>
              <div className="text-xs text-gray-400 mb-3">{p.description}</div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div>
                  <div className="text-xs text-gray-500 mb-1">Request</div>
                  <pre className="text-xs text-yellow-300 font-mono bg-gray-900 rounded p-2">{p.request}</pre>
                </div>
                <div>
                  <div className="text-xs text-gray-500 mb-1">Response</div>
                  <pre className="text-xs text-green-300 font-mono bg-gray-900 rounded p-2">{JSON.stringify(p.response, null, 2)}</pre>
                </div>
              </div>
              <div className="mt-2 text-xs text-blue-300">{p.benefit}</div>
            </div>
          ))}
        </div>
      )}
      {tab === 'aws' && (
        <div className="space-y-4">
          <div className="text-sm text-gray-300">{data.aws_secrets_manager.description}</div>
          <div className="grid grid-cols-2 gap-4">
            {Object.entries(data.aws_secrets_manager.vs_parameter_store).map(([name, features]: any) => (
              <div key={name} className="bg-gray-800 rounded-xl border border-gray-700 p-4">
                <div className="text-sm font-semibold text-blue-400 mb-2 capitalize">{name.replace('_', ' ')}</div>
                {features.map((f: string, i: number) => <div key={i} className="text-xs text-gray-300 mb-1">• {f}</div>)}
              </div>
            ))}
          </div>
          <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
            <div className="text-sm font-semibold text-white mb-2">Correct Pattern — Retrieve at Runtime</div>
            <pre className="text-xs text-green-300 font-mono bg-gray-900 rounded p-3 whitespace-pre">{data.aws_secrets_manager.retrieve_example}</pre>
          </div>
        </div>
      )}
    </div>
  )
}

const SECTIONS = ['Terraform Playground', 'GitOps Workflow', 'Secrets Management']

export default function IaC() {
  const [activeSection, setActiveSection] = useState(0)
  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-800 flex-shrink-0">
        <div className="flex items-center gap-3 mb-1">
          <span className="text-xs font-mono text-gray-500 bg-gray-800 px-2 py-0.5 rounded">MODULE 19</span>
          <h1 className="text-xl font-bold text-white">Infrastructure as Code (IaC)</h1>
        </div>
        <p className="text-sm text-gray-400">Terraform modules for data infrastructure, GitOps workflows, and secrets management patterns</p>
      </div>
      <div className="flex gap-2 px-6 py-3 border-b border-gray-800 flex-shrink-0">
        {SECTIONS.map((s, i) => <SectionTab key={s} label={s} active={activeSection === i} onClick={() => setActiveSection(i)} />)}
      </div>
      <div className="flex-1 overflow-y-auto p-6">
        {activeSection === 0 && <TerraformPlayground />}
        {activeSection === 1 && <GitOpsWorkflow />}
        {activeSection === 2 && <SecretsManagement />}
        <div className="mt-8 bg-blue-900/20 border border-blue-700/30 rounded-xl p-5">
          <div className="text-sm font-semibold text-blue-400 mb-2">IaC is Mandatory at Scale</div>
          <div className="text-sm text-gray-300 space-y-2">
            <p><strong className="text-white">Why IaC:</strong> Without IaC, your infrastructure is undocumented, unreproducible, and untested. Terraform gives you version control, code review, and automated change management for every infrastructure resource.</p>
            <p><strong className="text-white">Drift detection:</strong> Someone manually changes a security group in the console? Terraform detects the drift on the next plan run. The code becomes the source of truth, not the console.</p>
            <p><strong className="text-white">Never hardcode secrets:</strong> Use Vault dynamic secrets or AWS Secrets Manager. Credentials in code or environment variables get leaked in logs, container images, and git history. Dynamic secrets auto-expire — even if leaked, they're worthless in hours.</p>
          </div>
        </div>
      </div>
    </div>
  )
}
