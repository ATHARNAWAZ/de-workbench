import { useState, useEffect } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: 'http://localhost:8000/api' });

// ── helpers ──────────────────────────────────────────────────────────────────
const Card = ({ children, className = '', onClick }: { children: React.ReactNode; className?: string; onClick?: () => void }) => (
  <div className={`bg-gray-800 border border-gray-700 rounded-lg p-4 ${className}`} onClick={onClick}>{children}</div>
);

const Badge = ({ color, children }: { color: string; children: React.ReactNode }) => {
  const map: Record<string, string> = {
    green: 'bg-green-900 text-green-300',
    blue: 'bg-blue-900 text-blue-300',
    yellow: 'bg-yellow-900 text-yellow-300',
    red: 'bg-red-900 text-red-300',
    purple: 'bg-purple-900 text-purple-300',
    gray: 'bg-gray-700 text-gray-300',
  };
  return <span className={`px-2 py-0.5 rounded text-xs font-mono ${map[color] ?? map.gray}`}>{children}</span>;
};

const CodeBlock = ({ code }: { code: string }) => (
  <pre className="bg-gray-900 rounded p-3 text-xs text-green-300 overflow-x-auto whitespace-pre font-mono leading-relaxed">
    {code}
  </pre>
);

// ── DVC Simulator ─────────────────────────────────────────────────────────────
function DVCSimulator() {
  const [dvcConcepts, setDvcConcepts] = useState<any>(null);
  const [activeTab, setActiveTab] = useState<'dvc_file' | 'diff' | 'repro'>('dvc_file');
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    setLoading(true);
    api.get('/versioning/dvc-concepts')
      .then(r => setDvcConcepts(r.data))
      .finally(() => setLoading(false));
  }, []);

  const tabs: { key: 'dvc_file' | 'diff' | 'repro'; label: string }[] = [
    { key: 'dvc_file', label: '.dvc File Structure' },
    { key: 'diff', label: 'dvc diff Output' },
    { key: 'repro', label: 'dvc repro Pipeline' },
  ];

  if (loading) return <div className="text-gray-400 text-center py-8">Loading DVC concepts...</div>;
  if (!dvcConcepts) return null;

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card className="md:col-span-1">
          <h3 className="text-sm font-semibold text-white mb-3">DVC Overview</h3>
          <div className="space-y-2 text-xs text-gray-300">
            <p className="leading-relaxed">DVC (Data Version Control) works alongside Git to version datasets, models, and pipelines.</p>
            <div className="mt-3 space-y-1">
              <p className="text-yellow-300 font-semibold">Key Concepts:</p>
              <p>• <span className="text-green-300">.dvc files</span> — tiny pointers stored in Git</p>
              <p>• <span className="text-blue-300">Remote storage</span> — S3/GCS/Azure holds actual data</p>
              <p>• <span className="text-purple-300">dvc repro</span> — reruns only changed stages</p>
              <p>• <span className="text-red-300">dvc diff</span> — shows data changes between commits</p>
            </div>
            <div className="mt-3 pt-3 border-t border-gray-700">
              <p className="text-yellow-300 font-semibold mb-1">Common Commands:</p>
              <div className="space-y-1 font-mono text-green-300">
                <p>dvc init</p>
                <p>dvc add data/train.csv</p>
                <p>dvc push</p>
                <p>dvc pull</p>
                <p>dvc repro</p>
                <p>dvc exp run</p>
              </div>
            </div>
          </div>
        </Card>
        <Card className="md:col-span-2">
          <div className="flex gap-2 mb-3">
            {tabs.map(t => (
              <button
                key={t.key}
                onClick={() => setActiveTab(t.key)}
                className={`px-3 py-1.5 rounded text-xs font-medium ${activeTab === t.key ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300 hover:bg-gray-600'}`}
              >
                {t.label}
              </button>
            ))}
          </div>
          {activeTab === 'dvc_file' && (
            <div>
              <p className="text-xs text-gray-400 mb-2">Git-tracked pointer file — the actual data lives in remote storage</p>
              <CodeBlock code={dvcConcepts.dvc_file_structure} />
            </div>
          )}
          {activeTab === 'diff' && (
            <div>
              <p className="text-xs text-gray-400 mb-2">Run <span className="text-green-300 font-mono">dvc diff HEAD~1 HEAD</span> to see data changes between commits</p>
              <CodeBlock code={dvcConcepts.dvc_diff_output} />
            </div>
          )}
          {activeTab === 'repro' && (
            <div>
              <p className="text-xs text-gray-400 mb-2">dvc.yaml defines the pipeline — DVC tracks inputs/outputs per stage</p>
              <CodeBlock code={dvcConcepts.dvc_repro_pipeline} />
            </div>
          )}
        </Card>
      </div>

      {dvcConcepts.reproducibility_checklist && (
        <Card>
          <h3 className="text-sm font-semibold text-white mb-3">Reproducibility Checklist</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
            {dvcConcepts.reproducibility_checklist.map((item: any, i: number) => (
              <div key={i} className={`flex items-start gap-2 p-2 rounded border ${item.required ? 'border-green-700 bg-green-900/20' : 'border-gray-700 bg-gray-900/20'}`}>
                <span className={`mt-0.5 text-sm ${item.required ? 'text-green-400' : 'text-yellow-400'}`}>{item.required ? '✓' : '○'}</span>
                <div>
                  <p className="text-xs font-medium text-white">{item.item}</p>
                  <p className="text-xs text-gray-400">{item.how}</p>
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}
    </div>
  );
}

// ── Dataset Changelog ─────────────────────────────────────────────────────────
function DatasetChangelog() {
  const [versions, setVersions] = useState<any[]>([]);
  const [diff, setDiff] = useState<any>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [rollbackMsg, setRollbackMsg] = useState('');
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    api.get('/versioning/datasets').then(r => setVersions(r.data));
  }, []);

  const fetchDiff = async (versionId: string) => {
    const r = await api.get(`/versioning/diff?version_id=${versionId}`);
    setDiff(r.data);
    setSelected(versionId);
  };

  const rollback = async (versionId: string) => {
    setLoading(true);
    const r = await api.post('/versioning/rollback', { version_id: versionId });
    setRollbackMsg(r.data.message || 'Rollback complete');
    const updated = await api.get('/versioning/datasets');
    setVersions(updated.data);
    setLoading(false);
    setTimeout(() => setRollbackMsg(''), 3000);
  };

  const addVersion = async () => {
    const tags = ['feature_engineering_v3', 'hotfix_nulls', 'quarterly_refresh', 'schema_update'];
    await api.post('/versioning/datasets', {
      dataset_name: 'orders_features',
      source_path: `s3://datalake/features/orders_v${versions.length + 1}.parquet`,
      row_count: Math.floor(Math.random() * 50000) + 100000,
      schema_hash: Math.random().toString(36).substring(2, 10),
      tags: [tags[Math.floor(Math.random() * tags.length)]],
      change_description: 'Added rolling 7-day revenue feature',
    });
    const updated = await api.get('/versioning/datasets');
    setVersions(updated.data);
  };

  const changeColor = (type: string) => ({ added: 'text-green-400', removed: 'text-red-400', modified: 'text-yellow-400' }[type] ?? 'text-gray-400');

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div className="space-y-3">
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-semibold text-white">Dataset Versions</h3>
          <button onClick={addVersion} className="px-3 py-1 text-xs bg-blue-600 hover:bg-blue-700 text-white rounded">
            + Commit New Version
          </button>
        </div>
        {rollbackMsg && <div className="p-2 bg-yellow-900/30 border border-yellow-700 rounded text-xs text-yellow-300">{rollbackMsg}</div>}
        <div className="space-y-2">
          {versions.map((v) => (
            <Card key={v.version_id} className={`cursor-pointer transition-colors ${selected === v.version_id ? 'border-blue-500' : 'hover:border-gray-500'}`}>
              <div className="flex items-start justify-between">
                <div>
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-xs font-mono text-blue-400">{v.version_id}</span>
                    {v.tags?.map((t: string) => <Badge key={t} color="purple">{t}</Badge>)}
                  </div>
                  <p className="text-xs text-white">{v.dataset_name}</p>
                  <p className="text-xs text-gray-400 mt-0.5">{v.change_description}</p>
                  <div className="flex gap-3 mt-1 text-xs text-gray-500">
                    <span>{v.row_count?.toLocaleString()} rows</span>
                    <span>schema: {v.schema_hash?.substring(0, 8)}</span>
                  </div>
                </div>
                <div className="flex gap-1 flex-col items-end">
                  <button onClick={() => fetchDiff(v.version_id)} className="px-2 py-1 text-xs bg-gray-700 hover:bg-gray-600 text-gray-300 rounded">
                    Diff
                  </button>
                  <button onClick={() => rollback(v.version_id)} disabled={loading} className="px-2 py-1 text-xs bg-yellow-700 hover:bg-yellow-600 text-white rounded">
                    Rollback
                  </button>
                </div>
              </div>
            </Card>
          ))}
        </div>
      </div>

      <div>
        {diff ? (
          <div className="space-y-3">
            <h3 className="text-sm font-semibold text-white">Diff: {diff.version_id}</h3>
            <Card>
              <p className="text-xs text-gray-400 mb-2">Schema Changes</p>
              {diff.schema_changes?.length === 0
                ? <p className="text-xs text-gray-500">No schema changes</p>
                : diff.schema_changes?.map((c: any, i: number) => (
                    <div key={i} className={`text-xs font-mono py-0.5 ${changeColor(c.type)}`}>
                      {c.type === 'added' ? '+' : c.type === 'removed' ? '-' : '~'} {c.column} ({c.dtype})
                      {c.old_dtype && <span className="text-gray-500"> was: {c.old_dtype}</span>}
                    </div>
                  ))
              }
            </Card>
            <Card>
              <p className="text-xs text-gray-400 mb-2">Row Statistics</p>
              <div className="grid grid-cols-2 gap-2 text-xs">
                {diff.row_stats && Object.entries(diff.row_stats).map(([k, v]: any) => (
                  <div key={k} className="flex justify-between">
                    <span className="text-gray-400">{k.replace(/_/g, ' ')}</span>
                    <span className={`font-mono ${String(v).startsWith('+') ? 'text-green-400' : String(v).startsWith('-') ? 'text-red-400' : 'text-white'}`}>{String(v)}</span>
                  </div>
                ))}
              </div>
            </Card>
            <Card>
              <p className="text-xs text-gray-400 mb-2">Validation Results</p>
              {diff.validation_results?.map((r: any, i: number) => (
                <div key={i} className="flex items-center justify-between py-0.5 text-xs">
                  <span className="text-gray-300">{r.check}</span>
                  <span className={r.passed ? 'text-green-400' : 'text-red-400'}>{r.passed ? 'PASS' : 'FAIL'}</span>
                </div>
              ))}
            </Card>
          </div>
        ) : (
          <div className="flex items-center justify-center h-full text-gray-500 text-sm">
            Select a version and click Diff to view changes
          </div>
        )}
      </div>
    </div>
  );
}

// ── Experiment Tracking ───────────────────────────────────────────────────────
function ExperimentTracking() {
  const [experiments, setExperiments] = useState<any[]>([]);
  const [selected, setSelected] = useState<any>(null);

  useEffect(() => {
    api.get('/versioning/experiments').then(r => {
      setExperiments(r.data);
      if (r.data.length > 0) setSelected(r.data[0]);
    });
  }, []);

  const metricColor = (name: string, value: number) => {
    if (name.includes('loss') || name.includes('error')) return value < 0.2 ? 'text-green-400' : value < 0.4 ? 'text-yellow-400' : 'text-red-400';
    return value > 0.85 ? 'text-green-400' : value > 0.7 ? 'text-yellow-400' : 'text-red-400';
  };

  const statusColor = { running: 'yellow', completed: 'green', failed: 'red' } as Record<string, string>;

  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
      <div className="space-y-2">
        <h3 className="text-sm font-semibold text-white mb-2">Experiments</h3>
        {experiments.map((exp) => (
          <Card
            key={exp.experiment_id}
            className={`cursor-pointer transition-colors ${selected?.experiment_id === exp.experiment_id ? 'border-blue-500' : 'hover:border-gray-500'}`}
            onClick={() => setSelected(exp)}
          >
            <div className="flex items-center justify-between mb-1">
              <span className="text-xs font-mono text-blue-400">{exp.experiment_id}</span>
              <Badge color={statusColor[exp.status] ?? 'gray'}>{exp.status}</Badge>
            </div>
            <p className="text-xs text-white">{exp.name}</p>
            <p className="text-xs text-gray-400">{exp.model_type}</p>
            {exp.metrics?.accuracy && (
              <p className={`text-xs font-mono mt-1 ${metricColor('accuracy', exp.metrics.accuracy)}`}>
                acc: {exp.metrics.accuracy}
              </p>
            )}
          </Card>
        ))}
      </div>

      {selected && (
        <div className="md:col-span-2 space-y-3">
          <h3 className="text-sm font-semibold text-white">{selected.name}</h3>

          <div className="grid grid-cols-2 gap-3">
            <Card>
              <p className="text-xs text-gray-400 mb-2">Metrics</p>
              {selected.metrics && Object.entries(selected.metrics).map(([k, v]: any) => (
                <div key={k} className="flex justify-between py-0.5 text-xs">
                  <span className="text-gray-300">{k}</span>
                  <span className={`font-mono ${metricColor(k, v)}`}>{typeof v === 'number' ? v.toFixed(4) : v}</span>
                </div>
              ))}
            </Card>
            <Card>
              <p className="text-xs text-gray-400 mb-2">Parameters</p>
              {selected.params && Object.entries(selected.params).map(([k, v]: any) => (
                <div key={k} className="flex justify-between py-0.5 text-xs">
                  <span className="text-gray-300">{k}</span>
                  <span className="font-mono text-purple-300">{String(v)}</span>
                </div>
              ))}
            </Card>
          </div>

          <Card>
            <p className="text-xs text-gray-400 mb-2">Artifacts</p>
            <div className="space-y-1">
              {selected.artifacts?.map((a: any, i: number) => (
                <div key={i} className="flex items-center justify-between text-xs">
                  <div className="flex items-center gap-2">
                    <span className="text-blue-400 font-mono">{a.type}</span>
                    <span className="text-gray-300 font-mono">{a.path}</span>
                  </div>
                  <span className="text-gray-500">{a.size_mb} MB</span>
                </div>
              ))}
            </div>
          </Card>

          <Card>
            <p className="text-xs text-gray-400 mb-2">Data Lineage</p>
            <div className="text-xs space-y-1">
              <div className="flex items-center gap-2">
                <span className="text-gray-400">Dataset version:</span>
                <span className="font-mono text-blue-400">{selected.data_version_id}</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-gray-400">Git commit:</span>
                <span className="font-mono text-green-400">{selected.git_commit}</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-gray-400">Duration:</span>
                <span className="font-mono text-white">{selected.duration_seconds}s</span>
              </div>
            </div>
          </Card>

          {selected.mlflow_run_id && (
            <Card>
              <p className="text-xs text-gray-400 mb-2">MLflow Integration</p>
              <CodeBlock code={`import mlflow

with mlflow.start_run(run_id="${selected.mlflow_run_id}"):
    mlflow.log_params(${JSON.stringify(selected.params, null, 2)})
    mlflow.log_metrics(${JSON.stringify(selected.metrics, null, 2)})
    mlflow.sklearn.log_model(model, "model")`} />
            </Card>
          )}
        </div>
      )}
    </div>
  );
}

// ── Reproducibility Checklist ─────────────────────────────────────────────────
function ReproducibilityChecklist() {
  const [checks, setChecks] = useState<Record<string, boolean>>({});
  const items = [
    { id: 'code', label: 'Code is version-controlled (Git)', detail: 'git commit + git tag for every experiment run', required: true },
    { id: 'data', label: 'Data is versioned (DVC)', detail: 'dvc add + dvc push to remote storage', required: true },
    { id: 'env', label: 'Environment is pinned', detail: 'requirements.txt / conda env.yml / Docker image with SHA', required: true },
    { id: 'seed', label: 'Random seeds are fixed', detail: 'np.random.seed(), torch.manual_seed(), tf.random.set_seed()', required: true },
    { id: 'params', label: 'All hyperparameters are logged', detail: 'MLflow params / DVC params.yaml / W&B config', required: true },
    { id: 'metrics', label: 'Metrics are tracked', detail: 'MLflow metrics, per-epoch logs, confusion matrices', required: true },
    { id: 'artifacts', label: 'Model artifacts are stored', detail: 'mlflow.sklearn.log_model() or dvc push model/', required: true },
    { id: 'pipeline', label: 'Pipeline is defined as code', detail: 'dvc.yaml or Airflow DAG — not manual steps', required: false },
    { id: 'tests', label: 'Data validation tests pass', detail: 'Great Expectations / Pandera checks before training', required: false },
    { id: 'docs', label: 'Experiment is documented', detail: 'MLflow description, DVC README, Confluence page', required: false },
  ];

  const passed = items.filter(i => checks[i.id]).length;
  const requiredPassed = items.filter(i => i.required && checks[i.id]).length;
  const totalRequired = items.filter(i => i.required).length;

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-3 gap-3">
        <Card className="text-center">
          <p className="text-2xl font-bold text-white">{passed}/{items.length}</p>
          <p className="text-xs text-gray-400 mt-1">Items Complete</p>
        </Card>
        <Card className="text-center">
          <p className={`text-2xl font-bold ${requiredPassed === totalRequired ? 'text-green-400' : 'text-yellow-400'}`}>
            {requiredPassed}/{totalRequired}
          </p>
          <p className="text-xs text-gray-400 mt-1">Required Items</p>
        </Card>
        <Card className="text-center">
          <p className={`text-2xl font-bold ${passed === items.length ? 'text-green-400' : passed >= totalRequired ? 'text-yellow-400' : 'text-red-400'}`}>
            {Math.round((passed / items.length) * 100)}%
          </p>
          <p className="text-xs text-gray-400 mt-1">Reproducibility Score</p>
        </Card>
      </div>

      <div className="space-y-2">
        {items.map(item => (
          <Card key={item.id} className={`cursor-pointer transition-colors ${checks[item.id] ? 'border-green-700' : ''}`}
            onClick={() => setChecks(prev => ({ ...prev, [item.id]: !prev[item.id] }))}>
            <div className="flex items-start gap-3">
              <div className={`mt-0.5 w-5 h-5 rounded border-2 flex-shrink-0 flex items-center justify-center ${checks[item.id] ? 'border-green-500 bg-green-500' : 'border-gray-600'}`}>
                {checks[item.id] && <span className="text-white text-xs">✓</span>}
              </div>
              <div className="flex-1">
                <div className="flex items-center gap-2">
                  <p className={`text-sm ${checks[item.id] ? 'text-gray-400 line-through' : 'text-white'}`}>{item.label}</p>
                  {item.required && <Badge color="red">required</Badge>}
                </div>
                <p className="text-xs text-gray-500 mt-0.5 font-mono">{item.detail}</p>
              </div>
            </div>
          </Card>
        ))}
      </div>

      {requiredPassed === totalRequired && (
        <Card className="border-green-700 bg-green-900/20">
          <p className="text-sm text-green-300 font-semibold">All required items complete — your experiment is reproducible!</p>
          <p className="text-xs text-green-400 mt-1">Anyone can re-run this experiment and get the same results by checking out the same Git commit and running <span className="font-mono">dvc repro</span>.</p>
        </Card>
      )}
    </div>
  );
}

// ── Main Page ─────────────────────────────────────────────────────────────────
const SECTIONS = [
  { id: 'dvc', label: 'DVC Simulator' },
  { id: 'changelog', label: 'Dataset Changelog' },
  { id: 'experiments', label: 'Experiment Tracking' },
  { id: 'checklist', label: 'Reproducibility Checklist' },
];

export default function DataVersioning() {
  const [activeSection, setActiveSection] = useState('dvc');

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Data Versioning</h1>
        <p className="text-gray-400 text-sm mt-1">DVC, dataset lineage, experiment tracking, and reproducibility</p>
      </div>

      <div className="flex gap-2 flex-wrap">
        {SECTIONS.map(s => (
          <button
            key={s.id}
            onClick={() => setActiveSection(s.id)}
            className={`px-4 py-2 rounded text-sm font-medium transition-colors ${activeSection === s.id ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-300 hover:bg-gray-700 border border-gray-700'}`}
          >
            {s.label}
          </button>
        ))}
      </div>

      {activeSection === 'dvc' && <DVCSimulator />}
      {activeSection === 'changelog' && <DatasetChangelog />}
      {activeSection === 'experiments' && <ExperimentTracking />}
      {activeSection === 'checklist' && <ReproducibilityChecklist />}
    </div>
  );
}
