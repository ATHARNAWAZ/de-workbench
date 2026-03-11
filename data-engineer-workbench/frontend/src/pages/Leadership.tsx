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
    orange: 'bg-orange-900 text-orange-300',
  };
  return <span className={`px-2 py-0.5 rounded text-xs font-mono ${map[color] ?? map.gray}`}>{children}</span>;
};

const CodeBlock = ({ code }: { code: string }) => (
  <pre className="bg-gray-900 rounded p-3 text-xs text-green-300 overflow-x-auto whitespace-pre font-mono leading-relaxed">
    {code}
  </pre>
);

// ── TDD Builder ───────────────────────────────────────────────────────────────
function TDDBuilder() {
  const [template, setTemplate] = useState<any>(null);
  const [customForm, setCustomForm] = useState({
    project: 'Migrate to dbt Cloud',
    team: 'Data Platform',
    timeline: 'Q3 2025',
    problem: 'Current dbt Core runs are slow and require manual infra maintenance. CI/CD setup is complex and error-prone.',
    success_metric: 'p95 job run time < 15 min, zero infra maintenance toil, CI on every PR',
    risk: 'Model migration risk, Airflow integration changes',
  });
  const [generated, setGenerated] = useState<any>(null);

  useEffect(() => {
    api.get('/leadership/tdd-template').then(r => setTemplate(r.data));
  }, []);

  const generate = async () => {
    const r = await api.post('/leadership/generate-tdd', customForm);
    setGenerated(r.data);
  };

  if (!template) return <div className="text-gray-400 text-center py-8">Loading...</div>;

  const sectionColor: Record<string, string> = {
    'Problem Statement': 'border-red-700',
    'Proposed Solution': 'border-blue-700',
    'Success Metrics': 'border-green-700',
    'Technical Design': 'border-purple-700',
    'Risks & Mitigations': 'border-yellow-700',
    'Implementation Plan': 'border-orange-700',
    'Open Questions': 'border-gray-600',
  };

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="space-y-3">
          <h3 className="text-sm font-semibold text-white">Example TDD: {template.example?.title}</h3>
          {template.example?.sections?.map((sec: any) => (
            <Card key={sec.heading} className={`border-l-4 ${sectionColor[sec.heading] ?? 'border-gray-600'}`}>
              <p className="text-xs font-semibold text-gray-300 mb-1">{sec.heading}</p>
              <p className="text-xs text-gray-400 leading-relaxed">{sec.content}</p>
            </Card>
          ))}
        </div>

        <div className="space-y-3">
          <h3 className="text-sm font-semibold text-white">Generate Your Own TDD</h3>
          <div className="space-y-2">
            {Object.entries(customForm).map(([k, v]) => (
              <div key={k}>
                <label className="text-xs text-gray-400 capitalize">{k.replace(/_/g, ' ')}</label>
                <textarea
                  value={v}
                  onChange={e => setCustomForm(prev => ({ ...prev, [k]: e.target.value }))}
                  className="w-full mt-1 px-2 py-1 bg-gray-900 border border-gray-700 rounded text-xs text-white resize-none"
                  rows={k === 'problem' || k === 'success_metric' ? 3 : 1}
                />
              </div>
            ))}
            <button onClick={generate} className="w-full py-2 bg-blue-600 hover:bg-blue-700 text-white rounded text-sm">
              Generate TDD Document
            </button>
          </div>
          {generated && (
            <Card>
              <p className="text-xs font-semibold text-white mb-2">{generated.title}</p>
              {generated.sections?.map((sec: any) => (
                <div key={sec.heading} className="mb-3">
                  <p className="text-xs font-semibold text-blue-400 mb-1">{sec.heading}</p>
                  <p className="text-xs text-gray-300 leading-relaxed">{sec.content}</p>
                </div>
              ))}
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}

// ── Communication Templates ───────────────────────────────────────────────────
function CommunicationTemplates() {
  const [templates, setTemplates] = useState<any[]>([]);
  const [selected, setSelected] = useState<any>(null);

  useEffect(() => {
    api.get('/leadership/communication-templates').then(r => {
      setTemplates(r.data);
      if (r.data.length > 0) setSelected(r.data[0]);
    });
  }, []);

  const audienceColor = (a: string) => {
    if (a?.includes('Executive') || a?.includes('VP')) return 'red';
    if (a?.includes('Engineer') || a?.includes('Team')) return 'blue';
    if (a?.includes('Stakeholder') || a?.includes('Product')) return 'yellow';
    return 'gray';
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
      <div className="space-y-2">
        <h3 className="text-sm font-semibold text-white mb-1">Template Library</h3>
        {templates.map((t) => (
          <Card
            key={t.id}
            className={`cursor-pointer transition-colors ${selected?.id === t.id ? 'border-blue-500' : 'hover:border-gray-500'}`}
            onClick={() => setSelected(t)}
          >
            <p className="text-xs font-semibold text-white">{t.name}</p>
            <p className="text-xs text-gray-400 mt-0.5">{t.description}</p>
            <div className="flex flex-wrap gap-1 mt-1">
              {t.audience?.map((a: string) => <Badge key={a} color={audienceColor(a)}>{a}</Badge>)}
            </div>
          </Card>
        ))}
      </div>

      <div className="md:col-span-2">
        {selected && (
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <h3 className="text-sm font-semibold text-white">{selected.name}</h3>
              <button
                onClick={() => navigator.clipboard?.writeText(selected.template ?? '')}
                className="px-3 py-1 text-xs bg-gray-700 hover:bg-gray-600 text-gray-300 rounded"
              >
                Copy
              </button>
            </div>
            <Card>
              <p className="text-xs text-gray-400 mb-2">Writing Guidelines:</p>
              <ul className="space-y-1">
                {selected.guidelines?.map((g: string, i: number) => (
                  <li key={i} className="text-xs text-gray-300 flex items-start gap-1">
                    <span className="text-blue-400 mt-0.5">•</span> {g}
                  </li>
                ))}
              </ul>
            </Card>
            <Card>
              <p className="text-xs text-gray-400 mb-2">Template:</p>
              <div className="bg-gray-900 rounded p-3 text-xs text-gray-200 whitespace-pre-wrap font-mono leading-relaxed max-h-80 overflow-y-auto">
                {selected.template}
              </div>
            </Card>
            {selected.example && (
              <Card>
                <p className="text-xs text-gray-400 mb-2">Example:</p>
                <div className="bg-gray-900 rounded p-3 text-xs text-green-300 whitespace-pre-wrap leading-relaxed max-h-60 overflow-y-auto">
                  {selected.example}
                </div>
              </Card>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ── Estimation Tool ───────────────────────────────────────────────────────────
function EstimationTool() {
  const [tasks, setTasks] = useState([
    { name: 'Schema design & data contracts', optimistic: 3, most_likely: 5, pessimistic: 10 },
    { name: 'ETL pipeline development', optimistic: 5, most_likely: 8, pessimistic: 15 },
    { name: 'Data quality rules implementation', optimistic: 2, most_likely: 4, pessimistic: 7 },
    { name: 'Orchestration & scheduling', optimistic: 1, most_likely: 3, pessimistic: 6 },
    { name: 'Testing & validation', optimistic: 3, most_likely: 5, pessimistic: 10 },
  ]);
  const [result, setResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  const calculate = async () => {
    setLoading(true);
    const r = await api.post('/leadership/estimate', { tasks });
    setResult(r.data);
    setLoading(false);
  };

  const addTask = () => setTasks(prev => [...prev, { name: '', optimistic: 1, most_likely: 3, pessimistic: 5 }]);
  const removeTask = (i: number) => setTasks(prev => prev.filter((_, idx) => idx !== i));
  const updateTask = (i: number, field: string, value: string | number) =>
    setTasks(prev => prev.map((t, idx) => idx === i ? { ...t, [field]: value } : t));

  const pert = (o: number, m: number, p: number) => (o + 4 * m + p) / 6;
  const sd = (o: number, p: number) => (p - o) / 6;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold text-white">PERT Estimation</h3>
        <div className="flex gap-2">
          <button onClick={addTask} className="px-3 py-1 text-xs bg-gray-700 hover:bg-gray-600 text-gray-300 rounded">
            + Add Task
          </button>
          <button onClick={calculate} disabled={loading} className="px-3 py-1 text-xs bg-blue-600 hover:bg-blue-700 text-white rounded">
            {loading ? 'Calculating...' : 'Calculate'}
          </button>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="text-gray-400 border-b border-gray-700">
              <th className="pb-2 text-left">Task</th>
              <th className="pb-2 text-center">Optimistic</th>
              <th className="pb-2 text-center">Most Likely</th>
              <th className="pb-2 text-center">Pessimistic</th>
              <th className="pb-2 text-center">PERT Estimate</th>
              <th className="pb-2 text-center">Std Dev</th>
              <th className="pb-2"></th>
            </tr>
          </thead>
          <tbody>
            {tasks.map((task, i) => (
              <tr key={i} className="border-b border-gray-800">
                <td className="py-2 pr-2">
                  <input value={task.name}
                    onChange={e => updateTask(i, 'name', e.target.value)}
                    className="w-full px-2 py-1 bg-gray-900 border border-gray-700 rounded text-white" />
                </td>
                {(['optimistic', 'most_likely', 'pessimistic'] as const).map(f => (
                  <td key={f} className="py-2 px-1 text-center">
                    <input type="number" value={task[f]}
                      onChange={e => updateTask(i, f, Number(e.target.value))}
                      className="w-16 px-1 py-1 bg-gray-900 border border-gray-700 rounded text-white text-center" />
                  </td>
                ))}
                <td className="py-2 text-center font-mono text-blue-300">
                  {pert(task.optimistic, task.most_likely, task.pessimistic).toFixed(1)}d
                </td>
                <td className="py-2 text-center font-mono text-yellow-300">
                  ±{sd(task.optimistic, task.pessimistic).toFixed(1)}d
                </td>
                <td className="py-2 text-center">
                  <button onClick={() => removeTask(i)} className="text-red-400 hover:text-red-300">×</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {result && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Card>
            <h4 className="text-sm font-semibold text-white mb-3">Summary</h4>
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span className="text-gray-400">Total PERT estimate:</span>
                <span className="font-mono text-white font-bold">{result.total_pert_days?.toFixed(1)} days</span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-gray-400">Standard deviation:</span>
                <span className="font-mono text-yellow-300">±{result.total_std_dev?.toFixed(1)} days</span>
              </div>
              <div className="border-t border-gray-700 pt-2 mt-2 space-y-1">
                <div className="flex justify-between text-sm">
                  <span className="text-green-300">85% confidence (P85):</span>
                  <span className="font-mono text-green-300">{result.p85_days?.toFixed(1)} days</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-yellow-300">95% confidence (P95):</span>
                  <span className="font-mono text-yellow-300">{result.p95_days?.toFixed(1)} days</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-orange-300">99% confidence (P99):</span>
                  <span className="font-mono text-orange-300">{result.p99_days?.toFixed(1)} days</span>
                </div>
              </div>
            </div>
          </Card>

          <Card>
            <h4 className="text-sm font-semibold text-white mb-3">Stakeholder Communication</h4>
            <div className="space-y-2 text-xs">
              <div className="p-2 bg-blue-900/30 rounded border border-blue-800">
                <p className="text-blue-300 font-semibold">To Engineering Team:</p>
                <p className="text-gray-300 mt-1">"Best case {result.total_pert_days?.toFixed(0)} days. Plan for {result.p85_days?.toFixed(0)} days."</p>
              </div>
              <div className="p-2 bg-yellow-900/30 rounded border border-yellow-800">
                <p className="text-yellow-300 font-semibold">To Product Manager:</p>
                <p className="text-gray-300 mt-1">"We'll deliver in {result.p85_days?.toFixed(0)}–{result.p95_days?.toFixed(0)} days with 85–95% confidence."</p>
              </div>
              <div className="p-2 bg-red-900/30 rounded border border-red-800">
                <p className="text-red-300 font-semibold">To Executive / Deadline:</p>
                <p className="text-gray-300 mt-1">"Commit to {result.p95_days?.toFixed(0)} days. Buffer: {(result.p95_days - result.total_pert_days)?.toFixed(0)} days risk padding."</p>
              </div>
            </div>
          </Card>
        </div>
      )}

      <Card>
        <h4 className="text-sm font-semibold text-white mb-2">PERT Formula</h4>
        <div className="text-xs text-gray-300 space-y-1">
          <p><span className="text-green-300 font-mono">E = (O + 4M + P) / 6</span> — weighted average (O=optimistic, M=most likely, P=pessimistic)</p>
          <p><span className="text-yellow-300 font-mono">SD = (P - O) / 6</span> — standard deviation of the estimate</p>
          <p><span className="text-blue-300 font-mono">P85 = E + 1.036 × SD</span> — 85th percentile (recommend to PM)</p>
          <p><span className="text-orange-300 font-mono">P95 = E + 1.645 × SD</span> — 95th percentile (commit to stakeholders)</p>
        </div>
      </Card>
    </div>
  );
}

// ── Interview Questions ────────────────────────────────────────────────────────
function InterviewQuestions() {
  const [questions, setQuestions] = useState<any[]>([]);
  const [expanded, setExpanded] = useState<Set<number>>(new Set([0]));
  const [filter, setFilter] = useState('all');

  useEffect(() => {
    api.get('/leadership/interview-questions').then(r => setQuestions(r.data));
  }, []);

  const toggle = (i: number) => setExpanded(prev => {
    const n = new Set(prev);
    n.has(i) ? n.delete(i) : n.add(i);
    return n;
  });

  const categories = ['all', ...Array.from(new Set(questions.map(q => q.category)))];
  const filtered = filter === 'all' ? questions : questions.filter(q => q.category === filter);

  const difficultyColor = { easy: 'green', medium: 'yellow', hard: 'red', senior: 'purple' } as Record<string, string>;

  return (
    <div className="space-y-4">
      <div className="flex gap-2 flex-wrap">
        {categories.map(c => (
          <button key={c} onClick={() => setFilter(c)}
            className={`px-3 py-1 rounded text-xs font-medium ${filter === c ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-300 hover:bg-gray-700 border border-gray-700'}`}>
            {c}
          </button>
        ))}
      </div>

      <div className="space-y-2">
        {filtered.map((q, i) => (
          <Card key={q.id ?? i} className="cursor-pointer" onClick={() => toggle(i)}>
            <div className="flex items-start justify-between">
              <div className="flex-1">
                <div className="flex items-center gap-2 mb-1">
                  <Badge color={difficultyColor[q.difficulty] ?? 'gray'}>{q.difficulty}</Badge>
                  <Badge color="blue">{q.category}</Badge>
                </div>
                <p className="text-sm text-white">{q.question}</p>
              </div>
              <span className="text-gray-400 text-lg ml-2">{expanded.has(i) ? '−' : '+'}</span>
            </div>

            {expanded.has(i) && (
              <div className="mt-3 space-y-3 border-t border-gray-700 pt-3">
                {q.framework && (
                  <div>
                    <p className="text-xs text-gray-400 mb-1">Answer Framework:</p>
                    <p className="text-xs text-gray-300 leading-relaxed">{q.framework}</p>
                  </div>
                )}
                {q.key_points && (
                  <div>
                    <p className="text-xs text-gray-400 mb-1">Key Points to Cover:</p>
                    <ul className="space-y-1">
                      {q.key_points.map((pt: string, j: number) => (
                        <li key={j} className="text-xs text-gray-300 flex items-start gap-1">
                          <span className="text-green-400 mt-0.5">•</span> {pt}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                {q.example && (
                  <div>
                    <p className="text-xs text-gray-400 mb-1">Example Answer:</p>
                    <div className="bg-gray-900 rounded p-3 text-xs text-blue-300 leading-relaxed">
                      {q.example}
                    </div>
                  </div>
                )}
                {q.follow_ups && (
                  <div>
                    <p className="text-xs text-gray-400 mb-1">Likely Follow-ups:</p>
                    <ul className="space-y-0.5">
                      {q.follow_ups.map((f: string, j: number) => (
                        <li key={j} className="text-xs text-yellow-300">→ {f}</li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )}
          </Card>
        ))}
      </div>
    </div>
  );
}

// ── Career Ladder ─────────────────────────────────────────────────────────────
function CareerLadder() {
  const [ladder, setLadder] = useState<any[]>([]);
  const [assessment, setAssessment] = useState<Record<string, number>>({});
  const [assessmentResult, setAssessmentResult] = useState<any>(null);
  const [selectedLevel, setSelectedLevel] = useState<any>(null);

  useEffect(() => {
    api.get('/leadership/career-ladder').then(r => {
      setLadder(r.data);
      if (r.data.length > 0) setSelectedLevel(r.data[1]); // L4 by default
    });
  }, []);

  const submitAssessment = async () => {
    const r = await api.post('/leadership/self-assessment', { scores: assessment });
    setAssessmentResult(r.data);
  };

  const levelColor: Record<string, string> = {
    L3: 'border-gray-600', L4: 'border-blue-600', L5: 'border-purple-600',
    L6: 'border-yellow-600', Principal: 'border-orange-600',
  };
  const levelBg: Record<string, string> = {
    L3: 'bg-gray-800', L4: 'bg-blue-900/20', L5: 'bg-purple-900/20',
    L6: 'bg-yellow-900/20', Principal: 'bg-orange-900/20',
  };

  return (
    <div className="space-y-4">
      <div className="flex gap-3 overflow-x-auto pb-2">
        {ladder.map((level) => (
          <button
            key={level.level}
            onClick={() => setSelectedLevel(level)}
            className={`flex-shrink-0 px-4 py-2 rounded border-2 text-sm font-semibold transition-all ${levelColor[level.level]} ${selectedLevel?.level === level.level ? levelBg[level.level] + ' text-white' : 'bg-gray-800 text-gray-400 hover:text-gray-200'}`}
          >
            {level.level}
          </button>
        ))}
      </div>

      {selectedLevel && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="space-y-3">
            <Card>
              <div className="flex items-center justify-between mb-2">
                <h3 className="text-sm font-semibold text-white">{selectedLevel.level}: {selectedLevel.title}</h3>
                <Badge color="blue">{selectedLevel.years_experience}</Badge>
              </div>
              <p className="text-xs text-gray-300 leading-relaxed">{selectedLevel.summary}</p>
            </Card>

            <Card>
              <h4 className="text-xs text-gray-400 mb-2">Core Expectations</h4>
              <ul className="space-y-1">
                {selectedLevel.expectations?.map((e: string, i: number) => (
                  <li key={i} className="text-xs text-gray-300 flex items-start gap-1">
                    <span className="text-blue-400 mt-0.5">→</span> {e}
                  </li>
                ))}
              </ul>
            </Card>

            <Card>
              <h4 className="text-xs text-gray-400 mb-2">Technical Skills</h4>
              <div className="flex flex-wrap gap-1">
                {selectedLevel.technical_skills?.map((s: string) => (
                  <Badge key={s} color="purple">{s}</Badge>
                ))}
              </div>
            </Card>
          </div>

          <div className="space-y-3">
            <Card>
              <h4 className="text-xs text-gray-400 mb-2">Leadership & Influence</h4>
              <ul className="space-y-1">
                {selectedLevel.leadership?.map((l: string, i: number) => (
                  <li key={i} className="text-xs text-gray-300 flex items-start gap-1">
                    <span className="text-green-400 mt-0.5">•</span> {l}
                  </li>
                ))}
              </ul>
            </Card>

            <Card>
              <h4 className="text-xs text-gray-400 mb-2">Promotion Signals</h4>
              <ul className="space-y-1">
                {selectedLevel.promotion_signals?.map((s: string, i: number) => (
                  <li key={i} className="text-xs text-gray-300 flex items-start gap-1">
                    <span className="text-yellow-400 mt-0.5">★</span> {s}
                  </li>
                ))}
              </ul>
            </Card>

            {selectedLevel.anti_patterns && (
              <Card>
                <h4 className="text-xs text-gray-400 mb-2">Anti-patterns to Avoid</h4>
                <ul className="space-y-1">
                  {selectedLevel.anti_patterns.map((a: string, i: number) => (
                    <li key={i} className="text-xs text-red-300 flex items-start gap-1">
                      <span className="text-red-500 mt-0.5">✗</span> {a}
                    </li>
                  ))}
                </ul>
              </Card>
            )}
          </div>
        </div>
      )}

      <Card>
        <h3 className="text-sm font-semibold text-white mb-3">Self-Assessment</h3>
        <p className="text-xs text-gray-400 mb-3">Rate yourself 1–5 on each dimension to see your profile</p>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {[
            'Technical depth', 'System design', 'Data modeling', 'Performance optimization',
            'Cross-team collaboration', 'Documentation quality', 'Mentoring', 'Estimation accuracy',
            'Incident response', 'Strategic thinking',
          ].map(dim => (
            <div key={dim} className="flex items-center justify-between">
              <span className="text-xs text-gray-300">{dim}</span>
              <div className="flex gap-1">
                {[1, 2, 3, 4, 5].map(n => (
                  <button
                    key={n}
                    onClick={() => setAssessment(prev => ({ ...prev, [dim]: n }))}
                    className={`w-6 h-6 rounded text-xs font-mono ${assessment[dim] === n ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-400 hover:bg-gray-600'}`}
                  >
                    {n}
                  </button>
                ))}
              </div>
            </div>
          ))}
        </div>
        <button
          onClick={submitAssessment}
          disabled={Object.keys(assessment).length < 5}
          className="mt-4 px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white rounded text-sm"
        >
          Analyze Assessment
        </button>

        {assessmentResult && (
          <div className="mt-4 space-y-2">
            <div className="p-3 bg-blue-900/30 border border-blue-700 rounded">
              <p className="text-sm font-semibold text-blue-300">You are operating at: {assessmentResult.current_level}</p>
              <p className="text-xs text-blue-400 mt-1">{assessmentResult.summary}</p>
            </div>
            {assessmentResult.growth_areas && (
              <div>
                <p className="text-xs text-gray-400 mb-1">Growth Areas:</p>
                <div className="flex flex-wrap gap-1">
                  {assessmentResult.growth_areas.map((a: string) => <Badge key={a} color="yellow">{a}</Badge>)}
                </div>
              </div>
            )}
            {assessmentResult.strengths && (
              <div>
                <p className="text-xs text-gray-400 mb-1">Strengths:</p>
                <div className="flex flex-wrap gap-1">
                  {assessmentResult.strengths.map((s: string) => <Badge key={s} color="green">{s}</Badge>)}
                </div>
              </div>
            )}
          </div>
        )}
      </Card>
    </div>
  );
}

// ── Main Page ─────────────────────────────────────────────────────────────────
const SECTIONS = [
  { id: 'tdd', label: 'TDD Builder' },
  { id: 'communication', label: 'Communication Templates' },
  { id: 'estimation', label: 'PERT Estimation' },
  { id: 'interviews', label: 'Interview Questions' },
  { id: 'career', label: 'Career Ladder' },
];

export default function Leadership() {
  const [activeSection, setActiveSection] = useState('tdd');

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Senior Soft Skills & Leadership</h1>
        <p className="text-gray-400 text-sm mt-1">TDD, stakeholder communication, estimation, interview prep, and career growth</p>
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

      {activeSection === 'tdd' && <TDDBuilder />}
      {activeSection === 'communication' && <CommunicationTemplates />}
      {activeSection === 'estimation' && <EstimationTool />}
      {activeSection === 'interviews' && <InterviewQuestions />}
      {activeSection === 'career' && <CareerLadder />}
    </div>
  );
}
