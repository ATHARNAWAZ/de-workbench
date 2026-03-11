import { useState, useEffect } from 'react';
import axios from 'axios';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, Legend, LineChart, Line } from 'recharts';

const api = axios.create({ baseURL: 'http://localhost:8000/api' });

// ── helpers ──────────────────────────────────────────────────────────────────
const Card = ({ children, className = '' }: { children: React.ReactNode; className?: string }) => (
  <div className={`bg-gray-800 border border-gray-700 rounded-lg p-4 ${className}`}>{children}</div>
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

// ── Storage Calculator ────────────────────────────────────────────────────────
function StorageCalculator() {
  const [form, setForm] = useState({
    current_size_gb: 500,
    daily_growth_gb: 10,
    retention_days: 365,
    replication_factor: 3,
    compression_ratio: 0.3,
    cloud_provider: 'aws',
  });
  const [result, setResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  const calculate = async () => {
    setLoading(true);
    const r = await api.post('/capacity/storage', form);
    setResult(r.data);
    setLoading(false);
  };

  useEffect(() => { calculate(); }, []);

  const providerColor: Record<string, string> = { aws: 'orange', azure: 'blue', gcp: 'green' };

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card>
          <h3 className="text-sm font-semibold text-white mb-3">Parameters</h3>
          <div className="space-y-3">
            {[
              { key: 'current_size_gb', label: 'Current Size (GB)', min: 1, max: 100000 },
              { key: 'daily_growth_gb', label: 'Daily Growth (GB)', min: 1, max: 10000 },
              { key: 'retention_days', label: 'Retention (days)', min: 30, max: 3650 },
              { key: 'replication_factor', label: 'Replication Factor', min: 1, max: 5 },
            ].map(field => (
              <div key={field.key}>
                <label className="text-xs text-gray-400">{field.label}</label>
                <input
                  type="number"
                  value={form[field.key as keyof typeof form]}
                  onChange={e => setForm(prev => ({ ...prev, [field.key]: Number(e.target.value) }))}
                  className="w-full mt-1 px-2 py-1 bg-gray-900 border border-gray-700 rounded text-sm text-white"
                  min={field.min} max={field.max}
                />
              </div>
            ))}
            <div>
              <label className="text-xs text-gray-400">Compression Ratio</label>
              <input
                type="range" min="0.1" max="0.9" step="0.05"
                value={form.compression_ratio}
                onChange={e => setForm(prev => ({ ...prev, compression_ratio: Number(e.target.value) }))}
                className="w-full mt-1"
              />
              <p className="text-xs text-gray-500">{(form.compression_ratio * 100).toFixed(0)}% of original size</p>
            </div>
            <div>
              <label className="text-xs text-gray-400">Cloud Provider</label>
              <select
                value={form.cloud_provider}
                onChange={e => setForm(prev => ({ ...prev, cloud_provider: e.target.value }))}
                className="w-full mt-1 px-2 py-1 bg-gray-900 border border-gray-700 rounded text-sm text-white"
              >
                <option value="aws">AWS S3</option>
                <option value="azure">Azure ADLS</option>
                <option value="gcp">GCP GCS</option>
              </select>
            </div>
            <button onClick={calculate} disabled={loading} className="w-full py-2 bg-blue-600 hover:bg-blue-700 text-white rounded text-sm">
              {loading ? 'Calculating...' : 'Recalculate'}
            </button>
          </div>
        </Card>

        {result && (
          <div className="md:col-span-2 space-y-3">
            <div className="grid grid-cols-3 gap-3">
              <Card className="text-center">
                <p className="text-lg font-bold text-white">{result.month_6_tb?.toFixed(1)} TB</p>
                <p className="text-xs text-gray-400">6 Months</p>
              </Card>
              <Card className="text-center">
                <p className="text-lg font-bold text-yellow-400">{result.month_12_tb?.toFixed(1)} TB</p>
                <p className="text-xs text-gray-400">12 Months</p>
              </Card>
              <Card className="text-center">
                <p className="text-lg font-bold text-red-400">{result.month_24_tb?.toFixed(1)} TB</p>
                <p className="text-xs text-gray-400">24 Months</p>
              </Card>
            </div>

            <Card>
              <h4 className="text-xs text-gray-400 mb-2">Storage Growth Projection (24 months)</h4>
              <ResponsiveContainer width="100%" height={180}>
                <AreaChart data={result.monthly_projection?.slice(0, 24)}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis dataKey="month" tick={{ fontSize: 10, fill: '#9ca3af' }} />
                  <YAxis tick={{ fontSize: 10, fill: '#9ca3af' }} />
                  <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                    labelStyle={{ color: '#fff' }} itemStyle={{ color: '#60a5fa' }} />
                  <Area type="monotone" dataKey="storage_tb" stroke="#3b82f6" fill="#1d4ed8" fillOpacity={0.3} name="Storage (TB)" />
                </AreaChart>
              </ResponsiveContainer>
            </Card>

            <Card>
              <h4 className="text-xs text-gray-400 mb-2">Monthly Cloud Costs</h4>
              <ResponsiveContainer width="100%" height={150}>
                <LineChart data={result.monthly_projection?.slice(0, 24)}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis dataKey="month" tick={{ fontSize: 10, fill: '#9ca3af' }} />
                  <YAxis tick={{ fontSize: 10, fill: '#9ca3af' }} tickFormatter={(v) => `$${v}`} />
                  <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                    formatter={(v: any) => [`$${Number(v).toFixed(0)}`, 'Cost']} />
                  <Line type="monotone" dataKey="cost_usd" stroke="#f59e0b" dot={false} name="Monthly Cost ($)" />
                </LineChart>
              </ResponsiveContainer>
            </Card>

            {result.cost_summary && (
              <Card>
                <h4 className="text-xs text-gray-400 mb-2">Cost Summary</h4>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  {Object.entries(result.cost_summary).map(([k, v]: any) => (
                    <div key={k} className="flex justify-between">
                      <span className="text-gray-400">{k.replace(/_/g, ' ')}</span>
                      <span className="font-mono text-white">{typeof v === 'number' ? `$${v.toFixed(2)}` : v}</span>
                    </div>
                  ))}
                </div>
              </Card>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ── Pipeline Sizing ───────────────────────────────────────────────────────────
function PipelineSizing() {
  const [form, setForm] = useState({
    input_size_gb: 100,
    transformation_complexity: 'medium',
    target_sla_minutes: 30,
    concurrent_pipelines: 3,
  });
  const [result, setResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  const calculate = async () => {
    setLoading(true);
    const r = await api.post('/capacity/pipeline', form);
    setResult(r.data);
    setLoading(false);
  };

  useEffect(() => { calculate(); }, []);

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card>
          <h3 className="text-sm font-semibold text-white mb-3">Workload Parameters</h3>
          <div className="space-y-3">
            <div>
              <label className="text-xs text-gray-400">Input Data Size (GB)</label>
              <input type="number" value={form.input_size_gb}
                onChange={e => setForm(prev => ({ ...prev, input_size_gb: Number(e.target.value) }))}
                className="w-full mt-1 px-2 py-1 bg-gray-900 border border-gray-700 rounded text-sm text-white" />
            </div>
            <div>
              <label className="text-xs text-gray-400">Transformation Complexity</label>
              <select value={form.transformation_complexity}
                onChange={e => setForm(prev => ({ ...prev, transformation_complexity: e.target.value }))}
                className="w-full mt-1 px-2 py-1 bg-gray-900 border border-gray-700 rounded text-sm text-white">
                <option value="low">Low (filter, rename)</option>
                <option value="medium">Medium (joins, aggregations)</option>
                <option value="high">High (window functions, ML features)</option>
              </select>
            </div>
            <div>
              <label className="text-xs text-gray-400">SLA Target (minutes)</label>
              <input type="number" value={form.target_sla_minutes}
                onChange={e => setForm(prev => ({ ...prev, target_sla_minutes: Number(e.target.value) }))}
                className="w-full mt-1 px-2 py-1 bg-gray-900 border border-gray-700 rounded text-sm text-white" />
            </div>
            <div>
              <label className="text-xs text-gray-400">Concurrent Pipelines</label>
              <input type="number" value={form.concurrent_pipelines}
                onChange={e => setForm(prev => ({ ...prev, concurrent_pipelines: Number(e.target.value) }))}
                className="w-full mt-1 px-2 py-1 bg-gray-900 border border-gray-700 rounded text-sm text-white" />
            </div>
            <button onClick={calculate} disabled={loading} className="w-full py-2 bg-blue-600 hover:bg-blue-700 text-white rounded text-sm">
              {loading ? 'Calculating...' : 'Size Pipeline'}
            </button>
          </div>
        </Card>

        {result && (
          <div className="md:col-span-2 space-y-3">
            <div className="grid grid-cols-2 gap-3">
              <Card>
                <h4 className="text-sm font-semibold text-orange-400 mb-2">Spark Cluster</h4>
                {result.spark_recommendation && Object.entries(result.spark_recommendation).map(([k, v]: any) => (
                  <div key={k} className="flex justify-between py-0.5 text-xs">
                    <span className="text-gray-400">{k.replace(/_/g, ' ')}</span>
                    <span className="font-mono text-white">{String(v)}</span>
                  </div>
                ))}
              </Card>
              <Card>
                <h4 className="text-sm font-semibold text-purple-400 mb-2">Kafka Partitions</h4>
                {result.kafka_recommendation && Object.entries(result.kafka_recommendation).map(([k, v]: any) => (
                  <div key={k} className="flex justify-between py-0.5 text-xs">
                    <span className="text-gray-400">{k.replace(/_/g, ' ')}</span>
                    <span className="font-mono text-white">{String(v)}</span>
                  </div>
                ))}
              </Card>
            </div>

            <Card>
              <h4 className="text-sm font-semibold text-white mb-2">SLA Analysis</h4>
              <div className="grid grid-cols-3 gap-3 text-center text-xs mb-3">
                <div>
                  <p className="text-gray-400">Estimated Duration</p>
                  <p className="text-xl font-bold text-white">{result.estimated_duration_minutes?.toFixed(0)} min</p>
                </div>
                <div>
                  <p className="text-gray-400">SLA Target</p>
                  <p className="text-xl font-bold text-blue-400">{form.target_sla_minutes} min</p>
                </div>
                <div>
                  <p className="text-gray-400">SLA Status</p>
                  <p className={`text-xl font-bold ${result.sla_met ? 'text-green-400' : 'text-red-400'}`}>
                    {result.sla_met ? 'MET' : 'BREACH'}
                  </p>
                </div>
              </div>
              <div className="w-full bg-gray-700 rounded-full h-2">
                <div
                  className={`h-2 rounded-full transition-all ${result.sla_met ? 'bg-green-500' : 'bg-red-500'}`}
                  style={{ width: `${Math.min(100, (result.estimated_duration_minutes / form.target_sla_minutes) * 100)}%` }}
                />
              </div>
            </Card>

            {result.resource_utilization && (
              <Card>
                <h4 className="text-sm font-semibold text-white mb-2">Resource Utilization</h4>
                <ResponsiveContainer width="100%" height={150}>
                  <BarChart data={[result.resource_utilization]}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis tick={{ fontSize: 10, fill: '#9ca3af' }} />
                    <YAxis tick={{ fontSize: 10, fill: '#9ca3af' }} domain={[0, 100]} unit="%" />
                    <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151' }}
                      formatter={(v: any) => [`${v}%`]} />
                    <Legend wrapperStyle={{ fontSize: 11 }} />
                    <Bar dataKey="cpu_percent" name="CPU" fill="#3b82f6" />
                    <Bar dataKey="memory_percent" name="Memory" fill="#8b5cf6" />
                    <Bar dataKey="disk_io_percent" name="Disk I/O" fill="#f59e0b" />
                    <Bar dataKey="network_percent" name="Network" fill="#10b981" />
                  </BarChart>
                </ResponsiveContainer>
              </Card>
            )}

            {result.cost_per_run_usd && (
              <Card>
                <div className="flex justify-between items-center">
                  <div>
                    <p className="text-xs text-gray-400">Cost per Run</p>
                    <p className="text-2xl font-bold text-white">${result.cost_per_run_usd}</p>
                  </div>
                  <div>
                    <p className="text-xs text-gray-400">Monthly (daily runs)</p>
                    <p className="text-2xl font-bold text-yellow-400">${(result.cost_per_run_usd * 30).toFixed(2)}</p>
                  </div>
                  <div>
                    <p className="text-xs text-gray-400">Annual</p>
                    <p className="text-2xl font-bold text-red-400">${(result.cost_per_run_usd * 365).toFixed(2)}</p>
                  </div>
                </div>
              </Card>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ── TPC-DS Benchmarks ─────────────────────────────────────────────────────────
function TPCDSBenchmarks() {
  const [benchmarks, setBenchmarks] = useState<any[]>([]);
  const [costRecs, setCostRecs] = useState<any[]>([]);

  useEffect(() => {
    Promise.all([
      api.get('/capacity/benchmarks'),
      api.get('/capacity/cost-recommendations'),
    ]).then(([b, c]) => {
      setBenchmarks(b.data);
      setCostRecs(c.data);
    });
  }, []);

  const complexityColor = { low: 'green', medium: 'yellow', high: 'red', very_high: 'red' } as Record<string, string>;

  const chartData = benchmarks.map(b => ({
    query: b.query_id,
    'Snowflake XL': b.results?.snowflake_xl_seconds,
    'Redshift dc2': b.results?.redshift_dc2_seconds,
    'BigQuery on-demand': b.results?.bigquery_ondemand_seconds,
    'Databricks DBR': b.results?.databricks_dbr_seconds,
  }));

  return (
    <div className="space-y-4">
      {chartData.length > 0 && (
        <Card>
          <h3 className="text-sm font-semibold text-white mb-3">TPC-DS Query Execution Time by Platform (seconds)</h3>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="query" tick={{ fontSize: 10, fill: '#9ca3af' }} />
              <YAxis tick={{ fontSize: 10, fill: '#9ca3af' }} unit="s" />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151' }}
                formatter={(v: any) => [`${v}s`]} />
              <Legend wrapperStyle={{ fontSize: 10 }} />
              <Bar dataKey="Snowflake XL" fill="#60a5fa" />
              <Bar dataKey="Redshift dc2" fill="#f97316" />
              <Bar dataKey="BigQuery on-demand" fill="#34d399" />
              <Bar dataKey="Databricks DBR" fill="#a78bfa" />
            </BarChart>
          </ResponsiveContainer>
        </Card>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="space-y-2">
          <h3 className="text-sm font-semibold text-white">TPC-DS Queries</h3>
          {benchmarks.map((b) => (
            <Card key={b.query_id}>
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs font-mono text-blue-400">{b.query_id}</span>
                <Badge color={complexityColor[b.complexity] ?? 'gray'}>{b.complexity}</Badge>
              </div>
              <p className="text-xs text-white">{b.description}</p>
              <div className="flex gap-3 mt-2 text-xs text-gray-400">
                {b.results && Object.entries(b.results).map(([platform, secs]: any) => (
                  <span key={platform}><span className="text-gray-500">{platform.split('_')[0]}:</span> {secs}s</span>
                ))}
              </div>
            </Card>
          ))}
        </div>

        <div className="space-y-2">
          <h3 className="text-sm font-semibold text-white">Cost Optimization Recommendations</h3>
          {costRecs.map((rec, i) => (
            <Card key={i}>
              <div className="flex items-center justify-between mb-1">
                <p className="text-xs font-semibold text-white">{rec.category}</p>
                <Badge color={rec.impact === 'high' ? 'green' : rec.impact === 'medium' ? 'yellow' : 'gray'}>
                  {rec.savings_pct}% savings
                </Badge>
              </div>
              <p className="text-xs text-gray-300">{rec.recommendation}</p>
              <p className="text-xs text-gray-500 mt-1 font-mono">{rec.implementation}</p>
            </Card>
          ))}
        </div>
      </div>
    </div>
  );
}

// ── Load Testing ──────────────────────────────────────────────────────────────
function LoadTesting() {
  const [config, setConfig] = useState({
    target_endpoint: '/api/query',
    ramp_users: 100,
    duration_seconds: 120,
    think_time_ms: 500,
  });
  const [result, setResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  const runTest = async () => {
    setLoading(true);
    const r = await api.post('/capacity/loadtest', config);
    setResult(r.data);
    setLoading(false);
  };

  const stepColor = (step: any) => {
    if (step.error_rate > 5) return 'bg-red-900/30 border-red-700';
    if (step.error_rate > 1) return 'bg-yellow-900/30 border-yellow-700';
    return 'bg-gray-900/30 border-gray-700';
  };

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
        <div>
          <label className="text-xs text-gray-400">Target Endpoint</label>
          <input value={config.target_endpoint}
            onChange={e => setConfig(prev => ({ ...prev, target_endpoint: e.target.value }))}
            className="w-full mt-1 px-2 py-1 bg-gray-900 border border-gray-700 rounded text-sm text-white" />
        </div>
        <div>
          <label className="text-xs text-gray-400">Max Users</label>
          <input type="number" value={config.ramp_users}
            onChange={e => setConfig(prev => ({ ...prev, ramp_users: Number(e.target.value) }))}
            className="w-full mt-1 px-2 py-1 bg-gray-900 border border-gray-700 rounded text-sm text-white" />
        </div>
        <div>
          <label className="text-xs text-gray-400">Duration (sec)</label>
          <input type="number" value={config.duration_seconds}
            onChange={e => setConfig(prev => ({ ...prev, duration_seconds: Number(e.target.value) }))}
            className="w-full mt-1 px-2 py-1 bg-gray-900 border border-gray-700 rounded text-sm text-white" />
        </div>
        <div>
          <label className="text-xs text-gray-400">Think Time (ms)</label>
          <input type="number" value={config.think_time_ms}
            onChange={e => setConfig(prev => ({ ...prev, think_time_ms: Number(e.target.value) }))}
            className="w-full mt-1 px-2 py-1 bg-gray-900 border border-gray-700 rounded text-sm text-white" />
        </div>
      </div>

      <button onClick={runTest} disabled={loading} className="px-6 py-2 bg-red-600 hover:bg-red-700 text-white rounded text-sm font-medium">
        {loading ? 'Running Load Test...' : 'Run Load Test'}
      </button>

      {result && (
        <div className="space-y-4">
          {result.breaking_point && (
            <Card className="border-red-700 bg-red-900/20">
              <div className="flex items-center gap-3">
                <span className="text-2xl">⚠</span>
                <div>
                  <p className="text-sm font-semibold text-red-300">Breaking Point Detected</p>
                  <p className="text-xs text-red-400">{result.breaking_point.users} concurrent users — {result.breaking_point.reason}</p>
                </div>
              </div>
            </Card>
          )}

          <div className="grid grid-cols-4 gap-3">
            {[
              { label: 'Peak RPS', value: result.summary?.peak_rps, unit: '', color: 'text-blue-400' },
              { label: 'P95 Latency', value: result.summary?.p95_latency_ms, unit: 'ms', color: 'text-yellow-400' },
              { label: 'P99 Latency', value: result.summary?.p99_latency_ms, unit: 'ms', color: 'text-orange-400' },
              { label: 'Error Rate', value: result.summary?.error_rate_pct, unit: '%', color: result.summary?.error_rate_pct > 1 ? 'text-red-400' : 'text-green-400' },
            ].map(m => (
              <Card key={m.label} className="text-center">
                <p className={`text-xl font-bold ${m.color}`}>{m.value}{m.unit}</p>
                <p className="text-xs text-gray-400">{m.label}</p>
              </Card>
            ))}
          </div>

          <Card>
            <h4 className="text-sm font-semibold text-white mb-2">Load Ramp — Latency & Error Rate</h4>
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={result.ramp_steps}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="concurrent_users" tick={{ fontSize: 10, fill: '#9ca3af' }} label={{ value: 'Users', position: 'insideRight', fill: '#6b7280', fontSize: 10 }} />
                <YAxis yAxisId="left" tick={{ fontSize: 10, fill: '#9ca3af' }} />
                <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 10, fill: '#9ca3af' }} unit="%" />
                <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151' }} />
                <Legend wrapperStyle={{ fontSize: 11 }} />
                <Line yAxisId="left" type="monotone" dataKey="p95_latency_ms" stroke="#f59e0b" dot={false} name="P95 Latency (ms)" />
                <Line yAxisId="right" type="monotone" dataKey="error_rate" stroke="#ef4444" dot={false} name="Error Rate (%)" />
              </LineChart>
            </ResponsiveContainer>
          </Card>

          <div className="space-y-2">
            <h4 className="text-sm font-semibold text-white">Ramp Steps</h4>
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-gray-400 border-b border-gray-700">
                    <th className="pb-2 text-left">Users</th>
                    <th className="pb-2 text-right">RPS</th>
                    <th className="pb-2 text-right">P50 (ms)</th>
                    <th className="pb-2 text-right">P95 (ms)</th>
                    <th className="pb-2 text-right">Errors</th>
                    <th className="pb-2 text-right">CPU %</th>
                    <th className="pb-2 text-right">Mem %</th>
                  </tr>
                </thead>
                <tbody>
                  {result.ramp_steps?.map((step: any, i: number) => (
                    <tr key={i} className={`border-b border-gray-800 ${stepColor(step)}`}>
                      <td className="py-1 font-mono">{step.concurrent_users}</td>
                      <td className="py-1 text-right">{step.requests_per_second}</td>
                      <td className="py-1 text-right">{step.p50_latency_ms}</td>
                      <td className="py-1 text-right">{step.p95_latency_ms}</td>
                      <td className={`py-1 text-right ${step.error_rate > 5 ? 'text-red-400' : step.error_rate > 0 ? 'text-yellow-400' : 'text-green-400'}`}>{step.error_rate}%</td>
                      <td className="py-1 text-right">{step.cpu_percent}%</td>
                      <td className="py-1 text-right">{step.memory_percent}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ── Main Page ─────────────────────────────────────────────────────────────────
const SECTIONS = [
  { id: 'storage', label: 'Storage Calculator' },
  { id: 'pipeline', label: 'Pipeline Sizing' },
  { id: 'benchmarks', label: 'TPC-DS Benchmarks' },
  { id: 'loadtest', label: 'Load Testing' },
];

export default function CapacityPlanning() {
  const [activeSection, setActiveSection] = useState('storage');

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Capacity Planning</h1>
        <p className="text-gray-400 text-sm mt-1">Storage projections, pipeline sizing, benchmarks, and load testing</p>
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

      {activeSection === 'storage' && <StorageCalculator />}
      {activeSection === 'pipeline' && <PipelineSizing />}
      {activeSection === 'benchmarks' && <TPCDSBenchmarks />}
      {activeSection === 'loadtest' && <LoadTesting />}
    </div>
  );
}
