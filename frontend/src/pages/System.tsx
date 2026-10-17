import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { fmtTime, num } from "../api";
import { axisProps, Card, ChartTooltip, Empty, LevelBadge } from "../components/ui";
import { usePoll } from "../hooks/useLive";

interface Query { _key: string; name: string; batch_id: number; num_input_rows: number; input_rps: number; processed_rps: number; batch_ms: number; watermark: string; state_rows: number; active: boolean }
interface Health {
  hbase: { status: string; tables?: string[]; error?: string };
  kafka: { status: string; topics?: { topic: string; partitions: number; messages: number; rate_per_s: number | null }[]; error?: string };
  spark: { status: string; queries?: Query[] };
  tap_schemas?: Record<string, number>;
  recommender?: { last_run_wall?: number; issued_total?: number };
  sim_clock?: number;
  provenance: { mode: string; built_at: string; datasets: { name: string; type: string; url?: string; used_for: string }[]; stats: Record<string, number> };
  model: null | {
    model: string; dataset: string; target: string; features: string[]; rows_train: number; rows_test: number; split: string;
    gbt: Metrics; baseline_profile: Metrics; baseline_persistence: Metrics; feature_importance: Record<string, number>; trained_at: string;
  };
}
interface Metrics { rmse: number; mae: number; r2: number }
interface SchemaRow { table: string; row_key: string; example: string; purpose: string; families?: Record<string, Record<string, string | number>>; rows?: number; sample?: Record<string, unknown> | null }

const QUERY_DOC: Record<string, string> = {
  vehicle_state: "Upsert current vehicle state, append time series, headway/bunching",
  route_windows: "1-min tumbling windows per route (watermark 2 min)",
  tap_breakdown: "Schema normalisation + atomic card-type counters",
  flow_1m: "Taps ∪ density fusion, 1-min windows + MLlib crowding prediction",
  flow_15m: "15-min sliding windows (5-min slide)",
  incidents: "Incident upserts + impact analysis",
};

export default function System() {
  const { data: h, error } = usePoll<Health>("/api/system/health", 5000);
  const { data: schema } = usePoll<SchemaRow[]>("/api/system/schema", 15000);
  if (error && !h) return <Empty>{error}</Empty>;
  if (!h) return <Empty>Loading…</Empty>;
  const status = (s: string) => <LevelBadge level={s === "ok" ? "LOW" : s === "starting" ? "MODERATE" : "CRITICAL"} label={s} />;
  const m = h.model;
  const modelBars = m ? [
    { name: "Spark GBT", rmse: m.gbt.rmse, mae: m.gbt.mae, r2: m.gbt.r2, main: true },
    { name: "Historical profile", rmse: m.baseline_profile.rmse, mae: m.baseline_profile.mae, r2: m.baseline_profile.r2 },
    { name: "Persistence (last hour)", rmse: m.baseline_persistence.rmse, mae: m.baseline_persistence.mae, r2: m.baseline_persistence.r2 },
  ] : [];
  const fi = m ? Object.entries(m.feature_importance).map(([f, v]) => ({ f, v })).sort((a, b) => b.v - a.v) : [];
  const taps = h.tap_schemas ?? {};
  const tapTotal = Object.values(taps).reduce((a, b) => a + b, 0) || 1;

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
        <div className="card px-4 py-3"><div className="text-xs text-ink2">Apache Kafka</div><div className="mt-1">{status(h.kafka.status)}</div></div>
        <div className="card px-4 py-3"><div className="text-xs text-ink2">Apache Spark Structured Streaming</div><div className="mt-1">{status(h.spark.status)}</div>
          <div className="text-[11px] text-muted">{h.spark.queries?.filter((q) => q.active).length ?? 0} active queries</div></div>
        <div className="card px-4 py-3"><div className="text-xs text-ink2">Apache HBase (Thrift)</div><div className="mt-1">{status(h.hbase.status)}</div>
          <div className="text-[11px] text-muted">{h.hbase.tables?.length ?? 0} tables</div></div>
        <div className="card px-4 py-3"><div className="text-xs text-ink2">Recommendation engine</div>
          <div className="mt-1 text-sm">last run {h.recommender?.last_run_wall ? new Date(h.recommender.last_run_wall).toLocaleTimeString() : "–"}</div>
          <div className="text-[11px] text-muted">sim clock {fmtTime(h.sim_clock, true)}</div></div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <Card title="Spark streaming queries" subtitle="From a StreamingQueryListener that writes each progress event to HBase system_meta">
          <div className="overflow-x-auto">
            <table className="w-full text-xs tnum">
              <thead className="text-muted text-left"><tr><th className="py-1">Query</th><th className="text-right">Batch</th><th className="text-right">Rows in</th><th className="text-right">In/s</th><th className="text-right">Done/s</th><th className="text-right">Batch ms</th><th className="text-right">State rows</th></tr></thead>
              <tbody className="divide-y divide-[var(--line)]">
                {(h.spark.queries ?? []).map((q) => (
                  <tr key={q._key} title={QUERY_DOC[q.name]}>
                    <td className="py-1.5"><div className="flex items-center gap-1.5"><LevelBadge level={q.active ? "LOW" : "CRITICAL"} label={q.name} /></div>
                      <div className="text-[11px] text-muted">{QUERY_DOC[q.name]}</div></td>
                    <td className="text-right">{q.batch_id}</td><td className="text-right">{num(q.num_input_rows)}</td>
                    <td className="text-right">{num(q.input_rps, 1)}</td><td className="text-right">{num(q.processed_rps, 1)}</td>
                    <td className="text-right">{num(q.batch_ms)}</td><td className="text-right">{num(q.state_rows)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
        <Card title="Kafka topics" subtitle="Messages retained (6 h retention) and current publish rate">
          <table className="w-full text-xs tnum">
            <thead className="text-muted text-left"><tr><th className="py-1">Topic</th><th className="text-right">Partitions</th><th className="text-right">Messages</th><th className="text-right">Msgs/s</th></tr></thead>
            <tbody className="divide-y divide-[var(--line)]">
              {(h.kafka.topics ?? []).map((t) => (
                <tr key={t.topic}><td className="py-1.5 font-mono">{t.topic}</td><td className="text-right">{t.partitions}</td>
                  <td className="text-right">{num(t.messages)}</td><td className="text-right">{t.rate_per_s == null ? "…" : num(t.rate_per_s, 1)}</td></tr>
              ))}
            </tbody>
          </table>
          <div className="mt-4 text-xs text-ink2">
            <div className="font-medium text-ink mb-1">Semi-structured tap payloads parsed by Spark</div>
            {["v2", "legacy", "corrupt"].map((k) => (
              <div key={k} className="flex items-center gap-2 py-0.5">
                <span className="w-32">{k === "v2" ? "v2 nested JSON" : k === "legacy" ? "legacy flat JSON" : "corrupt (quarantined)"}</span>
                <span className="flex-1 h-2 rounded bg-raised overflow-hidden"><span className="block h-full rounded" style={{ width: `${(100 * (taps[k] ?? 0)) / tapTotal}%`, background: "var(--series-1)" }} /></span>
                <span className="w-24 text-right tnum">{num(taps[k] ?? 0)} ({num((100 * (taps[k] ?? 0)) / tapTotal, 1)}%)</span>
              </div>
            ))}
          </div>
        </Card>
      </div>

      {m && (
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
          <Card title="Crowding model – test error (RMSE, entries/hour, lower is better)"
            subtitle={`${m.model} · trained on ${num(m.rows_train)} rows of ${m.dataset}; ${m.split}`}>
            <div className="h-48">
              <ResponsiveContainer>
                <BarChart data={modelBars} layout="vertical" margin={{ top: 4, right: 40, left: 60, bottom: 0 }}>
                  <CartesianGrid stroke="var(--grid)" horizontal={false} />
                  <XAxis type="number" {...axisProps} />
                  <YAxis type="category" dataKey="name" {...axisProps} width={140} axisLine={false} />
                  <Tooltip cursor={{ fill: "var(--raised)" }} content={<ChartTooltip valueFormatter={(v) => num(v, 1)} />} />
                  <Bar dataKey="rmse" name="RMSE" radius={[0, 4, 4, 0]} isAnimationActive={false}
                    label={{ position: "right", fill: "var(--ink-2)", fontSize: 11, formatter: (v: number) => num(v, 0) }}>
                    {modelBars.map((b) => <Cell key={b.name} fill={b.main ? "var(--series-1)" : "var(--axis)"} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
            <table className="w-full text-xs tnum mt-2">
              <thead className="text-muted text-left"><tr><th>Model</th><th className="text-right">RMSE</th><th className="text-right">MAE</th><th className="text-right">R²</th></tr></thead>
              <tbody>{modelBars.map((b) => <tr key={b.name}><td>{b.name}</td><td className="text-right">{num(b.rmse, 1)}</td><td className="text-right">{num(b.mae, 1)}</td><td className="text-right">{num(b.r2, 3)}</td></tr>)}</tbody>
            </table>
          </Card>
          <Card title="Feature importance" subtitle={m.target}>
            <div className="h-60">
              <ResponsiveContainer>
                <BarChart data={fi} layout="vertical" margin={{ top: 4, right: 40, left: 30, bottom: 0 }}>
                  <CartesianGrid stroke="var(--grid)" horizontal={false} />
                  <XAxis type="number" {...axisProps} />
                  <YAxis type="category" dataKey="f" {...axisProps} width={100} axisLine={false} />
                  <Tooltip cursor={{ fill: "var(--raised)" }} content={<ChartTooltip valueFormatter={(v) => num(v, 3)} />} />
                  <Bar dataKey="v" name="Importance" fill="var(--series-1)" radius={[0, 4, 4, 0]} isAnimationActive={false} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </Card>
        </div>
      )}

      <Card title={`Data sources – mode ${h.provenance.mode}`} subtitle={`Built ${h.provenance.built_at} · ${Object.entries(h.provenance.stats).map(([k, v]) => `${k.replace(/_/g, " ")}: ${num(v)}`).join(" · ")}`}>
        <table className="w-full text-sm">
          <thead className="text-muted text-left text-xs"><tr><th className="py-1">Dataset</th><th>Type</th><th>Used for</th></tr></thead>
          <tbody className="divide-y divide-[var(--line)]">
            {h.provenance.datasets.map((d) => (
              <tr key={d.name}><td className="py-1.5 pr-3">{d.url ? <a className="underline" href={d.url} target="_blank" rel="noreferrer">{d.name}</a> : d.name}</td>
                <td className="pr-3"><span className={`text-xs px-1.5 py-0.5 rounded border ${d.type === "real" ? "border-ink text-ink" : "border-line text-ink2"}`}>{d.type}</span></td>
                <td className="text-ink2 text-xs">{d.used_for}</td></tr>
            ))}
          </tbody>
        </table>
      </Card>

      <Card title="HBase wide-column schema" subtitle="Live from the HBase Thrift API: column families, settings, and one sample row per table">
        {!schema ? <Empty>Loading schema…</Empty> : (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
            {schema.map((t) => (
              <div key={t.table} className="rounded-lg border border-line p-3 min-w-0">
                <div className="flex items-baseline gap-2"><span className="font-mono font-semibold">{t.table}</span>
                  {t.rows != null && <span className="text-xs text-muted tnum">{num(t.rows)} rows</span>}</div>
                <div className="text-xs text-ink2 mt-0.5">{t.purpose}</div>
                <div className="text-xs mt-1">Row key <code className="font-mono">{t.row_key}</code> <span className="text-muted">e.g. {t.example}</span></div>
                <div className="flex flex-wrap gap-1.5 mt-2">
                  {Object.entries(t.families ?? {}).map(([cf, o]) => (
                    <span key={cf} className="text-[11px] px-1.5 py-0.5 rounded bg-raised font-mono">
                      {cf}: VERSIONS={String(o.max_versions)}{o.time_to_live && Number(o.time_to_live) < 2147483647 ? ` TTL=${Math.round(Number(o.time_to_live) / 86400)}d` : ""}
                    </span>
                  ))}
                </div>
                {t.sample && (
                  <details className="mt-2">
                    <summary className="text-xs text-muted cursor-pointer">sample row</summary>
                    <pre className="text-[10px] mt-1 max-h-48 overflow-auto bg-raised rounded p-2">{JSON.stringify(t.sample, null, 1)}</pre>
                  </details>
                )}
              </div>
            ))}
          </div>
        )}
      </Card>
    </div>
  );
}
