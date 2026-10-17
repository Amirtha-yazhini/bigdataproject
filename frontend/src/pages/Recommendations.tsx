import { useMemo, useState } from "react";
import { CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api, fmtTime, num, Recommendation } from "../api";
import { axisProps, Card, ChartTooltip, Empty, RouteBullet } from "../components/ui";
import { useLive, usePoll } from "../hooks/useLive";

interface RouteWindow {
  window_start: number; avg_occ?: number; p90_occ?: number; max_occ?: number; avg_delay_s?: number;
  headway_actual_s?: number; headway_effective_s?: number; headway_sched_s?: number; bunching_idx?: number; vehicles?: number;
}

const ACTION_LABEL: Record<string, string> = {
  ADD_FREQUENCY: "Add trains", REDUCE_FREQUENCY: "Withdraw a train", HOLD: "Hold train", SHORT_TURN: "Short-turn", BUS_BRIDGE: "Bus bridge",
};
const ACTION_ICON: Record<string, string> = { ADD_FREQUENCY: "＋", REDUCE_FREQUENCY: "－", HOLD: "⏸", SHORT_TURN: "↶", BUS_BRIDGE: "⇄" };

export default function Recommendations() {
  const { network, refresh } = useLive();
  const { data: recs } = usePoll<Recommendation[]>("/api/recommendations?limit=200", 4000);
  const [route, setRoute] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);
  const routes = useMemo(() => Object.fromEntries((network?.routes ?? []).map((r) => [r.route_id, r])), [network]);
  const pending = (recs ?? []).filter((r) => r.status === "NEW" && (!route || r.route_id === route));
  const decided = (recs ?? []).filter((r) => r.status !== "NEW").slice(0, 15);
  const shownRoute = route ?? pending[0]?.route_id ?? network?.routes[0]?.route_id ?? null;

  const decide = async (r: Recommendation, d: "accept" | "dismiss") => {
    setBusy(r._key);
    try {
      const res = await api<{ status: string; command: unknown }>(`/api/recommendations/${encodeURIComponent(r._key)}/${d}`, { method: "POST", body: "{}" });
      setMsg(d === "accept"
        ? `Accepted: ${ACTION_LABEL[r.action]} on the ${r.route_id} line. The command was sent to Kafka control.commands; the simulator applies it within seconds.`
        : `Dismissed recommendation for the ${r.route_id} line.`);
      void res;
      refresh();
    } catch (e) {
      setMsg(String(e));
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)] gap-4">
      <div className="space-y-4 min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-lg font-semibold mr-2">Dynamic route & frequency recommendations</h1>
          <button onClick={() => setRoute(null)} className={`px-2.5 py-1 rounded-full text-xs border ${!route ? "border-ink" : "border-line text-ink2"}`}>All</button>
          {network?.routes.map((r) => (
            <button key={r.route_id} onClick={() => setRoute(r.route_id)} className={`rounded-full p-0.5 border-2 ${route === r.route_id ? "border-ink" : "border-transparent"}`}>
              <RouteBullet id={r.route_id} color={r.color} textColor={r.text_color} size={20} />
            </button>
          ))}
        </div>
        {msg && <div className="card px-4 py-2 text-sm text-ink2">{msg}</div>}
        {!recs && <Empty>Loading…</Empty>}
        {recs && !pending.length && <Card><Empty>No pending recommendations. The engine re-evaluates every 20 s from live load, headways and incidents.</Empty></Card>}
        {pending.map((r) => (
          <div key={r._key} className="card p-4">
            <div className="flex items-start gap-3">
              <RouteBullet id={r.route_id} color={routes[r.route_id]?.color ?? "#888"} textColor={routes[r.route_id]?.text_color} size={28} />
              <div className="flex-1 min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="text-base font-semibold"><span aria-hidden className="mr-1">{ACTION_ICON[r.action]}</span>{ACTION_LABEL[r.action] ?? r.action}</span>
                  <span className="text-xs text-muted">issued {fmtTime(r.created_ts)} · updated {fmtTime(r.updated_ts)}</span>
                  <span className="ml-auto text-xs text-ink2">confidence <b className="tnum">{num((r.confidence ?? 0) * 100)}%</b></span>
                </div>
                <p className="text-sm text-ink2 mt-1">{r.reason}</p>
                <div className="flex flex-wrap gap-x-5 gap-y-1 text-xs text-ink2 mt-2 tnum">
                  {r.current_headway_s != null && <span>Headway {num(r.current_headway_s / 60, 1)} min{r.suggested_headway_s != null && <> → <b className="text-ink">{num(r.suggested_headway_s / 60, 1)} min</b></>}</span>}
                  {r.extra_vehicles != null && <span>Trains {r.extra_vehicles > 0 ? "+" : ""}{r.extra_vehicles}</span>}
                  {r.predicted_load_pct != null && <span>Predicted peak load {num(r.predicted_load_pct)}%</span>}
                  {r.hold_s != null && <span>Hold {r.vehicle_id} for {r.hold_s} s</span>}
                  {r.incident_id && <span>Incident {r.incident_id}</span>}
                </div>
              </div>
            </div>
            <div className="flex gap-2 mt-3 justify-end">
              <button disabled={busy === r._key} onClick={() => decide(r, "dismiss")} className="px-3 py-1.5 text-sm rounded-md border border-line text-ink2 hover:bg-raised">Dismiss</button>
              <button disabled={busy === r._key} onClick={() => decide(r, "accept")} className="px-3 py-1.5 text-sm rounded-md bg-accent text-white hover:opacity-90">Accept & dispatch</button>
            </div>
          </div>
        ))}
        {decided.length > 0 && (
          <Card title="Recent decisions">
            <table className="w-full text-xs">
              <thead className="text-muted text-left"><tr><th className="py-1">Line</th><th>Action</th><th>Status</th><th className="text-right">Decided (sim)</th></tr></thead>
              <tbody className="divide-y divide-[var(--line)]">
                {decided.map((r) => (
                  <tr key={r._key}><td className="py-1"><RouteBullet id={r.route_id} color={routes[r.route_id]?.color ?? "#888"} size={16} /></td>
                    <td>{ACTION_LABEL[r.action] ?? r.action}</td><td>{r.status.toLowerCase()}</td><td className="text-right tnum">{fmtTime(r.decided_ts)}</td></tr>
                ))}
              </tbody>
            </table>
          </Card>
        )}
      </div>
      {shownRoute && <RoutePanel routeId={shownRoute} color={routes[shownRoute]?.color} name={routes[shownRoute]?.name} />}
    </div>
  );
}

function RoutePanel({ routeId, color, name }: { routeId: string; color?: string; name?: string }) {
  const { data } = usePoll<{ windows: RouteWindow[] }>(`/api/routes/${encodeURIComponent(routeId)}/metrics?limit=120`, 5000);
  const rows = (data?.windows ?? []).map((w) => ({
    t: w.window_start,
    actual: w.headway_actual_s != null ? w.headway_actual_s / 60 : null,
    effective: w.headway_effective_s != null ? w.headway_effective_s / 60 : null,
    sched: w.headway_sched_s != null ? w.headway_sched_s / 60 : null,
    avg: w.avg_occ, p90: w.p90_occ, bunching: w.bunching_idx, delay: w.avg_delay_s != null ? w.avg_delay_s / 60 : null,
  }));
  const last = rows[rows.length - 1];
  const chart = (title: string, subtitle: string, lines: [string, string, string, boolean?][], unit: string, ref?: number) => (
    <Card title={title} subtitle={subtitle}>
      <div className="h-48">
        {rows.length > 1 ? (
          <ResponsiveContainer>
            <LineChart data={rows} margin={{ top: 6, right: 12, left: -14, bottom: 0 }}>
              <CartesianGrid stroke="var(--grid)" vertical={false} />
              <XAxis dataKey="t" type="number" domain={["dataMin", "dataMax"]} tickFormatter={(t) => fmtTime(t)} {...axisProps} minTickGap={40} />
              <YAxis {...axisProps} axisLine={false} />
              {ref != null && <ReferenceLine y={ref} stroke="var(--axis)" strokeDasharray="3 3" />}
              <Tooltip content={<ChartTooltip labelFormatter={(l) => fmtTime(Number(l))} valueFormatter={(v) => `${num(v, unit === "" ? 2 : 1)}${unit}`} />} />
              {lines.length > 1 && <Legend wrapperStyle={{ fontSize: 12 }} />}
              {lines.map(([key, label, stroke, dashed]) => (
                <Line key={key} dataKey={key} name={label} stroke={stroke} strokeWidth={2} strokeDasharray={dashed ? "5 3" : undefined} dot={false} connectNulls isAnimationActive={false} />
              ))}
            </LineChart>
          </ResponsiveContainer>
        ) : <Empty>Collecting route windows…</Empty>}
      </div>
    </Card>
  );
  return (
    <div className="space-y-4 min-w-0">
      <div className="flex items-center gap-2">
        <RouteBullet id={routeId} color={color ?? "#888"} size={26} />
        <div>
          <div className="font-semibold">{name}</div>
          <div className="text-xs text-muted">HBase route_metrics, 1-minute event-time windows · {last ? `${num(last.avg)}% avg load, bunching ${num(last.bunching, 2)}` : ""}</div>
        </div>
      </div>
      {chart("Headway (minutes)", "Effective headway weights long gaps the way riders experience them; it rises above actual when trains bunch",
        [["actual", "Actual (mean gap)", "var(--series-1)"], ["effective", "Effective (rider-weighted)", "var(--series-2)"], ["sched", "Scheduled", "var(--series-3)", true]], " min")}
      {chart("Train load (%)", "Average and 90th percentile across trains on the line", [["avg", "Average", "var(--series-1)"], ["p90", "90th percentile", "var(--series-2)"]], "%", 100)}
      {chart("Bunching index", "Coefficient of variation of gaps between trains (0 = perfectly even); HOLD is recommended above 0.5", [["bunching", "Bunching index", "var(--series-1)"]], "", 0.5)}
    </div>
  );
}
