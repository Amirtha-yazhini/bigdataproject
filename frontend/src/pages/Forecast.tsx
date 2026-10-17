import { useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { crowdLevel, fmtTime, num, pct, StationMeta } from "../api";
import { axisProps, Card, ChartTooltip, Empty, LevelBadge } from "../components/ui";
import { useLive, usePoll } from "../hooks/useLive";

interface FlowWindow {
  window_start: number; in?: number; out?: number; wifi?: number; camera?: number; fused?: number;
  crowd_now?: number; crowd_15m?: number; crowd_30m?: number; rate_now?: number; rate_next_hour?: number; cards: Record<string, number>;
}
interface Flow { station: StationMeta; windows: FlowWindow[]; row_prefix: string }

const BLUE = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"];
const CARD_ORDER = ["OMNY_CONTACTLESS", "OMNY_MOBILE", "METROCARD", "STUDENT", "REDUCED_FARE"];

export default function Forecast() {
  const { live, network } = useLive();
  const [params, setParams] = useSearchParams();
  const [query, setQuery] = useState("");
  const defaultStation = useMemo(() => {
    const s = [...(live?.stations ?? [])].sort((a, b) => (b.crowd_30m ?? 0) - (a.crowd_30m ?? 0))[0];
    return s?.station_id ?? network?.stations[0]?.station_id ?? null;
  }, [live?.stations?.length, network]);
  const stationId = params.get("station") ?? defaultStation;
  const { data: flow } = usePoll<Flow>(stationId ? `/api/stations/${encodeURIComponent(stationId)}/flow?window=1m&limit=120` : null, 5000);
  const liveStation = live?.stations.find((s) => s.station_id === stationId);

  const series = useMemo(() => {
    const ws = flow?.windows ?? [];
    const ahead30 = new Map(ws.map((w) => [w.window_start + 30 * 60000, w.crowd_30m]));
    const ahead15 = new Map(ws.map((w) => [w.window_start + 15 * 60000, w.crowd_15m]));
    const pts = ws.map((w) => ({
      t: w.window_start, actual: w.crowd_now != null ? w.crowd_now * 100 : null,
      f15: ahead15.has(w.window_start) && ahead15.get(w.window_start) != null ? ahead15.get(w.window_start)! * 100 : null,
      f30: ahead30.has(w.window_start) && ahead30.get(w.window_start) != null ? ahead30.get(w.window_start)! * 100 : null,
      entries: w.in ?? 0, exits: w.out ?? 0,
    }));
    // forecasts issued in the last 30 minutes point into the future
    const last = ws[ws.length - 1];
    if (last) {
      for (const w of ws.slice(-30)) {
        for (const [h, key] of [[15, "crowd_15m"], [30, "crowd_30m"]] as const) {
          const t = w.window_start + h * 60000;
          if (t > last.window_start && w[key] != null) {
            pts.push({ t, actual: null, f15: h === 15 ? w[key]! * 100 : null, f30: h === 30 ? w[key]! * 100 : null, entries: null as unknown as number, exits: null as unknown as number });
          }
        }
      }
    }
    const merged = new Map<number, (typeof pts)[number]>();
    for (const p of pts) {
      const m = merged.get(p.t);
      merged.set(p.t, m ? { ...m, f15: m.f15 ?? p.f15, f30: m.f30 ?? p.f30 } : p);
    }
    return [...merged.values()].sort((a, b) => a.t - b.t);
  }, [flow]);

  const cards = useMemo(() => {
    const tot: Record<string, number> = {};
    for (const w of flow?.windows ?? []) for (const [k, v] of Object.entries(w.cards)) tot[k] = (tot[k] ?? 0) + v;
    return CARD_ORDER.filter((k) => tot[k]).map((k) => ({ card: k.replace("_", " ").toLowerCase(), riders: tot[k] }));
  }, [flow]);

  const profile = useMemo(() => {
    const p = flow?.station.profile;
    if (!p) return [];
    return Array.from({ length: 24 }, (_, h) => ({ hour: h, weekday: p[0][h], saturday: p[1][h], sunday: p[2][h] }));
  }, [flow]);
  const nowHour = live ? new Date(live.clock).getUTCHours() : null;

  const matches = (network?.stations ?? []).filter((s) => query && s.name.toLowerCase().includes(query.toLowerCase())).slice(0, 8);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end gap-3">
        <div>
          <h1 className="text-lg font-semibold">{flow?.station.name ?? "Station"} <span className="text-sm font-normal text-muted">{flow?.station.routes?.join(" ")}</span></h1>
          <p className="text-xs text-muted">Demand profile: {flow?.station.demand_source === "real" ? "real MTA hourly ridership" : "synthetic (no ridership match)"} · HBase prefix <code>{flow?.row_prefix}</code></p>
        </div>
        <div className="relative ml-auto">
          <input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Find a station…"
            className="card px-3 py-1.5 text-sm w-64 bg-surface outline-none" />
          {matches.length > 0 && (
            <ul className="card absolute right-0 mt-1 w-64 z-[1100] text-sm py-1">
              {matches.map((s) => (
                <li key={s.station_id}><button className="w-full text-left px-3 py-1 hover:bg-raised"
                  onClick={() => { setParams({ station: s.station_id }); setQuery(""); }}>
                  {s.name} <span className="text-muted text-xs">{s.routes.join(" ")}</span></button></li>
              ))}
            </ul>
          )}
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {([["Crowding now", liveStation?.crowd_now], ["In 15 min", liveStation?.crowd_15m], ["In 30 min", liveStation?.crowd_30m]] as const).map(([l, c]) => (
          <div key={l} className="card px-4 py-3">
            <div className="text-xs text-ink2">{l}</div>
            <div className="text-2xl font-semibold tnum">{pct(c)}</div>
            <LevelBadge level={crowdLevel(c)} />
          </div>
        ))}
        <div className="card px-4 py-3">
          <div className="text-xs text-ink2">Entries/hour: now → next hour</div>
          <div className="text-2xl font-semibold tnum">{num(liveStation?.rate_per_hour)} → {num(liveStation?.rate_next_hour)}</div>
          <div className="text-xs text-muted">{liveStation?.model}</div>
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <Card title="Crowding: actual vs. forecast (% of station capacity)"
          subtitle="Forecast lines are plotted at the time they predict; where they meet the actual line you can judge accuracy">
          <div className="h-72">
            {series.length > 1 ? (
              <ResponsiveContainer>
                <LineChart data={series} margin={{ top: 8, right: 12, left: -12, bottom: 0 }}>
                  <CartesianGrid stroke="var(--grid)" vertical={false} />
                  <XAxis dataKey="t" type="number" domain={["dataMin", "dataMax"]} tickFormatter={(t) => fmtTime(t)} {...axisProps} minTickGap={40} />
                  <YAxis {...axisProps} axisLine={false} unit="%" />
                  <ReferenceLine y={100} stroke="var(--axis)" strokeDasharray="3 3" />
                  <Tooltip content={<ChartTooltip labelFormatter={(l) => fmtTime(Number(l))} valueFormatter={(v) => `${num(v)}%`} />} />
                  <Legend wrapperStyle={{ fontSize: 12, color: "var(--ink-2)" }} />
                  <Line dataKey="actual" name="Actual (fused sensors)" stroke="var(--series-1)" strokeWidth={2} dot={false} connectNulls isAnimationActive={false} />
                  <Line dataKey="f15" name="Forecast made 15 min earlier" stroke="var(--series-2)" strokeWidth={2} strokeDasharray="5 3" dot={false} connectNulls isAnimationActive={false} />
                  <Line dataKey="f30" name="Forecast made 30 min earlier" stroke="var(--series-3)" strokeWidth={2} strokeDasharray="2 3" dot={false} connectNulls isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            ) : <Empty>Waiting for flow windows…</Empty>}
          </div>
        </Card>
        <Card title="Gate taps per minute (1-minute windows)" subtitle="Spark event-time windows over the tap stream; each tap event represents 10 riders">
          <div className="h-72">
            {series.some((p) => p.entries) ? (
              <ResponsiveContainer>
                <BarChart data={series.filter((p) => p.entries != null)} margin={{ top: 8, right: 12, left: -12, bottom: 0 }} barGap={2}>
                  <CartesianGrid stroke="var(--grid)" vertical={false} />
                  <XAxis dataKey="t" tickFormatter={(t) => fmtTime(t)} {...axisProps} minTickGap={40} />
                  <YAxis {...axisProps} axisLine={false} />
                  <Tooltip cursor={{ fill: "var(--raised)" }} content={<ChartTooltip labelFormatter={(l) => fmtTime(Number(l))} valueFormatter={(v) => num(v)} />} />
                  <Legend wrapperStyle={{ fontSize: 12 }} />
                  <Bar dataKey="entries" name="Entries" fill="var(--series-1)" radius={[4, 4, 0, 0]} isAnimationActive={false} />
                  <Bar dataKey="exits" name="Exits" fill="var(--series-2)" radius={[4, 4, 0, 0]} isAnimationActive={false} />
                </BarChart>
              </ResponsiveContainer>
            ) : <Empty>No taps yet</Empty>}
          </div>
        </Card>
        <Card title="Historical demand profile – entries per hour" subtitle={`Mean of 8 weeks of MTA hourly ridership (Sep–Oct 2024). Dashed line = current simulated hour`}>
          <div className="h-64">
            <ResponsiveContainer>
              <LineChart data={profile} margin={{ top: 8, right: 12, left: -6, bottom: 0 }}>
                <CartesianGrid stroke="var(--grid)" vertical={false} />
                <XAxis dataKey="hour" {...axisProps} tickFormatter={(h) => `${h}h`} />
                <YAxis {...axisProps} axisLine={false} />
                {nowHour != null && <ReferenceLine x={nowHour} stroke="var(--muted)" strokeDasharray="3 3" />}
                <Tooltip content={<ChartTooltip labelFormatter={(l) => `${l}:00–${l}:59`} valueFormatter={(v) => `${num(v)} /h`} />} />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                <Line dataKey="weekday" name="Weekday" stroke="var(--series-1)" strokeWidth={2} dot={false} isAnimationActive={false} />
                <Line dataKey="saturday" name="Saturday" stroke="var(--series-2)" strokeWidth={2} dot={false} isAnimationActive={false} />
                <Line dataKey="sunday" name="Sunday" stroke="var(--series-3)" strokeWidth={2} dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </Card>
        <Card title="Riders by fare medium (last 2 h)" subtitle="Dynamic HBase counter columns tap:card_<TYPE>, incremented atomically per micro-batch">
          <div className="h-64">
            {cards.length ? (
              <ResponsiveContainer>
                <BarChart data={cards} layout="vertical" margin={{ top: 4, right: 24, left: 40, bottom: 0 }}>
                  <CartesianGrid stroke="var(--grid)" horizontal={false} />
                  <XAxis type="number" {...axisProps} />
                  <YAxis type="category" dataKey="card" {...axisProps} width={110} axisLine={false} />
                  <Tooltip cursor={{ fill: "var(--raised)" }} content={<ChartTooltip valueFormatter={(v) => `${num(v)} riders`} />} />
                  <Bar dataKey="riders" name="Riders" fill="var(--series-1)" radius={[0, 4, 4, 0]} isAnimationActive={false} />
                </BarChart>
              </ResponsiveContainer>
            ) : <Empty>No counters yet</Empty>}
          </div>
        </Card>
      </div>
      <Heatmap stations={network?.stations ?? []} onPick={(id) => setParams({ station: id })} />
    </div>
  );
}

function Heatmap({ stations, onPick }: { stations: StationMeta[]; onPick: (id: string) => void }) {
  const top = [...stations].filter((s) => s.demand_source === "real").sort((a, b) => b.peak_hourly - a.peak_hourly).slice(0, 18);
  const max = Math.max(1, ...top.flatMap((s) => s.profile[0]));
  const [hover, setHover] = useState<string | null>(null);
  return (
    <Card title="Weekday demand heatmap – busiest stations (real MTA ridership)" subtitle="Entries per hour, mean of 8 weeks. Darker = more riders. Click a row to open the station.">
      <div className="overflow-x-auto">
        <table className="text-[11px] border-separate" style={{ borderSpacing: 2 }}>
          <thead><tr><th />{Array.from({ length: 24 }, (_, h) => <th key={h} className="text-muted font-normal w-7">{h}</th>)}</tr></thead>
          <tbody>
            {top.map((s) => (
              <tr key={s.station_id} className="cursor-pointer" onClick={() => onPick(s.station_id)}>
                <td className="pr-2 text-ink2 whitespace-nowrap">{s.name} <span className="text-muted">{s.routes.join("")}</span></td>
                {s.profile[0].map((v, h) => {
                  const step = Math.min(BLUE.length - 1, Math.floor((v / max) * BLUE.length));
                  const id = `${s.station_id}-${h}`;
                  return (
                    <td key={h} onMouseEnter={() => setHover(id)} onMouseLeave={() => setHover(null)}
                      className="h-5 rounded relative" style={{ background: BLUE[step], outline: hover === id ? "2px solid var(--ink)" : undefined }}
                      title={`${s.name}, ${h}:00 – ${num(v)} entries/h`} />
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="flex items-center gap-2 text-[11px] text-muted mt-2">
        <span>0</span>{BLUE.map((c) => <span key={c} className="w-6 h-2.5 rounded-sm" style={{ background: c }} />)}<span>{num(max)} / h</span>
      </div>
    </Card>
  );
}
