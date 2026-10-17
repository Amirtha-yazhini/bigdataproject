import { FormEvent, useMemo, useState } from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api, fmtTime, Incident, num } from "../api";
import TransitMap from "../components/TransitMap";
import { axisProps, Card, ChartTooltip, Empty, LevelBadge, RouteBullet } from "../components/ui";
import { useLive, usePoll } from "../hooks/useLive";

const CATEGORIES = ["Signals", "Track", "Persons on Trackbed/Police/Medical", "Subway Car", "Stations and Structure", "Other"];
const SEV_LEVEL = { 1: "MODERATE", 2: "HIGH", 3: "CRITICAL" } as const;
const SEV_LABEL = { 1: "Minor – trains slowed", 2: "Major – heavy slowdown", 3: "Severe – line blocked" } as const;

export default function Incidents() {
  const { network, live } = useLive();
  const { data: incidents } = usePoll<Incident[]>("/api/incidents", 4000);
  const [selected, setSelected] = useState<string | null>(null);
  const list = incidents ?? [];
  const open = list.filter((i) => i.status === "OPEN");
  const inc = list.find((i) => i.incident_id === selected) ?? open[0] ?? list[0] ?? null;
  const routes = useMemo(() => Object.fromEntries((network?.routes ?? []).map((r) => [r.route_id, r])), [network]);
  const stationName = useMemo(() => Object.fromEntries((network?.stations ?? []).map((s) => [s.station_id, s.name])), [network]);

  const delays = useMemo(() => {
    if (!inc || !routes[inc.route_id]) return [];
    return routes[inc.route_id].stops.filter((s) => inc.station_delays[s] != null)
      .map((s) => ({ station: stationName[s] ?? s, delay: inc.station_delays[s], incident: s === inc.station_id }));
  }, [inc, routes, stationName]);

  if (!network) return <Empty>Loading…</Empty>;
  return (
    <div className="grid grid-cols-1 xl:grid-cols-[360px_minmax(0,1fr)] gap-4">
      <div className="space-y-4 min-w-0">
        <InjectForm />
        <SurgeForm />
        <Card title={`Incidents (${open.length} open)`} subtitle="Rates and categories follow the real MTA Major Incidents dataset">
          <ul className="divide-y divide-[var(--line)] max-h-[420px] overflow-auto">
            {list.slice(0, 40).map((i) => (
              <li key={i.incident_id}>
                <button onClick={() => setSelected(i.incident_id)}
                  className={`w-full text-left py-2 px-1 flex gap-2 items-start ${inc?.incident_id === i.incident_id ? "bg-raised rounded" : ""}`}>
                  <RouteBullet id={i.route_id} color={routes[i.route_id]?.color ?? "#888"} size={20} />
                  <span className="flex-1 min-w-0">
                    <span className="block text-sm truncate">{i.type} · {i.station_name}</span>
                    <span className="block text-xs text-muted">{i.incident_id} · {fmtTime(i.start_ts)}{i.end_ts ? `–${fmtTime(i.end_ts)}` : ""} · {i.source.toLowerCase()}</span>
                  </span>
                  {i.status === "OPEN" ? <LevelBadge level={SEV_LEVEL[i.severity as 1 | 2 | 3] ?? "HIGH"} label={`Sev ${i.severity}`} /> : <span className="text-xs text-muted">cleared</span>}
                </button>
              </li>
            ))}
            {!list.length && <Empty>No incidents yet – inject one above.</Empty>}
          </ul>
        </Card>
      </div>

      <div className="space-y-4 min-w-0">
        {inc ? (
          <>
            <div className="flex flex-wrap items-center gap-3">
              <RouteBullet id={inc.route_id} color={routes[inc.route_id]?.color ?? "#888"} size={30} />
              <div>
                <h1 className="text-lg font-semibold">{inc.type} at {inc.station_name}</h1>
                <p className="text-xs text-muted">{inc.description} · {SEV_LABEL[inc.severity as 1 | 2 | 3]} · expected {inc.expected_duration_min} min · {inc.status.toLowerCase()}</p>
              </div>
            </div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <Stat label="Delay at incident" value={`${num(inc.impact.delay_min, 1)} min`} note={`projected ${num(inc.impact.projected_delay_min, 1)} min`} />
              <Stat label="Riders affected" value={num(inc.impact.affected_pax)} note={`${num(inc.impact.onboard_pax)} on board · ${num(inc.impact.waiting_pax)} waiting · ${num(inc.impact.inflow_pax)} arriving`} />
              <Stat label="Trains affected" value={num(inc.impact.affected_vehicles)} note={`${num(inc.impact.stations_affected as number)} stations`} />
              <Stat label="Passenger-delay" value={`${num(inc.impact.pax_delay_min)} min`} note={`elapsed ${num(inc.impact.elapsed_min, 0)} · remaining ${num(inc.impact.remaining_min, 0)} min`} />
            </div>
            <div className="grid grid-cols-1 2xl:grid-cols-2 gap-4">
              <Card title="Delay propagation along the line (minutes)" subtitle="HBase dynamic columns impact:st_<station>, ordered by line sequence">
                <div className="h-80">
                  {delays.length ? (
                    <ResponsiveContainer>
                      <BarChart data={delays} margin={{ top: 6, right: 8, left: -14, bottom: 60 }}>
                        <CartesianGrid stroke="var(--grid)" vertical={false} />
                        <XAxis dataKey="station" {...axisProps} angle={-50} textAnchor="end" interval={0} height={70} />
                        <YAxis {...axisProps} axisLine={false} />
                        <Tooltip cursor={{ fill: "var(--raised)" }} content={<ChartTooltip valueFormatter={(v) => `${num(v, 1)} min`} />} />
                        <Bar dataKey="delay" name="Estimated delay" fill="var(--series-1)" radius={[4, 4, 0, 0]} isAnimationActive={false} />
                      </BarChart>
                    </ResponsiveContainer>
                  ) : <Empty>Impact is computed once the incident reaches Spark…</Empty>}
                </div>
              </Card>
              <div className="card p-1 h-[360px] min-w-0">
                <TransitMap network={network} routeFilter={inc.route_id} stations={live?.stations}
                  vehicles={(live?.vehicles ?? []).filter((v) => v.route_id === inc.route_id)}
                  highlight={inc.station_delays} incidentStations={[inc.station_id]} />
              </div>
            </div>
          </>
        ) : <Card><Empty>No incidents yet. Simulated incidents appear every few minutes; you can also inject one.</Empty></Card>}
      </div>
    </div>
  );
}

function Stat({ label, value, note }: { label: string; value: string; note?: string }) {
  return (
    <div className="card px-4 py-3 min-w-0">
      <div className="text-xs text-ink2">{label}</div>
      <div className="text-xl font-semibold tnum">{value}</div>
      {note && <div className="text-[11px] text-muted">{note}</div>}
    </div>
  );
}

const field = "w-full card px-2 py-1.5 text-sm bg-surface";

function InjectForm() {
  const { network } = useLive();
  const [route, setRoute] = useState(network?.routes[0]?.route_id ?? "");
  const r = network?.routes.find((x) => x.route_id === route) ?? network?.routes[0];
  const [station, setStation] = useState("");
  const [type, setType] = useState(CATEGORIES[0]);
  const [severity, setSeverity] = useState(3);
  const [duration, setDuration] = useState(20);
  const [msg, setMsg] = useState<string | null>(null);
  const names = Object.fromEntries((network?.stations ?? []).map((s) => [s.station_id, s.name]));
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    if (!r) return;
    try {
      const ev = await api<{ incident_id: string }>("/api/incidents", {
        method: "POST", body: JSON.stringify({ route_id: r.route_id, station_id: station || r.stops[Math.floor(r.stops.length / 2)], type, severity, duration_min: duration }),
      });
      setMsg(`Published ${ev.incident_id} to Kafka. Spark computes the impact and the simulator holds trains.`);
    } catch (err) { setMsg(String(err)); }
  };
  return (
    <Card title="Inject an incident" subtitle="Goes through Kafka → simulator + Spark, exactly like a field report">
      <form onSubmit={submit} className="space-y-2 text-sm">
        <div className="grid grid-cols-2 gap-2">
          <label className="text-xs text-ink2">Line<select className={field} value={r?.route_id} onChange={(e) => { setRoute(e.target.value); setStation(""); }}>
            {network?.routes.map((x) => <option key={x.route_id} value={x.route_id}>{x.route_id}</option>)}</select></label>
          <label className="text-xs text-ink2">Severity<select className={field} value={severity} onChange={(e) => setSeverity(+e.target.value)}>
            {[1, 2, 3].map((s) => <option key={s} value={s}>{s} – {SEV_LABEL[s as 1 | 2 | 3].split(" – ")[1]}</option>)}</select></label>
        </div>
        <label className="text-xs text-ink2 block">Station<select className={field} value={station || r?.stops[Math.floor((r?.stops.length ?? 0) / 2)]} onChange={(e) => setStation(e.target.value)}>
          {r?.stops.map((s) => <option key={s} value={s}>{names[s] ?? s}</option>)}</select></label>
        <div className="grid grid-cols-[1fr_90px] gap-2">
          <label className="text-xs text-ink2">Category<select className={field} value={type} onChange={(e) => setType(e.target.value)}>
            {CATEGORIES.map((c) => <option key={c}>{c}</option>)}</select></label>
          <label className="text-xs text-ink2">Minutes<input type="number" min={5} max={120} className={field} value={duration} onChange={(e) => setDuration(+e.target.value)} /></label>
        </div>
        <button className="w-full py-1.5 rounded-md text-white text-sm" style={{ background: "var(--critical)" }}>Inject incident</button>
        {msg && <p className="text-xs text-ink2">{msg}</p>}
      </form>
    </Card>
  );
}

function SurgeForm() {
  const { network } = useLive();
  const busiest = [...(network?.stations ?? [])].sort((a, b) => b.peak_hourly - a.peak_hourly).slice(0, 40);
  const [station, setStation] = useState("");
  const [mult, setMult] = useState(3);
  const [msg, setMsg] = useState<string | null>(null);
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    const sid = station || busiest[0]?.station_id;
    try {
      await api("/api/simulate/surge", { method: "POST", body: JSON.stringify({ station_id: sid, multiplier: mult, minutes: 40 }) });
      setMsg("Demand surge sent. Watch the station's forecast rise on the Crowding forecast page.");
    } catch (err) { setMsg(String(err)); }
  };
  return (
    <Card title="Simulate a demand surge" subtitle="e.g. a concert or a game letting out">
      <form onSubmit={submit} className="grid grid-cols-[1fr_80px] gap-2 text-sm">
        <select className={field} value={station} onChange={(e) => setStation(e.target.value)}>
          {busiest.map((s) => <option key={s.station_id} value={s.station_id}>{s.name} ({s.routes.join("")})</option>)}
        </select>
        <select className={field} value={mult} onChange={(e) => setMult(+e.target.value)}>
          {[2, 3, 4, 5].map((m) => <option key={m} value={m}>×{m}</option>)}
        </select>
        <button className="col-span-2 py-1.5 rounded-md border border-line hover:bg-raised">Start 40-min surge</button>
        {msg && <p className="col-span-2 text-xs text-ink2">{msg}</p>}
      </form>
    </Card>
  );
}
