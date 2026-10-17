import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { crowdLevel, fmtTime, num, occLevel, pct, StationLive, Vehicle } from "../api";
import TransitMap from "../components/TransitMap";
import { axisProps, Card, ChartTooltip, Empty, Kpi, LevelBadge, RouteBullet } from "../components/ui";
import { useLive, usePoll } from "../hooks/useLive";

interface History { points: { ts: number; occupancy_pct: number; delay_s: number; next_stop_id: string }[]; row_prefix: string }

export default function LiveMap() {
  const { live, network } = useLive();
  const [route, setRoute] = useState<string | null>(null);
  const [metric, setMetric] = useState<"now" | "30m">("now");
  const [vehicleId, setVehicleId] = useState<string | null>(null);
  const [station, setStation] = useState<StationLive | null>(null);
  const routeMeta = useMemo(() => Object.fromEntries((network?.routes ?? []).map((r) => [r.route_id, r])), [network]);

  const vehicles = live?.vehicles ?? [];
  const stations = live?.stations ?? [];
  const vehicle = vehicles.find((v) => v.vehicle_id === vehicleId) ?? null;
  const hotStations = [...stations].filter((s) => s.crowd_30m != null && (!route || s.routes.includes(route)))
    .sort((a, b) => (b.crowd_30m ?? 0) - (a.crowd_30m ?? 0)).slice(0, 8);
  const hotTrains = [...vehicles].filter((v) => !route || v.route_id === route)
    .sort((a, b) => (b.pred_15m ?? b.occupancy_pct) - (a.pred_15m ?? a.occupancy_pct)).slice(0, 6);
  const k = live?.kpis;

  if (!network) return <Empty>Loading network…</Empty>;
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3">
        <Kpi label="Trains in service" value={num(k?.vehicles)} note={`${network.routes.length} lines`} />
        <Kpi label="Average train load" value={num(k?.avg_occupancy)} unit="%" tone={occLevel(k?.avg_occupancy)} note={`${k?.crowded_vehicles ?? 0} trains over 100%`} />
        <Kpi label="Average delay" value={num(k?.avg_delay_min, 1)} unit="min" note="negative = running early" />
        <Kpi label="Stations busy within 30 min" value={num(k?.stations_high)} tone={k && k.stations_high > 0 ? "HIGH" : "LOW"} note={`of ${k?.stations_reporting ?? 0} reporting`} />
        <Kpi label="Open incidents" value={num(k?.open_incidents)} tone={k && k.open_incidents > 0 ? "CRITICAL" : "LOW"} note={<Link className="underline" to="/incidents">impact analysis</Link>} />
        <Kpi label="Pending recommendations" value={num(k?.pending_recommendations)} note={<Link className="underline" to="/recommendations">review</Link>} />
      </div>

      <div className="flex flex-wrap items-center gap-2">
        <button onClick={() => setRoute(null)} className={`px-2.5 py-1 rounded-full text-xs border ${!route ? "border-ink text-ink" : "border-line text-ink2"}`}>All lines</button>
        {network.routes.map((r) => (
          <button key={r.route_id} onClick={() => setRoute(route === r.route_id ? null : r.route_id)}
            className={`rounded-full p-0.5 border-2 ${route === r.route_id ? "border-ink" : "border-transparent"}`} title={r.name}>
            <RouteBullet id={r.route_id} color={r.color} textColor={r.text_color} size={22} />
          </button>
        ))}
        <div className="ml-auto flex items-center gap-1 text-xs">
          <span className="text-ink2 mr-1">Station colour:</span>
          {(["now", "30m"] as const).map((m) => (
            <button key={m} onClick={() => setMetric(m)}
              className={`px-2.5 py-1 rounded-md border ${metric === m ? "border-ink text-ink" : "border-line text-ink2"}`}>
              {m === "now" ? "crowding now" : "predicted +30 min"}
            </button>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-[1fr_380px] gap-4">
        <div className="card p-1 h-[560px] xl:h-[680px] min-w-0">
          <TransitMap network={network} vehicles={vehicles} stations={stations} routeFilter={route} stationMetric={metric}
            incidentStations={(live?.incidents ?? []).map((i) => i.station_id)}
            onVehicle={(v) => { setVehicleId(v.vehicle_id); setStation(null); }}
            onStation={(s) => { setStation(s); setVehicleId(null); }} />
        </div>
        <div className="space-y-4 min-w-0">
          <Legend />
          {vehicle && <VehiclePanel v={vehicle} color={routeMeta[vehicle.route_id]?.color} onClose={() => setVehicleId(null)} />}
          {station && <StationPanel s={stations.find((x) => x.station_id === station.station_id) ?? station} onClose={() => setStation(null)} />}
          <Card title="Most crowded stations in 30 min" subtitle="Spark MLlib forecast, fused Wi-Fi + camera density">
            <ul className="divide-y divide-[var(--line)]">
              {hotStations.map((s) => (
                <li key={s.station_id} className="py-1.5 flex items-center gap-2 text-sm cursor-pointer" onClick={() => setStation(s)}>
                  <span className="truncate flex-1">{s.name} <span className="text-muted text-xs">{s.routes.join(" ")}</span></span>
                  <span className="tnum text-ink2 text-xs w-24 text-right">{pct(s.crowd_now)} → {pct(s.crowd_30m)}</span>
                  <span className="w-20"><LevelBadge level={crowdLevel(s.crowd_30m)} /></span>
                </li>
              ))}
              {!hotStations.length && <Empty>Waiting for the first flow windows…</Empty>}
            </ul>
          </Card>
          <Card title="Most loaded trains in 15 min">
            <ul className="divide-y divide-[var(--line)]">
              {hotTrains.map((v) => (
                <li key={v.vehicle_id} className="py-1.5 flex items-center gap-2 text-sm cursor-pointer" onClick={() => setVehicleId(v.vehicle_id)}>
                  <RouteBullet id={v.route_id} color={routeMeta[v.route_id]?.color ?? "#888"} size={18} />
                  <span className="truncate flex-1">{v.vehicle_id} <span className="text-muted text-xs">→ {v.next_stop_name}</span></span>
                  <span className="tnum text-ink2 text-xs">{num(v.occupancy_pct)}% → {v.pred_15m == null ? "–" : `${num(v.pred_15m)}%`}</span>
                </li>
              ))}
            </ul>
          </Card>
        </div>
      </div>
    </div>
  );
}

function Legend() {
  return (
    <div className="card px-4 py-3 text-xs text-ink2 flex flex-wrap gap-x-4 gap-y-1">
      <span className="font-medium text-ink w-full">Crowding (stations) / load (trains)</span>
      <LevelBadge level="LOW" label="Low <40%" /><LevelBadge level="MODERATE" label="Moderate" />
      <LevelBadge level="HIGH" label="High" /><LevelBadge level="CRITICAL" label="Critical ≥100%" />
      <span className="w-full text-muted">Trains are ringed in their line colour. Click a train or a station for details.</span>
    </div>
  );
}

function VehiclePanel({ v, color, onClose }: { v: Vehicle; color?: string; onClose: () => void }) {
  const { data } = usePoll<History>(`/api/vehicles/${encodeURIComponent(v.vehicle_id)}/history?minutes=60`, 6000);
  const points = data?.points ?? [];
  return (
    <Card title={<span className="flex items-center gap-2"><RouteBullet id={v.route_id} color={color ?? "#888"} size={18} /> Train {v.vehicle_id}</span>}
      subtitle={`Next stop ${v.next_stop_name} · ${v.status.replace("_", " ").toLowerCase()}`}
      right={<button onClick={onClose} className="text-muted text-xs hover:text-ink">Close</button>}>
      <div className="grid grid-cols-3 gap-2 text-center mb-3">
        <div><div className="text-lg font-semibold tnum">{num(v.occupancy_pct)}%</div><div className="text-[11px] text-muted">load now</div></div>
        <div><div className="text-lg font-semibold tnum">{v.pred_15m == null ? "–" : `${num(v.pred_15m)}%`}</div><div className="text-[11px] text-muted">in 15 min</div></div>
        <div><div className="text-lg font-semibold tnum">{num((v.delay_s || 0) / 60, 1)}</div><div className="text-[11px] text-muted">min delay</div></div>
      </div>
      <div className="text-xs text-ink2 mb-1">Load over the last hour (%) – HBase prefix scan <code className="text-muted">{data?.row_prefix}</code></div>
      <div className="h-36">
        {points.length > 1 ? (
          <ResponsiveContainer>
            <LineChart data={points} margin={{ top: 6, right: 8, left: -18, bottom: 0 }}>
              <CartesianGrid stroke="var(--grid)" vertical={false} />
              <XAxis dataKey="ts" type="number" domain={["dataMin", "dataMax"]} tickFormatter={(t) => fmtTime(t)} {...axisProps} minTickGap={40} />
              <YAxis {...axisProps} axisLine={false} />
              <ReferenceLine y={100} stroke="var(--axis)" strokeDasharray="3 3" />
              <Tooltip content={<ChartTooltip labelFormatter={(l) => fmtTime(Number(l), true)} valueFormatter={(x) => `${num(x)}%`} />} />
              <Line dataKey="occupancy_pct" name="Load" stroke="var(--series-1)" strokeWidth={2} dot={false} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        ) : <Empty>Collecting telemetry…</Empty>}
      </div>
    </Card>
  );
}

function StationPanel({ s, onClose }: { s: StationLive; onClose: () => void }) {
  return (
    <Card title={s.name} subtitle={`Lines ${s.routes.join(" ")} · capacity ~${num(s.capacity)} people`}
      right={<button onClick={onClose} className="text-muted text-xs hover:text-ink">Close</button>}>
      <div className="grid grid-cols-3 gap-2 text-center">
        {([["now", s.crowd_now], ["+15 min", s.crowd_15m], ["+30 min", s.crowd_30m]] as const).map(([l, c]) => (
          <div key={l}>
            <div className="text-lg font-semibold tnum">{pct(c)}</div>
            <div className="flex justify-center"><LevelBadge level={crowdLevel(c)} label={l} /></div>
          </div>
        ))}
      </div>
      <dl className="grid grid-cols-2 gap-x-3 gap-y-1 text-xs mt-3 text-ink2">
        <dt>People on site (fused)</dt><dd className="text-right tnum text-ink">{num(s.fused)}</dd>
        <dt>Wi-Fi devices / camera count</dt><dd className="text-right tnum text-ink">{num(s.wifi)} / {num(s.camera)}</dd>
        <dt>Entries per hour now</dt><dd className="text-right tnum text-ink">{num(s.rate_per_hour)}</dd>
        <dt>Predicted next hour</dt><dd className="text-right tnum text-ink">{num(s.rate_next_hour)}</dd>
        <dt>Model</dt><dd className="text-right text-ink">{s.model}</dd>
      </dl>
      <Link to={`/forecast?station=${encodeURIComponent(s.station_id)}`} className="text-xs underline mt-3 inline-block">Open forecast →</Link>
    </Card>
  );
}
