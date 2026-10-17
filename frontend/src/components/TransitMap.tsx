import { ReactNode, useMemo } from "react";
import { CircleMarker, MapContainer, Polyline, Popup, TileLayer, Tooltip } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import { crowdLevel, cssVar, LEVEL_COLOR, Network, num, occLevel, pct, StationLive, Vehicle } from "../api";

interface Props {
  network: Network;
  vehicles?: Vehicle[];
  stations?: StationLive[];
  routeFilter?: string | null;
  highlight?: Record<string, number>;          // station_id -> intensity (incident delays)
  incidentStations?: string[];
  onVehicle?: (v: Vehicle) => void;
  onStation?: (s: StationLive) => void;
  height?: number | string;
  stationMetric?: "now" | "30m";
  children?: ReactNode;
}

export default function TransitMap({ network, vehicles = [], stations = [], routeFilter, highlight, incidentStations = [],
  onVehicle, onStation, height = "100%", stationMetric = "now", children }: Props) {
  const colors = useMemo(() => Object.fromEntries(network.routes.map((r) => [r.route_id, r.color])), [network]);
  const stationLive = useMemo(() => Object.fromEntries(stations.map((s) => [s.station_id, s])), [stations]);
  const shownRoutes = network.routes.filter((r) => !routeFilter || r.route_id === routeFilter);
  const shownStations = network.stations.filter((s) => !routeFilter || s.routes.includes(routeFilter));
  const maxHl = highlight ? Math.max(1, ...Object.values(highlight)) : 1;
  const surface = cssVar("surface");

  return (
    <MapContainer center={network.center} zoom={11} style={{ height, width: "100%", borderRadius: 12 }} preferCanvas>
      <TileLayer attribution='&copy; OpenStreetMap contributors' url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
      {shownRoutes.map((r) => (
        <Polyline key={r.route_id} positions={r.shape} pathOptions={{ color: r.color, weight: 3, opacity: routeFilter ? 0.9 : 0.55 }} />
      ))}
      {shownStations.map((s) => {
        const live = stationLive[s.station_id];
        const crowd = live ? (stationMetric === "30m" ? live.crowd_30m : live.crowd_now) : null;
        const lvl = crowdLevel(crowd);
        const hl = highlight?.[s.station_id];
        const isInc = incidentStations.includes(s.station_id);
        return (
          <CircleMarker key={s.station_id} center={[s.lat, s.lon]}
            radius={hl ? 5 + 9 * (hl / maxHl) : 3 + Math.min(6, (crowd ?? 0) * 6)}
            pathOptions={{
              color: isInc ? LEVEL_COLOR.CRITICAL : surface, weight: isInc ? 3 : 1,
              fillColor: hl ? LEVEL_COLOR.CRITICAL : LEVEL_COLOR[lvl], fillOpacity: hl ? 0.35 + 0.5 * (hl / maxHl) : 0.9,
            }}
            eventHandlers={{ click: () => live && onStation?.(live) }}>
            <Tooltip direction="top" offset={[0, -4]}>
              <div className="text-xs">
                <b>{s.name}</b> <span className="opacity-70">({s.routes.join(" ")})</span><br />
                {hl != null && <>Estimated delay {num(hl, 1)} min<br /></>}
                Crowding now {pct(live?.crowd_now)} · in 30 min {pct(live?.crowd_30m)}<br />
                ~{num(live?.fused)} people · {num(live?.rate_per_hour)} entries/h
              </div>
            </Tooltip>
          </CircleMarker>
        );
      })}
      {vehicles.filter((v) => !routeFilter || v.route_id === routeFilter).map((v) => (
        <CircleMarker key={v.vehicle_id} center={[v.lat, v.lon]} radius={5}
          pathOptions={{ color: colors[v.route_id] || "#888", weight: 3, fillColor: LEVEL_COLOR[occLevel(v.occupancy_pct)], fillOpacity: 1 }}
          eventHandlers={{ click: () => onVehicle?.(v) }}>
          <Popup>
            <b>Train {v.vehicle_id}</b> ({v.route_id} line)<br />
            Next stop: {v.next_stop_name}<br />
            Load {num(v.load_count)} / {num(v.capacity)} ({num(v.occupancy_pct)}%)<br />
            In 15 min: {v.pred_15m == null ? "–" : `${num(v.pred_15m)}%`}<br />
            Delay {num((v.delay_s || 0) / 60, 1)} min · {v.status.replace("_", " ").toLowerCase()}
          </Popup>
        </CircleMarker>
      ))}
      {children}
    </MapContainer>
  );
}
