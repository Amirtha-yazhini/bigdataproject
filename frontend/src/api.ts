export type Level = "LOW" | "MODERATE" | "HIGH" | "CRITICAL" | "UNKNOWN";

export interface Route {
  route_id: string; name: string; color: string; text_color: string;
  stops: string[]; travel_s: number[]; headway_s: number; shape: [number, number][];
}
export interface StationMeta {
  station_id: string; name: string; lat: number; lon: number; routes: string[]; capacity: number;
  peak_hourly: number; demand_source: "real" | "synthetic"; profile: number[][];
}
export interface Network { mode: string; city: string; center: [number, number]; routes: Route[]; stations: StationMeta[] }

export interface Vehicle {
  vehicle_id: string; route_id: string; trip_id: string; lat: number; lon: number; bearing: number; speed_kmh: number;
  occupancy_pct: number; load_count: number; capacity: number; level: Level; pred_15m: number | null; pred_level: Level | null;
  delay_s: number; status: string; direction: number; next_stop_id: string; next_stop_name: string; prev_stop_id: string; ts: number;
}
export interface StationLive {
  station_id: string; name: string; lat: number; lon: number; routes: string[]; capacity: number;
  fused: number | null; wifi: number | null; camera: number | null; taps_in: number | null; taps_out: number | null;
  rate_per_hour: number | null; crowd_now: number | null; crowd_15m: number | null; crowd_30m: number | null;
  level: Level; level_30m: Level; rate_next_hour: number | null; model: string; ts: number;
}
export interface Incident {
  incident_id: string; type: string; route_id: string; station_id: string; station_name: string; severity: number;
  expected_duration_min: number; description: string; source: string; status: "OPEN" | "CLEARED";
  start_ts: number; end_ts?: number; row_key: string;
  impact: Record<string, number | string[] | undefined> & {
    delay_min?: number; projected_delay_min?: number; affected_pax?: number; onboard_pax?: number; waiting_pax?: number;
    inflow_pax?: number; affected_vehicles?: number; pax_delay_min?: number; remaining_min?: number; elapsed_min?: number;
  };
  station_delays: Record<string, number>;
}
export interface Recommendation {
  _key: string; id: string; route_id: string; action: string; status: string; reason: string; confidence: number;
  created_ts: number; updated_ts: number; current_headway_s?: number; suggested_headway_s?: number; extra_vehicles?: number;
  predicted_load_pct?: number; vehicles_now?: number; vehicle_id?: string; hold_s?: number; bunching_idx?: number;
  incident_id?: string; station_id?: string; decided_ts?: number;
}
export interface Live {
  clock: number; vehicles: Vehicle[]; stations: StationLive[]; incidents: Incident[];
  kpis: { vehicles: number; avg_occupancy: number | null; crowded_vehicles: number; avg_delay_min: number;
    stations_reporting: number; stations_high: number; open_incidents: number; pending_recommendations: number };
}

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, { ...init, headers: { "Content-Type": "application/json", ...(init?.headers || {}) } });
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
  return res.json() as Promise<T>;
}

/** simulated timestamps are city-local time encoded as UTC epoch ms */
export const fmtTime = (ms?: number | null, seconds = false) =>
  ms ? new Date(ms).toLocaleTimeString("en-US", { timeZone: "UTC", hour: "2-digit", minute: "2-digit", second: seconds ? "2-digit" : undefined, hour12: false }) : "–";
export const fmtDay = (ms?: number | null) =>
  ms ? new Date(ms).toLocaleDateString("en-US", { timeZone: "UTC", weekday: "short", month: "short", day: "numeric" }) : "";
export const pct = (x?: number | null, digits = 0) => (x == null ? "–" : `${(x * 100).toFixed(digits)}%`);
export const num = (x?: number | null, digits = 0) =>
  x == null ? "–" : x.toLocaleString("en-US", { maximumFractionDigits: digits, minimumFractionDigits: digits });

/** status palette is fixed across light/dark, so hex is safe for canvas/SVG attributes */
export const LEVEL_COLOR: Record<Level, string> = {
  LOW: "#0ca30c", MODERATE: "#fab219", HIGH: "#ec835a", CRITICAL: "#d03b3b", UNKNOWN: "#898781",
};
export const cssVar = (name: string) =>
  getComputedStyle(document.documentElement).getPropertyValue(`--${name}`).trim() || "#888";
export const crowdLevel = (c?: number | null): Level =>
  c == null ? "UNKNOWN" : c < 0.4 ? "LOW" : c < 0.7 ? "MODERATE" : c < 1 ? "HIGH" : "CRITICAL";
export const occLevel = (p?: number | null): Level =>
  p == null ? "UNKNOWN" : p < 40 ? "LOW" : p < 75 ? "MODERATE" : p < 100 ? "HIGH" : "CRITICAL";
