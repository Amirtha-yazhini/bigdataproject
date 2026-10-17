"""In-driver shared state. All streaming queries run in one Spark driver, so their foreachBatch handlers and the
recommender thread share the latest per-vehicle / per-route / per-station view here instead of re-reading HBase."""
import os
import threading
from collections import deque

FLEET_SCALE = float(os.getenv("FLEET_SCALE", "0.5"))


class LiveState:
    def __init__(self, net):
        self.net = net
        self.lock = threading.RLock()
        self.clock_ms = 0
        self.vehicles = {}        # vehicle_id -> latest telemetry dict (+ pred)
        self.routes = {}          # route_id -> window stats + headway snapshot
        self.stations = {}        # station_id -> latest flow + prediction
        self.flow_hist = {}       # station_id -> deque[(window_start_ms, taps_in, last_ts_ms)]
        self.incidents = {}       # incident_id -> incident dict (open only)

    def tick(self, ts_ms):
        with self.lock:
            self.clock_ms = max(self.clock_ms, int(ts_ms or 0))

    # ------------------------------------------------------------------ vehicles + headways
    def update_vehicles(self, latest):
        with self.lock:
            for v in latest:
                self.vehicles[v["vehicle_id"]] = v
            # drop vehicles silent for > 5 sim minutes (withdrawn from service)
            cutoff = self.clock_ms - 5 * 60_000
            for vid in [k for k, v in self.vehicles.items() if v["ts"] < cutoff]:
                del self.vehicles[vid]

    def route_headways(self):
        """gaps between consecutive vehicles on each route's cycle -> headway, bunching, hold candidates"""
        out = {}
        with self.lock:
            by_route = {}
            for v in self.vehicles.values():
                by_route.setdefault(v["route_id"], []).append(v)
        for rid, vs in by_route.items():
            r = self.net["route_by_id"].get(rid)
            if not r or len(vs) < 2:
                continue
            cycle = r["cycle_s"]
            vs = sorted(vs, key=lambda v: v["progress_s"])
            n = len(vs)
            ahead = []  # gap from each vehicle to the one in front of it (in scheduled seconds)
            for i, v in enumerate(vs):
                nxt = vs[(i + 1) % n]["progress_s"] + (cycle if i == n - 1 else 0)
                ahead.append(max(1.0, nxt - v["progress_s"]))
            mean = sum(ahead) / n
            var = sum((g - mean) ** 2 for g in ahead) / n
            effective = sum(g * g for g in ahead) / sum(ahead)  # headway as experienced by riders
            # hold candidate: vehicle closest behind its leader while having a large gap behind it
            best = None
            for i, v in enumerate(vs):
                gap_ahead, gap_behind = ahead[i], ahead[i - 1]
                score = gap_behind - gap_ahead
                if v.get("status") != "LAYOVER" and (best is None or score > best[0]):
                    best = (score, v["vehicle_id"], gap_ahead, gap_behind)
            out[rid] = {
                "vehicles": n,
                "headway_actual_s": round(mean, 1),
                "headway_effective_s": round(effective, 1),
                "headway_sched_s": round(r["headway_s"] / FLEET_SCALE, 1),
                "max_gap_s": round(max(ahead), 1),
                "min_gap_s": round(min(ahead), 1),
                "bunching_idx": round((var ** 0.5) / mean, 3),
                "hold_candidate": {"vehicle_id": best[1], "gap_ahead_s": round(best[2]), "gap_behind_s": round(best[3]),
                                   "hold_s": int(min(180, max(30, best[0] / 2)))} if best else None,
                "cycle_s": cycle,
            }
        return out

    def update_route(self, rid, data):
        with self.lock:
            self.routes.setdefault(rid, {}).update(data)

    # ------------------------------------------------------------------ stations
    def push_flow(self, sid, window_start, taps_in, last_ts):
        """keeps the last 12 one-minute windows per station; returns current entry rate (passengers/hour)"""
        with self.lock:
            h = self.flow_hist.setdefault(sid, deque(maxlen=12))
            for i, (ws, _, _) in enumerate(h):
                if ws == window_start:
                    h[i] = (window_start, taps_in, last_ts)
                    break
            else:
                h.append((window_start, taps_in, last_ts))
                if len(h) > 1 and h[-1][0] < h[-2][0]:  # late window: keep order
                    items = sorted(h)
                    h.clear()
                    h.extend(items)
            first_ws = h[0][0]
            last = max(x[2] for x in h)
            minutes = max(1.0, (last - first_ws) / 60000)
            return sum(x[1] for x in h) / minutes * 60

    def update_station(self, sid, data):
        with self.lock:
            self.stations.setdefault(sid, {}).update(data)

    def snapshot(self):
        with self.lock:
            return (self.clock_ms, dict(self.vehicles), {k: dict(v) for k, v in self.routes.items()},
                    {k: dict(v) for k, v in self.stations.items()}, dict(self.incidents))
