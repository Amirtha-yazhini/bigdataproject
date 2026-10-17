"""Dynamic route & frequency recommendations (runs every RECO_INTERVAL_S on the Spark driver).

  ADD_FREQUENCY     predicted peak load > target -> required headway = headway x target / predicted load
                    -> extra vehicles = ceil(cycle / required headway) - fleet on route
  REDUCE_FREQUENCY  sustained low load and spare fleet -> withdraw one vehicle
  HOLD              bunching index > threshold -> hold the vehicle that is closest to its leader
  SHORT_TURN        severity-2 incident -> turn trains before the affected station
  BUS_BRIDGE        severity-3 incident (line blocked) -> shuttle buses around the blocked section

Rows go to HBase `recommendations` (row key route#revTs#action). The dashboard can accept/dismiss them; accepted ones
are sent to Kafka control.commands and the simulator applies them -> closed loop.
"""
import math
import os
import threading
import time
import traceback

from common import reco_key
from incident_impact import refresh_open

RECO_INTERVAL_S = float(os.getenv("RECO_INTERVAL_S", "20"))
TARGET_LOAD = float(os.getenv("TARGET_LOAD", "0.85"))
BUNCHING_THRESHOLD = float(os.getenv("BUNCHING_THRESHOLD", "0.5"))
MAX_HEADWAY_S = float(os.getenv("MAX_HEADWAY_S", "720"))  # service standard: never thinner than 12 min
COOLDOWN_MS = 15 * 60_000  # simulated time before re-issuing the same action on a route


class Recommender(threading.Thread):
    def __init__(self, hb, live, net):
        super().__init__(daemon=True, name="recommender")
        self.hb, self.live, self.net = hb, live, net
        self.issued = {}  # (route, action) -> (row_key, created_ms)

    def run(self):
        while True:
            time.sleep(RECO_INTERVAL_S)
            try:
                self.cycle()
                refresh_open(self.hb, self.live, self.net)
                self.hb.put("system_meta", "recommender", {"s:last_run_wall": int(time.time() * 1000),
                                                            "s:issued_total": len(self.issued)})
            except Exception:  # noqa
                traceback.print_exc()

    def _status(self, key):
        row = self.hb.row("recommendations", key) or {}
        return (row.get(b"rec:status") or b"NEW").decode()

    def emit(self, now_ms, rid, action, data):
        prev = self.issued.get((rid, action))
        if prev:
            key, created = prev
            if self._status(key) == "NEW" and now_ms - created < COOLDOWN_MS:
                # still pending: refresh numbers, keep the same recommendation id
                self.hb.put("recommendations", key, dict({f"rec:{k}": v for k, v in data.items()}, **{"rec:updated_ts": now_ms}))
                return
            if self._status(key) != "NEW" and now_ms - created < COOLDOWN_MS:
                return  # operator already decided recently
        key = reco_key(rid, now_ms, action)
        payload = {f"rec:{k}": v for k, v in data.items()}
        payload.update({"rec:id": key, "rec:route_id": rid, "rec:action": action, "rec:status": "NEW",
                        "rec:created_ts": now_ms, "rec:updated_ts": now_ms, "rec:expires_ts": now_ms + COOLDOWN_MS})
        self.hb.put("recommendations", key, payload)
        self.issued[(rid, action)] = (key, now_ms)

    def cycle(self):
        now_ms, vehicles, routes_live, stations_live, incidents = self.live.snapshot()
        if not now_ms:
            return
        hour = (now_ms // 3_600_000) % 24  # sim time is city-local time encoded as UTC
        peak_hours = 6 <= hour < 10 or 16 <= hour < 20  # policy: never thin service in the rush hours
        by_route = {}
        for v in vehicles.values():
            by_route.setdefault(v["route_id"], []).append(v)
        for rid, vs in by_route.items():
            r = self.net["route_by_id"].get(rid)
            m = routes_live.get(rid, {})
            if not r or "headway_actual_s" not in m:
                continue
            n = len(vs)
            preds = sorted((v.get("pred_15m") or v["occupancy_pct"]) for v in vs)
            top = preds[-3:]
            load_pred = (sum(top) / len(top)) / 100 if top else 0
            load_now = (m.get("p90_occ") or m.get("avg_occ") or 0) / 100
            peak = max(load_pred, load_now)
            h = m["headway_actual_s"]
            busiest = max(vs, key=lambda v: v.get("pred_15m") or 0)
            fresh = 1.0 if now_ms - max(v["ts"] for v in vs) < 120_000 else 0.6
            if peak > TARGET_LOAD + 0.1:
                h_req = max(90.0, h * TARGET_LOAD / peak)
                extra = int(min(4, max(1, math.ceil(r["cycle_s"] / h_req) - n)))
                self.emit(now_ms, rid, "ADD_FREQUENCY", {
                    "current_headway_s": round(h), "suggested_headway_s": round(h_req), "extra_vehicles": extra,
                    "predicted_load_pct": round(peak * 100), "vehicles_now": n,
                    "reason": f"Predicted peak load {peak * 100:.0f}% (target {TARGET_LOAD * 100:.0f}%) on {rid}; "
                              f"busiest train {busiest['vehicle_id']} heading to "
                              f"{self.net['station_by_id'].get(busiest['next_stop_id'], {}).get('name', busiest['next_stop_id'])}. "
                              f"Cut headway {h / 60:.1f} -> {h_req / 60:.1f} min (+{extra} trains).",
                    "confidence": round(min(0.95, 0.55 + (peak - TARGET_LOAD)) * fresh, 2)})
            elif not peak_hours and peak < 0.25 and n > 4 and (m.get("max_occ") or 100) < 45 and h * n / (n - 1) <= MAX_HEADWAY_S:
                self.emit(now_ms, rid, "REDUCE_FREQUENCY", {
                    "current_headway_s": round(h), "suggested_headway_s": round(h * n / (n - 1)), "extra_vehicles": -1,
                    "predicted_load_pct": round(peak * 100), "vehicles_now": n,
                    "reason": f"Low demand on {rid}: peak load {peak * 100:.0f}% (max {m.get('max_occ', 0):.0f}%). "
                              f"Withdraw 1 train to the yard; headway {h / 60:.1f} -> {h * n / (n - 1) / 60:.1f} min.",
                    "confidence": round(0.6 * fresh, 2)})
            hold = m.get("hold_candidate")
            if m.get("bunching_idx", 0) > BUNCHING_THRESHOLD and hold:
                self.emit(now_ms, rid, "HOLD", {
                    "vehicle_id": hold["vehicle_id"], "hold_s": hold["hold_s"], "bunching_idx": m["bunching_idx"],
                    "current_headway_s": round(h),
                    "reason": f"Bunching on {rid} (index {m['bunching_idx']:.2f}): {hold['vehicle_id']} is "
                              f"{hold['gap_ahead_s'] / 60:.1f} min behind its leader but {hold['gap_behind_s'] / 60:.1f} min "
                              f"ahead of its follower. Hold {hold['hold_s']} s to even out gaps.",
                    "confidence": round(min(0.9, 0.5 + m["bunching_idx"] / 2) * fresh, 2)})
        for inc in incidents.values():
            rid, sev = inc["route_id"], int(inc.get("severity") or 1)
            r = self.net["route_by_id"].get(rid)
            if sev < 2 or not r or inc["station_id"] not in r["stops"]:
                continue
            i = r["stops"].index(inc["station_id"])
            name = lambda k: self.net["station_by_id"][r["stops"][k]]["name"]  # noqa
            a, b = max(0, i - 1), min(len(r["stops"]) - 1, i + 1)
            if sev >= 3:
                self.emit(now_ms, rid, "BUS_BRIDGE", {
                    "incident_id": inc["incident_id"], "station_id": inc["station_id"],
                    "from_station": r["stops"][a], "to_station": r["stops"][b],
                    "reason": f"{rid} line blocked at {inc['station_name']} ({inc['type']}). Run shuttle buses "
                              f"{name(a)} <-> {name(b)} and short-turn trains on both sides.",
                    "confidence": 0.9})
            else:
                self.emit(now_ms, rid, "SHORT_TURN", {
                    "incident_id": inc["incident_id"], "station_id": r["stops"][a],
                    "reason": f"Severity-2 {inc['type']} at {inc['station_name']} on {rid}: short-turn alternate trains "
                              f"at {name(a)} to protect headways on the rest of the line.",
                    "confidence": 0.75})
