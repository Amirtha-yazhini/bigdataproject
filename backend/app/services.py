"""Shared read models built from HBase (with a short cache so many dashboard clients share one scan)."""
import json
import os
import threading
import time

from .hbase_client import hb

DATA_DIR = os.getenv("DATA_DIR", "/data/processed")
MODEL_DIR = os.getenv("MODEL_DIR", "/models")
_cache, _cache_lock = {}, threading.Lock()


def cached(key, ttl, fn):
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    val = fn()
    with _cache_lock:
        _cache[key] = (now, val)
    return val


def network():
    def load():
        with open(os.path.join(DATA_DIR, "network.json")) as f:
            net = json.load(f)
        net["station_by_id"] = {s["station_id"]: s for s in net["stations"]}
        net["route_by_id"] = {r["route_id"]: r for r in net["routes"]}
        return net
    return cached("network", 3600, load)


def provenance():
    def load():
        with open(os.path.join(DATA_DIR, "provenance.json")) as f:
            return json.load(f)
    return cached("provenance", 600, load)


def model_metrics():
    p = os.path.join(MODEL_DIR, "metrics.json")
    if not os.path.exists(p):
        return None
    with open(p) as f:
        return json.load(f)


def sim_clock():
    row = hb.row("system_meta", "clock")
    return int(row["sim_ts"]) if row and "sim_ts" in row else int(time.time() * 1000)


def vehicles():
    def load():
        net = network()
        out = []
        for r in hb.scan("vehicle_state"):
            sid = r.get("next_stop")
            out.append({
                "vehicle_id": r["_key"], "route_id": r.get("route_id"), "trip_id": r.get("trip_id"),
                "lat": r.get("lat"), "lon": r.get("lon"), "bearing": r.get("bearing"), "speed_kmh": r.get("speed_kmh"),
                "occupancy_pct": r.get("pct"), "load_count": r.get("count"), "capacity": r.get("capacity"),
                "level": r.get("level"), "pred_15m": r.get("pred_15m"), "pred_level": r.get("pred_level"),
                "delay_s": r.get("delay_s"), "status": r.get("status"), "direction": r.get("direction"),
                "next_stop_id": sid, "next_stop_name": net["station_by_id"].get(sid, {}).get("name", sid),
                "prev_stop_id": r.get("prev_stop"), "ts": r.get("ts"),
            })
        return out
    return cached("vehicles", 1.5, load)


def stations():
    def load():
        net = network()
        out = []
        for r in hb.scan("station_state"):
            st = net["station_by_id"].get(r["_key"])
            if not st:
                continue
            out.append({
                "station_id": r["_key"], "name": st["name"], "lat": st["lat"], "lon": st["lon"], "routes": st["routes"],
                "capacity": st["capacity"], "fused": r.get("fused"), "wifi": r.get("wifi"), "camera": r.get("camera"),
                "taps_in": r.get("taps_in"), "taps_out": r.get("taps_out"), "rate_per_hour": r.get("rate_per_hour"),
                "crowd_now": r.get("crowd_now"), "crowd_15m": r.get("crowd_15m"), "crowd_30m": r.get("crowd_30m"),
                "level": r.get("level"), "level_30m": r.get("level_30m"), "rate_next_hour": r.get("rate_next_hour"),
                "model": r.get("model"), "ts": r.get("ts"),
            })
        return out
    return cached("stations", 2.0, load)


def incidents(status=None):
    def load():
        rows = hb.scan("incidents", flat=False)
        out = []
        for r in rows:
            info, imp = r.get("info", {}), r.get("impact", {})
            per_station = {k[3:]: v for k, v in imp.items() if k.startswith("st_")}
            out.append({**info, "row_key": r["_key"],
                        "impact": {k: v for k, v in imp.items() if not k.startswith("st_")},
                        "station_delays": per_station})
        out.sort(key=lambda x: x.get("start_ts", 0), reverse=True)
        return out
    rows = cached("incidents", 2.0, load)
    return [r for r in rows if not status or r.get("status") == status]


def route_latest():
    def load():
        out = {}
        for rid in network()["route_by_id"]:
            rows = hb.scan("route_metrics", row_prefix=f"{rid}#", limit=2)
            # newest window may still be filling: prefer the previous complete one for averages
            if rows:
                cur = rows[0]
                out[rid] = {**cur, "route_id": rid}
        return out
    return cached("route_latest", 3.0, load)


def recommendations(status=None, limit=100):
    def load():
        rows = []
        for rid in network()["route_by_id"]:
            rows += hb.scan("recommendations", row_prefix=f"{rid}#", limit=25)
        rows.sort(key=lambda r: r.get("updated_ts", 0), reverse=True)
        return rows
    rows = cached("recommendations", 2.0, load)
    if status:
        rows = [r for r in rows if r.get("status") == status]
    return rows[:limit]


def invalidate(*keys):
    with _cache_lock:
        for k in keys:
            _cache.pop(k, None)


def live_snapshot():
    def build():
        vs, ss = vehicles(), stations()
        incs = incidents("OPEN")
        occ = [v["occupancy_pct"] for v in vs if v["occupancy_pct"] is not None]
        crit = [s for s in ss if s["level"] in ("HIGH", "CRITICAL") or (s.get("crowd_30m") or 0) >= 0.7]
        recs = recommendations("NEW", 50)
        return {
            "clock": sim_clock(),
            "vehicles": vs,
            "stations": ss,
            "incidents": incs,
            "kpis": {
                "vehicles": len(vs),
                "avg_occupancy": round(sum(occ) / len(occ), 1) if occ else None,
                "crowded_vehicles": sum(1 for o in occ if o >= 100),
                "avg_delay_min": round(sum((v["delay_s"] or 0) for v in vs) / max(1, len(vs)) / 60, 1),
                "stations_reporting": len(ss),
                "stations_high": len(crit),
                "open_incidents": len(incs),
                "pending_recommendations": len(recs),
            },
        }
    return cached("live", 1.5, build)
