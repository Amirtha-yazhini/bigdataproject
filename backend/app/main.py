"""FastAPI backend: REST + WebSocket over HBase (Thrift) and Kafka."""
import asyncio
import time
import uuid
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from . import services as svc
from .hbase_client import hb, rev_ts
from .kafka_client import kafka

app = FastAPI(title="Transit Flow Optimizer API", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SALT = lambda key: str(__import__("zlib").crc32(key.encode()) % 4)  # noqa: E731 - same as Spark


# ----------------------------------------------------------------------------- network + live
@app.get("/api/network")
def get_network():
    net = svc.network()
    return {"mode": net["mode"], "city": net["city"], "center": net["center"],
            "routes": [{k: r[k] for k in ("route_id", "name", "color", "text_color", "stops", "travel_s", "headway_s", "shape")}
                       for r in net["routes"]],
            "stations": [{k: s.get(k) for k in ("station_id", "name", "lat", "lon", "routes", "capacity", "peak_hourly",
                                                 "demand_source", "profile")} for s in net["stations"]]}


@app.get("/api/live")
def get_live():
    return svc.live_snapshot()


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            data = await asyncio.get_running_loop().run_in_executor(None, svc.live_snapshot)
            await ws.send_json(data)
            await asyncio.sleep(2)
    except (WebSocketDisconnect, RuntimeError):
        pass
    except Exception as e:  # noqa - keep socket errors from crashing the worker
        try:
            await ws.send_json({"error": str(e)})
        except Exception:  # noqa
            pass


# ----------------------------------------------------------------------------- vehicles
@app.get("/api/vehicles")
def list_vehicles(route_id: Optional[str] = None):
    return [v for v in svc.vehicles() if not route_id or v["route_id"] == route_id]


@app.get("/api/vehicles/{vehicle_id}")
def get_vehicle(vehicle_id: str):
    row = hb.row("vehicle_state", vehicle_id, flat=False)
    if not row:
        raise HTTPException(404, "unknown vehicle")
    # VERSIONS on the loc/occ families give a short trail without touching the time-series table
    lats = hb.cells("vehicle_state", vehicle_id, "loc:lat", 20)
    lons = hb.cells("vehicle_state", vehicle_id, "loc:lon", 20)
    occ = hb.cells("vehicle_state", vehicle_id, "occ:pct", 20)
    row["trail"] = [{"lat": a[0], "lon": b[0], "occupancy_pct": c[0] if c else None, "hbase_ts": a[1]}
                    for a, b, c in zip(lats, lons, occ + [None] * (len(lats) - len(occ)))]
    return row


@app.get("/api/vehicles/{vehicle_id}/history")
def vehicle_history(vehicle_id: str, minutes: int = Query(30, ge=1, le=24 * 60), limit: int = Query(400, le=5000)):
    st = hb.row("vehicle_state", vehicle_id)
    if not st:
        raise HTTPException(404, "unknown vehicle")
    prefix = f"{SALT(vehicle_id)}#{st['route_id']}#{vehicle_id}#"
    since = svc.sim_clock() - minutes * 60_000
    rows = hb.scan("vehicle_ts", row_prefix=prefix, limit=limit)  # newest first thanks to reversed ts
    pts = [r for r in rows if r.get("ts", 0) >= since]
    pts.reverse()
    return {"vehicle_id": vehicle_id, "row_prefix": prefix, "points": [
        {k: r.get(k) for k in ("ts", "lat", "lon", "occupancy_pct", "load_count", "delay_s", "speed_kmh", "status", "next_stop_id")}
        for r in pts]}


# ----------------------------------------------------------------------------- stations
@app.get("/api/stations")
def list_stations():
    return svc.stations()


@app.get("/api/stations/{station_id}/flow")
def station_flow(station_id: str, window: str = Query("1m", pattern="^(1m|15m)$"), limit: int = Query(60, le=1000)):
    st = svc.network()["station_by_id"].get(station_id)
    if not st:
        raise HTTPException(404, "unknown station")
    rows = hb.scan("station_flow", row_prefix=f"{station_id}#{window}#", limit=limit)
    rows.reverse()
    cards = []
    for r in rows:
        cards.append({k[5:]: v for k, v in r.items() if k.startswith("card_")})
    return {"station": {k: st.get(k) for k in ("station_id", "name", "routes", "capacity", "profile", "demand_source")},
            "row_prefix": f"{station_id}#{window}#",
            "windows": [{**{k: v for k, v in r.items() if not k.startswith("card_")}, "cards": c} for r, c in zip(rows, cards)],
            "state": hb.row("station_state", station_id)}


# ----------------------------------------------------------------------------- routes
@app.get("/api/routes")
def list_routes():
    net, latest = svc.network(), svc.route_latest()
    vs = svc.vehicles()
    out = []
    for r in net["routes"]:
        m = latest.get(r["route_id"], {})
        mine = [v for v in vs if v["route_id"] == r["route_id"]]
        out.append({"route_id": r["route_id"], "name": r["name"], "color": r["color"], "stops": len(r["stops"]),
                    "vehicles": len(mine), "metrics": m})
    return out


@app.get("/api/routes/{route_id}/metrics")
def route_metrics(route_id: str, limit: int = Query(60, le=1440)):
    if route_id not in svc.network()["route_by_id"]:
        raise HTTPException(404, "unknown route")
    rows = hb.scan("route_metrics", row_prefix=f"{route_id}#", limit=limit)
    rows.reverse()
    return {"route_id": route_id, "windows": rows}


# ----------------------------------------------------------------------------- recommendations
@app.get("/api/recommendations")
def list_recommendations(status: Optional[str] = None, limit: int = Query(100, le=500)):
    return svc.recommendations(status, limit)


class Decision(BaseModel):
    note: Optional[str] = None


@app.post("/api/recommendations/{rec_id:path}/{decision}")
def decide(rec_id: str, decision: str, body: Optional[Decision] = None):
    if decision not in ("accept", "dismiss"):
        raise HTTPException(400, "decision must be accept or dismiss")
    rec = hb.row("recommendations", rec_id)
    if not rec:
        raise HTTPException(404, "unknown recommendation")
    if rec.get("status") != "NEW":
        raise HTTPException(409, f"already {rec.get('status')}")
    status = "ACCEPTED" if decision == "accept" else "DISMISSED"
    hb.put("recommendations", rec_id, {"rec:status": status, "rec:decided_ts": svc.sim_clock(),
                                       "rec:note": (body.note if body else None)})
    command = None
    if status == "ACCEPTED":
        command = {"command_id": uuid.uuid4().hex, "rec_id": rec_id, "action": rec.get("action"),
                   "route_id": rec.get("route_id"), "extra_vehicles": rec.get("extra_vehicles"),
                   "vehicle_id": rec.get("vehicle_id"), "hold_s": rec.get("hold_s"), "station_id": rec.get("station_id"),
                   "issued_wall": int(time.time() * 1000)}
        kafka.send("control.commands", rec.get("route_id"), command)
    svc.invalidate("recommendations", "live")
    return {"rec_id": rec_id, "status": status, "command": command}


# ----------------------------------------------------------------------------- incidents
@app.get("/api/incidents")
def list_incidents(status: Optional[str] = Query(None, pattern="^(OPEN|CLEARED)$")):
    return svc.incidents(status)


@app.get("/api/incidents/{incident_id}")
def get_incident(incident_id: str):
    for i in svc.incidents():
        if i.get("incident_id") == incident_id:
            return i
    raise HTTPException(404, "unknown incident")


class NewIncident(BaseModel):
    route_id: str
    station_id: str
    type: str = "Signals"
    severity: int = Field(2, ge=1, le=3)
    duration_min: int = Field(20, ge=5, le=120)
    description: Optional[str] = None


@app.post("/api/incidents", status_code=201)
def create_incident(body: NewIncident):
    net = svc.network()
    r = net["route_by_id"].get(body.route_id)
    if not r or body.station_id not in r["stops"]:
        raise HTTPException(400, "station is not on that route")
    name = net["station_by_id"][body.station_id]["name"]
    ts = svc.sim_clock()
    ev = {"incident_id": f"MAN-{uuid.uuid4().hex[:6].upper()}", "event": "OPEN", "status": "OPEN", "ts": ts,
          "started_ts": ts, "type": body.type, "route_id": body.route_id, "station_id": body.station_id,
          "station_name": name, "severity": body.severity, "expected_duration_min": body.duration_min,
          "description": body.description or f"{body.type} at {name} ({body.route_id} line), severity {body.severity} [manual]",
          "source": "MANUAL"}
    kafka.send("incidents", ev["incident_id"], ev)
    svc.invalidate("incidents", "live")
    return ev


# ----------------------------------------------------------------------------- demo controls
class Surge(BaseModel):
    station_id: str
    multiplier: float = Field(2.5, ge=1.1, le=6)
    minutes: int = Field(30, ge=5, le=180)


@app.post("/api/simulate/surge")
def surge(body: Surge):
    if body.station_id not in svc.network()["station_by_id"]:
        raise HTTPException(404, "unknown station")
    cmd = {"command_id": uuid.uuid4().hex, "action": "SURGE", **body.model_dump()}
    kafka.send("control.commands", body.station_id, cmd)
    return cmd


# ----------------------------------------------------------------------------- system
@app.get("/api/system/health")
def health():
    out = {"api": "ok", "wall_ms": int(time.time() * 1000)}
    try:
        out["hbase"] = {"status": "ok", "tables": hb.tables()}
    except Exception as e:  # noqa
        out["hbase"] = {"status": "down", "error": str(e)}
    try:
        out["kafka"] = {"status": "ok", "topics": kafka.topic_stats()}
    except Exception as e:  # noqa
        out["kafka"] = {"status": "down", "error": str(e)}
    try:
        queries = hb.scan("system_meta", row_prefix="query:")
        now = int(time.time() * 1000)
        for q in queries:
            q["active"] = now - int(q.get("updated_wall", 0)) < 90_000
        out["spark"] = {"status": "ok" if queries and any(q["active"] for q in queries) else "starting", "queries": queries}
        taps = hb.row("system_meta", "taps") or {}
        out["tap_schemas"] = {k: v for k, v in taps.items() if k != "_key"}
        reco = hb.row("system_meta", "recommender") or {}
        out["recommender"] = reco
        out["sim_clock"] = svc.sim_clock()
    except Exception as e:  # noqa
        out["spark"] = {"status": "unknown", "error": str(e)}
    out["provenance"] = svc.provenance()
    out["model"] = svc.model_metrics()
    return out


SCHEMA_DOC = [
    {"table": "vehicle_state", "row_key": "vehicle_id", "example": "A-007",
     "purpose": "Current state per vehicle, overwritten in place; VERSIONS on loc/occ keep a short trail."},
    {"table": "vehicle_ts", "row_key": "salt#route#vehicle#revTs", "example": "2#A#A-007#8207622999999",
     "purpose": "Telemetry time series. Salt spreads writes; reversed ts makes prefix scans newest-first; TTL 7 d."},
    {"table": "station_flow", "row_key": "station#window#revWindowStart", "example": "127#1m#8207622940000",
     "purpose": "Passenger-flow windows (1 m tumbling, 15 m sliding). Dynamic counter columns tap:card_<TYPE>."},
    {"table": "station_state", "row_key": "station_id", "example": "127",
     "purpose": "Latest flow + crowding prediction per station; VERSIONS keep recent predictions."},
    {"table": "route_metrics", "row_key": "route#revWindowStart", "example": "4#8207622940000",
     "purpose": "Route KPIs per 1-minute window: load, delay, headway, bunching."},
    {"table": "incidents", "row_key": "salt#incident_id", "example": "1#INC-3FA2B9C1",
     "purpose": "Incident info + impact; impact:st_<station> dynamic columns hold propagated delay."},
    {"table": "recommendations", "row_key": "route#revTs#action", "example": "6#8207622999999#HOLD",
     "purpose": "Frequency / holding / short-turn / bus-bridge recommendations with status workflow."},
    {"table": "system_meta", "row_key": "clock | pipeline | query:<name> | taps", "example": "query:flow_1m",
     "purpose": "Simulation clock, Spark query progress, tap schema counters, provenance."},
]


@app.get("/api/system/schema")
def schema():
    def build():
        out = []
        for t in SCHEMA_DOC:
            d = dict(t)
            try:
                d["families"] = hb.families(t["table"])
                if t["table"] not in ("vehicle_ts", "station_flow", "route_metrics"):
                    d["rows"] = hb.count(t["table"], 50000)
                sample = hb.scan(t["table"], flat=False, limit=1)
                d["sample"] = sample[0] if sample else None
            except Exception as e:  # noqa
                d["error"] = str(e)
            out.append(d)
        return out
    return svc.cached("schema", 10, build)


@app.get("/api/health")
def ping():
    return {"ok": True, "rev_ts_example": rev_ts(int(time.time() * 1000))}
