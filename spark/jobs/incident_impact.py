"""Kafka incidents -> HBase `incidents` (info + impact) and live delay-propagation estimate.

Impact model (recomputed every recommender cycle while the incident is open):
  delay at the incident station grows with elapsed time x severity factor (3 = line blocked);
  it propagates to neighbouring stations with decay 0.85 per stop (service gap travelling down the line).
  Affected riders = on board approaching trains + waiting at affected platforms + expected inflow until clearance.
"""
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import CHECKPOINT_DIR, TOPIC_INCIDENTS, incident_key
from ingest_vehicle_stream import kafka_source

INCIDENT_SCHEMA = T.StructType([
    T.StructField("incident_id", T.StringType()), T.StructField("event", T.StringType()),
    T.StructField("status", T.StringType()), T.StructField("ts", T.LongType()),
    T.StructField("started_ts", T.LongType()), T.StructField("type", T.StringType()),
    T.StructField("route_id", T.StringType()), T.StructField("station_id", T.StringType()),
    T.StructField("station_name", T.StringType()), T.StructField("severity", T.IntegerType()),
    T.StructField("expected_duration_min", T.IntegerType()), T.StructField("description", T.StringType()),
    T.StructField("source", T.StringType()),
])
SEV_FACTOR = {1: 0.2, 2: 0.45, 3: 1.0}
REACH = 10  # stops


def compute_impact(inc, now_ms, net, vehicles, stations_live):
    r = net["route_by_id"].get(inc["route_id"])
    if not r or inc["station_id"] not in r["stops"]:
        return {}
    idx = r["stops"].index(inc["station_id"])
    end_ms = inc.get("ended_ts") or now_ms
    elapsed = max(0.0, (end_ms - inc["started_ts"]) / 60000)
    expected = float(inc.get("expected_duration_min") or 20)
    f = SEV_FACTOR.get(int(inc.get("severity") or 1), 0.3)
    base = min(elapsed, expected) * f
    projected = expected * f
    per_station, waiting = {}, 0.0
    for k in range(max(0, idx - REACH), min(len(r["stops"]), idx + REACH + 1)):
        sid = r["stops"][k]
        d = round(base * 0.85 ** abs(k - idx), 2)
        if d >= 0.1:
            per_station[sid] = d
            st = net["station_by_id"][sid]
            fused = (stations_live.get(sid) or {}).get("fused") or 0
            waiting += fused / max(1, len(st["routes"]))
    onboard, n_veh, pax_min = 0.0, 0, 0.0
    affected_vehicles = []
    for v in vehicles.values():
        if v["route_id"] != inc["route_id"] or v["next_stop_id"] not in r["stops"]:
            continue
        j = r["stops"].index(v["next_stop_id"])
        if abs(j - idx) <= REACH:
            n_veh += 1
            onboard += v["load_count"]
            pax_min += v["load_count"] * per_station.get(v["next_stop_id"], 0)
            affected_vehicles.append(v["vehicle_id"])
    remaining = max(0.0, expected - elapsed) if not inc.get("ended_ts") else 0.0
    rate = (stations_live.get(inc["station_id"]) or {}).get("rate") or 0
    inflow = rate * remaining / 60
    pax_min += waiting * base * 0.7
    out = {"impact:delay_min": round(base, 2), "impact:projected_delay_min": round(projected, 2),
           "impact:elapsed_min": round(elapsed, 1), "impact:remaining_min": round(remaining, 1),
           "impact:affected_vehicles": n_veh, "impact:vehicle_ids": affected_vehicles,
           "impact:onboard_pax": int(onboard), "impact:waiting_pax": int(waiting), "impact:inflow_pax": int(inflow),
           "impact:affected_pax": int(onboard + waiting + inflow), "impact:pax_delay_min": int(pax_min),
           "impact:stations_affected": len(per_station), "impact:updated_ts": now_ms}
    for sid, d in per_station.items():
        out[f"impact:st_{sid}"] = d  # dynamic column per affected station
    return out


def start(spark, hb, live, net):
    parsed = (kafka_source(spark, TOPIC_INCIDENTS, 1000)
              .select(F.from_json(F.col("value").cast("string"), INCIDENT_SCHEMA).alias("i")).select("i.*")
              .where(F.col("incident_id").isNotNull()))

    def write_incidents(df, batch_id):
        rows = sorted((r.asDict() for r in df.collect()), key=lambda r: r["ts"] or 0)
        for inc in rows:
            key = incident_key(inc["incident_id"])
            with live.lock:
                if inc["event"] == "OPEN":
                    inc["started_ts"] = inc.get("started_ts") or inc["ts"]
                    live.incidents[inc["incident_id"]] = inc
                    cur = inc
                else:
                    cur = live.incidents.pop(inc["incident_id"], None) or dict(inc, started_ts=inc.get("started_ts") or inc["ts"])
                    cur["ended_ts"] = inc["ts"]
            info = {"info:incident_id": inc["incident_id"], "info:type": inc["type"], "info:route_id": inc["route_id"],
                    "info:station_id": inc["station_id"], "info:station_name": inc["station_name"],
                    "info:severity": inc["severity"], "info:expected_duration_min": inc["expected_duration_min"],
                    "info:description": inc["description"], "info:source": inc["source"],
                    "info:status": "OPEN" if inc["event"] == "OPEN" else "CLEARED",
                    "info:start_ts": cur["started_ts"]}
            if inc["event"] != "OPEN":
                info["info:end_ts"] = inc["ts"]
            clock, vehicles, _, stations_live, _ = live.snapshot()
            info.update(compute_impact(cur, max(clock, inc["ts"] or 0), net, vehicles, stations_live))
            hb.put("incidents", key, info)

    q = (parsed.writeStream.queryName("incidents").trigger(processingTime="5 seconds")
         .option("checkpointLocation", f"{CHECKPOINT_DIR}/incidents")
         .foreachBatch(write_incidents).start())
    return [q]


def refresh_open(hb, live, net):
    clock, vehicles, _, stations_live, incidents = live.snapshot()
    puts = [(incident_key(i), compute_impact(inc, clock, net, vehicles, stations_live)) for i, inc in incidents.items()]
    hb.put_many("incidents", [p for p in puts if p[1]])
