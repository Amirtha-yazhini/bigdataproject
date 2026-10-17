"""Shared config, Kafka schemas, HBase row-key helpers and a thread-safe HBase writer."""
import json
import os
import threading
import time
import zlib

import happybase

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
HBASE_HOST = os.getenv("HBASE_HOST", "hbase")
HBASE_PORT = int(os.getenv("HBASE_THRIFT_PORT", "9090"))
DATA_DIR = os.getenv("DATA_DIR", "/data/processed")
MODEL_DIR = os.getenv("MODEL_DIR", "/models")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/checkpoints")

TOPIC_VEHICLE = "vehicle.telemetry"
TOPIC_TAPS = "passenger.taps"
TOPIC_DENSITY = "station.density"
TOPIC_INCIDENTS = "incidents"
TOPIC_COMMANDS = "control.commands"

MAX_TS = 9999999999999  # reversed timestamps -> newest rows sort first

# --------------------------------------------------------------------------- HBase schema
# table -> {column family: options}. Mirrors hbase/create_tables.hbase.
TABLES = {
    "vehicle_state": {"loc": {"max_versions": 20}, "occ": {"max_versions": 20}, "ops": {"max_versions": 5}},
    "vehicle_ts": {"d": {"max_versions": 1, "time_to_live": 7 * 86400}},
    "station_flow": {"tap": {"max_versions": 1, "time_to_live": 30 * 86400},
                     "den": {"max_versions": 1, "time_to_live": 30 * 86400},
                     "pred": {"max_versions": 1, "time_to_live": 30 * 86400}},
    "station_state": {"cur": {"max_versions": 10}, "pred": {"max_versions": 10}},
    "route_metrics": {"m": {"max_versions": 1, "time_to_live": 30 * 86400}},
    "incidents": {"info": {"max_versions": 3}, "impact": {"max_versions": 3}},
    "recommendations": {"rec": {"max_versions": 3, "time_to_live": 7 * 86400}},
    "system_meta": {"s": {"max_versions": 1}},
}


def rev_ts(ts_ms):
    return f"{MAX_TS - int(ts_ms):013d}"


def salt(key, buckets=4):
    return str(zlib.crc32(key.encode()) % buckets)


def vehicle_ts_key(route_id, vehicle_id, ts_ms):
    return f"{salt(vehicle_id)}#{route_id}#{vehicle_id}#{rev_ts(ts_ms)}"


def station_flow_key(station_id, window, window_start_ms):
    return f"{station_id}#{window}#{rev_ts(window_start_ms)}"


def route_metrics_key(route_id, window_start_ms):
    return f"{route_id}#{rev_ts(window_start_ms)}"


def incident_key(incident_id):
    return f"{salt(incident_id)}#{incident_id}"


def reco_key(route_id, ts_ms, action):
    return f"{route_id}#{rev_ts(ts_ms)}#{action}"


def enc(d):
    """{'cf:col': value} -> bytes map for happybase (None values dropped)."""
    out = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, float):
            v = f"{v:.4f}".rstrip("0").rstrip(".") if abs(v) < 1e12 else str(int(v))
        elif isinstance(v, (dict, list)):
            v = json.dumps(v, separators=(",", ":"))
        out[k.encode()] = str(v).encode()
    return out


class HBase:
    """Small connection pool wrapper with retry (Thrift connections get dropped when idle)."""

    def __init__(self, size=6):
        self._pool = happybase.ConnectionPool(size=size, host=HBASE_HOST, port=HBASE_PORT, timeout=30000)
        self._lock = threading.Lock()

    def _run(self, fn, retries=3):
        last = None
        for i in range(retries):
            try:
                with self._pool.connection() as conn:
                    return fn(conn)
            except Exception as e:  # noqa - thrift raises many types
                last = e
                time.sleep(0.5 * (i + 1))
        raise last

    def put_many(self, table, rows, batch_size=500):
        """rows: iterable of (row_key, {'cf:col': val})"""
        rows = list(rows)
        if not rows:
            return 0

        def fn(conn):
            t = conn.table(table)
            with t.batch(batch_size=batch_size) as b:
                for key, data in rows:
                    b.put(key.encode(), enc(data))
            return len(rows)
        return self._run(fn)

    def put(self, table, key, data):
        return self.put_many(table, [(key, data)])

    def counter_inc(self, table, incs):
        """incs: iterable of (row_key, 'cf:col', n) -> atomic HBase counters"""
        def fn(conn):
            t = conn.table(table)
            for key, col, n in incs:
                t.counter_inc(key.encode(), col.encode(), int(n))
        return self._run(fn)

    def row(self, table, key):
        return self._run(lambda c: c.table(table).row(key.encode()))

    def scan(self, table, **kw):
        return self._run(lambda c: list(c.table(table).scan(**kw)))

    def ensure_tables(self):
        def fn(conn):
            existing = {t.decode() for t in conn.tables()}
            for name, fams in TABLES.items():
                if name not in existing:
                    conn.create_table(name, fams)
                    print(f"[hbase] created table {name} {list(fams)}", flush=True)
        return self._run(fn, retries=10)


def load_network():
    with open(os.path.join(DATA_DIR, "network.json")) as f:
        net = json.load(f)
    # derived per-route geometry on the scheduled "cycle": outbound (dir 0) then inbound (dir 1)
    for r in net["routes"]:
        cum = [0]
        for t in r["travel_s"]:
            cum.append(cum[-1] + t)
        r["cum_s"] = cum
        r["one_way_s"] = cum[-1]
        r["cycle_s"] = 2 * (cum[-1] + r["layover_s"])
    net["route_by_id"] = {r["route_id"]: r for r in net["routes"]}
    net["station_by_id"] = {s["station_id"]: s for s in net["stations"]}
    return net


def daytype_of(dt):
    wd = dt.weekday()
    return 0 if wd < 5 else (1 if wd == 5 else 2)
