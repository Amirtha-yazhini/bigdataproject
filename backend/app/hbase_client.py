"""HBase access via Thrift (happybase) with typed decoding of the wide-column rows."""
import json
import os
import threading
import time

import happybase

HBASE_HOST = os.getenv("HBASE_HOST", "hbase")
HBASE_PORT = int(os.getenv("HBASE_THRIFT_PORT", "9090"))
MAX_TS = 9999999999999

# columns written as HBase atomic counters (8-byte big-endian longs)
COUNTER_PREFIXES = (b"tap:card_", b"tap:transfers")
COUNTER_ROWS = {b"taps"}


def rev_ts(ts_ms):
    return f"{MAX_TS - int(ts_ms):013d}"


STRING_COLS = {"route_id", "station_id", "trip_id", "prev_stop", "next_stop", "vehicle_id", "incident_id", "id",
               "from_station", "to_station", "station_name", "description", "type", "source", "status", "action",
               "reason", "level", "level_30m", "pred_level", "model", "door", "name", "city", "mode", "watermark"}


def decode_value(col, raw, row_key=b""):
    if col.startswith(COUNTER_PREFIXES) or row_key in COUNTER_ROWS:
        if len(raw) == 8:
            return int.from_bytes(raw, "big", signed=True)
    s = raw.decode("utf-8", "replace")
    if col.split(b":", 1)[-1].decode() in STRING_COLS:
        return s
    if s[:1] in "{[":
        try:
            return json.loads(s)
        except ValueError:
            return s
    try:
        f = float(s)
        return int(f) if f.is_integer() and "." not in s and "e" not in s.lower() else f
    except ValueError:
        return s


def decode_row(key, data, flat=True):
    """{b'cf:col': b'v'} -> {'col': v} (flat) or {'cf': {'col': v}}"""
    out = {} if flat else {"_key": key.decode()}
    for k, v in data.items():
        cf, col = k.decode().split(":", 1)
        val = decode_value(k, v, key)
        if flat:
            out[col] = val
        else:
            out.setdefault(cf, {})[col] = val
    if flat:
        out["_key"] = key.decode()
    return out


class HBaseClient:
    def __init__(self):
        self._pool = None
        self._lock = threading.Lock()

    def pool(self):
        with self._lock:
            if self._pool is None:
                self._pool = happybase.ConnectionPool(size=8, host=HBASE_HOST, port=HBASE_PORT, timeout=20000)
            return self._pool

    def _run(self, fn, retries=2):
        last = None
        for i in range(retries + 1):
            try:
                with self.pool().connection() as conn:
                    return fn(conn)
            except Exception as e:  # noqa - thrift raises assorted transport errors
                last = e
                time.sleep(0.3 * (i + 1))
        raise last

    def tables(self):
        return self._run(lambda c: sorted(t.decode() for t in c.tables()))

    def families(self, table):
        return self._run(lambda c: {k.decode(): {kk.decode() if isinstance(kk, bytes) else kk: (vv.decode() if isinstance(vv, bytes) else vv)
                                                 for kk, vv in v.items()} for k, v in c.table(table).families().items()})

    def row(self, table, key, flat=True):
        data = self._run(lambda c: c.table(table).row(key.encode()))
        return decode_row(key.encode(), data, flat) if data else None

    def scan(self, table, flat=True, **kw):
        if "row_prefix" in kw and isinstance(kw["row_prefix"], str):
            kw["row_prefix"] = kw["row_prefix"].encode()
        if "filter" in kw and isinstance(kw["filter"], str):
            kw["filter"] = kw["filter"].encode()
        rows = self._run(lambda c: list(c.table(table).scan(**kw)))
        return [decode_row(k, v, flat) for k, v in rows]

    def cells(self, table, key, column, versions=20):
        res = self._run(lambda c: c.table(table).cells(key.encode(), column.encode(), versions=versions, include_timestamp=True))
        return [(decode_value(column.encode(), v), ts) for v, ts in res]

    def put(self, table, key, data):
        enc = {k.encode(): (json.dumps(v) if isinstance(v, (dict, list)) else str(v)).encode() for k, v in data.items() if v is not None}
        self._run(lambda c: c.table(table).put(key.encode(), enc))

    def count(self, table, limit=100000):
        def fn(c):
            n = 0
            for _ in c.table(table).scan(filter=b"FirstKeyOnlyFilter() AND KeyOnlyFilter()", limit=limit, batch_size=2000):
                n += 1
            return n
        return self._run(fn)


hb = HBaseClient()
