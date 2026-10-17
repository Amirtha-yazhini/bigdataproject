"""Live incident generator. Rates per line and the category mix come from the MTA Major Incidents dataset."""
import uuid

DURATION_MIN = {
    "Signals": (15, 40), "Track": (20, 55), "Persons on Trackbed/Police/Medical": (10, 30),
    "Subway Car": (10, 25), "Stations and Structure": (15, 45), "Other": (10, 30),
}
SEVERITY = [(1, 0.45), (2, 0.35), (3, 0.20)]
SLOWDOWN = {1: 1.6, 2: 2.8}  # severity 3 blocks the line


class Incidents:
    def __init__(self, stats, routes, stations, multiplier, max_open, rnd):
        self.rates = stats["rate_per_hour"]
        self.cats = list(stats["category_mix"].items()) or [("Other", 1.0)]
        self.routes, self.st = routes, stations
        self.mult, self.max_open, self.rnd = multiplier, max_open, rnd
        self.open = {}  # id -> dict(route_id, station_id, severity, ends_at(epoch s), ...)

    def _pick(self, weighted):
        x, acc = self.rnd.random(), 0.0
        for k, w in weighted:
            acc += w
            if x <= acc:
                return k
        return weighted[-1][0]

    def maybe_generate(self, now_s, dt, ts_ms):
        events = []
        if len(self.open) >= self.max_open:
            return events
        for rid, r in self.routes.items():
            p = self.rates.get(rid, 0.01) * self.mult * dt / 3600
            if self.rnd.random() < p:
                cat = self._pick(self.cats)
                sid = self.rnd.choice(r.stops[1:-1])
                lo, hi = DURATION_MIN.get(cat, (10, 30))
                events.append(self.open_incident(rid, sid, cat, self._pick(SEVERITY), self.rnd.randint(lo, hi), now_s, ts_ms, "SIMULATED"))
                if len(self.open) >= self.max_open:
                    break
        return events

    def open_incident(self, rid, sid, cat, severity, minutes, now_s, ts_ms, source, incident_id=None, description=None):
        iid = incident_id or f"INC-{uuid.uuid4().hex[:8].upper()}"
        name = self.st.get(sid, {}).get("name", sid)
        inc = {"incident_id": iid, "route_id": rid, "station_id": sid, "station_name": name, "type": cat,
               "severity": int(severity), "expected_duration_min": int(minutes), "source": source,
               "started_ts": ts_ms, "ends_at": now_s + minutes * 60,
               "description": description or f"{cat} at {name} ({rid} line), severity {severity}"}
        self.open[iid] = inc
        return self.event(inc, "OPEN", ts_ms)

    def mitigate(self, rid, now_s):
        """an accepted SHORT_TURN / BUS_BRIDGE shortens the disruption by 40 %"""
        for inc in self.open.values():
            if inc["route_id"] == rid:
                inc["ends_at"] = now_s + (inc["ends_at"] - now_s) * 0.6

    def clear_due(self, now_s, ts_ms):
        done = [i for i, inc in self.open.items() if inc["ends_at"] <= now_s]
        return [self.event(self.open.pop(i), "CLEARED", ts_ms) for i in done]

    @staticmethod
    def event(inc, kind, ts_ms):
        e = {k: v for k, v in inc.items() if k != "ends_at"}
        e.update({"event": kind, "ts": ts_ms, "status": "OPEN" if kind == "OPEN" else "CLEARED"})
        return e

    def factor(self, rid, a, b):
        """travel-time multiplier for the segment a->b; None = blocked"""
        worst = 1.0
        for inc in self.open.values():
            if inc["route_id"] == rid and inc["station_id"] in (a, b):
                if inc["severity"] >= 3:
                    return None
                worst = max(worst, SLOWDOWN[inc["severity"]])
        return worst
