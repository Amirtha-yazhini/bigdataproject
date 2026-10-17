"""Passenger demand: Poisson tap-ins calibrated on the (real) hourly ridership profiles, waiting pools
per station/route/direction, tap-outs on alighting, and Wi-Fi/camera density readings."""
import math
import random
import uuid

from network import haversine_m

CORE = (40.754, -73.984)  # Midtown: commuters head here in the morning and away in the evening

CARD_TYPES = [("OMNY_CONTACTLESS", 0.62), ("OMNY_MOBILE", 0.14), ("METROCARD", 0.17), ("STUDENT", 0.04), ("REDUCED_FARE", 0.03)]
DEVICE_KINDS = ["gate", "gate", "gate", "ada_gate", "turnstile"]
WIFI_SHARE = 0.55      # share of people carrying a phone that probes Wi-Fi
CAMERA_SHARE = 0.9     # cameras see ~90 % of people (occlusion)


def pick(rnd, weighted):
    x, acc = rnd.random(), 0.0
    for k, w in weighted:
        acc += w
        if x <= acc:
            return k
    return weighted[-1][0]


def poisson(rnd, lam):
    if lam <= 0:
        return 0
    if lam > 50:
        return max(0, int(round(rnd.gauss(lam, math.sqrt(lam)))))
    l, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rnd.random()
        if p <= l:
            return k
        k += 1


class Demand:
    def __init__(self, stations, routes, tap_weight, rnd=None):
        self.rnd = rnd or random.Random(11)
        self.st = stations
        self.routes = routes
        self.w = tap_weight
        # waiting[(station, route, direction)] = people on the platform
        self.waiting = {}
        self.walkers = {}
        self.noise = {sid: 1.0 for sid in stations}
        self.surges = {}  # station -> (multiplier, until_sim_seconds)
        self.terminal_dir = {}
        self.inbound_dir = {}  # (station, route) -> direction that moves towards the core
        for r in routes.values():
            self.terminal_dir[(r.stops[0], r.id)] = 0
            self.terminal_dir[(r.stops[-1], r.id)] = 1
            dist = [haversine_m(stations[x]["lat"], stations[x]["lon"], *CORE) for x in r.stops]
            for i, sid in enumerate(r.stops):
                ahead = dist[min(i + 1, len(dist) - 1)] if i < len(dist) - 1 else dist[i] + 1
                self.inbound_dir[(sid, r.id)] = 0 if ahead < dist[i] else 1
        # 1 in the core, ~0 in the outer boroughs
        self.coreness = {sid: math.exp(-haversine_m(s["lat"], s["lon"], *CORE) / 3000) for sid, s in stations.items()}
        self.hour = 12.0

    # ---------------------------------------------------------------- rates
    def rate_per_hour(self, sid, now, daytype, now_s):
        prof = self.st[sid]["profile"][daytype]
        h = now.hour + now.minute / 60
        a, b = int(h) % 24, (int(h) + 1) % 24
        f = h - int(h)
        base = prof[a] * (1 - f) + prof[b] * f
        mult = self.noise[sid]
        s = self.surges.get(sid)
        if s and s[1] > now_s:
            mult *= s[0]
        return base * mult

    def drift_noise(self):
        for sid in self.noise:
            self.noise[sid] = min(1.35, max(0.75, self.noise[sid] * self.rnd.lognormvariate(0, 0.02)))

    def add_surge(self, sid, mult, until_ts):
        self.surges[sid] = (mult, until_ts)

    # ---------------------------------------------------------------- tap-ins
    def p_inbound(self, sid):
        """share of riders heading towards the core: strong in the AM peak, reversed in the PM peak"""
        h, c = self.hour, self.coreness.get(sid, 0)
        tilt = 0.32 * math.exp(-((h - 8.3) ** 2) / 3) - 0.3 * math.exp(-((h - 17.7) ** 2) / 3.5)
        return 0.5 + tilt * (1 - c)

    def tick(self, now, daytype, dt, ts_ms):
        """returns list of tap events (each represents `w` passengers)"""
        self.hour = now.hour + now.minute / 60
        events = []
        for sid, st in self.st.items():
            rate = self.rate_per_hour(sid, now, daytype, ts_ms / 1000)
            self.walkers[sid] = rate * (2 / 60)  # people walking platform<->street (~2 min)
            n_ev = poisson(self.rnd, rate * dt / 3600 / self.w)
            if not n_ev:
                continue
            routes = [r for r in st["routes"] if r in self.routes]
            for _ in range(n_ev):
                rid = self.rnd.choice(routes)
                d = self.terminal_dir.get((sid, rid))
                if d is None:
                    inbound = self.inbound_dir.get((sid, rid), 0)
                    d = inbound if self.rnd.random() < self.p_inbound(sid) else 1 - inbound
                key = (sid, rid, d)
                self.waiting[key] = self.waiting.get(key, 0.0) + self.w
                events.append(self.tap_event(sid, "IN", ts_ms, rid))
        # impatient riders leave very crowded platforms
        for key, n in self.waiting.items():
            cap = self.st[key[0]]["capacity"]
            if n > cap:
                self.waiting[key] = n - (n - cap) * 0.02 * dt / 60
        return events

    def tap_event(self, sid, direction, ts_ms, route_hint=None):
        rnd = self.rnd
        card = pick(rnd, CARD_TYPES)
        x = rnd.random()
        if x < 0.004:  # corrupted payloads from flaky validators
            return "{\"tap_id\": \"" + uuid.uuid4().hex[:8] + "\", \"station_id\": "
        if x < 0.08:   # legacy validators: flat schema with different field names
            return {"id": uuid.uuid4().hex, "timestamp": ts_ms, "stationId": sid, "dir": direction,
                    "cardType": card, "w": self.w}
        ev = {"tap_id": uuid.uuid4().hex, "ts": ts_ms, "station_id": sid, "direction": direction, "weight": self.w,
              "card": {"type": card, "token": uuid.uuid4().hex[:12]},
              "device": {"kind": rnd.choice(DEVICE_KINDS), "id": f"{sid}-{rnd.randint(1, 12):02d}"}}
        if card.startswith("OMNY_MOBILE"):
            ev["device"]["os"] = rnd.choice(["ios", "android"])
        if direction == "IN":
            ev["fare"] = {"amount": 1.45 if card == "REDUCED_FARE" else (0 if card == "STUDENT" else 2.90),
                          "product": "PAY_PER_RIDE" if rnd.random() < 0.8 else "FARE_CAP"}
            if route_hint and rnd.random() < 0.18:
                ev["transfer_from"] = {"mode": rnd.choice(["bus", "subway"]), "minutes_ago": rnd.randint(1, 18)}
        return ev

    # ---------------------------------------------------------------- vehicle interaction
    def boarding(self, vehicle, sid):
        key = (sid, vehicle.route.id, vehicle.direction)
        waiting = self.waiting.get(key, 0.0)
        room = max(0.0, vehicle.capacity * 1.5 - vehicle.load)  # crush load = 150 %
        n = min(waiting, room)
        self.waiting[key] = waiting - n
        return n

    def alighting(self, vehicle, sid):
        st = self.st[sid]
        busy = min(1.0, st["peak_hourly"] / 8000)
        c = self.coreness.get(sid, 0)
        inbound = self.inbound_dir.get((sid, vehicle.route.id)) == vehicle.direction
        # commuters ride into the core in the morning (few get off on the way) and out of it in the evening
        p_in = self.p_inbound(sid)
        towards_core = p_in if inbound else 1 - p_in
        frac = 0.03 + 0.12 * busy + 0.45 * c * towards_core + 0.1 * (1 - c) * (1 - towards_core)
        return vehicle.load * min(0.9, frac * self.rnd.uniform(0.75, 1.25))

    def tap_outs(self, sid, n_people, ts_ms):
        return [self.tap_event(sid, "OUT", ts_ms) for _ in range(poisson(self.rnd, n_people / self.w))]

    # ---------------------------------------------------------------- density sensors
    def present(self, sid):
        return sum(v for (s, _, _), v in self.waiting.items() if s == sid) + self.walkers.get(sid, 0.0)

    def density_events(self, ts_ms):
        out = []
        by_station = {}
        for (s, _, _), v in self.waiting.items():
            by_station[s] = by_station.get(s, 0.0) + v
        for sid in self.st:
            people = by_station.get(sid, 0.0) + self.walkers.get(sid, 0.0)
            wifi = poisson(self.rnd, people * WIFI_SHARE)
            cam_ok = self.rnd.random() > 0.03
            camera = int(max(0, people * CAMERA_SHARE * self.rnd.gauss(1, 0.08))) if cam_ok else None
            out.append({"station_id": sid, "ts": ts_ms, "zone": "platform+mezzanine", "wifi_devices": wifi,
                        "camera_count": camera, "confidence": round(0.92 if cam_ok else 0.55, 2),
                        "sensor": {"wifi_aps": 2 + len(self.st[sid]["routes"]), "cameras": 4 if cam_ok else 3}})
        return out
