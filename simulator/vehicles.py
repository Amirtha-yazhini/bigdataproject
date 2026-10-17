"""Train movement, boarding/alighting and occupancy. Dwell time grows with passenger volume,
so a late train collects more riders, gets later still, and bunching emerges on its own."""
import random

BASE_DWELL = 20.0
DWELL_PER_PAX = 0.035
MAX_DWELL = 150.0


class Vehicle:
    def __init__(self, vid, route, direction, seg, frac, rnd):
        self.id = vid
        self.route = route
        self.direction = direction
        self.seg = seg
        self.frac = frac
        self.state = "MOVING"
        self.timer = 0.0           # remaining dwell/layover/hold seconds
        self.seg_factor = 1.0
        self.load = 0.0
        self.capacity = route.capacity
        self.delay = 0.0
        self.hold_extra = 0.0
        self.rnd = rnd
        self.trip_no = 0
        self.speed_kmh = 0.0
        self.lat = self.lon = self.bearing = 0.0
        self._new_segment()

    # ------------------------------------------------------------------ helpers
    @property
    def trip_id(self):
        return f"{self.route.id}_{self.id}_{self.trip_no:04d}_{self.direction}"

    def stop_seq(self):
        return self.route.stop_seq(self.direction)

    def prev_stop(self):
        return self.stop_seq()[self.seg]

    def next_stop(self):
        return self.stop_seq()[min(self.seg + 1, self.route.n - 1)]

    def progress(self):
        extra = 0.0
        if self.state == "LAYOVER":
            extra = self.route.layover * max(0.0, 1 - self.timer / max(1.0, self.route.layover))
        return self.route.progress(self.direction, self.seg, self.frac, extra) % self.route.cycle

    def _new_segment(self):
        self.seg_factor = self.rnd.lognormvariate(0, 0.06)

    # ------------------------------------------------------------------ simulation
    def step(self, dt, world):
        """advance dt sim-seconds. world provides boarding/alighting and incident slowdowns."""
        remaining = dt
        guard = 0
        while remaining > 0 and guard < 20:
            guard += 1
            if self.state in ("DWELL", "LAYOVER", "HELD"):
                use = min(remaining, self.timer)
                self.timer -= use
                remaining -= use
                if self.state == "HELD":
                    self.delay += use
                if self.timer <= 1e-6:
                    if self.state == "LAYOVER":
                        self._start_trip(world)
                    self.state = "MOVING"
                    self._new_segment()
                continue
            # MOVING
            sched = self.route.seg_travel(self.direction, self.seg)
            slow = world.incident_factor(self.route.id, self.prev_stop(), self.next_stop())
            if slow is None:  # line blocked ahead: wait in place
                self.delay += remaining
                self.speed_kmh = 0.0
                remaining = 0
                break
            actual = sched * self.seg_factor * slow
            left_s = (1 - self.frac) * actual
            use = min(remaining, left_s)
            self.frac += use / actual
            remaining -= use
            self.delay += use * (1 - 1 / (self.seg_factor * slow))
            if self.frac >= 1 - 1e-9:
                self._arrive(world)
        self._update_position(dt)

    def _arrive(self, world):
        self.seg += 1
        self.frac = 0.0
        stop = self.stop_seq()[self.seg]
        terminal = self.seg >= self.route.n - 1
        alight = self.load if terminal else world.alighting(self, stop)
        self.load -= alight
        board = 0.0 if terminal else world.boarding(self, stop)
        self.load += board
        world.on_alight(stop, alight)
        dwell = min(MAX_DWELL, BASE_DWELL + DWELL_PER_PAX * (alight + board)) + self.hold_extra
        self.delay = max(-120.0, self.delay + dwell - 24.0)  # timetable allows ~24 s dwell
        self.hold_extra = 0.0
        if terminal:
            # layover absorbs lateness (schedule recovery)
            recover = min(self.delay, self.route.layover * 0.7) if self.delay > 0 else 0
            self.delay -= recover
            self.state, self.timer = "LAYOVER", max(30.0, self.route.layover - recover)
            self.seg = self.route.n - 2
            self.frac = 1.0
        else:
            self.state, self.timer = "DWELL", dwell

    def _start_trip(self, world):
        self.direction = 1 - self.direction
        self.seg, self.frac = 0, 0.0
        self.trip_no += 1
        stop = self.stop_seq()[0]
        self.load = world.boarding(self, stop)

    def hold(self, seconds):
        if self.state in ("DWELL", "LAYOVER"):
            self.timer += seconds
        else:
            self.hold_extra += seconds

    def _update_position(self, dt):
        prev = (self.lat, self.lon)
        self.lat, self.lon, self.bearing, _ = self.route.position(self.direction, self.seg, min(1.0, self.frac))
        if self.state == "MOVING" and prev[0]:
            from network import haversine_m
            self.speed_kmh = haversine_m(prev[0], prev[1], self.lat, self.lon) / max(dt, 1) * 3.6
        elif self.state != "MOVING":
            self.speed_kmh = 0.0

    def status(self):
        if self.state == "MOVING" and self.speed_kmh < 1:
            return "STOPPED"
        return {"MOVING": "IN_TRANSIT", "DWELL": "AT_STOP", "LAYOVER": "LAYOVER", "HELD": "HELD"}[self.state]

    def telemetry(self, ts_ms):
        occ = self.load / self.capacity
        noisy = max(0.0, occ * self.rnd.gauss(1, 0.03))
        return {
            "vehicle_id": self.id, "route_id": self.route.id, "trip_id": self.trip_id, "ts": ts_ms,
            "lat": round(self.lat, 6), "lon": round(self.lon, 6), "bearing": round(self.bearing, 1),
            "speed_kmh": round(self.speed_kmh, 1), "occupancy_pct": round(noisy * 100, 1),
            "load_count": int(self.load), "capacity": self.capacity, "delay_s": int(self.delay),
            "direction": self.direction, "prev_stop_id": self.prev_stop(), "next_stop_id": self.next_stop(),
            "progress_s": round(self.progress(), 1), "cycle_s": self.route.cycle,
            "door_status": "OPEN" if self.state == "DWELL" else "CLOSED", "status": self.status(),
        }


def build_fleet(routes, fleet_scale, seed=7):
    rnd = random.Random(seed)
    fleet = {}
    for r in routes.values():
        headway = r.headway / fleet_scale
        n = max(2, round(r.cycle / headway))
        for k in range(n):
            p = (k * r.cycle / n + rnd.uniform(-20, 20)) % r.cycle
            d, seg, frac = r.locate(p)
            v = Vehicle(f"{r.id}-{k + 1:03d}", r, d, seg, frac, random.Random(rnd.random()))
            v.load = v.capacity * rnd.uniform(0.1, 0.35)
            fleet[v.id] = v
    return fleet


def spawn_in_largest_gap(fleet, route, rnd):
    vs = sorted((v.progress(), v) for v in fleet.values() if v.route is route)
    if not vs:
        p = 0.0
    else:
        best_gap, p = -1, 0.0
        for i, (pa, _) in enumerate(vs):
            pb = vs[(i + 1) % len(vs)][0] + (route.cycle if i == len(vs) - 1 else 0)
            if pb - pa > best_gap:
                best_gap, p = pb - pa, pa + (pb - pa) / 2
    d, seg, frac = route.locate(p)
    n = 1 + max([int(v.id.split("-")[-1]) for v in fleet.values() if v.route is route] or [0])
    v = Vehicle(f"{route.id}-{n:03d}", route, d, seg, frac, random.Random(rnd.random()))
    fleet[v.id] = v
    return v


def remove_from_smallest_gap(fleet, route):
    vs = sorted((v.progress(), v) for v in fleet.values() if v.route is route)
    if len(vs) <= 2:
        return None
    best, victim = 1e18, None
    for i, (pa, va) in enumerate(vs):
        pb = vs[(i + 1) % len(vs)][0] + (route.cycle if i == len(vs) - 1 else 0)
        if pb - pa < best:
            best, victim = pb - pa, va
    del fleet[victim.id]
    return victim
