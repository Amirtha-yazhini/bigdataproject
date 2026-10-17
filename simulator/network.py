"""Loads data/processed/network.json and provides route geometry helpers for the simulator."""
import json
import math
import os

DATA_DIR = os.getenv("DATA_DIR", "/data/processed")


def haversine_m(lat1, lon1, lat2, lon2):
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def bearing(lat1, lon1, lat2, lon2):
    y = math.sin(math.radians(lon2 - lon1)) * math.cos(math.radians(lat2))
    x = math.cos(math.radians(lat1)) * math.sin(math.radians(lat2)) - math.sin(math.radians(lat1)) * math.cos(
        math.radians(lat2)) * math.cos(math.radians(lon2 - lon1))
    return (math.degrees(math.atan2(y, x)) + 360) % 360


class Route:
    def __init__(self, d, stations):
        self.id = d["route_id"]
        self.name = d["name"]
        self.stops = d["stops"]
        self.travel = d["travel_s"]
        self.headway = d["headway_s"]
        self.layover = d["layover_s"]
        self.shape = d["shape"]
        self.idx = d["stop_shape_idx"]
        self.cum = [0]
        for t in self.travel:
            self.cum.append(self.cum[-1] + t)
        self.one_way = self.cum[-1]
        self.cycle = 2 * (self.one_way + self.layover)
        self.n = len(self.stops)
        # big trains on the numbered (A-division) lines are a bit narrower
        self.capacity = 1100 if self.id[:1].isdigit() else 1250
        # per-segment polylines (stop i -> stop i+1) with cumulative distances, for smooth movement
        self.seg_lines = []
        for i in range(self.n - 1):
            a, b = self.idx[i], self.idx[i + 1]
            pts = self.shape[a:b + 1] if b > a else [[stations[self.stops[i]]["lat"], stations[self.stops[i]]["lon"]],
                                                      [stations[self.stops[i + 1]]["lat"], stations[self.stops[i + 1]]["lon"]]]
            d = [0.0]
            for k in range(1, len(pts)):
                d.append(d[-1] + haversine_m(pts[k - 1][0], pts[k - 1][1], pts[k][0], pts[k][1]))
            self.seg_lines.append((pts, d))

    def stop_seq(self, direction):
        return self.stops if direction == 0 else list(reversed(self.stops))

    def seg_travel(self, direction, seg):
        """scheduled seconds from the seg-th stop to the next one in this direction"""
        return self.travel[seg] if direction == 0 else self.travel[self.n - 2 - seg]

    def position(self, direction, seg, frac):
        """lat, lon, bearing for a vehicle `frac` of the way through segment `seg`"""
        g = seg if direction == 0 else self.n - 2 - seg
        f = frac if direction == 0 else 1 - frac
        pts, d = self.seg_lines[g]
        total = d[-1] or 1
        target = f * total
        k = 1
        while k < len(d) - 1 and d[k] < target:
            k += 1
        span = (d[k] - d[k - 1]) or 1
        t = min(1, max(0, (target - d[k - 1]) / span))
        lat = pts[k - 1][0] + (pts[k][0] - pts[k - 1][0]) * t
        lon = pts[k - 1][1] + (pts[k][1] - pts[k - 1][1]) * t
        br = bearing(pts[k - 1][0], pts[k - 1][1], pts[k][0], pts[k][1])
        if direction == 1:
            br = (br + 180) % 360
        return lat, lon, br, haversine_m(pts[k - 1][0], pts[k - 1][1], pts[k][0], pts[k][1])

    def progress(self, direction, seg, frac, at_end_layover=0.0):
        """position on the scheduled cycle in seconds: outbound, layover, inbound, layover"""
        if direction == 0:
            return self.cum[seg] + frac * self.travel[seg] + at_end_layover
        base = self.one_way + self.layover
        return base + (self.one_way - self.cum[self.n - 1 - seg]) + frac * self.seg_travel(1, seg) + at_end_layover

    def locate(self, p):
        """inverse of progress(): cycle seconds -> (direction, seg, frac)"""
        p %= self.cycle
        half = self.one_way + self.layover
        direction = 0 if p < half else 1
        q = min(self.one_way - 1e-6, (p if direction == 0 else p - half))
        for seg in range(self.n - 1):
            t = self.seg_travel(direction, seg)
            if q < t:
                return direction, seg, q / t
            q -= t
        return direction, self.n - 2, 0.999


def load():
    with open(os.path.join(DATA_DIR, "network.json")) as f:
        net = json.load(f)
    with open(os.path.join(DATA_DIR, "incident_stats.json")) as f:
        inc = json.load(f)
    stations = {s["station_id"]: s for s in net["stations"]}
    routes = {r["route_id"]: Route(r, stations) for r in net["routes"]}
    return net, stations, routes, inc
