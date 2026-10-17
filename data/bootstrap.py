"""
Data bootstrap: builds everything the simulator, Spark and the API need under data/processed/.

REAL+SYNTH mode (default, needs internet):
  * MTA NYC Subway GTFS static feed  -> network (routes, stations, shapes, travel times, headways)
  * MTA Subway Hourly Ridership 2020-2024 (data.ny.gov wujg-7c2s) -> hourly demand per station
  * MTA Subway Major Incidents Beginning 2015 (data.ny.gov ereg-mcvp) -> incident rates/categories

FULL-SYNTH mode (fallback, or FORCE_SYNTHETIC=1): a generated 6-line city with the same files.

Outputs:
  processed/network.json          routes + stations + demand profiles
  processed/ridership_hourly.csv  station_id,ts,ridership   (training data for Spark MLlib)
  processed/incident_stats.json   per-route incident rate + category mix
  processed/provenance.json       which datasets were used and how
"""
import csv
import io
import json
import math
import os
import random
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(HERE, "raw")
OUT = os.path.join(HERE, "processed")

GTFS_URLS = [
    "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip",
    "http://web.mta.info/developers/data/nyct/subway/google_transit.zip",
]
RIDERSHIP_ID = "wujg-7c2s"   # MTA Subway Hourly Ridership: 2020-2024
INCIDENTS_ID = "ereg-mcvp"   # MTA Subway Major Incidents: Beginning 2015
SOCRATA = "https://data.ny.gov/resource/{}.json"

ROUTES = os.getenv("ROUTES", "1,2,3,4,5,6,7,A,E,L,N,Q").split(",")
RIDERSHIP_START = os.getenv("RIDERSHIP_START", "2024-09-02")  # a Monday
RIDERSHIP_WEEKS = int(os.getenv("RIDERSHIP_WEEKS", "8"))
FORCE_SYNTHETIC = os.getenv("FORCE_SYNTHETIC", "0") == "1"
FORCE_REBUILD = os.getenv("FORCE_REBUILD", "0") == "1"

LAYOVER_S = 180


def log(*a):
    print("[bootstrap]", *a, flush=True)


def haversine_m(lat1, lon1, lat2, lon2):
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def daytype(dt):
    wd = dt.weekday()
    return 0 if wd < 5 else (1 if wd == 5 else 2)  # 0 weekday, 1 saturday, 2 sunday


def gtfs_secs(t):
    h, m, s = (int(x) for x in t.split(":"))
    return h * 3600 + m * 60 + s


def decimate(points, min_m=40):
    out = [points[0]]
    for p in points[1:-1]:
        if haversine_m(out[-1][0], out[-1][1], p[0], p[1]) >= min_m:
            out.append(p)
    out.append(points[-1])
    return out


# --------------------------------------------------------------------------- REAL: GTFS network
def load_gtfs():
    os.makedirs(RAW, exist_ok=True)
    path = os.path.join(RAW, "gtfs_subway.zip")
    if not os.path.exists(path):
        last = None
        for url in GTFS_URLS:
            try:
                log("downloading GTFS", url)
                r = requests.get(url, timeout=120)
                r.raise_for_status()
                with open(path, "wb") as f:
                    f.write(r.content)
                break
            except Exception as e:  # noqa
                last = e
        else:
            raise RuntimeError(f"GTFS download failed: {last}")
    z = zipfile.ZipFile(path)

    def read(name):
        return list(csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8-sig")))

    routes = {r["route_id"]: r for r in read("routes.txt")}
    stops = {s["stop_id"]: s for s in read("stops.txt")}
    trips = read("trips.txt")
    log("GTFS: parsing stop_times (large file)...")
    weekday_trips = {t["trip_id"]: t for t in trips if t["route_id"] in ROUTES and "Weekday" in t["service_id"]}
    stop_times = defaultdict(list)
    with z.open("stop_times.txt") as fh:
        for row in csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8-sig")):
            if row["trip_id"] in weekday_trips:
                stop_times[row["trip_id"]].append(row)
    shapes = defaultdict(list)
    with z.open("shapes.txt") as fh:
        for row in csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8-sig")):
            shapes[row["shape_id"]].append((int(row["shape_pt_sequence"]), float(row["shape_pt_lat"]), float(row["shape_pt_lon"])))

    out_routes, stations = [], {}
    for rid in ROUTES:
        if rid not in routes:
            log("route not in feed, skipping", rid)
            continue
        cands = [t for t in weekday_trips.values() if t["route_id"] == rid and t["direction_id"] == "0"]
        best, best_n = None, -1
        dep_7_10 = 0
        for t in cands:
            st = stop_times[t["trip_id"]]
            if not st:
                continue
            first = gtfs_secs(min(st, key=lambda x: int(x["stop_sequence"]))["departure_time"])
            if 7 * 3600 <= first < 10 * 3600:
                dep_7_10 += 1
            # representative trip: most stops among midday trips
            if 10 * 3600 <= first <= 15 * 3600 and len(st) > best_n:
                best, best_n = t, len(st)
        if best is None:
            continue
        st = sorted(stop_times[best["trip_id"]], key=lambda x: int(x["stop_sequence"]))
        stop_ids, travel = [], []
        for i, s in enumerate(st):
            parent = stops[s["stop_id"]].get("parent_station") or s["stop_id"]
            stop_ids.append(parent)
            if i > 0:
                dt = gtfs_secs(s["arrival_time"]) - gtfs_secs(st[i - 1]["departure_time"])
                travel.append(max(45, dt))
        headway_s = int(min(900, max(120, (3 * 3600) / max(1, dep_7_10))))
        shape_pts = [(la, lo) for _, la, lo in sorted(shapes.get(best["shape_id"], []))]
        coords = [(float(stops[p]["stop_lat"]), float(stops[p]["stop_lon"])) for p in stop_ids]
        if len(shape_pts) < 2:
            shape_pts = coords
        shape_pts = decimate(shape_pts)
        # index of the nearest shape point for every stop (monotonic)
        idx, j = [], 0
        for la, lo in coords:
            bj, bd = j, 1e18
            for k in range(j, len(shape_pts)):
                d = haversine_m(la, lo, *shape_pts[k])
                if d < bd:
                    bj, bd = k, d
            idx.append(bj)
            j = bj
        r = routes[rid]
        out_routes.append({
            "route_id": rid,
            "name": f"{rid} — {r['route_long_name']}",
            "color": "#" + (r.get("route_color") or "888888"),
            "text_color": "#" + (r.get("route_text_color") or "FFFFFF"),
            "stops": stop_ids,
            "travel_s": travel,
            "headway_s": headway_s,
            "layover_s": LAYOVER_S,
            "shape": [[round(a, 6), round(b, 6)] for a, b in shape_pts],
            "stop_shape_idx": idx,
        })
        for p in stop_ids:
            s = stops[p]
            st_ = stations.setdefault(p, {"station_id": p, "name": s["stop_name"], "lat": float(s["stop_lat"]),
                                          "lon": float(s["stop_lon"]), "routes": []})
            if rid not in st_["routes"]:
                st_["routes"].append(rid)
        log(f"route {rid}: {len(stop_ids)} stops, one-way {sum(travel)//60} min, headway {headway_s}s")
    return out_routes, stations


# --------------------------------------------------------------------------- REAL: ridership
def socrata(dataset, params, timeout=180):
    for attempt in range(4):
        try:
            r = requests.get(SOCRATA.format(dataset), params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa
            log(f"socrata retry {attempt + 1}: {e}")
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"socrata query failed for {dataset}")


def load_ridership(stations):
    start = datetime.fromisoformat(RIDERSHIP_START)
    end = start + timedelta(weeks=RIDERSHIP_WEEKS) - timedelta(hours=1)
    first_day = f"transit_timestamp between '{start:%Y-%m-%dT%H:%M:%S}' and '{start + timedelta(hours=23):%Y-%m-%dT%H:%M:%S}'"
    complexes = socrata(RIDERSHIP_ID, {
        "$select": "station_complex_id,station_complex,max(latitude) as lat,max(longitude) as lon",
        "$where": first_day, "$group": "station_complex_id,station_complex", "$limit": 5000})
    # map every complex to the network stations it serves (all within 250 m, else nearest within 600 m)
    mapping = {}
    for c in complexes:
        try:
            la, lo = float(c["lat"]), float(c["lon"])
        except (KeyError, TypeError, ValueError):
            continue
        d = sorted((haversine_m(la, lo, s["lat"], s["lon"]), sid) for sid, s in stations.items())
        near = [sid for dist, sid in d if dist <= 250]
        if not near and d and d[0][0] <= 600:
            near = [d[0][1]]
        if near:
            mapping[c["station_complex_id"]] = near
    log(f"ridership: {len(mapping)} station complexes mapped onto the network")

    hourly = defaultdict(float)  # (station_id, ts) -> riders
    cache = os.path.join(RAW, f"ridership_{RIDERSHIP_START}_{RIDERSHIP_WEEKS}w.csv")
    if os.path.exists(cache):
        log("ridership: using cached raw download", cache)
        with open(cache) as f:
            for r in csv.DictReader(f):
                sids = mapping.get(r["station_complex_id"])
                for sid in sids or []:
                    hourly[(sid, r["ts"])] += float(r["ridership"]) / len(sids)
        return hourly
    raw_rows = []
    day = start
    while day <= end:
        chunk_end = min(end, day + timedelta(days=7) - timedelta(hours=1))
        where = f"transit_timestamp between '{day:%Y-%m-%dT%H:%M:%S}' and '{chunk_end:%Y-%m-%dT%H:%M:%S}'"
        offset = 0
        while True:
            rows = socrata(RIDERSHIP_ID, {
                "$select": "station_complex_id,transit_timestamp,sum(ridership) as ridership",
                "$where": where, "$group": "station_complex_id,transit_timestamp",
                "$order": "transit_timestamp,station_complex_id", "$limit": 50000, "$offset": offset})
            for r in rows:
                ts = r["transit_timestamp"][:19]
                raw_rows.append((r["station_complex_id"], ts, r.get("ridership") or 0))
                sids = mapping.get(r["station_complex_id"])
                if not sids:
                    continue
                v = float(r.get("ridership") or 0) / len(sids)
                for sid in sids:
                    hourly[(sid, ts)] += v
            if len(rows) < 50000:
                break
            offset += 50000
        log(f"ridership: fetched week starting {day:%Y-%m-%d}")
        day = chunk_end + timedelta(hours=1)
    with open(cache, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["station_complex_id", "ts", "ridership"])
        w.writerows(raw_rows)
    return hourly


def load_incident_stats(routes):
    rows = socrata(INCIDENTS_ID, {
        "$select": "line,category,sum(count) as n",
        "$where": "month >= '2023-01-01T00:00:00'", "$group": "line,category", "$limit": 5000})
    months = socrata(INCIDENTS_ID, {"$select": "count(distinct month) as m", "$where": "month >= '2023-01-01T00:00:00'"})
    n_months = max(1, int(float(months[0]["m"])))
    per_line, cats = defaultdict(float), defaultdict(float)
    for r in rows:
        n = float(r.get("n") or 0)
        if r.get("line"):
            per_line[r["line"].strip()] += n
        if r.get("category"):
            cats[r["category"]] += n
    total = sum(cats.values()) or 1
    rates = {}
    for rt in routes:
        rid = rt["route_id"]
        monthly = per_line.get(rid, sum(per_line.values()) / max(1, len(per_line)))
        rates[rid] = monthly / n_months / (30 * 24)  # incidents per hour
    return {"source": f"data.ny.gov {INCIDENTS_ID} (MTA Subway Major Incidents), months since 2023: {n_months}",
            "rate_per_hour": rates,
            "category_mix": {k: v / total for k, v in sorted(cats.items())}}


# --------------------------------------------------------------------------- SYNTHETIC city
def synthetic_city():
    rnd = random.Random(42)
    clat, clon = 40.735, -73.99
    colors = ["#D82233", "#009952", "#0062CF", "#EB6800", "#9A38A1", "#F6BC26"]
    names = ["Red", "Green", "Blue", "Orange", "Purple", "Yellow"]
    routes, stations = [], {}
    for li in range(6):
        ang = math.pi * li / 6
        n = 14
        stop_ids, travel = [], []
        for k in range(n):
            t = (k - (n - 1) / 2) / ((n - 1) / 2)
            la = clat + 0.09 * t * math.sin(ang) + 0.004 * math.sin(3 * t + li)
            lo = clon + 0.11 * t * math.cos(ang) + 0.004 * math.cos(2 * t + li)
            # lines cross near the centre: share the central station
            sid = "X00" if k == n // 2 else f"S{li}{k:02d}"
            if sid == "X00":
                la, lo = clat, clon
            stations.setdefault(sid, {"station_id": sid, "name": "Central Interchange" if sid == "X00" else f"{names[li]} Line St {k + 1}",
                                      "lat": round(la, 6), "lon": round(lo, 6), "routes": []})
            if f"L{li}" not in stations[sid]["routes"]:
                stations[sid]["routes"].append(f"L{li}")
            stop_ids.append(sid)
            if k:
                travel.append(rnd.randint(90, 160))
        coords = [[stations[s]["lat"], stations[s]["lon"]] for s in stop_ids]
        routes.append({"route_id": f"L{li}", "name": f"L{li} — {names[li]} Line", "color": colors[li], "text_color": "#FFFFFF",
                       "stops": stop_ids, "travel_s": travel, "headway_s": rnd.choice([240, 300, 360]), "layover_s": LAYOVER_S,
                       "shape": coords, "stop_shape_idx": list(range(len(coords)))})
    # synthetic ridership: double-peaked weekday curve, flatter weekends, station size lognormal
    start = datetime.fromisoformat(RIDERSHIP_START)
    hourly = {}
    size = {sid: (6 if sid == "X00" else 1) * rnd.lognormvariate(6.2, 0.6) for sid in stations}
    for h in range(RIDERSHIP_WEEKS * 7 * 24):
        ts = start + timedelta(hours=h)
        dt, hr = daytype(ts), ts.hour
        if dt == 0:
            shape = 0.15 + 1.0 * math.exp(-((hr - 8) ** 2) / 3) + 0.9 * math.exp(-((hr - 17.5) ** 2) / 4) + 0.35 * math.exp(-((hr - 13) ** 2) / 10)
        else:
            shape = 0.1 + 0.55 * math.exp(-((hr - 14) ** 2) / 18)
        if hr < 5:
            shape *= 0.2
        for sid, s in size.items():
            hourly[(sid, ts.strftime("%Y-%m-%dT%H:%M:%S"))] = max(0.0, s * shape * rnd.lognormvariate(0, 0.12))
    cats = {"Signals": 0.3, "Persons on Trackbed/Police/Medical": 0.26, "Track": 0.2, "Other": 0.1, "Subway Car": 0.08, "Stations and Structure": 0.06}
    inc = {"source": "synthetic", "rate_per_hour": {r["route_id"]: 0.02 for r in routes}, "category_mix": cats}
    return routes, stations, hourly, inc


# --------------------------------------------------------------------------- assemble outputs
def build_profiles(stations, hourly):
    acc = defaultdict(lambda: [[0.0] * 24 for _ in range(3)])
    cnt = defaultdict(lambda: [[0] * 24 for _ in range(3)])
    for (sid, ts), v in hourly.items():
        dt = datetime.fromisoformat(ts)
        acc[sid][daytype(dt)][dt.hour] += v
        cnt[sid][daytype(dt)][dt.hour] += 1
    real_ids = set(acc)
    if real_ids:
        peaks = sorted(max(acc[s][0][h] / max(1, cnt[s][0][h]) for h in range(24)) for s in real_ids)
        median_peak = peaks[len(peaks) // 2]
    else:
        median_peak = 500
    for sid, st in stations.items():
        if sid in real_ids:
            prof = [[round(acc[sid][d][h] / max(1, cnt[sid][d][h]), 1) for h in range(24)] for d in range(3)]
            st["demand_source"] = "real"
        else:  # no ridership complex matched: scaled-down generic profile
            ref = next(iter(real_ids), None)
            base = [[acc[ref][d][h] / max(1, cnt[ref][d][h]) for h in range(24)] for d in range(3)] if ref else [[100] * 24] * 3
            pk = max(max(r) for r in base) or 1
            prof = [[round(v / pk * median_peak * 0.3, 1) for v in row] for row in base]
            st["demand_source"] = "synthetic"
        st["profile"] = prof  # [daytype][hour] mean hourly entries
        peak = max(max(r) for r in prof)
        st["peak_hourly"] = round(peak, 1)
        # people a station can hold comfortably; busy stations saturate first
        st["capacity"] = int(min(3000, max(200, peak / 12 / 0.55)))
    return stations


def main():
    os.makedirs(OUT, exist_ok=True)
    if os.path.exists(os.path.join(OUT, "provenance.json")) and not FORCE_REBUILD:
        log("processed data already present; set FORCE_REBUILD=1 to rebuild")
        return
    mode = "REAL+SYNTH"
    try:
        if FORCE_SYNTHETIC:
            raise RuntimeError("FORCE_SYNTHETIC=1")
        routes, stations = load_gtfs()
        hourly = load_ridership(stations)
        if len(hourly) < 1000:
            raise RuntimeError("too little ridership data")
        try:
            inc = load_incident_stats(routes)
            inc_ds = {"name": "MTA Subway Major Incidents: Beginning 2015", "url": f"https://data.ny.gov/d/{INCIDENTS_ID}",
                      "type": "real", "used_for": "incident rates per line + category mix (since 2023)"}
        except Exception as e:  # noqa
            log("incident dataset unavailable, using synthetic incident rates:", e)
            inc = synthetic_city()[3]
            inc["rate_per_hour"] = {r["route_id"]: 0.015 for r in routes}
            inc_ds = {"name": "Synthetic incident rates", "type": "synthetic", "used_for": "incident rates + category mix"}
        datasets = [
            {"name": "MTA NYC Subway GTFS static", "url": GTFS_URLS[0], "type": "real", "used_for": "routes, stations, shapes, travel times, headways"},
            {"name": "MTA Subway Hourly Ridership: 2020-2024", "url": f"https://data.ny.gov/d/{RIDERSHIP_ID}", "type": "real",
             "used_for": f"hourly demand profiles + ML training ({RIDERSHIP_START}, {RIDERSHIP_WEEKS} weeks)"},
            inc_ds,
        ]
    except Exception as e:  # noqa
        log("REAL data unavailable -> FULL-SYNTH city. Reason:", e)
        mode = "FULL-SYNTH"
        routes, stations, hourly, inc = synthetic_city()
        datasets = [{"name": "Synthetic 6-line city", "type": "synthetic", "used_for": "network, ridership, incidents"}]
    datasets += [
        {"name": "Vehicle GPS + occupancy stream", "type": "synthetic", "used_for": "simulated along GTFS shapes; dwell depends on load (bunching emerges)"},
        {"name": "Passenger card / mobile taps", "type": "synthetic", "used_for": "Poisson arrivals calibrated on hourly ridership profiles"},
        {"name": "Station Wi-Fi / camera density", "type": "synthetic", "used_for": "derived from simulated waiting passengers + sensor noise"},
        {"name": "Live incidents", "type": "synthetic", "used_for": "generated with real per-line rates and category mix"},
    ]
    stations = build_profiles(stations, hourly)
    center = [sum(s["lat"] for s in stations.values()) / len(stations), sum(s["lon"] for s in stations.values()) / len(stations)]
    network = {"mode": mode, "city": "New York City (MTA Subway)" if mode == "REAL+SYNTH" else "Synthetic Metro City",
               "center": center, "routes": routes, "stations": sorted(stations.values(), key=lambda s: s["station_id"])}
    with open(os.path.join(OUT, "network.json"), "w") as f:
        json.dump(network, f)
    with open(os.path.join(OUT, "ridership_hourly.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["station_id", "ts", "ridership"])
        for (sid, ts), v in sorted(hourly.items(), key=lambda kv: (kv[0][1], kv[0][0])):
            w.writerow([sid, ts, round(v, 2)])
    with open(os.path.join(OUT, "incident_stats.json"), "w") as f:
        json.dump(inc, f, indent=2)
    prov = {"mode": mode, "built_at": datetime.now(timezone.utc).isoformat(), "datasets": datasets,
            "stats": {"routes": len(routes), "stations": len(stations), "ridership_rows": len(hourly),
                      "stations_with_real_demand": sum(1 for s in stations.values() if s.get("demand_source") == "real")}}
    with open(os.path.join(OUT, "provenance.json"), "w") as f:
        json.dump(prov, f, indent=2)
    log("done:", json.dumps(prov["stats"]), "mode", mode)


if __name__ == "__main__":
    sys.exit(main())
