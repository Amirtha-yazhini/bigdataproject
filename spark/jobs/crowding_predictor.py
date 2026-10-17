"""Live crowding prediction.

Station: the GBT model (trained by train_crowding_model.py on real hourly ridership) predicts the entry rate for
the next hour from the observed rate of the last minutes plus calendar/profile features. Crowding now comes from
fused Wi-Fi/camera density relative to station capacity; +15/+30 min crowding scales it with the predicted rate.

Vehicle: predicted load 15 min ahead = current load, minus alighting, plus expected boarding at the next stops
(from the live station density)."""
import os
from datetime import datetime, timedelta, timezone

from common import MODEL_DIR, daytype_of

FEATURES = ["hour", "daytype", "is_weekend", "station_mean", "profile_t", "profile_prev", "lag1", "lag_ratio"]
WIFI_SHARE, CAMERA_SHARE = 0.55, 0.9


def level(c):
    if c is None:
        return "UNKNOWN"
    return "LOW" if c < 0.4 else "MODERATE" if c < 0.7 else "HIGH" if c < 1.0 else "CRITICAL"


def fuse(wifi, camera):
    est = []
    if wifi is not None:
        est.append(wifi / WIFI_SHARE)
    if camera is not None:
        est.append(camera / CAMERA_SHARE)
    return sum(est) / len(est) if est else None


def to_dt(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


class CrowdingPredictor:
    def __init__(self, spark, net):
        self.spark = spark
        self.net = net
        self.model = None
        self.kind = "profile-ratio"
        path = os.path.join(MODEL_DIR, "crowding_gbt")
        if os.path.exists(path):
            try:
                from pyspark.ml import PipelineModel
                self.model = PipelineModel.load(path)
                self.kind = "spark-mllib-gbt"
            except Exception as e:  # noqa
                print("[predictor] could not load model:", e, flush=True)
        print("[predictor] using", self.kind, flush=True)
        for s in net["stations"]:
            p = s["profile"]
            s["station_mean"] = (5 * sum(p[0]) + sum(p[1]) + sum(p[2])) / (7 * 24)

    def _features(self, sid, now, rate_now):
        st = self.net["station_by_id"][sid]
        t = now + timedelta(hours=1)
        dt_t, dt_p = daytype_of(t), daytype_of(now)
        return {"station_id": sid, "hour": float(t.hour), "daytype": float(dt_t), "is_weekend": float(dt_t > 0),
                "station_mean": float(st["station_mean"]), "profile_t": float(st["profile"][dt_t][t.hour]),
                "profile_prev": float(st["profile"][dt_p][now.hour]), "lag1": float(rate_now),
                "lag_ratio": (float(rate_now) + 1) / (float(st["profile"][dt_p][now.hour]) + 1)}

    def predict_next_hour(self, items):
        """items: [(station_id, now_dt, rate_now)] -> {station_id: predicted entries next hour}"""
        feats = [self._features(*it) for it in items]
        if not feats:
            return {}
        if self.model is not None:
            try:
                df = self.spark.createDataFrame(feats)
                return {r["station_id"]: max(0.0, float(r["prediction"]) * (r["profile_t"] + 1) - 1)
                        for r in self.model.transform(df).select("station_id", "prediction", "profile_t").collect()}
            except Exception as e:  # noqa
                print("[predictor] model scoring failed, falling back:", e, flush=True)
        return {f["station_id"]: f["lag1"] * (f["profile_t"] + 1) / (f["profile_prev"] + 1) for f in feats}

    def station_forecast(self, sid, fused, rate_now, rate_next, incident_sev=0):
        cap = self.net["station_by_id"][sid]["capacity"]
        crowd_now = None if fused is None else fused / cap
        out = {"crowd_now": crowd_now, "rate_now": rate_now, "rate_next_hour": rate_next, "model": self.kind}
        for minutes, frac in ((15, 0.25), (30, 0.5)):
            r = rate_now + (rate_next - rate_now) * frac
            ratio = min(2.0, max(0.5, r / max(rate_now, 30.0))) if rate_now > 0 else 1.0
            c = None if crowd_now is None else crowd_now * ratio
            if c is not None and incident_sev >= 2:
                c += 0.08 * incident_sev * (minutes / 15)  # riders accumulate while service is disrupted
            out[f"crowd_{minutes}m"] = c
        out["level"] = level(crowd_now)
        out["level_30m"] = level(out["crowd_30m"])
        return out

    def vehicle_forecast(self, v, stations_live, horizon_s=900):
        r = self.net["route_by_id"].get(v["route_id"])
        if not r:
            return None
        stops = r["stops"] if v["direction"] == 0 else list(reversed(r["stops"]))
        travel = r["travel_s"] if v["direction"] == 0 else list(reversed(r["travel_s"]))
        try:
            i = stops.index(v["next_stop_id"])
        except ValueError:
            return None
        load, t = float(v["load_count"]), 0.0
        while i < len(stops) and t <= horizon_s:
            sid = stops[i]
            if i == len(stops) - 1:
                load = 0.0
                break
            st = self.net["station_by_id"].get(sid, {})
            busy = min(1.0, st.get("peak_hourly", 0) / 8000)
            load -= load * (0.06 + 0.3 * busy)
            fused = (stations_live.get(sid) or {}).get("fused")
            if fused:
                load += fused / max(1, 2 * len(st.get("routes", [1])))
            t += travel[i] if i < len(travel) else 0
            i += 1
        return round(100 * load / max(1, v["capacity"]), 1)
