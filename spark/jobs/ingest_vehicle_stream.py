"""Kafka vehicle.telemetry -> HBase.

Query `vehicle_state`  : every micro-batch upserts the current state per vehicle (VERSIONS keep a short trail),
                         appends the full time series to `vehicle_ts`, refreshes headway/bunching + load forecast.
Query `route_windows`  : event-time 1-minute tumbling windows per route (watermark 2 min) -> `route_metrics`.
"""
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import (CHECKPOINT_DIR, KAFKA, TOPIC_VEHICLE, route_metrics_key, vehicle_ts_key)

VEHICLE_SCHEMA = T.StructType([
    T.StructField("vehicle_id", T.StringType()), T.StructField("route_id", T.StringType()),
    T.StructField("trip_id", T.StringType()), T.StructField("ts", T.LongType()),
    T.StructField("lat", T.DoubleType()), T.StructField("lon", T.DoubleType()),
    T.StructField("bearing", T.DoubleType()), T.StructField("speed_kmh", T.DoubleType()),
    T.StructField("occupancy_pct", T.DoubleType()), T.StructField("load_count", T.IntegerType()),
    T.StructField("capacity", T.IntegerType()), T.StructField("delay_s", T.IntegerType()),
    T.StructField("direction", T.IntegerType()), T.StructField("prev_stop_id", T.StringType()),
    T.StructField("next_stop_id", T.StringType()), T.StructField("progress_s", T.DoubleType()),
    T.StructField("cycle_s", T.DoubleType()), T.StructField("door_status", T.StringType()),
    T.StructField("status", T.StringType()),
])


def kafka_source(spark, topic, max_offsets=20000):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", max_offsets)
            .load())


def occ_level(pct):
    return "LOW" if pct < 40 else "MODERATE" if pct < 75 else "HIGH" if pct < 100 else "CRITICAL"


def start(spark, hb, live, net, predictor):
    parsed = (kafka_source(spark, TOPIC_VEHICLE)
              .select(F.from_json(F.col("value").cast("string"), VEHICLE_SCHEMA).alias("v"))
              .select("v.*")
              .where(F.col("vehicle_id").isNotNull() & F.col("ts").isNotNull())
              .withColumn("event_time", (F.col("ts") / 1000).cast("timestamp")))

    def write_vehicles(df, batch_id):
        rows = [r.asDict() for r in df.drop("event_time").collect()]
        if not rows:
            return
        # 1) append-only time series, salted row key -> writes spread over regions
        hb.put_many("vehicle_ts", [
            (vehicle_ts_key(r["route_id"], r["vehicle_id"], r["ts"]),
             {f"d:{k}": v for k, v in r.items() if k not in ("vehicle_id", "route_id")}) for r in rows])
        # 2) current state = newest event per vehicle
        latest = {}
        for r in rows:
            if r["vehicle_id"] not in latest or r["ts"] > latest[r["vehicle_id"]]["ts"]:
                latest[r["vehicle_id"]] = r
        live.tick(max(r["ts"] for r in rows))
        live.update_vehicles(latest.values())
        _, _, _, stations_live, _ = live.snapshot()
        puts = []
        for v in latest.values():
            v["pred_15m"] = predictor.vehicle_forecast(v, stations_live)
            puts.append((v["vehicle_id"], {
                "loc:lat": v["lat"], "loc:lon": v["lon"], "loc:bearing": v["bearing"], "loc:speed_kmh": v["speed_kmh"],
                "occ:pct": v["occupancy_pct"], "occ:count": v["load_count"], "occ:capacity": v["capacity"],
                "occ:level": occ_level(v["occupancy_pct"]), "occ:pred_15m": v["pred_15m"],
                "occ:pred_level": occ_level(v["pred_15m"]) if v["pred_15m"] is not None else None,
                "ops:route_id": v["route_id"], "ops:trip_id": v["trip_id"], "ops:direction": v["direction"],
                "ops:prev_stop": v["prev_stop_id"], "ops:next_stop": v["next_stop_id"], "ops:delay_s": v["delay_s"],
                "ops:status": v["status"], "ops:door": v["door_status"], "ops:progress_s": v["progress_s"],
                "ops:ts": v["ts"]}))
        hb.put_many("vehicle_state", puts)
        # 3) live headway / bunching per route
        for rid, h in live.route_headways().items():
            live.update_route(rid, h)
        hb.put("system_meta", "clock", {"s:sim_ts": live.clock_ms})

    q1 = (parsed.writeStream.queryName("vehicle_state")
          .trigger(processingTime="5 seconds")
          .option("checkpointLocation", f"{CHECKPOINT_DIR}/vehicle_state")
          .foreachBatch(write_vehicles).start())

    windows = (parsed.withWatermark("event_time", "2 minutes")
               .groupBy(F.window("event_time", "1 minute"), "route_id")
               .agg(F.avg("occupancy_pct").alias("avg_occ"), F.max("occupancy_pct").alias("max_occ"),
                    F.expr("percentile_approx(occupancy_pct, 0.9)").alias("p90_occ"),
                    F.avg("delay_s").alias("avg_delay"), F.max("delay_s").alias("max_delay"),
                    F.avg("speed_kmh").alias("avg_speed"), F.sum("load_count").alias("sum_load"),
                    F.approx_count_distinct("vehicle_id").alias("vehicles"), F.count("*").alias("events")))

    def write_route_windows(df, batch_id):
        rows = df.collect()
        if not rows:
            return
        _, _, routes_live, _, _ = live.snapshot()
        newest = {}
        puts = []
        for r in rows:
            ws = int(r["window"]["start"].timestamp() * 1000)
            h = routes_live.get(r["route_id"], {})
            data = {"m:window_start": ws, "m:avg_occ": r["avg_occ"], "m:max_occ": r["max_occ"], "m:p90_occ": r["p90_occ"],
                    "m:avg_delay_s": r["avg_delay"], "m:max_delay_s": r["max_delay"], "m:avg_speed": r["avg_speed"],
                    "m:vehicles": r["vehicles"], "m:events": r["events"],
                    "m:headway_actual_s": h.get("headway_actual_s"), "m:headway_effective_s": h.get("headway_effective_s"),
                    "m:headway_sched_s": h.get("headway_sched_s"), "m:bunching_idx": h.get("bunching_idx"),
                    "m:max_gap_s": h.get("max_gap_s")}
            puts.append((route_metrics_key(r["route_id"], ws), data))
            if r["route_id"] not in newest or ws > newest[r["route_id"]][0]:
                newest[r["route_id"]] = (ws, r)
        hb.put_many("route_metrics", puts)
        for rid, (ws, r) in newest.items():
            live.update_route(rid, {"window_start": ws, "avg_occ": r["avg_occ"], "max_occ": r["max_occ"],
                                    "p90_occ": r["p90_occ"], "avg_delay_s": r["avg_delay"], "max_delay_s": r["max_delay"]})

    q2 = (windows.writeStream.queryName("route_windows").outputMode("update")
          .trigger(processingTime="10 seconds")
          .option("checkpointLocation", f"{CHECKPOINT_DIR}/route_windows")
          .foreachBatch(write_route_windows).start())
    return [q1, q2]
