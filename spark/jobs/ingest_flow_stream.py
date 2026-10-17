"""Passenger flow: semi-structured tap events + Wi-Fi/camera density -> station flow windows + crowding prediction.

Query `tap_breakdown` : normalises two tap schemas (v2 nested JSON + legacy flat) and corrupt payloads; per-batch
                        card-type counts become HBase *atomic counters* in dynamic columns tap:card_<TYPE>.
Query `flow_1m`       : union of taps + density, event-time 1-min tumbling windows (watermark 2 min) -> station_flow,
                        then live crowding prediction (Spark MLlib) -> station_flow.pred + station_state.
Query `flow_15m`      : same fusion over 15-min sliding windows (slide 5 min) -> station_flow.
"""
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import CHECKPOINT_DIR, TOPIC_DENSITY, TOPIC_TAPS, station_flow_key
from crowding_predictor import fuse, to_dt
from ingest_vehicle_stream import kafka_source

TAP_SCHEMA = T.StructType([
    # v2 (current validators / mobile)
    T.StructField("tap_id", T.StringType()), T.StructField("ts", T.LongType()),
    T.StructField("station_id", T.StringType()), T.StructField("direction", T.StringType()),
    T.StructField("weight", T.IntegerType()),
    T.StructField("card", T.StructType([T.StructField("type", T.StringType()), T.StructField("token", T.StringType())])),
    T.StructField("fare", T.StructType([T.StructField("amount", T.DoubleType()), T.StructField("product", T.StringType())])),
    T.StructField("device", T.StructType([T.StructField("kind", T.StringType()), T.StructField("id", T.StringType()),
                                          T.StructField("os", T.StringType())])),
    T.StructField("transfer_from", T.StructType([T.StructField("mode", T.StringType()),
                                                 T.StructField("minutes_ago", T.IntegerType())])),
    # legacy flat schema
    T.StructField("id", T.StringType()), T.StructField("timestamp", T.LongType()),
    T.StructField("stationId", T.StringType()), T.StructField("dir", T.StringType()),
    T.StructField("cardType", T.StringType()), T.StructField("w", T.IntegerType()),
])

DENSITY_SCHEMA = T.StructType([
    T.StructField("station_id", T.StringType()), T.StructField("ts", T.LongType()),
    T.StructField("zone", T.StringType()), T.StructField("wifi_devices", T.IntegerType()),
    T.StructField("camera_count", T.IntegerType()), T.StructField("confidence", T.DoubleType()),
])


def start(spark, hb, live, net, predictor):
    raw_taps = (kafka_source(spark, TOPIC_TAPS)
                .select(F.from_json(F.col("value").cast("string"), TAP_SCHEMA).alias("t")))
    taps = raw_taps.select(
        F.coalesce("t.station_id", "t.stationId").alias("station_id"),
        F.coalesce("t.ts", "t.timestamp").alias("ts"),
        F.upper(F.coalesce("t.direction", "t.dir")).alias("direction"),
        F.coalesce("t.card.type", "t.cardType", F.lit("UNKNOWN")).alias("card_type"),
        F.coalesce("t.weight", "t.w", F.lit(1)).alias("weight"),
        F.col("t.transfer_from").isNotNull().alias("is_transfer"),
        F.when(F.col("t.station_id").isNotNull(), "v2")
         .when(F.col("t.stationId").isNotNull(), "legacy").otherwise("corrupt").alias("schema"))

    # ------------------------------------------------------------- dynamic-column counters per card type
    def write_breakdown(df, batch_id):
        stats = {r["schema"]: r["n"] for r in df.groupBy("schema").agg(F.count("*").alias("n")).collect()}
        if not stats:
            return
        hb.counter_inc("system_meta", [("taps", f"s:{k}", v) for k, v in stats.items()])
        good = df.where("schema != 'corrupt' AND ts IS NOT NULL")
        agg = (good.withColumn("ws", (F.floor(F.col("ts") / 60000) * 60000).cast("long"))
               .groupBy("station_id", "ws", "card_type", "direction")
               .agg(F.sum("weight").alias("n"), F.sum(F.when(F.col("is_transfer"), F.col("weight")).otherwise(0)).alias("xfer"))
               .collect())
        incs = []
        for r in agg:
            key = station_flow_key(r["station_id"], "1m", r["ws"])
            if r["direction"] == "IN":
                incs.append((key, f"tap:card_{r['card_type']}", r["n"]))
                if r["xfer"]:
                    incs.append((key, "tap:transfers", r["xfer"]))
        hb.counter_inc("station_flow", incs)

    q0 = (taps.writeStream.queryName("tap_breakdown").trigger(processingTime="10 seconds")
          .option("checkpointLocation", f"{CHECKPOINT_DIR}/tap_breakdown")
          .foreachBatch(write_breakdown).start())

    # ------------------------------------------------------------- multi-stream fusion
    tap_part = (taps.where("schema != 'corrupt' AND ts IS NOT NULL AND station_id IS NOT NULL")
                .select("station_id", (F.col("ts") / 1000).cast("timestamp").alias("event_time"),
                        F.when(F.col("direction") == "IN", F.col("weight")).otherwise(0).cast("double").alias("tin"),
                        F.when(F.col("direction") == "OUT", F.col("weight")).otherwise(0).cast("double").alias("tout"),
                        F.lit(None).cast("double").alias("wifi"), F.lit(None).cast("double").alias("camera"),
                        F.lit(None).cast("double").alias("confidence")))
    dens_part = (kafka_source(spark, TOPIC_DENSITY)
                 .select(F.from_json(F.col("value").cast("string"), DENSITY_SCHEMA).alias("d"))
                 .where(F.col("d.station_id").isNotNull())
                 .select(F.col("d.station_id").alias("station_id"), (F.col("d.ts") / 1000).cast("timestamp").alias("event_time"),
                         F.lit(0.0).alias("tin"), F.lit(0.0).alias("tout"),
                         F.col("d.wifi_devices").cast("double").alias("wifi"),
                         F.col("d.camera_count").cast("double").alias("camera"),
                         F.col("d.confidence").alias("confidence")))
    fused = tap_part.unionByName(dens_part).withWatermark("event_time", "2 minutes")

    def agg(window_col):
        return (fused.groupBy(window_col, "station_id")
                .agg(F.sum("tin").alias("tin"), F.sum("tout").alias("tout"), F.avg("wifi").alias("wifi"),
                     F.avg("camera").alias("camera"), F.avg("confidence").alias("confidence"),
                     F.max("event_time").alias("last_ts")))

    def base_cols(r):
        fz = fuse(r["wifi"], r["camera"])
        return fz, {"tap:in": r["tin"], "tap:out": r["tout"], "den:wifi": r["wifi"], "den:camera": r["camera"],
                    "den:confidence": r["confidence"], "den:fused": fz,
                    "tap:window_start": int(r["window"]["start"].timestamp() * 1000),
                    "tap:last_ts": int(r["last_ts"].timestamp() * 1000)}

    def write_flow_1m(df, batch_id):
        rows = df.collect()
        if not rows:
            return
        puts, latest = [], {}
        for r in rows:
            fz, data = base_cols(r)
            ws = data["tap:window_start"]
            rate = live.push_flow(r["station_id"], ws, r["tin"] or 0.0, data["tap:last_ts"])
            puts.append((station_flow_key(r["station_id"], "1m", ws), data))
            cur = latest.get(r["station_id"])
            if cur is None or ws > cur["ws"] or (fz is not None and cur["fused"] is None):
                latest[r["station_id"]] = {"ws": ws, "fused": fz if fz is not None else (cur or {}).get("fused"),
                                           "rate": rate, "tin": r["tin"], "tout": r["tout"], "wifi": r["wifi"],
                                           "camera": r["camera"], "last_ts": data["tap:last_ts"]}
        hb.put_many("station_flow", puts)

        # ---- crowding prediction for every station touched in this batch
        clock, _, _, stations_live, incidents = live.snapshot()
        sev = {}
        for inc in incidents.values():
            sev[inc["station_id"]] = max(sev.get(inc["station_id"], 0), int(inc.get("severity", 0)))
        now = to_dt(clock or max(v["last_ts"] for v in latest.values()))
        next_hour = predictor.predict_next_hour([(sid, now, v["rate"]) for sid, v in latest.items()
                                                 if sid in net["station_by_id"]])
        pred_puts, state_puts = [], []
        for sid, v in latest.items():
            if sid not in net["station_by_id"]:
                continue
            fz = v["fused"] if v["fused"] is not None else (stations_live.get(sid) or {}).get("fused")
            f = predictor.station_forecast(sid, fz, v["rate"], next_hour.get(sid, v["rate"]), sev.get(sid, 0))
            pred = {"pred:crowd_now": f["crowd_now"], "pred:crowd_15m": f["crowd_15m"], "pred:crowd_30m": f["crowd_30m"],
                    "pred:level": f["level"], "pred:level_30m": f["level_30m"], "pred:rate_now": f["rate_now"],
                    "pred:rate_next_hour": f["rate_next_hour"], "pred:model": f["model"]}
            pred_puts.append((station_flow_key(sid, "1m", v["ws"]), pred))
            state_puts.append((sid, {"cur:ts": v["last_ts"], "cur:window_start": v["ws"], "cur:taps_in": v["tin"],
                                     "cur:taps_out": v["tout"], "cur:wifi": v["wifi"], "cur:camera": v["camera"],
                                     "cur:fused": fz, "cur:rate_per_hour": v["rate"], **pred}))
            live.update_station(sid, {"fused": fz, "rate": v["rate"], **f})
        hb.put_many("station_flow", pred_puts)
        hb.put_many("station_state", state_puts)

    q1 = (agg(F.window("event_time", "1 minute")).writeStream.queryName("flow_1m").outputMode("update")
          .trigger(processingTime="10 seconds")
          .option("checkpointLocation", f"{CHECKPOINT_DIR}/flow_1m")
          .foreachBatch(write_flow_1m).start())

    def write_flow_15m(df, batch_id):
        puts = []
        for r in df.collect():
            _, data = base_cols(r)
            puts.append((station_flow_key(r["station_id"], "15m", data["tap:window_start"]), data))
        hb.put_many("station_flow", puts)

    q2 = (agg(F.window("event_time", "15 minutes", "5 minutes")).writeStream.queryName("flow_15m").outputMode("update")
          .trigger(processingTime="30 seconds")
          .option("checkpointLocation", f"{CHECKPOINT_DIR}/flow_15m")
          .foreachBatch(write_flow_15m).start())
    return [q0, q1, q2]
