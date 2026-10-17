"""Batch Spark MLlib job: train the crowding (station entry-rate) model on REAL MTA hourly ridership.

label    : ratio = (entries(s, t) + 1) / (profile(s, t) + 1)  -> scale-free across small and huge stations;
           prediction = ratio * (profile + 1) - 1, evaluated in passengers/hour
features : hour(t), daytype(t), is_weekend, station mean, historical profile at (s, daytype, hour) for t and t-1,
           lag1 = entries during hour t-1 (live: the observed rate of the last minutes), lag_ratio = lag1 vs its profile
split    : time-based - the last 2 weeks are held out
model    : GBTRegressor; compared against two baselines (historical profile, persistence)
"""
import json
import os
import time

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from common import DATA_DIR, MODEL_DIR
from crowding_predictor import FEATURES


def daytype(col):
    dow = F.dayofweek(col)  # 1 = Sunday ... 7 = Saturday
    return F.when(dow == 7, 1).when(dow == 1, 2).otherwise(0)


def main():
    spark = (SparkSession.builder.appName("train-crowding-model")
             .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()
    df = (spark.read.csv(os.path.join(DATA_DIR, "ridership_hourly.csv"), header=True,
                         schema="station_id string, ts string, ridership double")
          .withColumn("ts", F.to_timestamp("ts")))
    w = Window.partitionBy("station_id").orderBy("ts")
    df = (df.withColumn("prev_ts", F.lag("ts").over(w)).withColumn("lag1", F.lag("ridership").over(w))
          .where(F.col("prev_ts") == F.col("ts") - F.expr("INTERVAL 1 HOUR"))
          .withColumn("hour", F.hour("ts").cast("double")).withColumn("daytype", daytype("ts").cast("double"))
          .withColumn("is_weekend", (F.col("daytype") > 0).cast("double"))
          .withColumn("prev_hour", F.hour("prev_ts").cast("double")).withColumn("prev_daytype", daytype("prev_ts").cast("double")))

    split_ts = df.agg(F.max("ts")).first()[0] - __import__("datetime").timedelta(days=14)
    train_raw, test_raw = df.where(F.col("ts") <= split_ts), df.where(F.col("ts") > split_ts)

    # profiles from the training period only (no leakage)
    prof = train_raw.groupBy("station_id", "daytype", "hour").agg(F.avg("ridership").alias("profile_t"))
    prof_prev = prof.select("station_id", F.col("daytype").alias("prev_daytype"), F.col("hour").alias("prev_hour"),
                            F.col("profile_t").alias("profile_prev"))
    smean = train_raw.groupBy("station_id").agg(F.avg("ridership").alias("station_mean"))

    def enrich(d):
        return (d.join(prof, ["station_id", "daytype", "hour"]).join(prof_prev, ["station_id", "prev_daytype", "prev_hour"])
                .join(smean, "station_id").na.drop()
                .withColumn("lag_ratio", (F.col("lag1") + 1) / (F.col("profile_prev") + 1))
                .withColumn("ratio", (F.col("ridership") + 1) / (F.col("profile_t") + 1)))

    train, test = enrich(train_raw).cache(), enrich(test_raw).cache()
    n_train, n_test = train.count(), test.count()
    print(f"[train] rows train={n_train} test={n_test}", flush=True)

    gbt = GBTRegressor(featuresCol="features", labelCol="ratio", maxIter=40, maxDepth=6, stepSize=0.1,
                       subsamplingRate=0.8, seed=42)
    model = Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol="features"), gbt]).fit(train)
    pred = model.transform(test).withColumn("pred_riders", F.greatest(F.lit(0.0), F.col("prediction") * (F.col("profile_t") + 1) - 1))

    def scores(col):
        out = {}
        for m in ("rmse", "mae", "r2"):
            out[m] = round(RegressionEvaluator(labelCol="ridership", predictionCol=col, metricName=m).evaluate(pred), 4)
        return out

    metrics = {
        "model": "Spark MLlib GBTRegressor (40 trees, depth 6)",
        "dataset": "MTA Subway Hourly Ridership 2020-2024 (data.ny.gov wujg-7c2s)",
        "target": "station entries in the next hour (learned as ratio to the historical profile)",
        "features": FEATURES,
        "rows_train": n_train, "rows_test": n_test, "split": f"time-based, test = after {split_ts:%Y-%m-%d %H:%M}",
        "gbt": scores("pred_riders"),
        "baseline_profile": scores("profile_t"),
        "baseline_persistence": scores("lag1"),
        "feature_importance": {f: round(float(v), 4) for f, v in zip(FEATURES, model.stages[-1].featureImportances.toArray())},
        "trained_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "train_seconds": round(time.time() - t0, 1),
    }
    os.makedirs(MODEL_DIR, exist_ok=True)
    model.write().overwrite().save(os.path.join(MODEL_DIR, "crowding_gbt"))
    with open(os.path.join(MODEL_DIR, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)
    print("[train]", json.dumps(metrics, indent=2), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
