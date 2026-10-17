"""Spark driver that runs every streaming query of the pipeline plus the recommender thread.

queries: vehicle_state, route_windows, tap_breakdown, flow_1m, flow_15m, incidents
"""
import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener

import incident_impact
import ingest_flow_stream
import ingest_vehicle_stream
from common import CHECKPOINT_DIR, DATA_DIR, MODEL_DIR, HBase, load_network
from crowding_predictor import CrowdingPredictor
from live_state import LiveState
from recommender import Recommender


class ProgressListener(StreamingQueryListener):
    """publishes per-query throughput / latency into HBase system_meta for the dashboard"""

    def __init__(self, hb):
        self.hb = hb

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        try:
            self.hb.put("system_meta", f"query:{p.name}", {
                "s:name": p.name, "s:batch_id": p.batchId, "s:num_input_rows": p.numInputRows,
                "s:input_rps": round(p.inputRowsPerSecond or 0, 2), "s:processed_rps": round(p.processedRowsPerSecond or 0, 2),
                "s:batch_ms": (p.durationMs or {}).get("triggerExecution", 0),
                "s:watermark": (p.eventTime or {}).get("watermark", ""), "s:updated_wall": int(time.time() * 1000),
                "s:state_rows": sum(s.numRowsTotal for s in (p.stateOperators or []))})
        except Exception as e:  # noqa
            print("[listener]", e, flush=True)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print("[listener] query terminated", event.id, event.exception, flush=True)


def repair_checkpoints(root=CHECKPOINT_DIR):
    """A hard stop (e.g. Docker Desktop shutting down) can leave a truncated offset/commit file, after which the
    query fails with 'Incomplete log file'. Drop such trailing entries so Spark replays that batch from Kafka."""
    if not os.path.isdir(root):
        return
    for q in os.listdir(root):
        for sub, min_lines in (("offsets", 3), ("commits", 2)):
            d = os.path.join(root, q, sub)
            if not os.path.isdir(d):
                continue
            for name in os.listdir(d):
                if name.endswith(".tmp") or (name.endswith(".crc") and not os.path.exists(os.path.join(d, name[1:-4]))):
                    os.remove(os.path.join(d, name))  # temp files / orphaned checksums from an interrupted rename
                    continue
                if not name.isdigit():
                    continue
                p = os.path.join(d, name)
                try:
                    with open(p) as f:
                        lines = [ln for ln in f.read().splitlines() if ln.strip()]
                    ok = len(lines) >= min_lines and all(ln.startswith("v") or ln.startswith("{") for ln in lines[:1])
                    if ok:
                        for ln in lines[1:]:
                            json.loads(ln)
                except (ValueError, OSError):
                    ok = False
                if not ok:
                    print(f"[app] removing incomplete checkpoint file {p}", flush=True)
                    dirs = [d] + ([os.path.join(root, q, "commits")] if sub == "offsets" else [])
                    for dd in dirs:  # the entry, its checksum sibling, and the matching commit
                        for f in (name, f".{name}.crc"):
                            if os.path.exists(os.path.join(dd, f)):
                                os.remove(os.path.join(dd, f))


def main():
    repair_checkpoints()
    spark = (SparkSession.builder.appName("transit-flow-optimizer-streaming")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "4"))
             .config("spark.sql.streaming.stateStore.providerClass",
                     "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
             .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    net = load_network()
    hb = HBase(size=8)
    hb.ensure_tables()

    meta = {"s:mode": net["mode"], "s:city": net["city"], "s:started_wall": int(time.time() * 1000)}
    for name in ("provenance.json",):
        with open(os.path.join(DATA_DIR, name)) as f:
            meta["s:provenance"] = json.load(f)
    mpath = os.path.join(MODEL_DIR, "metrics.json")
    if os.path.exists(mpath):
        with open(mpath) as f:
            meta["s:model_metrics"] = json.load(f)
    hb.put("system_meta", "pipeline", meta)

    live = LiveState(net)
    predictor = CrowdingPredictor(spark, net)
    spark.streams.addListener(ProgressListener(hb))
    queries = []
    queries += ingest_vehicle_stream.start(spark, hb, live, net, predictor)
    queries += ingest_flow_stream.start(spark, hb, live, net, predictor)
    queries += incident_impact.start(spark, hb, live, net)
    Recommender(hb, live, net).start()
    print(f"[app] started {len(queries)} streaming queries: {[q.name for q in queries]}", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
