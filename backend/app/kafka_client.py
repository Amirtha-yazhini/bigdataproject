"""Kafka producer (manual incidents, accepted recommendations) + topic offset stats for the system page."""
import json
import os
import threading
import time

from confluent_kafka import Consumer, Producer, TopicPartition

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPICS = ["vehicle.telemetry", "passenger.taps", "station.density", "incidents", "control.commands"]


class KafkaClient:
    def __init__(self):
        self._producer = None
        self._consumer = None
        self._lock = threading.Lock()
        self._last = {}  # topic -> (wall_s, total)

    def producer(self):
        with self._lock:
            if self._producer is None:
                self._producer = Producer({"bootstrap.servers": KAFKA, "linger.ms": 5})
            return self._producer

    def send(self, topic, key, value):
        p = self.producer()
        p.produce(topic, key=str(key), value=json.dumps(value))
        p.flush(5)

    def topic_stats(self):
        with self._lock:
            if self._consumer is None:
                self._consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "dashboard-stats",
                                           "enable.auto.commit": False})
            c = self._consumer
            md = c.list_topics(timeout=5)
            out = []
            now = time.time()
            for t in TOPICS:
                if t not in md.topics:
                    out.append({"topic": t, "partitions": 0, "messages": 0, "rate_per_s": 0})
                    continue
                total = 0
                parts = md.topics[t].partitions
                for pid in parts:
                    lo, hi = c.get_watermark_offsets(TopicPartition(t, pid), timeout=5)
                    total += max(0, hi - lo)
                prev = self._last.get(t)
                rate = (total - prev[1]) / (now - prev[0]) if prev and now > prev[0] and total >= prev[1] else None
                self._last[t] = (now, total)
                out.append({"topic": t, "partitions": len(parts), "messages": total,
                            "rate_per_s": round(rate, 1) if rate is not None else None})
            return out


kafka = KafkaClient()
