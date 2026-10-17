"""Real-time transit simulator -> Kafka.

Topics produced: vehicle.telemetry, passenger.taps, station.density, incidents
Topics consumed: incidents (MANUAL ones injected from the dashboard), control.commands (accepted recommendations)

Simulated time runs SIM_SPEED x faster than wall time. Timestamps are the city's local time encoded as epoch ms
(treated as UTC everywhere in the pipeline) so hour-of-day features line up with the ridership data.
"""
import calendar
import json
import os
import random
import signal
import time
from datetime import datetime, timedelta

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

import network
from demand import Demand
from incidents import Incidents
from vehicles import build_fleet, remove_from_smallest_gap, spawn_in_largest_gap

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SIM_SPEED = float(os.getenv("SIM_SPEED", "6"))
SIM_START = os.getenv("SIM_START", "07:00")           # "now" or HH:MM (city time)
SIM_DAY = os.getenv("SIM_DAY", "weekday")             # "weekday" -> next weekday if today is a weekend, "today"
TICK_REAL_S = float(os.getenv("TICK_REAL_S", "1.0"))
TELEMETRY_EVERY_TICKS = int(os.getenv("TELEMETRY_EVERY_TICKS", "2"))
DENSITY_EVERY_SIM_S = float(os.getenv("DENSITY_EVERY_SIM_S", "60"))
TAP_WEIGHT = int(os.getenv("TAP_WEIGHT", "10"))       # each tap event represents N passengers
FLEET_SCALE = float(os.getenv("FLEET_SCALE", "0.5"))  # share of the scheduled fleet that is simulated
INCIDENT_MULTIPLIER = float(os.getenv("INCIDENT_MULTIPLIER", "40"))
MAX_OPEN_INCIDENTS = int(os.getenv("MAX_OPEN_INCIDENTS", "3"))
STATE_DIR = os.getenv("STATE_DIR", "/state")
TOPICS = ["vehicle.telemetry", "passenger.taps", "station.density", "incidents", "control.commands"]

running = True


def log(*a):
    print("[sim]", *a, flush=True)


def to_ms(dt):
    return calendar.timegm(dt.timetuple()) * 1000 + dt.microsecond // 1000


def initial_clock():
    path = os.path.join(STATE_DIR, "clock.json")
    if os.path.exists(path):
        try:
            with open(path) as f:
                saved = datetime.fromisoformat(json.load(f)["sim_now"])
            log("resuming simulated clock at", saved)
            return saved
        except Exception:  # noqa
            pass
    try:
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo("America/New_York")).replace(tzinfo=None)
    except Exception:  # noqa
        today = datetime.utcnow() - timedelta(hours=4)
    if SIM_START == "now":
        return today.replace(microsecond=0)
    hh, mm = (int(x) for x in SIM_START.split(":"))
    start = today.replace(hour=hh, minute=mm, second=0, microsecond=0)
    while SIM_DAY == "weekday" and start.weekday() >= 5:
        start += timedelta(days=1)
    return start


def save_clock(now):
    os.makedirs(STATE_DIR, exist_ok=True)
    tmp = os.path.join(STATE_DIR, "clock.json.tmp")
    with open(tmp, "w") as f:
        json.dump({"sim_now": now.isoformat(), "speed": SIM_SPEED}, f)
    os.replace(tmp, os.path.join(STATE_DIR, "clock.json"))


def ensure_topics():
    admin = AdminClient({"bootstrap.servers": KAFKA})
    for attempt in range(60):
        try:
            existing = set(admin.list_topics(timeout=5).topics)
            missing = [NewTopic(t, num_partitions=3, replication_factor=1) for t in TOPICS if t not in existing]
            if missing:
                for t, fut in admin.create_topics(missing).items():
                    try:
                        fut.result()
                    except Exception as e:  # noqa - already exists
                        log("topic", t, e)
            return
        except Exception as e:  # noqa
            log("waiting for kafka...", e)
            time.sleep(3)
    raise SystemExit("kafka not reachable")


class World:
    """glue object the vehicles call into"""

    def __init__(self, demand, incidents, producer):
        self.demand, self.incidents, self.producer = demand, incidents, producer
        self.ts_ms = 0
        self.out_taps = 0

    def incident_factor(self, rid, a, b):
        return self.incidents.factor(rid, a, b)

    def boarding(self, v, sid):
        return self.demand.boarding(v, sid)

    def alighting(self, v, sid):
        return self.demand.alighting(v, sid)

    def on_alight(self, sid, n):
        for ev in self.demand.tap_outs(sid, n, self.ts_ms):
            send(self.producer, "passenger.taps", sid, ev)
            self.out_taps += 1


def send(producer, topic, key, value):
    payload = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
    while True:
        try:
            producer.produce(topic, key=str(key), value=payload)
            return
        except BufferError:
            producer.poll(0.2)


def handle_message(msg, fleet, routes, demand, incidents, producer, now, now_s, ts_ms, rnd):
    try:
        data = json.loads(msg.value())
    except Exception:  # noqa
        return
    if msg.topic() == "incidents":
        if data.get("source") == "MANUAL" and data.get("event") == "OPEN" and data.get("incident_id") not in incidents.open:
            if data.get("route_id") in routes:
                incidents.open_incident(data["route_id"], data["station_id"], data.get("type", "Other"),
                                        int(data.get("severity", 2)), int(data.get("expected_duration_min", 20)),
                                        now_s, ts_ms, "MANUAL", incident_id=data["incident_id"],
                                        description=data.get("description"))
                log("manual incident registered", data["incident_id"])
        return
    # control.commands
    action, rid = data.get("action"), data.get("route_id")
    r = routes.get(rid)
    if action == "ADD_FREQUENCY" and r:
        for _ in range(max(1, int(data.get("extra_vehicles", 1)))):
            v = spawn_in_largest_gap(fleet, r, rnd)
            v.load = 0
            log("dispatched", v.id)
    elif action == "REDUCE_FREQUENCY" and r:
        for _ in range(max(1, int(abs(data.get("extra_vehicles", 1))))):
            v = remove_from_smallest_gap(fleet, r)
            if v:
                log("withdrew", v.id)
    elif action == "HOLD" and data.get("vehicle_id") in fleet:
        fleet[data["vehicle_id"]].hold(float(data.get("hold_s", 60)))
        log("holding", data["vehicle_id"], data.get("hold_s"))
    elif action in ("SHORT_TURN", "BUS_BRIDGE") and r:
        incidents.mitigate(rid, now_s)
        log("mitigation", action, rid)
    elif action == "SURGE":
        sid = data.get("station_id")
        if sid in demand.st:
            demand.add_surge(sid, float(data.get("multiplier", 2.5)), now_s + 60 * float(data.get("minutes", 30)))
            log("demand surge", sid)


def main():
    signal.signal(signal.SIGTERM, lambda *_: globals().__setitem__("running", False))
    net, stations, routes, inc_stats = network.load()
    log(f"network: {net['mode']} {len(routes)} routes, {len(stations)} stations")
    ensure_topics()
    producer = Producer({"bootstrap.servers": KAFKA, "linger.ms": 50, "compression.type": "lz4",
                         "queue.buffering.max.messages": 200000})
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "simulator", "auto.offset.reset": "latest",
                         "enable.auto.commit": True})
    consumer.subscribe(["incidents", "control.commands"])
    rnd = random.Random(int(time.time()))
    fleet = build_fleet(routes, FLEET_SCALE)
    demand = Demand(stations, routes, TAP_WEIGHT, rnd)
    incidents = Incidents(inc_stats, routes, stations, INCIDENT_MULTIPLIER, MAX_OPEN_INCIDENTS, rnd)
    world = World(demand, incidents, producer)
    log(f"fleet: {len(fleet)} vehicles, speed {SIM_SPEED}x")

    now = initial_clock()
    dt = TICK_REAL_S * SIM_SPEED
    tick, next_density, sent, last_report = 0, 0.0, 0, time.time()
    # warm-up: fill platforms so the first minutes are not empty
    for _ in range(int(600 / dt)):
        demand.tick(now, 0 if now.weekday() < 5 else (1 if now.weekday() == 5 else 2), dt, to_ms(now))
    while running:
        started = time.time()
        now += timedelta(seconds=dt)
        ts_ms, now_s = to_ms(now), to_ms(now) / 1000
        world.ts_ms = ts_ms
        daytype = 0 if now.weekday() < 5 else (1 if now.weekday() == 5 else 2)

        for msg in consumer.consume(num_messages=200, timeout=0):
            if msg is None or msg.error():
                continue
            handle_message(msg, fleet, routes, demand, incidents, producer, now, now_s, ts_ms, rnd)

        for ev in demand.tick(now, daytype, dt, ts_ms):
            send(producer, "passenger.taps", ev.get("station_id", ev.get("stationId", "x")) if isinstance(ev, dict) else "bad", ev)
            sent += 1
        for v in list(fleet.values()):
            v.step(dt, world)
            if (tick + hash(v.id)) % TELEMETRY_EVERY_TICKS == 0:
                send(producer, "vehicle.telemetry", v.id, v.telemetry(ts_ms))
                sent += 1
        if now_s >= next_density:
            for ev in demand.density_events(ts_ms):
                send(producer, "station.density", ev["station_id"], ev)
                sent += 1
            demand.drift_noise()
            next_density = now_s + DENSITY_EVERY_SIM_S
        for ev in incidents.maybe_generate(now_s, dt, ts_ms) + incidents.clear_due(now_s, ts_ms):
            send(producer, "incidents", ev["incident_id"], ev)
            log(ev["event"], ev["incident_id"], ev["route_id"], ev["station_name"], "sev", ev["severity"])
        # random demand surges (concerts, games...) somewhere in the network ~ once per sim-hour
        if rnd.random() < dt / 3600:
            sid = rnd.choice(list(stations))
            demand.add_surge(sid, rnd.uniform(1.8, 3.0), now_s + rnd.randint(20, 50) * 60)
            log("random demand surge at", stations[sid]["name"])

        producer.poll(0)
        tick += 1
        if tick % 10 == 0:
            save_clock(now)
        if time.time() - last_report > 30:
            total_wait = sum(demand.waiting.values())
            log(f"sim {now:%a %H:%M} | {len(fleet)} vehicles | {sent + world.out_taps} msgs | waiting {int(total_wait)} | open incidents {len(incidents.open)}")
            last_report = time.time()
        time.sleep(max(0.0, TICK_REAL_S - (time.time() - started)))
    producer.flush(10)
    save_clock(now)


if __name__ == "__main__":
    main()
