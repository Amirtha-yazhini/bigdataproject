"""API tests with HBase/Kafka replaced by in-memory fakes (no containers needed)."""
import json
import os

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data", "processed"))

from app import main, services  # noqa: E402
from app.hbase_client import decode_value, decode_row, rev_ts  # noqa: E402


class FakeHB:
    def __init__(self):
        self.rows = {"recommendations": {"6#8000000000000": {"status": "NEW", "action": "ADD_FREQUENCY", "route_id": "6",
                                                            "extra_vehicles": 2, "_key": "6#8000000000000"}},
                     "system_meta": {"clock": {"sim_ts": 1792400000000}}}
        self.puts = []

    def row(self, table, key, flat=True):
        return self.rows.get(table, {}).get(key)

    def put(self, table, key, data):
        self.puts.append((table, key, data))
        self.rows.setdefault(table, {}).setdefault(key, {}).update({k.split(":")[1]: v for k, v in data.items()})

    def scan(self, *a, **k):
        return []


class FakeKafka:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))


@pytest.fixture()
def client(monkeypatch):
    hb, kf = FakeHB(), FakeKafka()
    monkeypatch.setattr(main, "hb", hb)
    monkeypatch.setattr(services, "hb", hb)
    monkeypatch.setattr(main, "kafka", kf)
    services.invalidate("recommendations", "live", "incidents")
    c = TestClient(main.app)
    c.hb, c.kafka = hb, kf
    return c


def test_decoding_keeps_ids_as_strings():
    assert decode_value(b"ops:route_id", b"1") == "1"
    assert decode_value(b"occ:pct", b"42.5") == 42.5
    assert decode_value(b"ops:ts", b"1792400000000") == 1792400000000
    assert decode_value(b"tap:card_OMNY", (37).to_bytes(8, "big")) == 37
    assert decode_row(b"k", {b"a:x": b"[1,2]"})["x"] == [1, 2]


def test_reversed_timestamp_sorts_newest_first():
    assert rev_ts(2000) < rev_ts(1000)
    assert len(rev_ts(1792400000000)) == 13


def test_network_endpoint(client):
    data = client.get("/api/network").json()
    assert data["routes"] and data["stations"]
    assert {"route_id", "shape", "color"} <= set(data["routes"][0])


def test_accept_recommendation_publishes_command(client):
    r = client.post("/api/recommendations/6%238000000000000/accept")
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "ACCEPTED"
    topic, key, cmd = client.kafka.sent[0]
    assert topic == "control.commands" and cmd["action"] == "ADD_FREQUENCY" and cmd["extra_vehicles"] == 2
    assert client.post("/api/recommendations/6%238000000000000/dismiss").status_code == 409


def test_manual_incident_validation(client):
    net = services.network()
    rt = net["routes"][0]
    ok = client.post("/api/incidents", json={"route_id": rt["route_id"], "station_id": rt["stops"][3], "severity": 3})
    assert ok.status_code == 201
    ev = client.kafka.sent[-1][2]
    assert ev["source"] == "MANUAL" and ev["ts"] == 1792400000000
    bad = client.post("/api/incidents", json={"route_id": rt["route_id"], "station_id": "nope"})
    assert bad.status_code == 400
