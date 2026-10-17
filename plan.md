# Real-Time Public Transport & Passenger Flow Optimizer — Plan

**Domain:** City metro (subway) + bus system
**Goal:** Show time-series data, streaming and wide-column storage working together in one end-to-end system: data is ingested, processed with Apache Spark, stored in HBase, served by an API and shown on a live dashboard.

---

## 1. Data Sources — Real vs. Synthetic

The base city is **New York City (MTA)**, because it publishes open data for the network, ridership and incidents. Sensor-level feeds (GPS, occupancy, Wi-Fi) are not public at the granularity we need, so we **generate synthetic data from the real datasets**. That keeps the synthetic data realistic in both shape and volume.

| # | Data source (from idea) | Dataset used | Real / Synthetic | How it's used |
|---|---|---|---|---|
| 1 | Network topology (stations, routes, stop sequences, shapes, schedules) | **MTA NYC Subway GTFS static feed** (`http://web.mta.info/developers/data/nyct/subway/google_transit.zip`) | **Real** | Station coordinates, route lines, scheduled headways, vehicle paths on the map |
| 2 | Historical passenger demand | **MTA Subway Hourly Ridership: Beginning 2022** (data.ny.gov, Socrata id `wujg-7c2s`) | **Real** (subset: a few months, selected stations) | Hourly entries per station and payment method. Used to train the crowding model and to calibrate the synthetic tap rate |
| 3 | Incident & maintenance reports | **MTA Subway Major Incidents: Beginning 2020** (data.ny.gov, Socrata id `j6d2-s8m2`) | **Real** (historical stats) + **Synthetic** (live events) | Real incident frequencies and categories per line set the probabilities of the live synthetic incident generator |
| 4 | Vehicle GPS + occupancy sensors (stream) | Simulator moves trains/buses along GTFS shapes | **Synthetic** | 1 event per vehicle every 5 s: `lat, lon, speed, bearing, occupancy_pct, load_count, delay_s, door_status` |
| 5 | Passenger card / mobile ticket taps | Simulator, Poisson rate taken from dataset #2 (time-of-day × station profile) | **Synthetic** (calibrated on real data) | Semi-structured JSON with optional/nested fields (`card_type`, `fare`, `device{}`, `transfer_from`) |
| 6 | Station camera / Wi-Fi density | Simulator derived from tap inflow, dwell time and noise | **Synthetic** | `station_id, zone, wifi_devices, camera_count, confidence` every 10 s |

**Offline fallback:** if the downloads fail (network, or a changed URL/dataset ID), `data/bootstrap.py` generates a fully synthetic 6-line / ~60-station city with the same schemas, so the project still runs end-to-end. The README and the UI footer always show **which mode is active** (`REAL+SYNTH` or `FULL-SYNTH`).

> The dataset URLs and IDs above will be checked at implementation time. If one has moved, I'll update it here and in the README.

---

## 2. Architecture

```
 ┌──────────────────────┐      ┌──────────┐     ┌───────────────────────────────┐     ┌─────────┐
 │  Simulators (Python) │─────▶│  Kafka   │────▶│ Spark Structured Streaming    │────▶│  HBase  │
 │  - vehicle telemetry │      │ topics:  │     │  - parse / validate / enrich  │     │ (wide-  │
 │  - ticket taps       │      │ vehicle.*│     │  - event-time windows + water-│     │ column) │
 │  - wifi/camera dens. │      │ taps     │     │    marks (1m tumbling,        │     └────┬────┘
 │  - incidents         │      │ density  │     │    15m sliding)               │          │
 └──────────▲───────────┘      │ incidents│     │  - crowding prediction (MLlib)│          │
            │                  └──────────┘     │  - incident impact            │          │
  real GTFS + ridership                         │  - frequency recommendations  │          │
  + incident stats                              └──────────────▲────────────────┘          │
                                                               │                           │
                                    ┌──────────────────────────┴───┐                       │
                                    │ Spark batch (MLlib)          │                       │
                                    │ train crowding model on real │                       │
                                    │ hourly ridership + features  │                       │
                                    └──────────────────────────────┘                       │
                                                                                           ▼
                         ┌──────────────────────────────────────────┐        ┌──────────────────────┐
                         │ Frontend: React + Vite + TS              │◀──────▶│ Backend: FastAPI      │
                         │ Leaflet live map, Recharts, WebSocket    │  REST  │ happybase (Thrift)    │
                         └──────────────────────────────────────────┘  + WS  └──────────────────────┘
```

Everything runs with **Docker Compose**: Kafka (KRaft, no ZooKeeper), HBase (standalone with Thrift server), Spark 3.5 (master + 1 worker), simulator, backend and frontend.

### Tech stack

| Layer | Choice |
|---|---|
| Message bus | Apache Kafka 3.x (KRaft mode) |
| Stream + batch processing | **Apache Spark 3.5 (PySpark)**: Structured Streaming + MLlib |
| Storage | **Apache HBase 2.x** (wide-column), reached through the Thrift server |
| Backend API | Python 3.11, FastAPI, happybase, WebSocket push |
| Frontend | React 18 + TypeScript + Vite, Leaflet (OpenStreetMap tiles), Recharts, TailwindCSS |
| Orchestration | Docker Compose |

---

## 3. HBase Schema (wide-column design)

The row key is designed around how each table is read. Keys use a **salt prefix** to avoid region hotspots, and a **reversed timestamp** (`Long.MAX - ts`) so the newest data is scanned first.

| Table | Row key | Column families (versions / TTL) | Purpose |
|---|---|---|---|
| `vehicle_state` | `vehicle_id` | `loc` (lat, lon, bearing, speed) · `occ` (pct, count, level) · `ops` (route, trip, next_stop, delay_s, status) — **VERSIONS=20** | **Current vehicle state**. One row per vehicle, overwritten in place. Older versions give a short GPS trail without extra tables |
| `vehicle_ts` | `salt(2)#route#vehicle_id#revTs` | `d` (all metrics) — TTL 7d | Full telemetry time series, for range scans per route/vehicle |
| `station_flow` | `station_id#window_size#revWindowStart` | `tap` (in, out, by_card_type:*) · `den` (wifi, camera, fused) · `pred` (crowd_now, crowd_15m, crowd_30m, level) — TTL 30d | **Passenger flow windows** (1 m / 15 m). Columns grow dynamically (e.g. `tap:card_OMNY`, `tap:card_MetroCard`) |
| `route_metrics` | `route_id#revWindowStart` | `m` (avg_load, max_load, avg_delay, headway_actual, headway_sched, bunching_idx) | Route-level KPIs per window |
| `incidents` | `salt#incident_id` | `info` (type, line, station, severity, start, end, status) · `impact` (delay_min, affected_pax, affected_stations:*) | Incident log + computed impact |
| `recommendations` | `route_id#revTs` | `rec` (action, current_headway, suggested_headway, extra_vehicles, reason, confidence) | Dynamic frequency/route recommendations |

`hbase/create_tables.hbase` contains the DDL. The README explains each row-key choice. This design is the main HBase part of the project.

---

## 4. Spark Jobs

| Job | Type | What it does |
|---|---|---|
| `ingest_vehicle_stream.py` | Structured Streaming | Kafka `vehicle.telemetry` → schema validation → upsert `vehicle_state` + append `vehicle_ts`; 1-min windows per route → `route_metrics` (avg load, delay, **headway & bunching** from the gaps between vehicles) |
| `ingest_flow_stream.py` | Structured Streaming | Kafka `taps` (semi-structured JSON, `from_json` with permissive mode + `_corrupt_record`) and `density` → **stream–stream join** on station + event-time window (watermark 2 min) → 1 m & 15 m windows → `station_flow` |
| `crowding_predictor.py` | Streaming + MLlib model | Loads the trained model and scores each station/vehicle window to give `crowd_now`, `crowd_15m`, `crowd_30m` and a level (LOW / MED / HIGH / CRITICAL) |
| `incident_impact.py` | Structured Streaming | Kafka `incidents` + live route state → finds downstream stations and vehicles on the affected line → estimated delay and affected passengers → `incidents.impact` |
| `recommender.py` | Micro-batch (every 60 s) | Rule + optimisation engine: `required_headway = capacity × target_load / predicted_demand` (clamped to fleet limits). Outputs ADD/REDUCE frequency, SHORT-TURN, HOLD (anti-bunching) and REROUTE/BUS-BRIDGE on incidents → `recommendations` |
| `train_crowding_model.py` | Batch (MLlib) | Real hourly ridership (#2) + features (hour, weekday, station, lag-1h/lag-24h, holiday flag, incident flag) → **GBTRegressor** pipeline; reports RMSE/MAE; saves the model to `/models` |

Every streaming job uses event time, watermarks and checkpointing, and is restart-safe. Writes to HBase go through `foreachBatch` with batched Thrift puts.

---

## 5. Backend (FastAPI)

REST endpoints:
- `GET /api/vehicles` — current state of all vehicles (`vehicle_state`)
- `GET /api/vehicles/{id}/history?minutes=30` — time series (`vehicle_ts`)
- `GET /api/stations` · `GET /api/stations/{id}/flow?window=1m|15m&hours=3` — flow windows + predictions
- `GET /api/routes` · `GET /api/routes/{id}/metrics`
- `GET /api/recommendations?active=true`
- `GET /api/incidents?status=open` · `GET /api/incidents/{id}/impact`
- `POST /api/incidents` — inject a manual incident (demo button). Publishes to Kafka, so it flows through the Spark pipeline
- `GET /api/system/health` — status of Kafka, HBase and the Spark streaming queries, plus the data mode (REAL+SYNTH / FULL-SYNTH)
- `WS /ws/live` — pushes vehicle positions and alerts every 2 s

---

## 6. Frontend (React dashboard)

1. **Live Map** — Leaflet map with GTFS route lines, vehicles coloured by occupancy, station circles sized/coloured by predicted crowding, and a click popup with details and a sparkline.
2. **Crowding Forecast** — per station/vehicle: chart of actual vs. predicted load (now / +15 / +30 min) and a heatmap of station × hour.
3. **Recommendations** — live table/cards (e.g. "Line 4: reduce headway 6→4 min, +2 trains, reason: predicted 92 % load at Grand Central in 15 min") with an accept/dismiss UI.
4. **Incidents & Impact** — incident list, impacted stations highlighted on the map, delay propagation chart, and an **"Inject incident"** button for live demos.
5. **System / Pipeline** — Kafka throughput, Spark batch durations, HBase row counts, an HBase schema explainer, and the data-source provenance (real vs. synthetic).

---

## 7. Project Structure

```
bigdataproject/
├── plan.md
├── README.md                    # setup, datasets used, architecture, schema rationale
├── docker-compose.yml
├── .env.example
├── data/
│   ├── bootstrap.py             # download real datasets or generate a fully synthetic city
│   ├── raw/  processed/         # (gitignored)
├── simulator/                   # synthetic stream generators → Kafka
│   ├── vehicles.py  taps.py  density.py  incidents.py  main.py
├── spark/
│   ├── jobs/                    # streaming + batch jobs (section 4)
│   ├── common/                  # schemas, hbase writer, config
│   └── Dockerfile
├── hbase/
│   └── create_tables.hbase
├── backend/
│   ├── app/ (main.py, routers/, hbase_client.py, kafka_client.py, models.py)
│   ├── tests/
│   └── Dockerfile
└── frontend/
    ├── src/ (pages/, components/, api/, hooks/)
    └── Dockerfile
```

---

## 8. Implementation Phases

| Phase | Deliverable |
|---|---|
| **1. Infra** | `docker-compose.yml` with Kafka, HBase+Thrift, Spark master/worker; table-creation script; health checks |
| **2. Data** | `bootstrap.py`: download GTFS + ridership subset + incidents, clean them into Parquet, fall back to fully synthetic |
| **3. Simulators** | Four generators publishing to Kafka, configurable speed (1×–60× time compression) |
| **4. Spark streaming** | Ingest + windowing jobs writing to HBase |
| **5. ML** | Train the GBT crowding model, plug it into the streaming predictor, report metrics |
| **6. Analytics** | Incident impact + recommendation engine |
| **7. Backend** | FastAPI REST + WebSocket, unit tests with a mocked HBase |
| **8. Frontend** | The five dashboard pages |
| **9. Polish** | README (datasets, schema rationale, screenshots), `make`/PowerShell one-command start, demo script |

**Run target:** `docker compose up -d` → open `http://localhost:5173`. Docker Desktop needs **at least 8 GB RAM** allocated (HBase + Spark + Kafka).

---

## 9. Feature → Big-Data Concept Map

| Feature | Time-series | Streaming | Wide-column |
|---|---|---|---|
| Live crowding prediction | windowed load history, lag features | Spark Structured Streaming + MLlib scoring | `station_flow.pred`, `vehicle_state.occ` |
| Dynamic frequency recommendations | headway/bunching per window | micro-batch optimiser | `recommendations`, `route_metrics` |
| Delay & incident impact | delay propagation over time | Kafka incident stream joined with live state | `incidents.impact:*` dynamic columns |
| Current vehicle state + flow windows | VERSIONS-based trail, reversed-ts keys | continuous upserts | salted keys, column families, TTLs |

---

## 10. Assumptions / Open Choices

- **City:** NYC MTA subway (real open data). Bus GTFS can be added later. An alternative city is possible if you prefer one.
- **Real data volume:** a subset (≈3 months of hourly ridership, ~100 busiest stations) keeps the laptop load reasonable.
- **Simulated fleet:** ~150 vehicles at a default 10× time compression.
- **No auth** on the dashboard (demo project).
