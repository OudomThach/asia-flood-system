# Architecture & Design

Pan-Asian Multi-Hazard & Flash-Flood Early Warning Platform
CSCI 841 — Advanced Software Engineering, Fort Hays State University · Author: Oudom Thach

This document describes the system architecture, data flow, domain model, and the
flash-flood contribution model. All diagrams are [Mermaid](https://mermaid.js.org/) and
render directly on GitHub.

---

## 1. System Architecture (containers & components)

```mermaid
flowchart TB
    subgraph EXT["External Data Sources (real, live)"]
        GLOFAS["Copernicus GloFAS<br/>+ Open-Meteo<br/>(discharge, rain, soil)"]
        USGS["USGS FDSN<br/>(earthquakes)"]
        EONET["NASA EONET v3<br/>(storms, volcanoes,<br/>fires, floods, drought)"]
        GDACS["GDACS RSS<br/>(severity-graded alerts)"]
        FIRMS["NASA FIRMS<br/>(active fires)"]
    end

    subgraph INTAKE["Ingestion Layer"]
        OM["OpenMeteoIntakeService<br/>data_intake.py"]
        LH["LiveHazardIntake<br/>live_hazards.py"]
    end

    subgraph CORE["Domain / Engines"]
        RISK["RiskClassificationEngine<br/>+ CompoundRiskEngine<br/>+ BASIN_PROFILES<br/>risk_engine.py"]
        FL["Flood-Flood Contribution Model<br/>flood_linkage.py"]
        PIPE["FloodAlertPipeline<br/>integration.py"]
    end

    subgraph DATA["Persistence"]
        REPO["FloodDataRepository<br/>storage.py"]
        DB[("SQLite WAL<br/>asia_flood.db")]
        CACHE["In-memory hot cache<br/>+ TTL caches"]
    end

    subgraph DELIVERY["Delivery"]
        API["FastAPI app<br/>admin_server.py<br/>REST + GeoJSON + CAP"]
        UI["Command Dashboard<br/>(HTML/Leaflet)"]
        CAP["CAPProtocolEngine<br/>cap_protocol.py"]
        NOTIF["Notifications<br/>notifications.py"]
        TG["InteractiveTelegramBot<br/>telegram_bot.py"]
    end

    GLOFAS --> OM
    USGS & EONET & GDACS & FIRMS --> LH
    OM --> PIPE --> RISK
    OM --> REPO
    LH --> FL
    RISK --> REPO
    REPO <--> DB
    REPO --- CACHE
    FL --> API
    REPO --> API
    RISK --> API
    API --> UI
    API --> CAP
    API --> NOTIF
    REPO --> TG
    FL --> TG
    LH --> TG
```

---

## 2. Monitoring Cycle (sequence)

Every station is polled on a schedule; readings are classified per basin and persisted.

```mermaid
sequenceDiagram
    autonumber
    participant SCH as Background Scheduler
    participant PIPE as FloodAlertPipeline
    participant OM as OpenMeteoIntakeService
    participant RISK as RiskClassificationEngine
    participant REPO as FloodDataRepository
    participant DB as SQLite

    SCH->>PIPE: run_cycle(area_id)
    PIPE->>REPO: get_area(area_id)
    PIPE->>OM: fetch_live_data(area)
    OM-->>PIPE: Reading (discharge, rain)
    PIPE->>REPO: save_reading(reading)
    PIPE->>RISK: evaluate(reading, area)
    RISK->>RISK: look up BASIN_PROFILES[area.basin_category]
    RISK-->>PIPE: RiskState (Normal/Caution/Danger)
    PIPE->>REPO: save_risk_state(state)
    PIPE->>REPO: log_audit_event(...)
```

---

## 3. Live Hazard Pipeline & Flash-Flood Contribution Model

Real hazards are fused, de-duplicated, then judged by **how they contribute to a flash flood** —
never overriding the hydrological risk level, only annotating a transparent "flood pressure".

```mermaid
flowchart LR
    subgraph SRC["4 real feeds"]
        A["USGS"]:::s
        B["NASA EONET"]:::s
        C["GDACS"]:::s
        D["NASA FIRMS"]:::s
    end
    A & B & C & D --> N["Normalize to HazardEvent<br/>(type, severity, lat/lon, source)"]
    N --> G["Asia geo-filter<br/>(drop Africa/Oceania corners)"]
    G --> DD["De-duplicate<br/>(type + 0.25° grid)"]
    DD --> CL{"classify_flood_role()"}
    CL -->|cyclone, flood, GLOF| P["PRIMARY_DRIVER"]
    CL -->|quake near dam,<br/>landslide, volcano+rain| CT["COMPOUNDING_TRIGGER"]
    CL -->|wildfire, drought| AA["ANTECEDENT_AMPLIFIER"]
    CL -->|desert quake, dust| NR["NOT_FLOOD_RELEVANT"]
    P & CT & AA --> SC["contribution_score()<br/>role x severity x proximity"]
    SC --> SP["station_flood_pressure()<br/>saturating combine -> 0..1 + reasons"]
    SP --> OUT["/api/hazards/cascading-matrix<br/>map overlay + live feed"]

    classDef s fill:#1e293b,stroke:#38bdf8,color:#e2e8f0;
```

---

## 4. Domain Model (key classes)

```mermaid
classDiagram
    class Area {
        +str area_id
        +str name_en
        +str country
        +str basin_category
        +float latitude
        +float longitude
        +str timezone_name
    }
    class Reading {
        +float discharge
        +float precipitation
        +datetime observed_at
        +bool is_stale
    }
    class RiskState {
        +str level
        +str reason
        +str rule_version
    }
    class BasinThresholdProfile {
        +float danger_discharge_m3s
        +float caution_discharge_m3s
        +float severe_rain_mm
        +float historical_benchmark_m3s
    }
    class HazardEvent {
        +str event_type
        +str severity
        +float latitude
        +float longitude
        +str source
        +str flood_role
        +str mechanism
    }
    class RiskClassificationEngine {
        +evaluate(reading, area) RiskState
    }
    class CompoundRiskEngine {
        +evaluate_compound_risk(...) CompoundRiskAssessment
    }
    class LiveHazardIntake {
        +get_live_hazards(days) List~HazardEvent~
        +to_geojson()
        +to_feed()
    }
    class FloodDataRepository {
        +get_all_areas(country) List~Area~
        +save_reading(r)
        +save_risk_state(s)
    }
    class FloodAlertPipeline {
        +run_cycle(area_id) dict
    }

    Area "1" --> "*" Reading : has
    Reading "1" --> "1" RiskState : classified into
    RiskClassificationEngine ..> BasinThresholdProfile : looks up
    RiskClassificationEngine ..> Reading : consumes
    RiskClassificationEngine ..> RiskState : produces
    LiveHazardIntake ..> HazardEvent : produces
    FloodAlertPipeline ..> RiskClassificationEngine : uses
    FloodAlertPipeline ..> FloodDataRepository : persists via
    FloodDataRepository ..> Area
    FloodDataRepository ..> Reading
    FloodDataRepository ..> RiskState
```

---

## 5. Persistence Schema (SQLite)

```mermaid
erDiagram
    AREAS ||--o{ READINGS : has
    AREAS ||--o{ RISK_STATES : has
    AREAS ||--o{ SUBSCRIPTIONS : has
    AREAS ||--o{ SOIL_MOISTURE_READINGS : has
    AREAS ||--o{ USER_ALERT_LOGS : targets

    AREAS {
        text area_id PK
        text name_en
        text country
        text basin_category
        real latitude
        real longitude
        real utc_offset_hours
        text timezone_name
    }
    READINGS {
        text reading_id PK
        text area_id FK
        real discharge
        real precipitation
        text fetched_at
        int is_stale
    }
    RISK_STATES {
        text risk_id PK
        text area_id FK
        text level
        text reason
        text calculated_at
    }
    SUBSCRIPTIONS {
        text chat_id PK
        text area_id PK
    }
    AUDIT_EVENTS {
        text event_id PK
        text event_type
        text timestamp_utc
    }
    USER_ALERT_LOGS {
        text alert_id PK
        text recipient_id
        text channel
        text alert_level
    }
    SOIL_MOISTURE_READINGS {
        text area_id PK
        real saturation_percent
        real runoff_coefficient
    }
```

---

## 6. Design Principles

- **Per-basin correctness** — one global threshold cannot serve both a small urban river and the
  Yangtze, so risk is evaluated against `BASIN_PROFILES` keyed on each station's `basin_category`.
- **Real data, honestly labeled** — the hazard layer carries only observed detections; every event
  shows its source (USGS / EONET / GDACS / FIRMS).
- **No false panic** — indirect hazards raise a transparent *flood pressure* with reasons but never
  auto-escalate the discharge/rain-driven Normal/Caution/Danger level.
- **Fail soft** — external feeds are TTL-cached and isolated; one outage never breaks the page.
- **Auditable** — every evaluation and alert dispatch is logged with UTC + local timestamps.
```
