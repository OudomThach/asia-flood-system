"""
Unit & integration tests for the REAL live multi-hazard layer.

The platform pulls observed hazard events from USGS, NASA EONET, GDACS, and NASA FIRMS
and normalizes them into a single HazardEvent stream. These tests exercise the
normalization, de-duplication, GeoJSON/feed shaping, the Asia geo-filter, and the
FastAPI endpoints — all WITHOUT hitting the network (the cache is injected directly),
so they are deterministic and offline-safe.
"""

import time
import asyncio
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from asia_flood_core.models import HazardEvent
from asia_flood_core.live_hazards import LiveHazardIntake, _severity_from_magnitude
from asia_flood_core import admin_server
from asia_flood_core.admin_server import app


def _evt(event_type="earthquake", lat=12.0, lon=105.0, severity="orange",
         mag=5.2, source="USGS", eid=None):
    return HazardEvent(
        event_id=eid or f"{source}-{event_type}-{lat}-{lon}",
        event_type=event_type,
        title=f"Test {event_type} @ {lat},{lon}",
        severity=severity,
        latitude=lat, longitude=lon,
        magnitude=mag, value_label=(f"M{mag}" if mag else None),
        observed_at=datetime.now(timezone.utc),
        source=source,
        url="https://example.org/e",
    )


def _inject(events):
    """Force the shared live-hazard service to serve `events` from cache (no network)."""
    admin_server.live_hazards._cache = events
    admin_server.live_hazards._cache_ts = time.time()


@pytest.fixture
def client():
    return TestClient(app)


# ---- model & normalization ---------------------------------------------------

def test_hazard_event_is_observed_by_default():
    e = _evt()
    assert e.observed is True
    assert e.event_type == "earthquake"


def test_severity_normalization_bands():
    assert _severity_from_magnitude("earthquake", 6.4) == "red"
    assert _severity_from_magnitude("earthquake", 5.1) == "orange"
    assert _severity_from_magnitude("earthquake", 4.2) == "green"
    assert _severity_from_magnitude("cyclone", 200.0) == "red"
    assert _severity_from_magnitude("earthquake", None) == "info"


def test_asia_geofilter_excludes_africa_and_oceania():
    svc = LiveHazardIntake()
    assert svc._is_asia(12.5, 105.0) is True          # Cambodia
    assert svc._is_asia(35.0, 139.0) is True           # Japan
    assert svc._is_asia(9.0, 40.0) is False            # Ethiopia (Horn of Africa)
    assert svc._is_asia(-6.0, 147.0) is False          # Papua New Guinea
    assert svc._is_asia(90.0, 200.0) is False          # outside bbox


def test_dedupe_collapses_same_event_keeps_most_severe():
    svc = LiveHazardIntake()
    a = _evt(severity="green", source="EONET", eid="a")
    b = _evt(severity="red", source="GDACS", eid="b")   # same type + ~same location
    out = svc._dedupe([a, b])
    assert len(out) == 1
    assert out[0].severity == "red"


# ---- output shapes -----------------------------------------------------------

def test_to_geojson_is_valid_featurecollection():
    svc = LiveHazardIntake()
    svc._cache = [_evt(event_type="flood", severity="red")]
    svc._cache_ts = time.time()
    gj = asyncio.run(svc.to_geojson())
    assert gj["type"] == "FeatureCollection"
    f = gj["features"][0]
    assert f["geometry"]["type"] == "Point"
    assert f["properties"]["event_type"] == "flood"
    assert f["properties"]["observed"] is True
    assert "color" in f["properties"]


def test_to_feed_counts_by_type_and_source():
    svc = LiveHazardIntake()
    svc._cache = [
        _evt(event_type="earthquake", source="USGS"),
        _evt(event_type="cyclone", lat=15.0, lon=120.0, source="NASA EONET"),
    ]
    svc._cache_ts = time.time()
    feed = asyncio.run(svc.to_feed())
    assert feed["count"] == 2
    assert feed["by_type"]["earthquake"] == 1
    assert feed["by_source"]["USGS"] == 1


# ---- endpoints (offline via injected cache) ----------------------------------

def test_endpoint_live_geojson(client):
    _inject([_evt(event_type="volcano", severity="orange")])
    r = client.get("/api/hazards/live.geojson")
    assert r.status_code == 200
    body = r.json()
    assert body["type"] == "FeatureCollection"
    assert body["features"][0]["properties"]["source"] == "USGS"


def test_endpoint_feed(client):
    _inject([_evt(), _evt(event_type="wildfire", lat=20.0, lon=100.0, source="NASA FIRMS")])
    r = client.get("/api/hazards/feed?limit=10")
    assert r.status_code == 200
    assert r.json()["count"] == 2


def test_endpoint_earthquakes_filters_type_and_magnitude(client):
    _inject([
        _evt(event_type="earthquake", mag=5.5),
        _evt(event_type="cyclone", lat=15.0, lon=120.0, mag=150.0, source="NASA EONET"),
        _evt(event_type="earthquake", lat=13.0, lon=106.0, mag=3.0, eid="small"),
    ])
    r = client.get("/api/hazards/earthquakes?min_magnitude=4.0")
    assert r.status_code == 200
    quakes = r.json()["earthquakes"]
    assert all(q["event_type"] == "earthquake" and q["magnitude"] >= 4.0 for q in quakes)
    assert len(quakes) == 1


def test_endpoint_cascading_matrix_reports_flood_pressure(client):
    # kratie-central is ~12.49N, 106.02E — a red flood on top of it should drive high pressure.
    _inject([_evt(event_type="flood", lat=12.5, lon=106.0, severity="red")])
    r = client.get("/api/hazards/cascading-matrix?area_id=kratie-central")
    assert r.status_code == 200
    data = r.json()
    assert data["flood_pressure"] > 0.0
    assert data["pressure_band"] in ("LOW", "ELEVATED", "HIGH")
    assert data["contributing_count"] >= 1
    assert data["reasons"][0]["flood_role"] == "PRIMARY_DRIVER"
    assert "mechanism" in data["reasons"][0]


def test_flood_role_classification_and_pressure():
    from asia_flood_core.flood_linkage import classify_flood_role, station_flood_pressure
    # desert earthquake far from any dam -> not flood-relevant, zero contribution
    desert_q = _evt(event_type="earthquake", lat=40.0, lon=60.0, severity="red", mag=6.5)
    role, _ = classify_flood_role(desert_q)
    assert role == "NOT_FLOOD_RELEVANT"
    # cyclone next to a station -> primary driver, real pressure
    cyc = _evt(event_type="cyclone", lat=12.6, lon=106.1, severity="red", mag=185)
    cyc.flood_role, cyc.mechanism = classify_flood_role(cyc)
    p = station_flood_pressure(12.49, 106.02, [cyc])
    assert p["flood_pressure"] > 0.5
    assert p["pressure_band"] in ("ELEVATED", "HIGH")
