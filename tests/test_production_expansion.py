"""
Automated Test Suite for National & Production Scale Early Warning Expansion.
Validates:
- OASIS Common Alerting Protocol (CAP v1.2) XML & JSON Compliance
- Copernicus Soil Moisture Saturation & Infiltration Index
- Compound Risk Index (CRI v2.0) Multi-Factor Physics
- Historical Mega-Flood Benchmarking (2000, 2011, 2020)
- Mekong River Commission (MRC) Stage-Discharge Telemetry
- Transboundary Hydrodynamic Surge Kinematic Routing
- Cambodia EWS 1294 Voice IVR Audio Broadcast Generation
- RFC 7946 GeoJSON GIS FeatureCollection Export
- Prometheus /metrics and Container Health Probes (/health/live, /health/ready)
"""

import unittest
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from starlette.testclient import TestClient

from asia_flood_core.models import (
    Area, Reading, RiskState, SoilMoistureReading,
    MRCStationReading, CompoundRiskAssessment, HistoricalFloodBenchmark
)
from asia_flood_core.data_intake import OpenMeteoIntakeService
from asia_flood_core.risk_engine import RiskClassificationEngine, CompoundRiskEngine
from asia_flood_core.cap_protocol import CAPProtocolEngine
from asia_flood_core.notifications import generate_ews1294_voice_call_script, dispatch_all
from asia_flood_core.storage import FloodDataRepository
from asia_flood_core.admin_server import app


class TestProductionExpansion(unittest.TestCase):

    def setUp(self):
        self.repo = FloodDataRepository(db_path=":memory:")
        self.intake = OpenMeteoIntakeService()
        self.engine = RiskClassificationEngine()
        self.area_kratie = Area(
            area_id="kratie-central",
            name_en="Kratie (Central Station)",
            name_km="ក្រុងក្រចេះ (ស្ថានីយ៍កណ្ដាល)",
            latitude=12.4888,
            longitude=106.0188
        )
        self.client = TestClient(app)

    def tearDown(self):
        self.repo.close()

    def test_cap_protocol_xml_generation(self):
        """Validates OASIS CAP v1.2 compliant XML formatting and bilingual info blocks."""
        reading = Reading(
            reading_id="test-cap-1",
            area_id="kratie-central",
            observed_at=datetime.now(timezone.utc),
            discharge=23500.0,
            precipitation=85.0,
            source="Test Fixture",
            fetched_at=datetime.now(timezone.utc)
        )
        risk = self.engine.evaluate(reading)
        self.assertEqual(risk.level, "Danger")

        cap_alert = CAPProtocolEngine.create_cap_alert(self.area_kratie, risk, reading)
        self.assertEqual(cap_alert.severity, "Extreme")
        self.assertEqual(cap_alert.urgency, "Immediate")

        # Verify XML serialization & parsing
        xml_output = CAPProtocolEngine.to_xml(cap_alert)
        self.assertIn("urn:oasis:names:tc:emergency:cap:1.2", xml_output)
        self.assertIn("en-US", xml_output)
        self.assertIn("km-KH", xml_output)
        self.assertIn("Kratie", xml_output)

        # XML parsing validation
        root = ET.fromstring(xml_output.encode("utf-8"))
        self.assertEqual(root.tag, "{urn:oasis:names:tc:emergency:cap:1.2}alert")
        status_elem = root.find("{urn:oasis:names:tc:emergency:cap:1.2}status")
        self.assertIsNotNone(status_elem)
        self.assertEqual(status_elem.text, "Actual")

    def test_cap_protocol_json_serialization(self):
        """Validates OASIS CAP v1.2 JSON representation."""
        reading = Reading(
            reading_id="test-cap-2",
            area_id="kratie-central",
            observed_at=datetime.now(timezone.utc),
            discharge=14000.0,
            precipitation=15.0,
            source="Test Fixture",
            fetched_at=datetime.now(timezone.utc)
        )
        risk = self.engine.evaluate(reading)
        cap_alert = CAPProtocolEngine.create_cap_alert(self.area_kratie, risk, reading)
        json_output = CAPProtocolEngine.to_json(cap_alert)

        self.assertEqual(json_output["cap_version"], "1.2")
        self.assertEqual(len(json_output["info"]), 2)
        languages = [i["language"] for i in json_output["info"]]
        self.assertIn("en-US", languages)
        self.assertIn("km-KH", languages)

    def test_compound_risk_engine_physics(self):
        """Validates multi-factor compound risk index calculation across all threat tiers."""
        # 1. Extreme Flood & Saturated Ground
        extreme_reading = Reading(
            reading_id="cra-test-1",
            area_id="kratie-central",
            observed_at=datetime.now(timezone.utc),
            discharge=24000.0,
            precipitation=80.0,
            source="Test Fixture"
        )
        saturated_soil = SoilMoistureReading(
            area_id="kratie-central",
            observed_at=datetime.now(timezone.utc),
            moisture_surface_m3m3=0.45,
            moisture_rootzone_m3m3=0.48,
            saturation_percent=92.0,
            runoff_coefficient=0.84
        )

        assessment = CompoundRiskEngine.evaluate_compound_risk(
            reading=extreme_reading,
            soil=saturated_soil,
            surge_momentum=0.85,
            upstream_station="laos-pakse",
            estimated_lead_time_hours=24.0
        )
        self.assertEqual(assessment.level, "Danger")
        self.assertGreaterEqual(assessment.compound_score, 0.70)
        self.assertEqual(assessment.upstream_trigger_station, "laos-pakse")
        self.assertEqual(assessment.estimated_lead_time_hours, 24.0)

        # 2. Baseline Dry Season Flow
        dry_reading = Reading(
            reading_id="cra-test-2",
            area_id="kratie-central",
            observed_at=datetime.now(timezone.utc),
            discharge=6500.0,
            precipitation=0.0,
            source="Test Fixture"
        )
        dry_soil = SoilMoistureReading(
            area_id="kratie-central",
            observed_at=datetime.now(timezone.utc),
            saturation_percent=35.0,
            runoff_coefficient=0.25
        )
        dry_assessment = CompoundRiskEngine.evaluate_compound_risk(
            reading=dry_reading,
            soil=dry_soil,
            surge_momentum=0.0
        )
        self.assertEqual(dry_assessment.level, "Normal")
        self.assertLess(dry_assessment.compound_score, 0.28)

    def test_historical_flood_benchmark(self):
        """Validates benchmarking against 2000, 2011, and 2020 Mekong flood crests."""
        # Test 2011-scale flood surge (46,000 m3/s)
        benchmark = CompoundRiskEngine.evaluate_historical_benchmark("kratie-central", 46000.0)
        self.assertEqual(benchmark.year_2000_peak_discharge, 52000.0)
        self.assertAlmostEqual(benchmark.percent_of_2000_peak, 88.5, places=1)
        self.assertAlmostEqual(benchmark.percent_of_2011_peak, 94.8, places=1)
        self.assertIn("2011", benchmark.historical_context)

    def test_mrc_gauge_stage_calculation(self):
        """Validates Mekong River Commission (MRC) stage-discharge calculation and alarm threshold."""
        # At high discharge (24,000 m3/s), Kratie river stage should exceed official MRC flood stage (23.00m)
        mrc_reading = self.intake.fetch_mrc_gauge_reading(self.area_kratie, 24000.0)
        self.assertEqual(mrc_reading.station_id, "MRC-010501")
        self.assertEqual(mrc_reading.alarm_level_meters, 22.00)
        self.assertEqual(mrc_reading.flood_level_meters, 23.00)
        self.assertTrue(mrc_reading.is_official_stage_exceeded)
        self.assertEqual(mrc_reading.trend_24h, "Rising")

    def test_transboundary_surge_routing(self):
        """Validates hydrodynamic lag-time and surge propagation from Laos to Cambodia."""
        station_readings = {
            "laos-pakse": Reading(
                reading_id="p-1", area_id="laos-pakse", observed_at=datetime.now(timezone.utc),
                discharge=23000.0, precipitation=60.0, source="Test"
            ),
            "kratie-central": Reading(
                reading_id="k-1", area_id="kratie-central", observed_at=datetime.now(timezone.utc),
                discharge=13000.0, precipitation=10.0, source="Test"
            )
        }
        routing = OpenMeteoIntakeService.calculate_transboundary_surge_routing(station_readings)
        self.assertTrue(routing["surge_active"])
        self.assertEqual(routing["upstream_trigger_station"], "laos-pakse")
        self.assertEqual(routing["estimated_kratie_lead_time_hours"], 24.0)

    def test_ews1294_voice_ivr_script_generation(self):
        """Validates automated audio prompt generation matching Cambodia's 1294 voice early warning system."""
        reading = Reading(
            reading_id="ivr-test-1",
            area_id="kratie-central",
            observed_at=datetime.now(timezone.utc),
            discharge=23000.0,
            precipitation=75.0,
            source="Test Fixture"
        )
        risk = self.engine.evaluate(reading)
        voice_script = generate_ews1294_voice_call_script(self.area_kratie, risk, reading)

        self.assertEqual(voice_script["channel"], "ews1294_voice_ivr")
        self.assertEqual(voice_script["urgency_tier"], "Critical")
        self.assertIn("១២៩៤", voice_script["khmer_audio_script"])
        self.assertIn("EWS 1294", voice_script["english_audio_script"])
        self.assertGreater(voice_script["estimated_target_rural_subscribers"], 1000)

    def test_geojson_gis_export(self):
        """Validates RFC 7946 GeoJSON FeatureCollection generation."""
        geojson = self.repo.get_stations_geojson()
        self.assertEqual(geojson["type"], "FeatureCollection")
        self.assertGreaterEqual(len(geojson["features"]), 43)

        first = geojson["features"][0]
        self.assertEqual(first["geometry"]["type"], "Point")
        self.assertEqual(len(first["geometry"]["coordinates"]), 2)
        self.assertIn("risk_level", first["properties"])
        self.assertIn("marker_color", first["properties"])

        # Test risk heatmap polygon buffer
        heatmap = self.repo.get_risk_heatmap_geojson()
        self.assertEqual(heatmap["type"], "FeatureCollection")

    def test_production_api_endpoints(self):
        """Validates production endpoints via FastAPI TestClient."""
        # 1. Liveness Probe
        resp = self.client.get("/health/live")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "alive")

        # 2. Readiness Probe
        resp = self.client.get("/health/ready")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "ready")

        # 3. Prometheus Metrics
        resp = self.client.get("/metrics")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("cambodia_flood_stations_total", resp.text)
        self.assertIn("cambodia_flood_uptime_seconds", resp.text)

        # 4. CAP v1.2 XML Feed
        resp = self.client.get("/api/cap/feed.xml?area_id=kratie-central")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("urn:oasis:names:tc:emergency:cap:1.2", resp.text)

        # 5. CAP v1.2 JSON Feed
        resp = self.client.get("/api/cap/feed.json?area_id=kratie-central")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["cap_version"], "1.2")

        # 6. GeoJSON Stations
        resp = self.client.get("/api/gis/stations.geojson")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["type"], "FeatureCollection")

        # 7. Compound Risk Analytics
        resp = self.client.get("/api/analytics/compound-risk?area_id=kratie-central")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "success")
        self.assertIn("compound_score", data["assessment"])

        # 8. Historical Comparison
        resp = self.client.get("/api/analytics/historical-compare?area_id=kratie-central")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("percent_of_2000_peak", resp.json()["benchmark"])

        # 9. Transboundary Surge Routing
        resp = self.client.get("/api/analytics/transboundary-surge")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("routing", resp.json())

        # 10. Voice IVR Broadcast Endpoint
        resp = self.client.post("/api/dispatch/voice-ivr", json={"area_id": "kratie-central"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "queued_for_telephony_gateway")


if __name__ == "__main__":
    unittest.main()
