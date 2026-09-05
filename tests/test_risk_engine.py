"""
Unit & Acceptance Tests for Risk Engine (AT2 / FR3).
Author: Oudom Thach
"""

import unittest
from datetime import datetime, timezone
from asia_flood_core.models import Reading, Area
from asia_flood_core.risk_engine import RiskClassificationEngine, HistoricalFloodBenchmark, BASIN_PROFILES


class TestRiskClassificationEngine(unittest.TestCase):

    def setUp(self):
        self.engine = RiskClassificationEngine(rule_version="v1.0")

    def test_normal_baseline_flow(self):
        """Normal reading: low discharge, low rain."""
        reading = Reading(
            reading_id="test-01",
            area_id="kratie-central",
            observed_at=datetime.now(timezone.utc),
            discharge=12500.0,
            precipitation=10.0,
            source="Test Fixture",
            fetched_at=datetime.now(timezone.utc)
        )
        state = self.engine.evaluate(reading)
        self.assertEqual(state.level, "Normal")
        self.assertEqual(state.rule_version, "v1.0")
        self.assertIn("NORMAL", state.reason)

    def test_caution_elevated_discharge(self):
        """Caution reading: discharge in watch range (16,000 - 22,000 m3/s)."""
        reading = Reading(
            reading_id="test-02",
            area_id="kratie-central",
            observed_at=datetime.now(timezone.utc),
            discharge=18500.0,
            precipitation=15.0,
            source="Test Fixture",
            fetched_at=datetime.now(timezone.utc)
        )
        state = self.engine.evaluate(reading)
        self.assertEqual(state.level, "Caution")
        self.assertIn("CAUTION", state.reason)

    def test_caution_heavy_rain_compound(self):
        """Caution reading: moderate discharge + heavy rain (>= 50 mm)."""
        reading = Reading(
            reading_id="test-03",
            area_id="kratie-central",
            observed_at=datetime.now(timezone.utc),
            discharge=13500.0,
            precipitation=60.0,
            source="Test Fixture",
            fetched_at=datetime.now(timezone.utc)
        )
        state = self.engine.evaluate(reading)
        self.assertEqual(state.level, "Caution")

    def test_danger_discharge_threshold_exceeded(self):
        """Danger reading: discharge >= 22,000 m3/s."""
        reading = Reading(
            reading_id="test-04",
            area_id="kratie-central",
            observed_at=datetime.now(timezone.utc),
            discharge=23400.0,
            precipitation=20.0,
            source="Test Fixture",
            fetched_at=datetime.now(timezone.utc)
        )
        state = self.engine.evaluate(reading)
        self.assertEqual(state.level, "Danger")
        self.assertIn("DANGER", state.reason)
        self.assertIn("23,400.0 m³/s", state.reason)

    def test_danger_compound_surge(self):
        """Danger reading: high discharge (>= 18,000 m3/s) + severe rain (>= 80 mm)."""
        reading = Reading(
            reading_id="test-05",
            area_id="kratie-central",
            observed_at=datetime.now(timezone.utc),
            discharge=19000.0,
            precipitation=95.0,
            source="Test Fixture",
            fetched_at=datetime.now(timezone.utc)
        )
        state = self.engine.evaluate(reading)
        self.assertEqual(state.level, "Danger")

    def test_stale_missing_data_does_not_escalate_risk(self):
        """Section 4.4 Rule: Missing data must NOT cause a false Danger alert."""
        reading = Reading(
            reading_id="test-06",
            area_id="kratie-central",
            observed_at=datetime.now(timezone.utc),
            discharge=0.0,
            precipitation=0.0,
            source="Test Stale Fixture",
            fetched_at=datetime.now(timezone.utc),
            is_stale=True
        )
        state = self.engine.evaluate(reading)
        self.assertEqual(state.level, "Normal")
        self.assertIn("unavailable or stale", state.reason)

    def test_per_basin_thresholds_yangtze_and_indus(self):
        """Phase 1: Validates per-basin calibrated discharge thresholds for Yangtze & Indus."""
        # Yangtze: Caution >= 40,000, Danger >= 55,000
        area_yangtze = Area(area_id="cn-wuhan", name_en="Wuhan", basin_category="yangtze", country="China")
        r_yangtze_caution = Reading(
            reading_id="read-yz-1", area_id="cn-wuhan", observed_at=datetime.now(timezone.utc),
            discharge=45000.0, precipitation=10.0, source="Test", fetched_at=datetime.now(timezone.utc)
        )
        state_yz = self.engine.evaluate(r_yangtze_caution, area_yangtze)
        self.assertEqual(state_yz.level, "Caution")
        self.assertIn("Yangtze", state_yz.reason)

        # Indus: Danger >= 20,000
        area_indus = Area(area_id="pk-sukkur", name_en="Sukkur", basin_category="indus", country="Pakistan")
        r_indus_danger = Reading(
            reading_id="read-ind-1", area_id="pk-sukkur", observed_at=datetime.now(timezone.utc),
            discharge=21500.0, precipitation=20.0, source="Test", fetched_at=datetime.now(timezone.utc)
        )
        state_ind = self.engine.evaluate(r_indus_danger, area_indus)
        self.assertEqual(state_ind.level, "Danger")
        self.assertIn("Indus", state_ind.reason)

    def test_per_basin_historical_benchmark(self):
        """Phase 2: Validates historical benchmark evaluation across Asian basins."""
        from asia_flood_core.risk_engine import CompoundRiskEngine
        bm_yangtze = CompoundRiskEngine.evaluate_historical_benchmark("yangtze", 65000.0)
        self.assertIn("1998 Yangtze Catastrophic Flood", bm_yangtze.historical_context)
        self.assertGreaterEqual(bm_yangtze.percent_of_2000_peak, 100.0)

        bm_kratie = CompoundRiskEngine.evaluate_historical_benchmark("kratie-central", 54000.0)
        self.assertIn("2000 Mekong Centennial Disaster", bm_kratie.historical_context)


if __name__ == "__main__":
    unittest.main()
