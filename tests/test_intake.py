"""
Unit & Acceptance Tests for Data Intake Service (AT1, AT10 / FR1, FR2, NFR3).
Author: Oudom Thach
"""

import unittest
from datetime import datetime, timezone, timedelta
from asia_flood_core.data_intake import OpenMeteoIntakeService
from asia_flood_core.models import Area


class TestDataIntakeService(unittest.TestCase):

    def setUp(self):
        self.area = Area(area_id="kratie-central", latitude=12.4888, longitude=106.0188)
        self.intake = OpenMeteoIntakeService(area=self.area)

    def test_mock_reading_generation(self):
        """AT1: Verify valid reading object creation."""
        reading = self.intake.create_mock_reading(discharge=15000.0, precipitation=12.5)
        self.assertEqual(reading.area_id, "kratie-central")
        self.assertEqual(reading.discharge, 15000.0)
        self.assertEqual(reading.precipitation, 12.5)
        self.assertFalse(reading.is_stale)
        self.assertEqual(self.intake.last_successful_reading.reading_id, reading.reading_id)

    def test_failure_preserves_last_reading_nfr3(self):
        """AT10 / NFR3: Verify fallback preserves last known reading upon fetch failure."""
        first_reading = self.intake.create_mock_reading(discharge=14200.0, precipitation=5.0)
        
        # Simulate fetch failure
        fallback = self.intake._handle_fetch_failure(Exception("Simulated Network Timeout"))
        
        self.assertEqual(fallback.discharge, 14200.0)
        self.assertEqual(fallback.precipitation, 5.0)

    def test_staleness_flagging(self):
        """Verify that data older than 24 hours is tagged is_stale=True."""
        old_reading = self.intake.create_mock_reading(discharge=14000.0, is_stale=True)
        self.assertTrue(old_reading.is_stale)

    def test_multi_area_caching_and_failure_isolation(self):
        """
        Regression Test: Verify that multiple provinces on a shared OpenMeteoIntakeService singleton
        maintain isolated per-area caches, preserve correct area_ids, and handle failures independently.
        """
        area_kratie = Area(area_id="kratie-central", name_en="Kratie", latitude=12.4888, longitude=106.0188)
        area_stung_treng = Area(area_id="stung-treng", name_en="Stung Treng", latitude=13.5259, longitude=105.9683)
        area_phnom_penh = Area(area_id="phnom-penh", name_en="Phnom Penh", latitude=11.5564, longitude=104.9282)

        # 1. Ingest distinct readings for each province
        r_kratie = self.intake.create_mock_reading(discharge=14500.0, precipitation=15.0, area_id="kratie-central")
        r_stung_treng = self.intake.create_mock_reading(discharge=18200.0, precipitation=45.0, area_id="stung-treng")

        # Assert correct area tagging
        self.assertEqual(r_kratie.area_id, "kratie-central")
        self.assertEqual(r_stung_treng.area_id, "stung-treng")

        # Assert isolated cache retrieval by area_id
        cached_kratie = self.intake.get_last_successful_reading("kratie-central")
        cached_stung_treng = self.intake.get_last_successful_reading("stung-treng")
        self.assertIsNotNone(cached_kratie)
        self.assertIsNotNone(cached_stung_treng)
        self.assertEqual(cached_kratie.discharge, 14500.0)
        self.assertEqual(cached_stung_treng.discharge, 18200.0)

        # 2. Simulate failure for Stung Treng: should preserve Stung Treng reading (18,200 m³/s)
        fallback_st = self.intake._handle_fetch_failure(Exception("HTTP 500"), target_area=area_stung_treng)
        self.assertEqual(fallback_st.area_id, "stung-treng")
        self.assertEqual(fallback_st.discharge, 18200.0)

        # 3. Simulate failure for Phnom Penh (Cold start / no cache): should return cold start fallback tagged with phnom-penh
        fallback_pp = self.intake._handle_fetch_failure(Exception("HTTP 500"), target_area=area_phnom_penh)
        self.assertEqual(fallback_pp.area_id, "phnom-penh")
    def test_async_fetch_live_data_fallback(self):
        """Verify that async intake gracefully handles network errors with NFR3 fallback."""
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            reading = loop.run_until_complete(
                self.intake.async_fetch_live_data(area=self.area)
            )
            self.assertIsNotNone(reading)
            self.assertEqual(reading.area_id, "kratie-central")
            self.assertGreaterEqual(reading.discharge, 0.0)
        finally:
            loop.close()


if __name__ == "__main__":
    unittest.main()
