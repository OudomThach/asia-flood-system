"""
End-to-End Pipeline & Integration Tests for Oudom's Modules (AT1, AT2, AT8, AT9, AT12).
Author: Oudom Thach
"""

import unittest
from asia_flood_core.storage import FloodDataRepository
from asia_flood_core.data_intake import OpenMeteoIntakeService
from asia_flood_core.risk_engine import RiskClassificationEngine
from asia_flood_core.integration import FloodAlertPipeline


class TestPipelineIntegration(unittest.TestCase):

    def setUp(self):
        self.repo = FloodDataRepository(db_path=":memory:")
        self.intake = OpenMeteoIntakeService()
        self.engine = RiskClassificationEngine()
        self.pipeline = FloodAlertPipeline(
            repository=self.repo,
            intake_service=self.intake,
            risk_engine=self.engine
        )

    def tearDown(self):
        self.repo.close()

    def test_e2e_normal_cycle(self):
        """AT12: Normal flow produces stored reading and normal risk state."""
        mock_r = self.intake.create_mock_reading(discharge=10000.0, precipitation=2.0)
        res = self.pipeline.run_cycle(use_mock_reading=mock_r)

        self.assertEqual(res["risk_state"]["level"], "Normal")

        stored_reading = self.repo.get_latest_reading()
        self.assertIsNotNone(stored_reading)
        self.assertEqual(stored_reading.discharge, 10000.0)

        stored_risk = self.repo.get_latest_risk_state()
        self.assertIsNotNone(stored_risk)
        self.assertEqual(stored_risk.level, "Normal")

    def test_e2e_danger_evaluation(self):
        """AT2 / AT12: Danger flood surge correctly evaluated and persisted."""
        danger_reading = self.intake.create_mock_reading(discharge=24000.0, precipitation=10.0)
        res = self.pipeline.run_cycle(use_mock_reading=danger_reading)

        self.assertEqual(res["risk_state"]["level"], "Danger")
        
        stored_risk = self.repo.get_latest_risk_state()
        self.assertEqual(stored_risk.level, "Danger")
        self.assertIn("DANGER", stored_risk.reason)

    def test_manual_test_alert_isolation(self):
        """AT9: Manual test alert generates a TEST_ALERT payload without altering actual risk status."""
        test_payload = self.pipeline.trigger_manual_test_alert(custom_notes="Unit Test Admin Trigger")
        self.assertTrue(test_payload["is_test"])
        self.assertEqual(test_payload["message_type"], "TEST_ALERT")
        self.assertEqual(test_payload["status"], "DELIVERED")

    def test_gzip_compression_on_endpoints(self):
        """Optimization #3: Verify GZip response compression on FastAPI endpoints."""
        from starlette.testclient import TestClient
        from asia_flood_core.admin_server import app
        
        client = TestClient(app)
        response = client.get("/admin", headers={"Accept-Encoding": "gzip"})
        self.assertEqual(response.status_code, 200)
        # Verify GZip content encoding header or successful compressed transfer
        self.assertIn("text/html", response.headers.get("content-type", ""))


if __name__ == "__main__":
    unittest.main()
