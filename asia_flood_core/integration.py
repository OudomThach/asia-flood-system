"""
Component Integration & Pipeline Controller for Kratie Flash-Flood Alert System.
Primary Owner: Oudom Thach
Fulfills: Component Integration, FR1, FR2, FR3, FR9, FR10, Acceptance Tests AT1, AT2, AT8, AT9, AT10, AT12.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Callable

from .models import Reading, RiskState, Area
from .data_intake import OpenMeteoIntakeService
from .risk_engine import RiskClassificationEngine
from .storage import FloodDataRepository

logger = logging.getLogger(__name__)


class FloodAlertPipeline:
    """
    Coordinates Oudom's core pipeline:
    Data Intake (Open-Meteo) -> Persistence -> Risk Classification Engine (v1.0) -> Output Contract
    """

    def __init__(
        self,
        repository: Optional[FloodDataRepository] = None,
        intake_service: Optional[OpenMeteoIntakeService] = None,
        risk_engine: Optional[RiskClassificationEngine] = None,
        on_risk_evaluated: Optional[Callable[[RiskState], None]] = None
    ):
        self.repo = repository or FloodDataRepository()
        self.intake = intake_service or OpenMeteoIntakeService()
        self.risk_engine = risk_engine or RiskClassificationEngine()
        self.on_risk_evaluated = on_risk_evaluated

    def run_cycle(self, area_id: str = "kratie-central", use_mock_reading: Optional[Reading] = None) -> Dict[str, Any]:
        """
        Executes a single operational monitoring cycle for the specified area:
        1. Ingest reading from Open-Meteo Flood + Weather APIs (or fixture).
        2. Validate & store reading in database (FR1, FR2).
        3. Evaluate reading through Risk Classification Engine (FR3).
        4. Persist resulting RiskState (FR3).
        5. Invoke external notification callback if registered.
        """
        target_area = self.repo.get_area(area_id) or Area(area_id=area_id)

        # Step 1: Ingest data
        if use_mock_reading is not None:
            reading = use_mock_reading
        else:
            reading = self.intake.fetch_live_data(area=target_area)

        # Step 2: Store Reading
        self.repo.save_reading(reading)

        # Step 3: Evaluate Risk
        risk_state = self.risk_engine.evaluate(reading, target_area)

        # Step 4: Store RiskState
        self.repo.save_risk_state(risk_state)

        # Step 5: Callback hand-off (if downstream consumers are listening)
        if self.on_risk_evaluated:
            try:
                self.on_risk_evaluated(risk_state)
            except Exception as exc:
                logger.warning(f"Downstream callback error: {exc}")

        # Step 6: Log immutable audit event by time (FR2 Provenance)
        self.repo.log_audit_event(
            event_type="TELEMETRY_EVALUATION",
            area_id=area_id,
            details={
                "discharge": reading.discharge,
                "precipitation": reading.precipitation,
                "risk_level": risk_state.level,
                "rule_version": risk_state.rule_version,
                "is_stale": reading.is_stale,
                "source": reading.source
            }
        )

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "area": target_area.model_dump(),
            "reading": reading.model_dump(),
            "risk_state": risk_state.model_dump()
        }

    def trigger_manual_test_alert(self, area_id: str = "kratie-central", custom_notes: str = "Admin Test Alert") -> Dict[str, Any]:
        """
        Fulfills FR10 & Acceptance Test AT9:
        Simulates an administrator-initiated test alert payload without corrupting real risk data.
        """
        target_area = self.repo.get_area(area_id) or Area(area_id=area_id)
        latest_reading = self.repo.get_latest_reading(area_id=area_id)
        test_payload = {
            "test_id": f"test-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
            "area_id": area_id,
            "area_name": target_area.name_en,
            "is_test": True,
            "status": "DELIVERED",
            "message_type": "TEST_ALERT",
            "message_en": f"[TEST ALERT - {target_area.name_en}] Controlled test triggered by Administrator: {custom_notes}",
            "message_km": f"[សារសាកល្បង - {target_area.name_km}] ការសាកល្បងប្រព័ន្ធបញ្ជូនសារអាសន្នដោយរដ្ឋបាល: {custom_notes}",
            "current_discharge_m3s": latest_reading.discharge if latest_reading else 0.0,
            "triggered_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Log audit trail for manual test
        self.repo.log_audit_event(
            event_type="MANUAL_TEST_ALERT",
            area_id=area_id,
            details={
                "test_id": test_payload["test_id"],
                "notes": custom_notes,
                "status": "DELIVERED"
            }
        )
        logger.info(f"Admin Manual Test Alert Dispatched: {test_payload}")
        return test_payload
