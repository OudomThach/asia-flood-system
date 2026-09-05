"""
Pan-Asian Multi-Hazard & Flash-Flood Early Warning Platform - Core Engineering Package
Lead & Primary Owner: Oudom Thach
Modules: Data Intake, Live Multi-Hazard Feeds, Flash-Flood Contribution Model,
         Risk Classification Engine, Admin Command Center, Storage & Rate Limiter
"""

import sys

from .models import (
    Area, Reading, RiskState, TestAlertEvent, Subscription,
    SoilMoistureReading, MRCStationReading, CompoundRiskAssessment,
    HistoricalFloodBenchmark, CAPAlert, VoiceAlertPayload, WebhookAlertPayload,
    HazardEvent, BasinThresholdProfile,
)
from .data_intake import OpenMeteoIntakeService
from .live_hazards import LiveHazardIntake
from .flood_linkage import (
    classify_flood_role, contribution_score, station_flood_pressure, group_by_role,
)
from .risk_engine import RiskClassificationEngine, CompoundRiskEngine, BASIN_PROFILES
from .cap_protocol import CAPProtocolEngine
from .storage import FloodDataRepository
from .integration import FloodAlertPipeline
from .rate_limiter import SlidingWindowRateLimiter

__all__ = [
    "Area",
    "Reading",
    "RiskState",
    "TestAlertEvent",
    "Subscription",
    "SoilMoistureReading",
    "MRCStationReading",
    "CompoundRiskAssessment",
    "HistoricalFloodBenchmark",
    "CAPAlert",
    "VoiceAlertPayload",
    "WebhookAlertPayload",
    "HazardEvent",
    "BasinThresholdProfile",
    "BASIN_PROFILES",
    "OpenMeteoIntakeService",
    "LiveHazardIntake",
    "classify_flood_role",
    "contribution_score",
    "station_flood_pressure",
    "group_by_role",
    "RiskClassificationEngine",
    "CompoundRiskEngine",
    "CAPProtocolEngine",
    "FloodDataRepository",
    "FloodAlertPipeline",
    "SlidingWindowRateLimiter",
]

# Legacy package compatibility alias (old imports of `cambodia_flood_core` still resolve).
sys.modules.setdefault("cambodia_flood_core", sys.modules[__name__])
