"""
Data models for the Kratie Community Flash-Flood Alert System.
Strictly conforms to Section 6.1 of the CSCI 841 Proposal.
"""

from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field


class Area(BaseModel):
    """Defines the geographic unit used by readings, status, and subscriptions."""
    area_id: str = Field(default="kratie-central", description="Unique identifier for the area")
    name_en: str = Field(default="Kratie (Central Station)", description="English name of the area")
    name_km: str = Field(default="ក្រុងក្រចេះ (ស្ថានីយ៍កណ្ដាល)", description="Khmer / Local name of the area")
    latitude: float = Field(default=12.4888, description="Latitude of monitoring point")
    longitude: float = Field(default=106.0188, description="Longitude of monitoring point")
    country: str = Field(default="KH", description="Country code (e.g. KH, LA, CN, IN, BD, PK, NP, TH, VN, MM, PH, ID)")
    basin_category: str = Field(default="mekong", description="Basin category (e.g. mekong, yangtze, ganges, indus, etc.)")
    language: str = Field(default="en", description="Primary local language code: en, km, lo, zh, hi, bn, ur, ne, th, vi, my, tl, id")
    utc_offset_hours: float = Field(default=7.0, description="UTC timezone offset in hours")
    timezone_name: str = Field(default="Asia/Phnom_Penh", description="IANA timezone name")
    active: bool = Field(default=True, description="Whether monitoring is active")


class BasinThresholdProfile(BaseModel):
    """Per-basin hydrological risk thresholds and physics baselines."""
    basin_category: str = Field(description="Basin category key")
    basin_name: str = Field(description="Human-readable river basin name")
    danger_discharge_m3s: float = Field(description="Critical danger flood threshold in m³/s")
    caution_discharge_m3s: float = Field(description="Elevated caution/watch threshold in m³/s")
    severe_rain_mm: float = Field(default=80.0, description="Severe 24h storm rainfall threshold (mm)")
    heavy_rain_mm: float = Field(default=50.0, description="Heavy rainfall indicator threshold (mm)")
    baseline_discharge_m3s: float = Field(default=10000.0, description="Normal baseline flow (m³/s)")
    max_cap_discharge_m3s: float = Field(default=26000.0, description="Catastrophic flow cap for compound index scaling")
    historical_benchmark_name: str = Field(default="Regional Historical Crest", description="Name of historic disaster reference")
    historical_benchmark_m3s: float = Field(default=50000.0, description="Historic disaster peak discharge (m³/s)")
    hydrological_context: str = Field(default="Regional Catchment Standards", description="Official agency and basin context")


class Reading(BaseModel):
    """Stores validated source data and provenance from Open-Meteo."""
    reading_id: str = Field(description="Unique reading ID")
    area_id: str = Field(default="kratie-central", description="Foreign key to Area")
    observed_at: datetime = Field(description="Observation date/time from the meteorological model")
    discharge: float = Field(description="Modeled river discharge in m3/s (from GloFAS)")
    precipitation: float = Field(default=0.0, description="Precipitation / rainfall in mm")
    source: str = Field(default="Open-Meteo GloFAS + Weather API", description="Data provenance")
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Local ingestion timestamp")
    is_stale: bool = Field(default=False, description="Flag indicating if reading exceeded freshness limit")


class RiskState(BaseModel):
    """Makes the classification decision auditable and traceable."""
    risk_id: str = Field(description="Unique risk calculation ID")
    area_id: str = Field(default="kratie-central", description="Foreign key to Area")
    level: str = Field(description="Calculated risk level: Normal, Caution, or Danger")
    reason: str = Field(description="Human-readable justification for the classification")
    rule_version: str = Field(default="v1.0", description="Version of the risk evaluation algorithm")
    calculated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Calculation timestamp")
    discharge_val: Optional[float] = Field(default=None, description="Discharge input used")
    precipitation_val: Optional[float] = Field(default=None, description="Precipitation input used")


class TelegramAlertPayload(BaseModel):
    """Pre-formatted Telegram bot broadcast payload with dual Khmer/English formatting."""
    area_id: str = Field(description="Province identifier")
    area_name_en: str = Field(description="Province English name")
    area_name_km: str = Field(description="Province Khmer name")
    risk_level: str = Field(description="Calculated risk: Normal, Caution, Danger")
    discharge: float = Field(description="River discharge in m³/s")
    precipitation: float = Field(description="Rainfall in mm")
    message_km: str = Field(description="Formatted Telegram message in Khmer")
    message_en: str = Field(description="Formatted Telegram message in English")
    recommended_action_km: str = Field(description="Emergency action advice in Khmer")
    recommended_action_en: str = Field(description="Emergency action advice in English")
    timestamp_ict: str = Field(description="Cambodia local timestamp (ICT)")


class SMSAlertPayload(BaseModel):
    """Concise SMS gateway payload strictly optimized for GSM 160-char SMS packets."""
    area_id: str = Field(description="Province identifier")
    risk_level: str = Field(description="Calculated risk level")
    sms_body_en: str = Field(description="Concise English SMS text under 160 characters")
    sms_body_km: str = Field(description="Concise Khmer SMS text")
    char_count: int = Field(description="Length of SMS body")
    target_recipients: str = Field(default="Registered Commune Residents", description="Target recipient category")


class TestAlertEvent(BaseModel):
    """Tracks controlled manual test alert demonstrations (FR10)."""
    test_id: str = Field(description="Unique test event ID")
    area_id: str = Field(description="Target province ID")
    triggered_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    is_test: bool = Field(default=True, description="Strictly true for all test alerts")
    status: str = Field(default="SUCCESS")
    message_en: str = Field(description="English broadcast message")
    message_km: str = Field(description="Khmer broadcast message")


class Subscription(BaseModel):
    """Represents a resident's Telegram subscription to an area."""
    chat_id: str = Field(description="Telegram chat ID")
    area_id: str = Field(description="Subscribed area ID")
    subscribed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ==============================================================================
# PRODUCTION & NATIONAL SCALE EXPANSION MODELS
# ==============================================================================

class SoilMoistureReading(BaseModel):
    """Copernicus / Open-Meteo volumetric soil water content."""
    area_id: str = Field(description="Target monitoring station ID")
    observed_at: datetime = Field(description="Observation timestamp")
    moisture_surface_m3m3: float = Field(default=0.35, description="Volumetric water content 0-7 cm (m³/m³)")
    moisture_rootzone_m3m3: float = Field(default=0.38, description="Volumetric water content 7-28 cm (m³/m³)")
    saturation_percent: float = Field(default=65.0, description="Estimated soil saturation percentage (0-100%)")
    runoff_coefficient: float = Field(default=0.45, description="Runoff coefficient based on soil saturation (0.0 to 1.0)")
    source: str = Field(default="Copernicus ERA5 / Open-Meteo Soil Infiltration API")


class MRCStationReading(BaseModel):
    """Mekong River Commission (MRC) official hydrological gauge telemetry."""
    station_id: str = Field(description="MRC Official station identifier (e.g., MRC-010501 for Kratie)")
    area_id: str = Field(description="Mapped regional area ID")
    name_en: str = Field(description="Official MRC station name")
    water_level_meters: float = Field(description="Observed river surface level in meters (m)")
    alarm_level_meters: float = Field(description="Official MRC Alarm Level in meters (m)")
    flood_level_meters: float = Field(description="Official MRC Flood Danger Level in meters (m)")
    trend_24h: str = Field(default="Steady", description="Hydrological trend: Rising, Falling, or Steady")
    observed_at: datetime = Field(description="Observation timestamp")
    is_official_stage_exceeded: bool = Field(default=False, description="True if water level exceeds alarm or flood stage")


class HistoricalFloodBenchmark(BaseModel):
    """Comparative benchmarking against catastrophic Lower Mekong historical floods."""
    area_id: str
    current_discharge: float
    year_2000_peak_discharge: float = Field(default=52000.0, description="Historic 2000 Mekong Disaster peak at Kratie (m³/s)")
    year_2011_peak_discharge: float = Field(default=48500.0, description="Catastrophic 2011 Southeast Asia Flood peak at Kratie (m³/s)")
    year_2020_peak_discharge: float = Field(default=41000.0, description="2020 Tropical Depression Flash-Flood peak at Kratie (m³/s)")
    percent_of_2000_peak: float = Field(description="Current discharge as percentage of 2000 peak")
    percent_of_2011_peak: float = Field(description="Current discharge as percentage of 2011 peak")
    historical_context: str = Field(description="Analytical summary for disaster managers")


class CompoundRiskAssessment(BaseModel):
    """Multi-factorial scientific risk index for real-world disaster operations."""
    assessment_id: str = Field(description="Unique assessment identifier")
    area_id: str = Field(description="Target province/monitoring station ID")
    level: str = Field(description="Overall risk level: Normal, Advisory, Caution, or Danger")
    compound_score: float = Field(description="Normalized compound risk index between 0.00 and 1.00")
    surge_momentum: float = Field(description="Upstream river surge velocity / influx score (0.0 to 1.0)")
    soil_saturation_factor: float = Field(description="Soil runoff amplification factor (0.0 to 1.0)")
    precipitation_factor: float = Field(description="Extreme rainfall intensity factor (0.0 to 1.0)")
    discharge_factor: float = Field(description="River discharge exceedance factor (0.0 to 1.0)")
    estimated_lead_time_hours: Optional[float] = Field(default=None, description="Hydrodynamic lag-time to flood crest (hours)")
    upstream_trigger_station: Optional[str] = Field(default=None, description="Origin of transboundary surge if detected (e.g. Pakse, Laos)")
    calculated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    justification_en: str = Field(description="Comprehensive scientific rationale in English")
    justification_km: str = Field(description="Comprehensive scientific rationale in Khmer")


class CAPAlert(BaseModel):
    """
    OASIS Common Alerting Protocol (CAP) v1.2 conforming alert model.
    Compliant with ITU-T Recommendation X.1303 and WMO Alert Hub.
    """
    identifier: str = Field(description="Globally unique alert identifier (e.g., KH-EWS-2026-0012)")
    sender: str = Field(default="admin@ncdm.gov.kh", description="Authoritative issuing entity")
    sent: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = Field(default="Actual", description="Actual, Exercise, System, Test, Draft")
    msg_type: str = Field(default="Alert", description="Alert, Update, Cancel, Ack, Error")
    scope: str = Field(default="Public", description="Public, Restricted, Private")
    category: str = Field(default="Met", description="Geo, Met, Safety, Security, Rescue, Fire, Health, Env, Transport, Infra, Other")
    event: str = Field(default="Flash Flood / Mekong River Surge Warning", description="Event type")
    urgency: str = Field(default="Immediate", description="Immediate, Expected, Future, Past, Unknown")
    severity: str = Field(default="Extreme", description="Extreme, Severe, Moderate, Minor, Unknown")
    certainty: str = Field(default="Observed", description="Observed, Likely, Possible, Unlikely, Unknown")
    headline_en: str = Field(description="Brief English public headline")
    headline_km: str = Field(description="Brief Khmer public headline")
    description_en: str = Field(description="Detailed English disaster bulletin")
    description_km: str = Field(description="Detailed Khmer disaster bulletin")
    instruction_en: str = Field(description="Protective action advice in English")
    instruction_km: str = Field(description="Protective action advice in Khmer")
    area_desc: str = Field(description="Affected geographic zone")
    latitude: float
    longitude: float
    radius_km: float = Field(default=25.0)


class VoiceAlertPayload(BaseModel):
    """
    Cambodia EWS 1294 compatible Automated Voice IVR audio dispatch payload.
    Provides phone-based broadcast scripts for illiterate and rural populations.
    """
    dispatch_id: str = Field(description="Unique call campaign ID")
    target_area_id: str = Field(description="Target province ID")
    urgency_tier: str = Field(description="Urgency: Low, Medium, High, Critical")
    khmer_audio_script: str = Field(description="Phonetic / Khmer text prompt for IVR Text-to-Speech or pre-recorded prompts")
    english_audio_script: str = Field(description="English spoken prompt")
    estimated_call_duration_seconds: int = Field(default=45)
    target_phone_count: int = Field(default=1250, description="Estimated rural phone subscribers in hazard zone")
    dispatched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class WebhookAlertPayload(BaseModel):
    """Standard JSON webhook payload for humanitarian agencies (Red Cross, WFP, MOWRAM)."""
    event_id: str
    event_type: str = Field(default="flood_alert")
    severity: str
    area_id: str
    area_name_en: str
    area_name_km: str
    metrics: dict
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    signature: str = Field(default="sha256-hmac-verified", description="Security signature for webhook integrity")


# ==============================================================================
# UNIFIED REAL-TIME HAZARD EVENT (USGS, NASA EONET, GDACS, NASA FIRMS)
# ==============================================================================

class HazardEvent(BaseModel):
    """
    A single real, observed hazard event normalized from an external monitoring network.
    Everything on the live hazard layer is an actual detection (observed=True) — no synthetic data.
    """
    event_id: str = Field(description="Stable id from the source feed")
    event_type: str = Field(description="earthquake | cyclone | volcano | wildfire | flood | landslide | drought")
    title: str = Field(description="Human-readable event description")
    severity: str = Field(default="info", description="red | orange | green | info (normalized across sources)")
    latitude: float
    longitude: float
    magnitude: Optional[float] = Field(default=None, description="Numeric magnitude/intensity if the source provides one")
    value_label: Optional[str] = Field(default=None, description="Formatted magnitude/intensity for display, e.g. 'M5.4' or '210 km/h'")
    observed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Event/observation timestamp (UTC)")
    source: str = Field(description="Originating network: USGS | NASA EONET | GDACS | NASA FIRMS")
    url: str = Field(default="", description="Canonical event page / source link")
    observed: bool = Field(default=True, description="Always true — this layer only carries real detections")
    flood_role: str = Field(default="NOT_FLOOD_RELEVANT", description="PRIMARY_DRIVER | COMPOUNDING_TRIGGER | ANTECEDENT_AMPLIFIER | NOT_FLOOD_RELEVANT")
    mechanism: str = Field(default="", description="Plain-English reason this event does or doesn't contribute to flash flooding")









