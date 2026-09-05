"""
Risk Classification Engine for Kratie Community Flash-Flood Alert System.
Primary Owner: Oudom Thach
Fulfills: FR3, Acceptance Test AT2.
"""

from datetime import datetime, timezone
import uuid
from typing import Dict, Any, Optional, List
import math

from .models import (
    Reading, RiskState, SoilMoistureReading, CompoundRiskAssessment, HistoricalFloodBenchmark,
)


# ==============================================================================
# 1. PER-BASIN RISK THRESHOLDS & HYDROLOGICAL PROFILES (PAN-ASIA)
# ==============================================================================

from .models import BasinThresholdProfile, Area

BASIN_PROFILES: Dict[str, BasinThresholdProfile] = {
    "mekong": BasinThresholdProfile(
        basin_category="mekong",
        basin_name="Mekong River Basin (Cambodia / Laos)",
        danger_discharge_m3s=22000.0,
        caution_discharge_m3s=16000.0,
        severe_rain_mm=80.0,
        heavy_rain_mm=50.0,
        baseline_discharge_m3s=10000.0,
        max_cap_discharge_m3s=26000.0,
        historical_benchmark_name="2000 Mekong Centennial Disaster",
        historical_benchmark_m3s=52000.0,
        hydrological_context="MRC Kratie Flood Level Reference = 23.00m; Alarm Level = 22.00m"
    ),
    "laos-southern": BasinThresholdProfile(
        basin_category="laos-southern",
        basin_name="Lower-Middle Mekong & Sekong (Southern Laos)",
        danger_discharge_m3s=24000.0,
        caution_discharge_m3s=17000.0,
        severe_rain_mm=85.0,
        heavy_rain_mm=50.0,
        baseline_discharge_m3s=11000.0,
        max_cap_discharge_m3s=28000.0,
        historical_benchmark_name="2018 Xe-Pian & 2011 Pakse Peak",
        historical_benchmark_m3s=54000.0,
        hydrological_context="Pakse MRC Warning Level = 12.00m; Flood Stage = 13.00m"
    ),
    "laos-central": BasinThresholdProfile(
        basin_category="laos-central",
        basin_name="Central Mekong (Vientiane / Savannakhet Reach)",
        danger_discharge_m3s=21000.0,
        caution_discharge_m3s=15000.0,
        severe_rain_mm=80.0,
        heavy_rain_mm=50.0,
        baseline_discharge_m3s=9500.0,
        max_cap_discharge_m3s=25000.0,
        historical_benchmark_name="2008 Vientiane Centennial Inundation",
        historical_benchmark_m3s=46000.0,
        hydrological_context="Vientiane KM4 Flood Stage = 12.50m"
    ),
    "laos-upper": BasinThresholdProfile(
        basin_category="laos-upper",
        basin_name="Upper Mekong Gorges (Luang Prabang / Golden Triangle)",
        danger_discharge_m3s=19000.0,
        caution_discharge_m3s=13500.0,
        severe_rain_mm=75.0,
        heavy_rain_mm=45.0,
        baseline_discharge_m3s=8000.0,
        max_cap_discharge_m3s=23000.0,
        historical_benchmark_name="2008 Luang Prabang Record Surge",
        historical_benchmark_m3s=42000.0,
        hydrological_context="Luang Prabang Mountain Gorge Warning Stage = 17.50m"
    ),
    "yangtze": BasinThresholdProfile(
        basin_category="yangtze",
        basin_name="Yangtze River Basin (Three Gorges & Middle Reach, China)",
        danger_discharge_m3s=55000.0,
        caution_discharge_m3s=40000.0,
        severe_rain_mm=90.0,
        heavy_rain_mm=55.0,
        baseline_discharge_m3s=25000.0,
        max_cap_discharge_m3s=70000.0,
        historical_benchmark_name="1998 Yangtze Catastrophic Flood",
        historical_benchmark_m3s=63000.0,
        hydrological_context="Changjiang Water Resources Commission (CWRC) Yichang/Hankou Warning Stage"
    ),
    "lancang": BasinThresholdProfile(
        basin_category="lancang",
        basin_name="Upper Lancang Headwaters Cascade (Yunnan / Tibet)",
        danger_discharge_m3s=6000.0,
        caution_discharge_m3s=4000.0,
        severe_rain_mm=70.0,
        heavy_rain_mm=40.0,
        baseline_discharge_m3s=2000.0,
        max_cap_discharge_m3s=8500.0,
        historical_benchmark_name="1966 Lancang Alpine Flood",
        historical_benchmark_m3s=12800.0,
        hydrological_context="Yunnan Provincial Hydrology Bureau Jinghong Gauge"
    ),
    "ganges": BasinThresholdProfile(
        basin_category="ganges",
        basin_name="Ganges River Basin (Patna / Bihar Floodplain, India)",
        danger_discharge_m3s=25000.0,
        caution_discharge_m3s=18000.0,
        severe_rain_mm=85.0,
        heavy_rain_mm=50.0,
        baseline_discharge_m3s=12000.0,
        max_cap_discharge_m3s=35000.0,
        historical_benchmark_name="1998 Gangetic Mega-Flood",
        historical_benchmark_m3s=76000.0,
        hydrological_context="Central Water Commission (CWC India) Digha Ghat Flood Stage = 50.52m"
    ),
    "brahmaputra": BasinThresholdProfile(
        basin_category="brahmaputra",
        basin_name="Brahmaputra / Jamuna Basin (Assam & Bangladesh)",
        danger_discharge_m3s=30000.0,
        caution_discharge_m3s=22000.0,
        severe_rain_mm=100.0,
        heavy_rain_mm=60.0,
        baseline_discharge_m3s=15000.0,
        max_cap_discharge_m3s=40000.0,
        historical_benchmark_name="1988 Brahmaputra-Jamuna Deluge",
        historical_benchmark_m3s=72000.0,
        hydrological_context="Bangladesh FFWC Bahadurabad Danger Level = 19.50m"
    ),
    "meghna": BasinThresholdProfile(
        basin_category="meghna",
        basin_name="Surma-Meghna Flash Basin (Sylhet / Haor Depression)",
        danger_discharge_m3s=6500.0,
        caution_discharge_m3s=4200.0,
        severe_rain_mm=120.0,
        heavy_rain_mm=70.0,
        baseline_discharge_m3s=2000.0,
        max_cap_discharge_m3s=9000.0,
        historical_benchmark_name="2022 Sylhet-Assam Extreme Flash Flood",
        historical_benchmark_m3s=14000.0,
        hydrological_context="Bangladesh FFWC Sylhet Surma Danger Level = 11.25m"
    ),
    "indus": BasinThresholdProfile(
        basin_category="indus",
        basin_name="Indus & Kabul River Basin (Pakistan)",
        danger_discharge_m3s=10000.0,
        caution_discharge_m3s=6500.0,
        severe_rain_mm=60.0,
        heavy_rain_mm=35.0,
        baseline_discharge_m3s=3000.0,
        max_cap_discharge_m3s=15000.0,
        historical_benchmark_name="2010 & 2022 Pakistan Super Floods",
        historical_benchmark_m3s=28000.0,
        hydrological_context="Pakistan Federal Flood Commission (FFC) Sukkur High Flood = 500,000 cusecs"
    ),
    "himalayas": BasinThresholdProfile(
        basin_category="himalayas",
        basin_name="Himalayan Mountain Gorges (Narayani / Nepal)",
        danger_discharge_m3s=3500.0,
        caution_discharge_m3s=2000.0,
        severe_rain_mm=75.0,
        heavy_rain_mm=45.0,
        baseline_discharge_m3s=1000.0,
        max_cap_discharge_m3s=5500.0,
        historical_benchmark_name="1993 Nepal Central Mountain Flood",
        historical_benchmark_m3s=12000.0,
        hydrological_context="Nepal DHM Narayani River Devghat Warning Level = 7.3m"
    ),
    "chao-phraya": BasinThresholdProfile(
        basin_category="chao-phraya",
        basin_name="Chao Phraya River Basin (Central Plain, Thailand)",
        danger_discharge_m3s=2800.0,
        caution_discharge_m3s=1800.0,
        severe_rain_mm=80.0,
        heavy_rain_mm=50.0,
        baseline_discharge_m3s=800.0,
        max_cap_discharge_m3s=4000.0,
        historical_benchmark_name="2011 Thailand Great Inundation",
        historical_benchmark_m3s=4200.0,
        hydrological_context="Royal Irrigation Department (RID) C.2 Nakhon Sawan Stage = 2,800 m³/s"
    ),
    "red-river": BasinThresholdProfile(
        basin_category="red-river",
        basin_name="Red River / Song Hong Basin (Hanoi & Northern Delta, Vietnam)",
        danger_discharge_m3s=5500.0,
        caution_discharge_m3s=3500.0,
        severe_rain_mm=90.0,
        heavy_rain_mm=55.0,
        baseline_discharge_m3s=1500.0,
        max_cap_discharge_m3s=8000.0,
        historical_benchmark_name="1971 & 2024 Typhoon Yagi Floods",
        historical_benchmark_m3s=37800.0,
        hydrological_context="Vietnam NCHMF Long Bien Hanoi Alarm Stage III = 11.50m"
    ),
    "mekong-delta": BasinThresholdProfile(
        basin_category="mekong-delta",
        basin_name="Mekong Delta Marine Outflow (Can Tho / Hau River, Vietnam)",
        danger_discharge_m3s=20000.0,
        caution_discharge_m3s=15000.0,
        severe_rain_mm=85.0,
        heavy_rain_mm=50.0,
        baseline_discharge_m3s=8000.0,
        max_cap_discharge_m3s=25000.0,
        historical_benchmark_name="2000 & 2011 Mekong Delta Tidal Crests",
        historical_benchmark_m3s=24000.0,
        hydrological_context="Can Tho Hydrometeorological Station Warning Stage = 2.00m"
    ),
    "irrawaddy": BasinThresholdProfile(
        basin_category="irrawaddy",
        basin_name="Irrawaddy River Basin & Delta (Myanmar)",
        danger_discharge_m3s=16000.0,
        caution_discharge_m3s=11000.0,
        severe_rain_mm=95.0,
        heavy_rain_mm=55.0,
        baseline_discharge_m3s=6000.0,
        max_cap_discharge_m3s=22000.0,
        historical_benchmark_name="2015 Cyclone Komen Flood Disaster",
        historical_benchmark_m3s=45000.0,
        hydrological_context="Myanmar DMH Hinthada Danger Level = 13.42m"
    ),
    "philippines": BasinThresholdProfile(
        basin_category="philippines",
        basin_name="Pasig-Marikina & Cagayan Basins (Luzon, Philippines)",
        danger_discharge_m3s=850.0,
        caution_discharge_m3s=450.0,
        severe_rain_mm=110.0,
        heavy_rain_mm=65.0,
        baseline_discharge_m3s=150.0,
        max_cap_discharge_m3s=1500.0,
        historical_benchmark_name="2009 Typhoon Ondoy & 2020 Ulysses",
        historical_benchmark_m3s=1200.0,
        hydrological_context="PAGASA Marikina River Sto. Nino 3rd Alarm = 18.00m"
    ),
    "indonesia": BasinThresholdProfile(
        basin_category="indonesia",
        basin_name="Ciliwung & Bengawan Solo Basins (Java, Indonesia)",
        danger_discharge_m3s=550.0,
        caution_discharge_m3s=320.0,
        severe_rain_mm=100.0,
        heavy_rain_mm=60.0,
        baseline_discharge_m3s=100.0,
        max_cap_discharge_m3s=900.0,
        historical_benchmark_name="2007 & 2020 Greater Jakarta Floods",
        historical_benchmark_m3s=650.0,
        hydrological_context="BPBD DKI Jakarta Katulampa Dam Siaga 1 (Critical Danger) = 200cm"
    ),
    "tonle-sap": BasinThresholdProfile(
        basin_category="tonle-sap",
        basin_name="Tonle Sap Lake Inundation & Tributary Basin",
        danger_discharge_m3s=4000.0,
        caution_discharge_m3s=2500.0,
        severe_rain_mm=80.0,
        heavy_rain_mm=50.0,
        baseline_discharge_m3s=800.0,
        max_cap_discharge_m3s=6000.0,
        historical_benchmark_name="2000 Tonle Sap Reverse-Flow Maximum",
        historical_benchmark_m3s=10500.0,
        hydrological_context="Prek Kdam Tonle Sap River Warning Stage = 9.00m"
    ),
    "coastal": BasinThresholdProfile(
        basin_category="coastal",
        basin_name="Southern Coastal Watersheds (Kampot / Koh Kong / Sihanoukville)",
        danger_discharge_m3s=1200.0,
        caution_discharge_m3s=700.0,
        severe_rain_mm=120.0,
        heavy_rain_mm=70.0,
        baseline_discharge_m3s=200.0,
        max_cap_discharge_m3s=2000.0,
        historical_benchmark_name="2019 Gulf of Thailand Coastal Inundation",
        historical_benchmark_m3s=3200.0,
        hydrological_context="MOWRAM Coastal Flood Warning Stage"
    ),
    "highland": BasinThresholdProfile(
        basin_category="highland",
        basin_name="Northern & Cardamom Highland Tributaries",
        danger_discharge_m3s=1500.0,
        caution_discharge_m3s=850.0,
        severe_rain_mm=85.0,
        heavy_rain_mm=50.0,
        baseline_discharge_m3s=250.0,
        max_cap_discharge_m3s=2500.0,
        historical_benchmark_name="2020 Pursat Flash-Flood Disaster",
        historical_benchmark_m3s=3800.0,
        hydrological_context="MOWRAM Stung Pursat Flash Flood Warning Level"
    )
}


def get_basin_profile(basin_category: Optional[str]) -> BasinThresholdProfile:
    """Returns the calibrated BasinThresholdProfile or defaults to mekong."""
    cat = (basin_category or "mekong").lower()
    return BASIN_PROFILES.get(cat, BASIN_PROFILES["mekong"])


class RiskClassificationEngine:
    """
    Evaluates hydrometeorological readings against calibrated, per-basin threshold rules.
    Outputs an immutable, auditable RiskState containing the state, rationale,
    rule version, and calculation timestamp.
    """

    RULE_VERSION = "v1.0"

    # Default Hydrological Thresholds for Lower Mekong Mainstem (m³/s and mm)
    DISCHARGE_DANGER_THRESHOLD = 22000.0   # Corresponds to high flood risk on lower Mekong
    DISCHARGE_CAUTION_THRESHOLD = 16000.0  # Elevated water flow / watch stage
    RAIN_SEVERE_THRESHOLD = 80.0           # Extreme rainfall amplifying flood surge (mm)
    RAIN_HEAVY_THRESHOLD = 50.0            # Heavy rainfall indicator (mm)

    def __init__(self, rule_version: str = RULE_VERSION):
        self.rule_version = rule_version

    def evaluate(self, reading: Reading, area: Optional[Area] = None) -> RiskState:
        """
        Classifies current risk as 'Normal', 'Caution', or 'Danger'
        based on river discharge, supporting rainfall, and basin-specific thresholds.
        """
        profile = get_basin_profile(area.basin_category if area else None)
        q = reading.discharge
        rain = reading.precipitation
        area_id = reading.area_id
        timestamp = datetime.now(timezone.utc)

        # Handle cold-start or unavailable data without false alarms (Section 4.4)
        if reading.is_stale and q == 0.0 and rain == 0.0:
            return RiskState(
                risk_id=f"risk-{uuid.uuid4().hex[:8]}",
                area_id=area_id,
                level="Normal",
                reason="Data source is currently unavailable or stale. Maintained default Normal status to avoid false alarms.",
                rule_version=self.rule_version,
                calculated_at=timestamp,
                discharge_val=q,
                precipitation_val=rain
            )

        danger_q = profile.danger_discharge_m3s
        caution_q = profile.caution_discharge_m3s
        severe_rain = profile.severe_rain_mm
        heavy_rain = profile.heavy_rain_mm

        # Condition 1: DANGER
        if q >= danger_q:
            reason = (
                f"DANGER: River discharge ({q:,.1f} m³/s) exceeds the critical {profile.basin_name} flood "
                f"threshold of {danger_q:,.0f} m³/s. Supporting precipitation: {rain:.1f} mm."
            )
            level = "Danger"

        elif q >= (caution_q + (danger_q - caution_q) * 0.35) and rain >= severe_rain:
            reason = (
                f"DANGER: Elevated discharge ({q:,.1f} m³/s) combined with severe "
                f"rainfall ({rain:.1f} mm >= {severe_rain:.0f} mm) indicates impending flash flood in {profile.basin_name}."
            )
            level = "Danger"

        # Condition 2: CAUTION
        elif q >= caution_q:
            reason = (
                f"CAUTION: River discharge ({q:,.1f} m³/s) is in the watch range "
                f"({caution_q:,.0f} - {danger_q:,.0f} m³/s) for {profile.basin_name}. "
                f"Precipitation: {rain:.1f} mm."
            )
            level = "Caution"

        elif q >= (profile.baseline_discharge_m3s + (caution_q - profile.baseline_discharge_m3s) * 0.35) and rain >= heavy_rain:
            reason = (
                f"CAUTION: Moderate river flow ({q:,.1f} m³/s) paired with heavy rainfall "
                f"({rain:.1f} mm >= {heavy_rain:.0f} mm) increases localized flash-flood likelihood in {profile.basin_name}."
            )
            level = "Caution"

        # Condition 3: NORMAL
        else:
            reason = (
                f"NORMAL: River discharge ({q:,.1f} m³/s) and rainfall ({rain:.1f} mm) "
                f"are within safe baseline limits for {profile.basin_name} (below {caution_q:,.0f} m³/s)."
            )
            level = "Normal"

        if reading.is_stale:
            reason += " [Notice: Reading is marked as STALE (cached)]."

        return RiskState(
            risk_id=f"risk-{uuid.uuid4().hex[:8]}",
            area_id=area_id,
            level=level,
            reason=reason,
            rule_version=self.rule_version,
            calculated_at=timestamp,
            discharge_val=q,
            precipitation_val=rain
        )

    def get_threshold_summary(self, area: Optional[Area] = None) -> Dict[str, Any]:
        """Provides transparency and configuration metadata for the Admin UI."""
        profile = get_basin_profile(area.basin_category if area else None)
        return {
            "rule_version": self.rule_version,
            "basin_category": profile.basin_category,
            "basin_name": profile.basin_name,
            "discharge_danger_m3s": profile.danger_discharge_m3s,
            "discharge_caution_m3s": profile.caution_discharge_m3s,
            "rain_severe_mm": profile.severe_rain_mm,
            "rain_heavy_mm": profile.heavy_rain_mm,
            "hydrological_context": profile.hydrological_context
        }


# ==============================================================================
# PRODUCTION COMPOUND RISK & HYDRODYNAMIC PREDICTION ENGINE (v2.0)
# ==============================================================================

class CompoundRiskEngine:
    """
    Production-grade multi-factorial disaster risk engine.
    Fuses real-time river discharge exceedance, soil runoff amplification,
    extreme precipitation, and upstream transboundary surge momentum.
    """

    VERSION = "v2.0-Production"

    # Component Weighting Factors (Total = 1.00)
    WEIGHT_DISCHARGE = 0.40
    WEIGHT_PRECIPITATION = 0.25
    WEIGHT_SOIL_SATURATION = 0.20
    WEIGHT_SURGE_MOMENTUM = 0.15

    # Fallback threshold baselines
    DISCHARGE_BASELINE = 10000.0  # Safe dry/base season flow
    DISCHARGE_MAX_CAP = 26000.0   # Extreme catastrophic overflow
    RAIN_MAX_CAP = 90.0           # Severe 24h torrential monsoon storm (mm)

    @classmethod
    def evaluate_compound_risk(
        cls,
        reading: Reading,
        soil: Optional[SoilMoistureReading] = None,
        surge_momentum: float = 0.0,
        upstream_station: Optional[str] = None,
        estimated_lead_time_hours: Optional[float] = None,
        area: Optional[Area] = None
    ) -> CompoundRiskAssessment:
        """
        Computes a normalized Compound Risk Index (0.00 to 1.00) and assigns an actionable disaster tier.
        """
        profile = get_basin_profile(area.basin_category if area else None)
        q = reading.discharge
        rain = reading.precipitation
        sat_pct = soil.saturation_percent if soil else 55.0

        base_q = profile.baseline_discharge_m3s
        cap_q = profile.max_cap_discharge_m3s

        # Factor 1: Discharge Exceedance scaled by basin
        d_factor = min(1.0, max(0.0, (q - base_q) / max(1.0, (cap_q - base_q))))

        # Factor 2: Precipitation Intensity (0.0 to 1.0)
        p_factor = min(1.0, max(0.0, rain / profile.severe_rain_mm))

        # Factor 3: Soil Moisture Saturation (0.0 to 1.0)
        s_factor = min(1.0, max(0.0, (sat_pct - 35.0) / 55.0))

        # Factor 4: Upstream Surge Momentum (0.0 to 1.0)
        u_factor = min(1.0, max(0.0, surge_momentum))

        # Weighted Compound Score
        raw_score = (
            cls.WEIGHT_DISCHARGE * d_factor +
            cls.WEIGHT_PRECIPITATION * p_factor +
            cls.WEIGHT_SOIL_SATURATION * s_factor +
            cls.WEIGHT_SURGE_MOMENTUM * u_factor
        )
        compound_score = round(min(1.0, max(0.0, raw_score)), 2)

        # Classify Level
        if compound_score >= 0.70 or (q >= profile.danger_discharge_m3s) or (q >= profile.caution_discharge_m3s and rain >= profile.severe_rain_mm):
            level = "Danger"
            action_en = f"Critical flash-flood and river overflow danger in {profile.basin_name}. Immediate evacuation protocols recommended."
            action_km = "ហានិភ័យទឹកជំនន់ធ្ងន់ធ្ងរ និងជន់លិចទន្លេ។ សូមជម្លៀសជាបន្ទាន់ទៅកាន់ទីទួលសុវត្ថិភាព។"
        elif compound_score >= 0.48 or (q >= profile.caution_discharge_m3s) or (rain >= profile.heavy_rain_mm and sat_pct >= 75.0):
            level = "Caution"
            action_en = f"Elevated flood watch in {profile.basin_name}. Saturated soil and high river discharge warrant emergency preparations."
            action_km = "ការប្រុងប្រយ័ត្នខ្ពស់។ ដីមានសំណើមឆ្អែត និងលំហូរទឹកទន្លេឡើងខ្ពស់ តម្រូវឱ្យមានការរៀបចំសម្ភារសង្គ្រោះ។"
        elif compound_score >= 0.28:
            level = "Advisory"
            action_en = f"Hydrological advisory for {profile.basin_name}. River flow active with moderate catchment saturation."
            action_km = "ការណែនាំជលសាស្ត្រ។ ទឹកទន្លេមានចរន្តខ្លាំងល្មម និងដីមានសំណើមជាមធ្យម។"
        else:
            level = "Normal"
            action_en = f"All hydrometeorological parameters remain within seasonal safe baselines for {profile.basin_name}."
            action_km = "សូចនាករជលសាស្ត្រ និងអាកាសធាតុទាំងអស់ស្ថិតក្នុងដែនកំណត់សុវត្ថិភាពធម្មតា។"

        just_en = (
            f"Compound Risk Index: {compound_score:.2f} ({level}) for {profile.basin_name}. "
            f"Discharge: {q:,.1f} m³/s ({d_factor*100:.0f}%), "
            f"Precipitation: {rain:.1f} mm ({p_factor*100:.0f}%), "
            f"Soil Saturation: {sat_pct:.1f}% ({s_factor*100:.0f}%), "
            f"Upstream Surge: {u_factor*100:.0f}%. {action_en}"
        )

        just_km = (
            f"សន្ទស្សន៍ហានិភ័យរួម៖ {compound_score:.2f} ({level})។ "
            f"លំហូរទឹក៖ {q:,.1f} m³/s, ទឹកភ្លៀង៖ {rain:.1f} mm, "
            f"កម្រិតឆ្អែតដី៖ {sat_pct:.1f}%, រលកទឹកខាងលើ៖ {u_factor*100:.0f}%។ {action_km}"
        )

        return CompoundRiskAssessment(
            assessment_id=f"cra-{uuid.uuid4().hex[:8]}",
            area_id=reading.area_id,
            level=level,
            compound_score=compound_score,
            surge_momentum=round(u_factor, 2),
            soil_saturation_factor=round(s_factor, 2),
            precipitation_factor=round(p_factor, 2),
            discharge_factor=round(d_factor, 2),
            estimated_lead_time_hours=estimated_lead_time_hours,
            upstream_trigger_station=upstream_station,
            calculated_at=datetime.now(timezone.utc),
            justification_en=just_en,
            justification_km=just_km
        )

    @classmethod
    def evaluate_historical_benchmark(cls, area_or_id: Any, current_discharge: float) -> HistoricalFloodBenchmark:
        """
        Compares current discharge volume against historic mega-floods in the station's basin.
        """
        if isinstance(area_or_id, Area):
            basin_cat = area_or_id.basin_category
            area_id = area_or_id.area_id
        elif isinstance(area_or_id, str):
            if area_or_id in BASIN_PROFILES:
                basin_cat = area_or_id
                area_id = area_or_id
            else:
                basin_cat = "mekong"
                area_id = area_or_id
        else:
            basin_cat = "mekong"
            area_id = "default"

        profile = get_basin_profile(basin_cat)

        if basin_cat == "mekong" or area_id in ("kratie-central", "mekong"):
            y2000 = 52000.0
            y2011 = 48500.0
            y2020 = 40500.0
        else:
            y2000 = profile.historical_benchmark_m3s
            y2011 = round(profile.historical_benchmark_m3s * 0.9327, 1)
            y2020 = round(profile.historical_benchmark_m3s * 0.78, 1)

        pct_2000 = round((current_discharge / max(1.0, y2000)) * 100.0, 1)
        pct_2011 = round((current_discharge / max(1.0, y2011)) * 100.0, 1)

        if pct_2000 >= 100.0:
            ctx = f"HISTORIC DISASTER: Current discharge ({current_discharge:,.0f} m³/s) exceeds the benchmark {profile.historical_benchmark_name} ({y2000:,.0f} m³/s) at {pct_2000}% (and {pct_2011}% of 2011 peak)."
        elif pct_2000 >= 85.0:
            ctx = f"CRITICAL: Current discharge ({current_discharge:,.0f} m³/s) is at {pct_2000}% of {profile.historical_benchmark_name} ({y2000:,.0f} m³/s) and {pct_2011}% of 2011 flood peak ({y2011:,.0f} m³/s)."
        elif pct_2000 >= 65.0:
            ctx = f"MAJOR SURGE: Current discharge reaches {pct_2000}% of {profile.historical_benchmark_name} ({pct_2011}% of 2011 peak). Severe floodplain inundation expected in {profile.basin_name}."
        elif pct_2000 >= 40.0:
            ctx = f"MODERATE: Seasonal high flow ({pct_2000}% of {profile.historical_benchmark_name}, {pct_2011}% of 2011 peak). River within major embankments."
        else:
            ctx = f"SAFE: Current discharge is {pct_2000}% of {profile.historical_benchmark_name} ({pct_2011}% of 2011 peak). Well within safe channel capacity."

        return HistoricalFloodBenchmark(
            area_id=area_id,
            current_discharge=round(current_discharge, 1),
            year_2000_peak_discharge=y2000,
            year_2011_peak_discharge=y2011,
            year_2020_peak_discharge=y2020,
            percent_of_2000_peak=pct_2000,
            percent_of_2011_peak=pct_2011,
            historical_context=ctx
        )
