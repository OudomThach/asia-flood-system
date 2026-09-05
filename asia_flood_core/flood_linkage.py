"""
Flash-Flood Contribution Model.

The platform is a *flash-flood* early-warning system, so a raw hazard feed is not enough:
each real hazard must be judged by HOW it contributes to a flash flood, and linked back to
the monitoring stations. This module does exactly that — no fabricated discharge numbers,
just a transparent role + a 0..1 contribution score driven by hazard type, severity, and
proximity (to the station, and for earthquakes, to an upstream mega-dam).

Roles:
- PRIMARY_DRIVER      : directly generates the flood water (cyclones/storms, floods, GLOF).
- COMPOUNDING_TRIGGER : indirectly causes a surge (quake near an upstream dam, landslide
                        river-damming, volcano+rain lahar) — proximity-gated.
- ANTECEDENT_AMPLIFIER: makes the *next* rain worse (wildfire burn scars, drought).
- NOT_FLOOD_RELEVANT  : everything else (a desert earthquake, dust/haze, ...). Score 0.

Design decision (locked with the user): this NEVER overrides the hydrological
Normal/Caution/Danger level. It only produces an annotation + compound "flash-flood
pressure" score with human-readable reasons.
"""

import math
from typing import List, Dict, Any, Tuple, Optional

from .models import HazardEvent

# Upstream Asian mega-dams whose seismic failure could send a catastrophic surge downstream.
# Used to decide whether a real earthquake is a flash-flood threat (proximity-gated).
_DAMS = [
    {"name": "Three Gorges Mega-Dam (Yangtze, China)", "lat": 30.827, "lon": 111.001},
    {"name": "Baihetan Mega-Dam (Jinsha, China)", "lat": 27.224, "lon": 102.901},
    {"name": "Tarbela Mega-Dam (Indus, Pakistan)", "lat": 34.088, "lon": 72.699},
    {"name": "Tehri Mega-Dam (Ganges/Bhagirathi, India)", "lat": 30.378, "lon": 78.480},
    {"name": "Nuozhadu Mega-Dam (Lancang/Mekong, Yunnan)", "lat": 22.642, "lon": 100.435},
    {"name": "Xiaowan Dam (Lancang/Mekong, Yunnan)", "lat": 24.702, "lon": 100.091},
    {"name": "Xayaburi Dam (Mekong, Laos)", "lat": 19.246, "lon": 101.815},
    {"name": "Don Sahong Dam (Laos/Cambodia border)", "lat": 13.947, "lon": 105.952},
    {"name": "Nam Theun 2 (Laos)", "lat": 17.996, "lon": 104.972},
    {"name": "Bhumibol Dam (Chao Phraya/Ping, Thailand)", "lat": 17.243, "lon": 98.972},
    {"name": "Hoa Binh Dam (Red River/Black River, Vietnam)", "lat": 20.816, "lon": 105.318},
    {"name": "San Roque Dam (Agno River, Philippines)", "lat": 16.148, "lon": 120.686},
]

ROLE_PRIMARY = "PRIMARY_DRIVER"
ROLE_COMPOUND = "COMPOUNDING_TRIGGER"
ROLE_ANTECEDENT = "ANTECEDENT_AMPLIFIER"
ROLE_NONE = "NOT_FLOOD_RELEVANT"

# Base weight of each role toward flash-flood pressure.
_ROLE_WEIGHT = {ROLE_PRIMARY: 1.0, ROLE_COMPOUND: 0.6, ROLE_ANTECEDENT: 0.3, ROLE_NONE: 0.0}

# Severity multiplier.
_SEV_MULT = {"red": 1.0, "orange": 0.66, "green": 0.33, "info": 0.25}

# How far each hazard type can still influence a station (km) — a rain-bearing cyclone reaches
# much farther than a localized landslide.
_INFLUENCE_KM = {
    "cyclone": 600.0,
    "flood": 400.0,
    "earthquake": 300.0,
    "volcano": 200.0,
    "landslide": 200.0,
    "wildfire": 250.0,
    "drought": 350.0,
    "other": 0.0,
}

# An earthquake only threatens flooding if it is close to an upstream mega-dam.
_QUAKE_DAM_THREAT_KM = 150.0
_QUAKE_MIN_MAG = 5.5


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _nearest_dam(lat: float, lon: float) -> Tuple[Optional[dict], float]:
    best, best_d = None, float("inf")
    for d in _DAMS:
        dist = _haversine(lat, lon, d["lat"], d["lon"])
        if dist < best_d:
            best, best_d = d, dist
    return best, round(best_d, 1)


def classify_flood_role(event: HazardEvent) -> Tuple[str, str]:
    """Return (flood_role, mechanism) for a hazard event, from a flash-flood standpoint."""
    et = event.event_type

    if et == "cyclone":
        return ROLE_PRIMARY, "Tropical cyclone / severe storm — torrential rainfall drives direct runoff and river surge."
    if et == "flood":
        return ROLE_PRIMARY, "Active flood already reported here — direct downstream inundation risk."
    if et == "landslide":
        return ROLE_COMPOUND, "Landslide can dam a valley or unleash a debris flow, producing a sudden outburst surge."
    if et == "volcano":
        return ROLE_COMPOUND, "Volcanic ash + rain can mobilise into a lahar (mudflow) that chokes and floods river channels."
    if et == "wildfire":
        return ROLE_ANTECEDENT, "Burn scars leave hydrophobic soil — the next rain runs off almost instantly, amplifying flash floods for weeks."
    if et == "drought":
        return ROLE_ANTECEDENT, "Drought-hardened, crusted soil sheds rain instead of absorbing it, making the next storm flashier."
    if et == "earthquake":
        dam, dist = _nearest_dam(event.latitude, event.longitude)
        if dam and dist <= _QUAKE_DAM_THREAT_KM and (event.magnitude or 0.0) >= _QUAKE_MIN_MAG:
            return ROLE_COMPOUND, f"M{event.magnitude:.1f} quake {dist:.0f} km from {dam['name']} — dam breach could release a catastrophic downstream surge."
        return ROLE_NONE, "Earthquake not near an upstream dam or major river — no direct flash-flood pathway."
    return ROLE_NONE, "Not a recognised flash-flood driver."


def annotate_events(events: List[HazardEvent]) -> List[HazardEvent]:
    """Set flood_role + mechanism on each event in place; returns the same list."""
    for e in events:
        role, mech = classify_flood_role(e)
        e.flood_role = role
        e.mechanism = mech
    return events


def contribution_score(event: HazardEvent, station_lat: float, station_lon: float) -> float:
    """0..1 contribution of this event to the given station's flash-flood risk."""
    role = event.flood_role or classify_flood_role(event)[0]
    base = _ROLE_WEIGHT.get(role, 0.0)
    if base <= 0.0:
        return 0.0
    reach = _INFLUENCE_KM.get(event.event_type, 0.0)
    if reach <= 0.0:
        return 0.0
    dist = _haversine(station_lat, station_lon, event.latitude, event.longitude)
    if dist >= reach:
        return 0.0
    proximity = 1.0 - (dist / reach)          # linear decay to 0 at the influence radius
    sev = _SEV_MULT.get(event.severity, 0.25)
    return round(min(1.0, base * sev * proximity), 3)


def station_flood_pressure(station_lat: float, station_lon: float, events: List[HazardEvent],
                           top_n: int = 4) -> Dict[str, Any]:
    """
    Aggregate contributing hazards into a single 0..1 flash-flood pressure for a station,
    with the top human-readable reasons. Uses a saturating (probabilistic-OR) combine so
    several moderate hazards don't overflow past 1.0.
    """
    scored = []
    for e in events:
        s = contribution_score(e, station_lat, station_lon)
        if s > 0.0:
            dist = round(_haversine(station_lat, station_lon, e.latitude, e.longitude), 0)
            scored.append((s, dist, e))
    scored.sort(key=lambda x: x[0], reverse=True)

    # saturating combine: 1 - product(1 - s_i)
    prod = 1.0
    for s, _, _ in scored:
        prod *= (1.0 - s)
    pressure = round(1.0 - prod, 3)

    from collections import Counter
    by_role = Counter(e.flood_role for _, _, e in scored)
    reasons = [{
        "event_type": e.event_type,
        "flood_role": e.flood_role,
        "severity": e.severity,
        "distance_km": dist,
        "score": s,
        "mechanism": e.mechanism,
        "title": e.title,
        "source": e.source,
    } for s, dist, e in scored[:top_n]]

    if pressure >= 0.66:
        band = "HIGH"
    elif pressure >= 0.33:
        band = "ELEVATED"
    elif pressure > 0.0:
        band = "LOW"
    else:
        band = "NONE"

    return {
        "flood_pressure": pressure,
        "pressure_band": band,
        "contributing_count": len(scored),
        "by_role": dict(by_role),
        "reasons": reasons,
    }


def group_by_role(events: List[HazardEvent]) -> Dict[str, List[HazardEvent]]:
    """Bucket events by flood role for the UI (all kept visible)."""
    groups: Dict[str, List[HazardEvent]] = {ROLE_PRIMARY: [], ROLE_COMPOUND: [], ROLE_ANTECEDENT: [], ROLE_NONE: []}
    for e in events:
        groups.setdefault(e.flood_role or ROLE_NONE, groups[ROLE_NONE]).append(e)
    return groups
