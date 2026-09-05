"""
Live Multi-Hazard Ingestion for the Pan-Asian Flood & Hazard Platform.

Pulls REAL, observed hazard events from public monitoring networks and normalizes
them into a single `HazardEvent` stream for the map overlay and the live feed panel.
No synthetic data — every event here is an actual detection from:

- USGS FDSN            -> earthquakes           (keyless)
- NASA EONET v3        -> cyclones/severe storms, volcanoes, wildfires, floods,
                          landslides, drought    (keyless)
- GDACS RSS            -> severity-graded alerts: EQ / TC / FL / VO / WF / DR (keyless)
- NASA FIRMS           -> satellite active fire detections (needs free FIRMS_MAP_KEY)

Results are TTL-cached (~10 min) so the dashboard's frequent polling does not hammer
the upstream services, and every source fails soft (an outage in one feed never breaks
the others or the page).
"""

import os
import csv
import time
import uuid
import logging
import asyncio
import io
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
import xml.etree.ElementTree as ET

import httpx

from .models import HazardEvent

logger = logging.getLogger(__name__)

# Asia bounding box (approx): covers Türkiye/Levant & Arabia in the west, Japan/Philippines
# in the east, Kazakhstan/Mongolia in the north, and Indonesia/Timor in the south.
# (min_lon, min_lat, max_lon, max_lat)
ASIA_BBOX: Tuple[float, float, float, float] = (25.0, -11.0, 150.0, 55.0)

_TYPE_COLORS = {
    "earthquake": "#f97316",
    "cyclone": "#a855f7",
    "volcano": "#ef4444",
    "wildfire": "#f59e0b",
    "flood": "#3b82f6",
    "landslide": "#b45309",
    "drought": "#eab308",
    "other": "#94a3b8",
}

# EONET category id -> our normalized event_type
_EONET_TYPE = {
    "severeStorms": "cyclone",
    "volcanoes": "volcano",
    "wildfires": "wildfire",
    "floods": "flood",
    "landslides": "landslide",
    "drought": "drought",
    "earthquakes": "earthquake",
}

# GDACS eventtype code -> normalized event_type
_GDACS_TYPE = {
    "EQ": "earthquake",
    "TC": "cyclone",
    "FL": "flood",
    "VO": "volcano",
    "WF": "wildfire",
    "DR": "drought",
}


def _severity_from_magnitude(event_type: str, mag: Optional[float]) -> str:
    """Normalize a numeric intensity into red/orange/green bands per hazard type."""
    if mag is None:
        return "info"
    if event_type == "earthquake":
        if mag >= 6.0:
            return "red"
        if mag >= 5.0:
            return "orange"
        return "green"
    if event_type == "cyclone":  # sustained wind km/h
        if mag >= 178.0:
            return "red"
        if mag >= 118.0:
            return "orange"
        return "green"
    return "info"


class LiveHazardIntake:
    """Fetches and unifies real hazard events across Asia. Instantiate once and reuse."""

    USGS_API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    EONET_URL = "https://eonet.gsfc.nasa.gov/api/v3/events/geojson"
    GDACS_RSS_URL = "https://www.gdacs.org/xml/rss.xml"
    FIRMS_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

    CACHE_TTL_SECONDS = 600  # 10 minutes
    HTTP_TIMEOUT = 15.0
    USER_AGENT = "Asia-Flood-MultiHazard-Platform/4.0"

    def __init__(self, bbox: Tuple[float, float, float, float] = ASIA_BBOX):
        self.bbox = bbox
        self._cache: List[HazardEvent] = []
        self._cache_ts: float = 0.0

    # ---- bbox helpers ---------------------------------------------------------
    def _in_bbox(self, lat: float, lon: float) -> bool:
        min_lon, min_lat, max_lon, max_lat = self.bbox
        return (min_lat <= lat <= max_lat) and (min_lon <= lon <= max_lon)

    def _is_asia(self, lat: float, lon: float) -> bool:
        """
        Inside the Asia bbox, minus the two corners the rectangle wrongly captures:
        - East/Horn of Africa (western-low corner)
        - Papua New Guinea / Oceania (eastern-southern corner).
        """
        if not self._in_bbox(lat, lon):
            return False
        if lon < 52.0 and lat < 12.0:      # NE / Horn of Africa (Ethiopia, Somalia, Kenya, Tanzania...)
            return False
        if lon > 141.0 and lat < 0.0:      # Papua New Guinea / western Pacific islands
            return False
        return True

    # ---- USGS earthquakes -----------------------------------------------------
    async def _fetch_usgs(self, client: httpx.AsyncClient, days: int, min_mag: float = 4.0) -> List[HazardEvent]:
        min_lon, min_lat, max_lon, max_lat = self.bbox
        params = {
            "format": "geojson",
            "starttime": (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d"),
            "minmagnitude": str(min_mag),
            "minlatitude": str(min_lat), "maxlatitude": str(max_lat),
            "minlongitude": str(min_lon), "maxlongitude": str(max_lon),
            "orderby": "time",
        }
        out: List[HazardEvent] = []
        resp = await client.get(self.USGS_API_URL, params=params)
        if resp.status_code != 200:
            return out
        for f in resp.json().get("features", []):
            props = f.get("properties", {}) or {}
            coords = (f.get("geometry", {}) or {}).get("coordinates", [0, 0, 0])
            lon, lat = float(coords[0]), float(coords[1])
            if not self._is_asia(lat, lon):
                continue
            mag = float(props.get("mag") or 0.0)
            t_ms = props.get("time")
            when = datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc) if t_ms else datetime.now(timezone.utc)
            out.append(HazardEvent(
                event_id=str(f.get("id") or f"usgs-{uuid.uuid4().hex[:8]}"),
                event_type="earthquake",
                title=props.get("place") or f"Earthquake ({lat:.2f}, {lon:.2f})",
                severity=_severity_from_magnitude("earthquake", mag),
                latitude=lat, longitude=lon,
                magnitude=mag, value_label=f"M{mag:.1f}",
                observed_at=when,
                source="USGS",
                url=props.get("url") or "",
            ))
        return out

    # ---- NASA EONET (multi-category) -----------------------------------------
    async def _fetch_eonet(self, client: httpx.AsyncClient, days: int) -> List[HazardEvent]:
        min_lon, min_lat, max_lon, max_lat = self.bbox
        # EONET bbox order: min-lon, max-lat, max-lon, min-lat
        params = {
            "status": "open",
            "days": str(days),
            "bbox": f"{min_lon},{max_lat},{max_lon},{min_lat}",
        }
        out: List[HazardEvent] = []
        resp = await client.get(self.EONET_URL, params=params)
        if resp.status_code != 200:
            return out
        for f in resp.json().get("features", []):
            props = f.get("properties", {}) or {}
            cats = props.get("categories", []) or []
            cat_id = cats[0].get("id") if cats else None
            etype = _EONET_TYPE.get(cat_id, "other")
            geom = f.get("geometry", {}) or {}
            lat, lon = self._last_point(geom.get("coordinates"), geom.get("type"))
            if lat is None or not self._is_asia(lat, lon):
                continue
            mag = props.get("magnitudeValue")
            mag = float(mag) if mag is not None else None
            unit = props.get("magnitudeUnit") or ""
            when = self._parse_iso(props.get("date"))
            sources = props.get("sources") or []
            link = props.get("link") or (sources[0].get("url") if sources else "")
            out.append(HazardEvent(
                event_id=str(props.get("id") or f.get("id") or f"eonet-{uuid.uuid4().hex[:8]}"),
                event_type=etype,
                title=props.get("title") or etype.title(),
                severity=_severity_from_magnitude(etype, mag),
                latitude=lat, longitude=lon,
                magnitude=mag,
                value_label=(f"{mag:g} {unit}".strip() if mag is not None else None),
                observed_at=when,
                source="NASA EONET",
                url=link or "",
            ))
        return out

    # ---- GDACS (severity-graded RSS) -----------------------------------------
    async def _fetch_gdacs(self, client: httpx.AsyncClient) -> List[HazardEvent]:
        out: List[HazardEvent] = []
        resp = await client.get(self.GDACS_RSS_URL)
        if resp.status_code != 200:
            return out
        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as exc:
            logger.warning(f"GDACS RSS parse failed: {exc}")
            return out

        def local(tag: str) -> str:
            return tag.rsplit("}", 1)[-1]  # strip namespace

        for item in root.iter():
            if local(item.tag) != "item":
                continue
            fields: Dict[str, str] = {}
            for child in item:
                fields[local(child.tag)] = (child.text or "").strip()
            etype = _GDACS_TYPE.get(fields.get("eventtype", ""), None)
            if etype is None:
                continue
            # GDACS carries coordinates in a "point" field as "lat lon" (space separated).
            try:
                parts = (fields.get("point") or "").split()
                lat, lon = float(parts[0]), float(parts[1])
            except (ValueError, IndexError):
                continue
            if not self._is_asia(lat, lon):
                continue
            alert = (fields.get("alertlevel", "") or "").lower()
            severity = alert if alert in ("red", "orange", "green") else "info"
            mag = None  # GDACS "severity" is a descriptive string (e.g. "Magnitude 5.5M, Depth:114km")
            when = self._parse_rss_date(fields.get("pubDate"))
            out.append(HazardEvent(
                event_id=fields.get("eventid") or fields.get("guid") or f"gdacs-{uuid.uuid4().hex[:8]}",
                event_type=etype,
                title=fields.get("title") or etype.title(),
                severity=severity,
                latitude=lat, longitude=lon,
                magnitude=mag,
                value_label=(fields.get("severity") or None),
                observed_at=when,
                source="GDACS",
                url=fields.get("link") or "",
            ))
        return out

    # ---- NASA FIRMS (active fires, key required) ------------------------------
    async def _fetch_firms(self, client: httpx.AsyncClient, days: int) -> List[HazardEvent]:
        key = os.getenv("FIRMS_MAP_KEY", "").strip()
        if not key:
            return []  # graceful: EONET still supplies wildfires
        min_lon, min_lat, max_lon, max_lat = self.bbox
        d = max(1, min(days, 10))  # FIRMS area API caps at 10 days
        # FIRMS area order: west,south,east,north
        url = f"{self.FIRMS_URL}/{key}/VIIRS_SNPP_NRT/{min_lon},{min_lat},{max_lon},{max_lat}/{d}"
        out: List[HazardEvent] = []
        resp = await client.get(url)
        if resp.status_code != 200 or resp.text.lstrip().lower().startswith("invalid"):
            return out
        reader = csv.DictReader(io.StringIO(resp.text))
        for i, row in enumerate(reader):
            if i >= 400:  # cap to keep the map/feed responsive
                break
            try:
                lat = float(row["latitude"]); lon = float(row["longitude"])
            except (KeyError, ValueError):
                continue
            if not self._is_asia(lat, lon):
                continue
            conf = (row.get("confidence") or "").lower()
            severity = "orange" if conf in ("h", "high") else "green"
            frp = row.get("frp")
            when = self._parse_firms_dt(row.get("acq_date"), row.get("acq_time"))
            out.append(HazardEvent(
                event_id=f"firms-{row.get('acq_date','')}-{lat:.3f}-{lon:.3f}",
                event_type="wildfire",
                title=f"Active fire detection ({lat:.2f}, {lon:.2f})",
                severity=severity,
                latitude=lat, longitude=lon,
                magnitude=(float(frp) if frp else None),
                value_label=(f"{frp} MW FRP" if frp else None),
                observed_at=when,
                source="NASA FIRMS",
                url="https://firms.modaps.eosdis.nasa.gov/map/",
            ))
        return out

    # ---- orchestration --------------------------------------------------------
    async def get_live_hazards(self, days: int = 7, force: bool = False) -> List[HazardEvent]:
        """Unified, de-duplicated, TTL-cached list of real hazard events across Asia."""
        now = time.time()
        if not force and self._cache and (now - self._cache_ts) < self.CACHE_TTL_SECONDS:
            return self._cache

        events: List[HazardEvent] = []
        async with httpx.AsyncClient(timeout=self.HTTP_TIMEOUT, headers={"User-Agent": self.USER_AGENT}) as client:
            results = await asyncio.gather(
                self._fetch_usgs(client, days),
                self._fetch_eonet(client, days),
                self._fetch_gdacs(client),
                self._fetch_firms(client, days),
                return_exceptions=True,
            )
        for r in results:
            if isinstance(r, Exception):
                logger.warning(f"A hazard feed failed: {r}")
                continue
            events.extend(r)

        events = self._dedupe(events)
        # Classify each event's flash-flood contribution role + mechanism (Phase 7).
        try:
            from .flood_linkage import annotate_events
            annotate_events(events)
        except Exception as exc:
            logger.warning(f"Flood-role annotation failed: {exc}")
        # Sort: severity first (red>orange>green>info), then most recent
        rank = {"red": 0, "orange": 1, "green": 2, "info": 3}
        events.sort(key=lambda e: (rank.get(e.severity, 4), -(e.observed_at.timestamp())))

        if events:  # only replace cache on a successful non-empty pull
            self._cache = events
            self._cache_ts = now
            return events
        return self._cache  # everything failed -> serve last good (possibly empty)

    def _dedupe(self, events: List[HazardEvent]) -> List[HazardEvent]:
        """Collapse the same physical event reported by multiple feeds (type + ~0.25° grid)."""
        seen: Dict[Tuple[str, int, int], HazardEvent] = {}
        rank = {"red": 0, "orange": 1, "green": 2, "info": 3}
        for e in events:
            key = (e.event_type, round(e.latitude * 4), round(e.longitude * 4))
            cur = seen.get(key)
            if cur is None or rank.get(e.severity, 4) < rank.get(cur.severity, 4):
                seen[key] = e
        return list(seen.values())

    # ---- output shapes --------------------------------------------------------
    async def to_geojson(self, days: int = 7) -> Dict[str, Any]:
        events = await self.get_live_hazards(days=days)
        features = [{
            "type": "Feature",
            "id": e.event_id,
            "geometry": {"type": "Point", "coordinates": [e.longitude, e.latitude]},
            "properties": {
                "event_type": e.event_type,
                "title": e.title,
                "severity": e.severity,
                "color": _TYPE_COLORS.get(e.event_type, _TYPE_COLORS["other"]),
                "magnitude": e.magnitude,
                "value_label": e.value_label,
                "observed_at": e.observed_at.isoformat(),
                "source": e.source,
                "url": e.url,
                "observed": True,
                "flood_role": e.flood_role,
                "mechanism": e.mechanism,
            },
        } for e in events]
        return {"type": "FeatureCollection", "features": features}

    async def to_feed(self, days: int = 7, limit: int = 100) -> Dict[str, Any]:
        events = await self.get_live_hazards(days=days)
        from collections import Counter
        by_type = Counter(e.event_type for e in events)
        by_source = Counter(e.source for e in events)
        return {
            "count": len(events),
            "by_type": dict(by_type),
            "by_source": dict(by_source),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "events": [e.model_dump() for e in events[:limit]],
        }

    # ---- small parsers --------------------------------------------------------
    @staticmethod
    def _last_point(coords: Any, gtype: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
        """Return (lat, lon) from an EONET geometry; storms are tracks (take the latest point)."""
        try:
            if gtype == "Point":
                return float(coords[1]), float(coords[0])
            if gtype in ("LineString", "MultiPoint"):
                last = coords[-1]
                return float(last[1]), float(last[0])
            if gtype in ("Polygon",):
                last = coords[0][-1]
                return float(last[1]), float(last[0])
        except (TypeError, IndexError, ValueError):
            pass
        return None, None

    @staticmethod
    def _parse_iso(s: Optional[str]) -> datetime:
        if not s:
            return datetime.now(timezone.utc)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return datetime.now(timezone.utc)

    @staticmethod
    def _parse_rss_date(s: Optional[str]) -> datetime:
        if not s:
            return datetime.now(timezone.utc)
        for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S GMT"):
            try:
                dt = datetime.strptime(s.strip(), fmt)
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return datetime.now(timezone.utc)

    @staticmethod
    def _parse_firms_dt(d: Optional[str], t: Optional[str]) -> datetime:
        try:
            hhmm = (t or "0").zfill(4)
            return datetime.strptime(f"{d} {hhmm}", "%Y-%m-%d %H%M").replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            return datetime.now(timezone.utc)
