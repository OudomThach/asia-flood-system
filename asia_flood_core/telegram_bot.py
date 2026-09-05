"""
Interactive Telegram Bot Service for Cambodia Community Flash-Flood Alert Platform.
Lead & Author: Oudom Thach

Features:
- /start & /menu: Interactive location/province selection keyboard & basins
- Live GPS Location Sharing: Calculates nearest monitored province via Haversine distance
- Dual-language (Khmer & English) real-time hydrological telemetry & risk levels
- One-click Province Subscription: Saves user chat_id for automated disaster alerts
- Real-time 25-province status queries (/status <province>) and active alerts (/alerts)
"""

import os
import math
import time
import logging
import threading
from typing import Dict, Any, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from .models import Area, Reading, RiskState
from .storage import FloodDataRepository
from .risk_engine import RiskClassificationEngine
from .integration import FloodAlertPipeline
from .notifications import format_telegram_and_sms_messages

logger = logging.getLogger(__name__)

BASIN_CATEGORIES = {
    "mekong": {
        "title_km": "🌊 ទន្លេមេគង្គកម្ពុជា (Cambodia Mekong)",
        "title_en": "Cambodia Mekong Corridor",
        "station_ids": ["kratie-central", "stung-treng", "kampong-cham", "phnom-penh", "kandal", "prey-veng", "tbong-khmum"]
    },
    "tonle-sap": {
        "title_km": "🏞️ បឹងទន្លេសាប (Tonle Sap Basin)",
        "title_en": "Tonle Sap Basin",
        "station_ids": ["siem-reap", "battambang", "kampong-chhnang", "kampong-thom", "pursat", "banteay-meanchey"]
    },
    "coastal": {
        "title_km": "🏖️ តំបន់ឆ្នេរ (Coastal & Southern)",
        "title_en": "Coastal & Southern",
        "station_ids": ["kampot", "koh-kong", "preah-sihanouk", "kep", "takeo", "svay-rieng", "kampong-speu"]
    },
    "highland": {
        "title_km": "⛰️ តំបន់ខ្ពង់រាប (Highland Tributaries)",
        "title_en": "Highland Tributaries",
        "station_ids": ["preah-vihear", "oddar-meanchey", "ratanakiri", "mondulkiri", "pailin"]
    },
    "laos-southern": {
        "title_km": "🇱🇦 ឡាវខាងត្បូង (Southern Laos Inflow)",
        "title_en": "Southern Laos Inflow",
        "station_ids": ["laos-champasak", "laos-attapeu", "laos-sekong", "laos-salavan"]
    },
    "laos-central": {
        "title_km": "🇱🇦 ឡាវកណ្ដាល (Central Laos Corridor)",
        "title_en": "Central Laos Corridor",
        "station_ids": ["laos-savannakhet", "laos-khammouane", "laos-bolikhamsai", "laos-vientiane-cap", "laos-vientiane-prov"]
    },
    "laos-upper": {
        "title_km": "🇱🇦 ឡាវខាងជើង (Upper Laos Catchment)",
        "title_en": "Upper Laos Catchment",
        "station_ids": ["laos-luang-prabang", "laos-sayaboury", "laos-bokeo", "laos-luang-namtha", "laos-oudomxay", "laos-phongsaly", "laos-houaphanh", "laos-xiangkhouang", "laos-xaisomboun"]
    }
}


# ISO-3166 country display names for the browse-by-country picker (Asia-wide).
COUNTRY_NAMES = {
    "KH": "Cambodia", "LA": "Laos", "TH": "Thailand", "VN": "Vietnam", "MM": "Myanmar",
    "MY": "Malaysia", "SG": "Singapore", "BN": "Brunei", "ID": "Indonesia", "PH": "Philippines",
    "TL": "Timor-Leste", "CN": "China", "JP": "Japan", "KR": "South Korea", "KP": "North Korea",
    "MN": "Mongolia", "IN": "India", "BD": "Bangladesh", "PK": "Pakistan", "NP": "Nepal",
    "LK": "Sri Lanka", "BT": "Bhutan", "MV": "Maldives", "AF": "Afghanistan", "KZ": "Kazakhstan",
    "UZ": "Uzbekistan", "KG": "Kyrgyzstan", "TJ": "Tajikistan", "TM": "Turkmenistan", "IR": "Iran",
    "IQ": "Iraq", "TR": "Türkiye", "SY": "Syria", "LB": "Lebanon", "JO": "Jordan", "IL": "Israel",
    "SA": "Saudi Arabia", "YE": "Yemen", "OM": "Oman", "AE": "UAE", "QA": "Qatar", "BH": "Bahrain",
    "KW": "Kuwait", "GE": "Georgia", "AM": "Armenia", "AZ": "Azerbaijan", "CY": "Cyprus",
}


def country_flag(code: str) -> str:
    """Return the emoji flag for a 2-letter ISO country code (regional indicators)."""
    code = (code or "").upper()
    if len(code) != 2 or not code.isalpha():
        return "🏳️"
    return "".join(chr(0x1F1E6 + ord(c) - ord("A")) for c in code)


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates great-circle distance between two GPS coordinates in kilometers."""
    r = 6371.0  # Earth radius in kilometers
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def find_nearest_station(lat: float, lon: float, areas: List[Area]) -> Tuple[Area, float]:
    """Finds the closest Cambodian monitoring station to given GPS coordinates."""
    best_area = areas[0]
    min_dist = float("inf")
    for a in areas:
        dist = haversine_distance_km(lat, lon, a.latitude, a.longitude)
        if dist < min_dist:
            min_dist = dist
            best_area = a
    return best_area, round(min_dist, 1)


class InteractiveTelegramBot:
    def __init__(self, repo: FloodDataRepository, engine: RiskClassificationEngine, pipeline: FloodAlertPipeline):
        self.repo = repo
        self.engine = engine
        self.pipeline = pipeline
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.offset = 0
        self._running = False

    def send_api(self, method: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.token:
            return None
        try:
            resp = requests.post(f"{self.base_url}/{method}", json=payload, timeout=12)
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"Telegram API {method} error ({resp.status_code}): {resp.text}")
        except Exception as e:
            logger.warning(f"Telegram API request failed: {e}")
        return None

    def send_message(self, chat_id: int | str, text: str, reply_markup: Optional[Dict[str, Any]] = None, parse_mode: str = "HTML"):
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        return self.send_api("sendMessage", payload)

    def edit_message_text(self, chat_id: int | str, message_id: int, text: str, reply_markup: Optional[Dict[str, Any]] = None, parse_mode: str = "HTML"):
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        return self.send_api("editMessageText", payload)

    def answer_callback_query(self, callback_query_id: str, text: Optional[str] = None):
        payload = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
            payload["show_alert"] = False
        return self.send_api("answerCallbackQuery", payload)

    # --- Keyboards ---

    def get_main_reply_keyboard(self) -> Dict[str, Any]:
        """Persistent bottom keyboard — location-first, English-led (Asia-wide)."""
        return {
            "keyboard": [
                [{"text": "📍 Share My Location (find nearest station)", "request_location": True}],
                [{"text": "🗺️ Choose Station"}, {"text": "🚨 Active Alerts"}],
                [{"text": "⚠️ Nearby Hazards"}, {"text": "🔔 My Subscriptions"}],
                [{"text": "🧹 Reset / Clear"}, {"text": "ℹ️ Help"}]
            ],
            "resize_keyboard": True,
            "is_persistent": True
        }

    def get_basins_inline_keyboard(self) -> Dict[str, Any]:
        """Inline menu of the 4 Cambodian Hydrological Regions + Popular Quick Selects."""
        return {
            "inline_keyboard": [
                [
                    {"text": "🌊 ទន្លេមេគង្គ (Mekong)", "callback_data": "basin:mekong"},
                    {"text": "🏞️ បឹងទន្លេសាប (Tonle Sap)", "callback_data": "basin:tonle-sap"}
                ],
                [
                    {"text": "🏖️ តំបន់ឆ្នេរ (Coastal)", "callback_data": "basin:coastal"},
                    {"text": "⛰️ ខ្ពង់រាប (Highland)", "callback_data": "basin:highland"}
                ],
                [
                    {"text": "⚡ ក្រចេះ (Kratie)", "callback_data": "loc:kratie-central"},
                    {"text": "⚡ ស្ទឹងត្រែង (Stung Treng)", "callback_data": "loc:stung-treng"}
                ],
                [
                    {"text": "⚡ ភ្នំពេញ (Phnom Penh)", "callback_data": "loc:phnom-penh"},
                    {"text": "⚡ សៀមរាប (Siem Reap)", "callback_data": "loc:siem-reap"}
                ],
                [
                    {"text": "📋 បង្ហាញខេត្តទាំងអស់ទាំង ២៥ (All 25)", "callback_data": "list:all"}
                ]
            ]
        }

    def get_countries_keyboard(self, page: int = 0) -> Dict[str, Any]:
        """Paginated picker of every monitored country (Asia-wide), with station counts."""
        areas = self.repo.get_all_areas()
        counts: Dict[str, int] = {}
        for a in areas:
            counts[a.country] = counts.get(a.country, 0) + 1
        # Sort by station count desc, then name
        codes = sorted(counts.keys(), key=lambda c: (-counts[c], COUNTRY_NAMES.get(c, c)))

        per_page = 12
        total_pages = max(1, (len(codes) + per_page - 1) // per_page)
        page = max(0, min(page, total_pages - 1))
        subset = codes[page * per_page:(page + 1) * per_page]

        buttons, row = [], []
        for code in subset:
            label = f"{country_flag(code)} {COUNTRY_NAMES.get(code, code)} ({counts[code]})"
            row.append({"text": label, "callback_data": f"ctry:{code}:0"})
            if len(row) == 2:
                buttons.append(row); row = []
        if row:
            buttons.append(row)

        nav = []
        if page > 0:
            nav.append({"text": "⬅️ Prev", "callback_data": f"cpage:{page - 1}"})
        nav.append({"text": f"🌏 {page + 1}/{total_pages}", "callback_data": "noop"})
        if page < total_pages - 1:
            nav.append({"text": "Next ➡️", "callback_data": f"cpage:{page + 1}"})
        buttons.append(nav)
        buttons.append([{"text": "📍 Share My Location instead", "callback_data": "noop"}])
        return {"inline_keyboard": buttons}

    def get_country_stations_keyboard(self, code: str, page: int = 0) -> Dict[str, Any]:
        """Paginated list of the monitoring stations within one country."""
        stations = [a for a in self.repo.get_all_areas() if a.country == code]
        per_page = 8
        total_pages = max(1, (len(stations) + per_page - 1) // per_page)
        page = max(0, min(page, total_pages - 1))
        subset = stations[page * per_page:(page + 1) * per_page]

        buttons, row = [], []
        for a in subset:
            clean_en = a.name_en.split(" (")[0]
            row.append({"text": f"📍 {clean_en}", "callback_data": f"loc:{a.area_id}"})
            if len(row) == 2:
                buttons.append(row); row = []
        if row:
            buttons.append(row)

        nav = []
        if page > 0:
            nav.append({"text": "⬅️ Prev", "callback_data": f"cstn:{code}:{page - 1}"})
        nav.append({"text": f"📄 {page + 1}/{total_pages}", "callback_data": "noop"})
        if page < total_pages - 1:
            nav.append({"text": "Next ➡️", "callback_data": f"cstn:{code}:{page + 1}"})
        buttons.append(nav)
        buttons.append([{"text": "🔙 Back to Countries", "callback_data": "menu:countries"}])
        return {"inline_keyboard": buttons}

    def get_basin_stations_keyboard(self, basin_key: str) -> Dict[str, Any]:
        """Lists provinces for a selected hydrological basin."""
        info = BASIN_CATEGORIES.get(basin_key, BASIN_CATEGORIES["mekong"])
        all_areas = {a.area_id: a for a in self.repo.get_all_areas()}
        buttons = []
        row = []
        for sid in info["station_ids"]:
            a = all_areas.get(sid)
            if not a:
                continue
            clean_name = a.name_km.split(' (')[0]
            clean_en = a.name_en.split(' (')[0]
            row.append({"text": f"📍 {clean_name} ({clean_en})", "callback_data": f"loc:{a.area_id}"})
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([{"text": "🔙 ត្រឡប់ក្រោយ (Back to Basins)", "callback_data": "menu:basins"}])
        return {"inline_keyboard": buttons}

    def get_all_provinces_keyboard(self, page: int = 0) -> Dict[str, Any]:
        """Paginated list of all 25 provinces."""
        areas = self.repo.get_all_areas()
        per_page = 8
        total_pages = (len(areas) + per_page - 1) // per_page
        start = page * per_page
        subset = areas[start:start + per_page]

        buttons = []
        row = []
        for a in subset:
            clean_km = a.name_km.split(' (')[0]
            clean_en = a.name_en.split(' (')[0]
            row.append({"text": f"📍 {clean_km} ({clean_en})", "callback_data": f"loc:{a.area_id}"})
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)

        nav_row = []
        if page > 0:
            nav_row.append({"text": "⬅️ មុន", "callback_data": f"page:{page - 1}"})
        nav_row.append({"text": f"📄 {page + 1}/{total_pages}", "callback_data": "noop"})
        if page < total_pages - 1:
            nav_row.append({"text": "បន្ទាប់ ➡️", "callback_data": f"page:{page + 1}"})
        buttons.append(nav_row)
        buttons.append([{"text": "🔙 ត្រឡប់ក្រោយ (Back to Basins)", "callback_data": "menu:basins"}])
        return {"inline_keyboard": buttons}

    def get_station_action_keyboard(self, area_id: str, chat_id: int | str) -> Dict[str, Any]:
        """Action buttons attached to a province's flood status report."""
        user_subs = self.repo.get_user_subscriptions(str(chat_id))
        is_subbed = area_id in user_subs
        sub_text = "🔕 ឈប់តាមដាន (Unsubscribe)" if is_subbed else "🔔 តាមដានខេត្តនេះ (Subscribe Alert)"
        sub_action = f"unsub:{area_id}" if is_subbed else f"sub:{area_id}"

        return {
            "inline_keyboard": [
                [
                    {"text": sub_text, "callback_data": sub_action},
                    {"text": "🔄 ពិនិត្យឡើងវិញ (Refresh)", "callback_data": f"loc:{area_id}"}
                ],
                [
                    {"text": "📈 ការព្យាករណ៍ ៧ ថ្ងៃ (7-Day Forecast)", "callback_data": f"forecast:{area_id}"},
                    {"text": "📍 ជ្រើសរើសទីតាំងផ្សេង (Other Province)", "callback_data": "menu:basins"}
                ]
            ]
        }

    # --- Message Formatting ---

    def format_welcome_message(self, user_first_name: str = "") -> str:
        name_str = f" <b>{user_first_name}</b>" if user_first_name else ""
        return (
            f"🌊 <b>Asia Flash-Flood Early Warning System</b>\n"
            f"<i>112 river stations across 47 Asian countries · live hazards from USGS, NASA &amp; GDACS</i>\n\n"
            f"👋 Hi{name_str}! The fastest way to start: tap <b>📍 Share My Location</b> below — "
            f"I'll find your nearest monitored river station, its current flood risk, and any real hazards nearby.\n\n"
            f"You can also:\n"
            f"• 🗺️ <b>Choose Station</b> — browse by region\n"
            f"• 🚨 <b>Active Alerts</b> — everywhere on Caution/Danger now\n"
            f"• ⚠️ <b>Nearby Hazards</b> — live storms/quakes/floods affecting you\n"
            f"• 🧹 <b>Reset / Clear</b> — wipe your subscriptions &amp; start over\n\n"
            f"<i>ខ្មែរ៖ ចុច 📍 ដើម្បីស្វែងរកស្ថានីយ៍ជិតបំផុត។</i>"
        )

    def format_station_status(self, area: Area, extra_note: str = "") -> str:
        """Retrieves latest reading & risk state for area and formats detailed bilingual message."""
        reading = self.repo.get_latest_reading(area.area_id)
        risk = self.repo.get_latest_risk_state(area.area_id)

        if not reading or not risk:
            try:
                res = self.pipeline.run_cycle(area_id=area.area_id)
                reading = Reading(**res["reading"])
                risk = RiskState(**res["risk_state"])
            except Exception:
                reading = reading or Reading(
                    reading_id=f"fb-{area.area_id}",
                    area_id=area.area_id,
                    observed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    discharge=12000.0,
                    precipitation=0.0
                )
                risk = risk or self.engine.evaluate(reading)

        formatted = format_telegram_and_sms_messages(area, reading, risk)
        msg = formatted["message_km"] + "\n\n" + formatted["message_en"]
        if extra_note:
            msg = f"{extra_note}\n\n" + msg
        return msg

    def format_forecast_message(self, area: Area) -> str:
        """Builds a 7-day hydrological projection trajectory for Telegram."""
        reading = self.repo.get_latest_reading(area.area_id)
        base_dis = reading.discharge if reading else 12000.0
        multipliers = [1.0, 1.05, 1.10, 1.06, 0.98, 0.95, 0.92]
        lines = [
            f"📈 <b>ការព្យាករណ៍កម្ពស់ទឹក ៧ ថ្ងៃ (7-Day Forecast)</b>",
            f"📍 <b>ទីតាំង៖</b> {area.name_km} ({area.name_en})",
            f"🌊 <b>កម្រិតគ្រោះថ្នាក់ (Danger Threshold):</b> ≥ 22,000 m³/s\n"
        ]
        days_km = ["ថ្ងៃនេះ", "ថ្ងៃស្អែក", "+2 ថ្ងៃ", "+3 ថ្ងៃ", "+4 ថ្ងៃ", "+5 ថ្ងៃ", "+6 ថ្ងៃ"]
        for i, mult in enumerate(multipliers):
            d_val = round(base_dis * mult, 1)
            icon = "🔴" if d_val >= 22000 else ("🟡" if d_val >= 16000 else "🟢")
            lines.append(f"{icon} <b>{days_km[i]}:</b> <code>{d_val:,.1f} m³/s</code>")
        return "\n".join(lines)

    def format_hazard_pressure(self, area: Area) -> str:
        """Live flash-flood pressure near a station (real USGS/EONET/GDACS/FIRMS events)."""
        try:
            import asyncio
            from .live_hazards import LiveHazardIntake
            from .flood_linkage import station_flood_pressure
            if not hasattr(self, "_lh"):
                self._lh = LiveHazardIntake()
            events = asyncio.run(self._lh.get_live_hazards(days=7))
            p = station_flood_pressure(area.latitude, area.longitude, events)
            if not p["reasons"]:
                return "🟢 <b>No live hazards are contributing to flash-flood risk near you right now.</b>"
            pct = round(p["flood_pressure"] * 100)
            band = p["pressure_band"]
            icon = "🔴" if band == "HIGH" else ("🟡" if band == "ELEVATED" else "🟢")
            lines = [f"{icon} <b>Flash-flood pressure: {pct}% ({band})</b>",
                     "<i>Real events nearby that could worsen flooding:</i>"]
            for r in p["reasons"][:3]:
                lines.append(f"• {r['title']} — ~{r['distance_km']:.0f} km ({r['source']})\n   <i>{r['mechanism']}</i>")
            return "\n".join(lines)
        except Exception as e:
            logger.debug(f"hazard pressure text failed: {e}")
            return ""

    # --- Update Handlers ---

    def handle_message(self, message: Dict[str, Any]):
        chat_id = message.get("chat", {}).get("id")
        if not chat_id:
            return
        user_name = message.get("from", {}).get("first_name", "")
        text = (message.get("text") or "").strip()
        location = message.get("location")

        # 1. User shared GPS location
        if location:
            lat = location.get("latitude")
            lon = location.get("longitude")
            areas = self.repo.get_all_areas()
            if areas and lat and lon:
                nearest_area, dist_km = find_nearest_station(lat, lon, areas)
                note = f"🎯 <b>Nearest monitored station to you:</b> <b>{nearest_area.name_en}</b> (~{dist_km} km away)"
                msg = self.format_station_status(nearest_area, extra_note=note)
                hazard_txt = self.format_hazard_pressure(nearest_area)
                if hazard_txt:
                    msg += "\n\n" + hazard_txt
                kb = self.get_station_action_keyboard(nearest_area.area_id, chat_id)
                self.send_message(chat_id, msg, reply_markup=kb)
                return

        # 2. Text Commands
        if text.startswith("/start") or text.startswith("/menu") or text in ["ℹ️ Help", "ℹ️ ជំនួយ (Help)", "help", "menu", "hi", "hello", "Hello"]:
            msg = self.format_welcome_message(user_name)
            self.send_message(chat_id, "🌊 Welcome to the Asia Flash-Flood Early Warning System!", reply_markup=self.get_main_reply_keyboard())
            self.send_message(chat_id, msg, reply_markup=self.get_basins_inline_keyboard())

        elif text.startswith("/clear") or text.startswith("/reset") or text in ["🧹 Reset / Clear", "clear", "reset"]:
            self.handle_clear_command(chat_id)

        elif text.startswith("/hazards") or text.startswith("/nearby") or text in ["⚠️ Nearby Hazards", "hazards"]:
            self.handle_hazards_command(chat_id)

        elif text.startswith("/countries") or text.startswith("/location") or text.startswith("/provinces") or text in ["🗺️ Choose Station", "🗺️ ជ្រើសរើសខេត្ត (Choose Province)", "provinces", "location", "stations", "countries"]:
            msg = "🌏 <b>Choose a country</b> to browse its monitoring stations, or tap 📍 Share My Location for the nearest one:"
            self.send_message(chat_id, msg, reply_markup=self.get_countries_keyboard())

        elif text.startswith("/alerts") or text in ["🚨 Active Alerts", "🚨 ស្ថានភាពអាសន្ន (Active Alerts)"]:
            self.handle_alerts_command(chat_id)

        elif text in ["🔔 My Subscriptions", "🔔 ការចុះឈ្មោះរបស់ខ្ញុំ (My Subscriptions)", "/subscriptions"]:
            self.handle_subscriptions_command(chat_id)

        elif text.startswith("/status"):
            query = text[7:].strip().lower()
            if not query:
                msg = "📍 <b>សូមបញ្ជាក់ឈ្មោះខេត្ត ឧទាហរណ៍៖</b> <code>/status kratie</code> ឬ <code>/status stung treng</code>"
                self.send_message(chat_id, msg, reply_markup=self.get_basins_inline_keyboard())
            else:
                self.handle_status_search(chat_id, query)

        else:
            # Check if user typed province name directly (e.g. "Kratie", "Battambang", "ក្រចេះ")
            matched = self.search_area(text)
            if matched:
                msg = self.format_station_status(matched)
                kb = self.get_station_action_keyboard(matched.area_id, chat_id)
                self.send_message(chat_id, msg, reply_markup=kb)
            else:
                msg = (
                    f"❓ មិនស្គាល់ពាក្យបញ្ជា <b>'{text}'</b>\n\n"
                    f"📍 <b>សូមជ្រើសរើសខេត្តខាងក្រោម ឬផ្ញើទីតាំង GPS៖</b>"
                )
                self.send_message(chat_id, msg, reply_markup=self.get_basins_inline_keyboard())

    def handle_callback_query(self, callback: Dict[str, Any]):
        callback_id = callback.get("id")
        data = callback.get("data", "")
        message = callback.get("message", {})
        chat_id = message.get("chat", {}).get("id")
        msg_id = message.get("message_id")

        if not chat_id or not msg_id:
            return

        self.answer_callback_query(callback_id)

        if data == "menu:countries" or data == "menu:basins":
            msg = "🌏 <b>Choose a country</b> to browse its monitoring stations:"
            self.edit_message_text(chat_id, msg_id, msg, reply_markup=self.get_countries_keyboard())

        elif data.startswith("cpage:"):
            page = int(data.split(":", 1)[1]) if data.split(":", 1)[1].isdigit() else 0
            self.edit_message_text(chat_id, msg_id, "🌏 <b>Choose a country:</b>", reply_markup=self.get_countries_keyboard(page))

        elif data.startswith("ctry:"):
            parts = data.split(":")
            code = parts[1]
            page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            name = COUNTRY_NAMES.get(code, code)
            msg = f"{country_flag(code)} <b>{name}</b>\nSelect a monitoring station:"
            self.edit_message_text(chat_id, msg_id, msg, reply_markup=self.get_country_stations_keyboard(code, page))

        elif data.startswith("cstn:"):
            parts = data.split(":")
            code = parts[1]
            page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            name = COUNTRY_NAMES.get(code, code)
            msg = f"{country_flag(code)} <b>{name}</b>\nSelect a monitoring station:"
            self.edit_message_text(chat_id, msg_id, msg, reply_markup=self.get_country_stations_keyboard(code, page))

        elif data.startswith("basin:"):
            basin_key = data.split(":", 1)[1]
            info = BASIN_CATEGORIES.get(basin_key, BASIN_CATEGORIES["mekong"])
            msg = f"📍 <b>{info['title_km']}</b>\n<i>{info['title_en']}</i>\n\nសូមជ្រើសរើសស្ថានីយ៍ខេត្ត៖"
            self.edit_message_text(chat_id, msg_id, msg, reply_markup=self.get_basin_stations_keyboard(basin_key))

        elif data.startswith("list:all") or data.startswith("page:"):
            page = int(data.split(":", 1)[1]) if ":" in data and data.split(":", 1)[1].isdigit() else 0
            msg = "📋 <b>បញ្ជីស្ថានីយ៍ខេត្តទាំង ២៥ នៅកម្ពុជា៖</b>\n<i>All 25 Cambodian Monitored Stations:</i>"
            self.edit_message_text(chat_id, msg_id, msg, reply_markup=self.get_all_provinces_keyboard(page))

        elif data.startswith("loc:"):
            area_id = data.split(":", 1)[1]
            area = self.repo.get_area(area_id) or Area(area_id=area_id)
            msg = self.format_station_status(area)
            kb = self.get_station_action_keyboard(area.area_id, chat_id)
            self.edit_message_text(chat_id, msg_id, msg, reply_markup=kb)

        elif data.startswith("sub:"):
            area_id = data.split(":", 1)[1]
            area = self.repo.get_area(area_id) or Area(area_id=area_id)
            self.repo.add_subscription(str(chat_id), area_id)
            self.answer_callback_query(callback_id, f"✅ បានចុះឈ្មោះតាមដាន {area.name_km}!")
            msg = self.format_station_status(area, extra_note=f"✅ <b>អ្នកបានចុះឈ្មោះទទួលដំណឹងអាសន្នសម្រាប់ {area.name_km} ដោយជោគជ័យ!</b>\n<i>(You are now subscribed to real-time flood alerts for this province)</i>")
            kb = self.get_station_action_keyboard(area_id, chat_id)
            self.edit_message_text(chat_id, msg_id, msg, reply_markup=kb)

        elif data.startswith("unsub:"):
            area_id = data.split(":", 1)[1]
            area = self.repo.get_area(area_id) or Area(area_id=area_id)
            self.repo.remove_subscription(str(chat_id), area_id)
            self.answer_callback_query(callback_id, f"❌ បានឈប់តាមដាន {area.name_km}")
            msg = self.format_station_status(area, extra_note=f"ℹ️ <b>អ្នកបានឈប់តាមដានការប្រកាសអាសន្នសម្រាប់ {area.name_km}</b>\n<i>(Unsubscribed from this province)</i>")
            kb = self.get_station_action_keyboard(area_id, chat_id)
            self.edit_message_text(chat_id, msg_id, msg, reply_markup=kb)

        elif data.startswith("forecast:"):
            area_id = data.split(":", 1)[1]
            area = self.repo.get_area(area_id) or Area(area_id=area_id)
            msg = self.format_forecast_message(area)
            kb = {
                "inline_keyboard": [
                    [{"text": "⬅️ ត្រឡប់ទៅស្ថានភាពបច្ចុប្បន្ន", "callback_data": f"loc:{area_id}"}],
                    [{"text": "📍 ជ្រើសរើសខេត្តផ្សេង", "callback_data": "menu:basins"}]
                ]
            }
            self.edit_message_text(chat_id, msg_id, msg, reply_markup=kb)

    def handle_alerts_command(self, chat_id: int | str):
        areas = self.repo.get_all_areas()
        danger_list = []
        caution_list = []
        for a in areas:
            risk = self.repo.get_latest_risk_state(a.area_id)
            if not risk:
                continue
            lvl = (risk.level or "Normal").upper()
            if lvl == "DANGER":
                danger_list.append(a)
            elif lvl == "CAUTION":
                caution_list.append(a)

        if not danger_list and not caution_list:
            msg = (
                f"🟢 <b>ស្ថានភាពទឹកទន្លេទូទាំងប្រទេសធម្មតា</b>\n"
                f"<i>All 25 Cambodian monitoring stations are currently at NORMAL level.</i>\n\n"
                f"គ្មានការប្រកាសអាសន្នទឹកជំនន់នៅពេលនេះទេ។\n"
                f"No active flood alerts detected."
            )
            self.send_message(chat_id, msg, reply_markup=self.get_basins_inline_keyboard())
            return

        lines = ["🚨 <b>បញ្ជីខេត្តដែលមានការប្រកាសអាសន្នទឹកជំនន់បច្ចុប្បន្ន៖</b>\n"]
        buttons = []
        for a in danger_list:
            r = self.repo.get_latest_reading(a.area_id)
            dis = r.discharge if r else 0.0
            lines.append(f"🔴 <b>{a.name_km} ({a.name_en})</b>: <code>DANGER</code> ({dis:,.0f} m³/s)")
            buttons.append([{"text": f"🔴 ពិនិត្យ {a.name_km}", "callback_data": f"loc:{a.area_id}"}])
        for a in caution_list:
            r = self.repo.get_latest_reading(a.area_id)
            dis = r.discharge if r else 0.0
            lines.append(f"🟡 <b>{a.name_km} ({a.name_en})</b>: <code>CAUTION</code> ({dis:,.0f} m³/s)")
            buttons.append([{"text": f"🟡 ពិនិត្យ {a.name_km}", "callback_data": f"loc:{a.area_id}"}])
        buttons.append([{"text": "📍 ជ្រើសរើសទីតាំងផ្សេង", "callback_data": "menu:basins"}])
        self.send_message(chat_id, "\n".join(lines), reply_markup={"inline_keyboard": buttons})

    def handle_clear_command(self, chat_id: int | str):
        """Wipes the user's subscriptions and resets their session."""
        subs = self.repo.get_user_subscriptions(str(chat_id))
        for sid in subs:
            self.repo.remove_subscription(str(chat_id), sid)
        msg = (
            f"🧹 <b>All clear.</b>\n"
            f"Removed <b>{len(subs)}</b> subscription(s) and reset your session.\n\n"
            f"📍 Tap <b>Share My Location</b> or /start to begin again."
        )
        self.send_message(chat_id, msg, reply_markup=self.get_main_reply_keyboard())

    def handle_hazards_command(self, chat_id: int | str):
        """Shows live flash-flood hazards near the user's subscribed (or a default) station."""
        subs = self.repo.get_user_subscriptions(str(chat_id))
        if subs:
            area = self.repo.get_area(subs[0])
        else:
            area = self.repo.get_area("kratie-central") or (self.repo.get_all_areas() or [None])[0]
        if not area:
            self.send_message(chat_id, "📍 Please share your location first so I can find hazards near you.", reply_markup=self.get_main_reply_keyboard())
            return
        header = f"⚠️ <b>Live hazards near {area.name_en}</b>\n<i>(Share your location for hazards at your exact position.)</i>\n"
        hazard_txt = self.format_hazard_pressure(area) or "🟢 No live hazards contributing to flash-flood risk right now."
        self.send_message(chat_id, header + "\n" + hazard_txt, reply_markup=self.get_station_action_keyboard(area.area_id, chat_id))

    def handle_subscriptions_command(self, chat_id: int | str):
        sub_ids = self.repo.get_user_subscriptions(str(chat_id))
        if not sub_ids:
            msg = (
                f"ℹ️ <b>អ្នកមិនទាន់បានចុះឈ្មោះតាមដានខេត្តណាមួយនៅឡើយទេ។</b>\n\n"
                f"📍 <b>សូមជ្រើសរើសខេត្តខាងក្រោមដើម្បីចុះឈ្មោះទទួលដំណឹងអាសន្ន៖</b>"
            )
            self.send_message(chat_id, msg, reply_markup=self.get_basins_inline_keyboard())
            return

        areas = {a.area_id: a for a in self.repo.get_all_areas()}
        lines = ["🔔 <b>បញ្ជីខេត្តដែលអ្នកបានចុះឈ្មោះតាមដាន (Your Subscriptions):</b>\n"]
        buttons = []
        for sid in sub_ids:
            a = areas.get(sid)
            if a:
                lines.append(f"• <b>{a.name_km}</b> ({a.name_en})")
                buttons.append([{"text": f"📍 {a.name_km}", "callback_data": f"loc:{a.area_id}"}])
        buttons.append([{"text": "➕ បន្ថែមខេត្តផ្សេងទៀត", "callback_data": "menu:basins"}])
        self.send_message(chat_id, "\n".join(lines), reply_markup={"inline_keyboard": buttons})

    def handle_status_search(self, chat_id: int | str, query: str):
        matched = self.search_area(query)
        if matched:
            msg = self.format_station_status(matched)
            kb = self.get_station_action_keyboard(matched.area_id, chat_id)
            self.send_message(chat_id, msg, reply_markup=kb)
        else:
            msg = f"❓ រកមិនឃើញខេត្ត <b>'{query}'</b> ទេ។ សូមជ្រើសរើសពីបញ្ជីខាងក្រោម៖"
            self.send_message(chat_id, msg, reply_markup=self.get_basins_inline_keyboard())

    def search_area(self, text: str) -> Optional[Area]:
        q = text.lower().strip()
        areas = self.repo.get_all_areas()
        for a in areas:
            if q == a.area_id.lower() or q in a.name_en.lower() or q in a.name_km:
                return a
            clean_en = a.name_en.lower().split(" (")[0]
            if q == clean_en or q in clean_en:
                return a
        return None

    # --- Polling Worker ---

    def poll_once(self):
        """Fetches and processes pending Telegram updates via getUpdates."""
        if not self.token:
            return
        try:
            url = f"{self.base_url}/getUpdates?offset={self.offset}&timeout=15"
            resp = requests.get(url, timeout=20)
            if resp.status_code != 200:
                return
            data = resp.json()
            if not data.get("ok"):
                return
            updates = data.get("result", [])
            for u in updates:
                self.offset = max(self.offset, u["update_id"] + 1)
                if "message" in u:
                    self.handle_message(u["message"])
                elif "callback_query" in u:
                    self.handle_callback_query(u["callback_query"])
        except Exception as exc:
            logger.debug(f"Telegram poller exception: {exc}")

    def run_polling_loop(self):
        """Continuous background poller thread."""
        self._running = True
        logger.info("🤖 Cambodia Flood Alert Telegram Bot poller started.")
        while self._running:
            self.poll_once()
            time.sleep(0.5)

    def start_background_thread(self):
        t = threading.Thread(target=self.run_polling_loop, daemon=True, name="TelegramBotPoller")
        t.start()
        return t
