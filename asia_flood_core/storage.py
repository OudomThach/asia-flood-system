import os
import time
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Dict, Any, Callable

from .models import Area, Reading, RiskState, SoilMoistureReading
from .risk_engine import RiskClassificationEngine


DEFAULT_DB_PATH = os.getenv("FLOOD_DB_PATH", os.getenv("CAMBODIA_FLOOD_DB_PATH", "asia_flood.db"))


class FloodDataRepository:
    """
    Thread-safe SQLite repository for hydro-meteorological data persistence.
    Provides WAL mode, multi-tier caching (in-memory hot cache + SQLite secondary), and
    atomic bulk transactions for high-throughput station synchronization.
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path if db_path is not None else DEFAULT_DB_PATH
        self._shared_conn = None
        # High-Performance In-Memory Hot Caches
        self._latest_reading_cache: Dict[str, Reading] = {}
        self._latest_risk_cache: Dict[str, RiskState] = {}
        self._areas_cache: Optional[List[Area]] = None
        self._ttl_cache: Dict[str, Tuple[float, Any]] = {}

        if self.db_path == ":memory:":
            self._shared_conn = sqlite3.connect(":memory:")
            self._shared_conn.row_factory = sqlite3.Row
        self._init_db()
        self._load_hot_cache()

    def _get_connection(self) -> sqlite3.Connection:
        if self._shared_conn is not None:
            return self._shared_conn
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def _connection(self):
        """
        Yields a connection and commits on success. For the file-backed case,
        a fresh sqlite3.Connection is opened per call and MUST be explicitly closed here --
        sqlite3.Connection's own context manager only commits/rolls back, it never closes,
        so relying on `with self._connection() as conn:` alone leaks a connection/file
        handle on every call under the 25-province concurrent sync.
        """
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        finally:
            if self._shared_conn is None:
                conn.close()

    def close(self):
        if self._shared_conn:
            self._shared_conn.close()
            self._shared_conn = None

    def _init_db(self):
        """Creates tables, enables WAL performance mode, creates indexes, and seeds all 43 stations (25 Cambodia + 18 Laos)."""
        with self._connection() as conn:
            cursor = conn.cursor()
            
            # Storage Optimization Pragmas (High Performance & Low Disk I/O)
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("PRAGMA synchronous=NORMAL;")
            cursor.execute("PRAGMA cache_size = -64000;")  # 64MB cache
            cursor.execute("PRAGMA temp_store = MEMORY;")

            # Area Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS areas (
                    area_id TEXT PRIMARY KEY,
                    name_en TEXT NOT NULL,
                    name_km TEXT NOT NULL,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    country TEXT DEFAULT 'KH',
                    basin_category TEXT DEFAULT 'mekong',
                    language TEXT DEFAULT 'en',
                    utc_offset_hours REAL DEFAULT 7.0,
                    timezone_name TEXT DEFAULT 'Asia/Phnom_Penh',
                    active INTEGER DEFAULT 1
                )
            """)

            # Automatic Column Migration for existing databases
            cursor.execute("PRAGMA table_info(areas);")
            columns = [row["name"] for row in cursor.fetchall()]
            if "country" not in columns:
                cursor.execute("ALTER TABLE areas ADD COLUMN country TEXT DEFAULT 'KH';")
            if "basin_category" not in columns:
                cursor.execute("ALTER TABLE areas ADD COLUMN basin_category TEXT DEFAULT 'mekong';")
            if "language" not in columns:
                cursor.execute("ALTER TABLE areas ADD COLUMN language TEXT DEFAULT 'en';")
            if "utc_offset_hours" not in columns:
                cursor.execute("ALTER TABLE areas ADD COLUMN utc_offset_hours REAL DEFAULT 7.0;")
            if "timezone_name" not in columns:
                cursor.execute("ALTER TABLE areas ADD COLUMN timezone_name TEXT DEFAULT 'Asia/Phnom_Penh';")

            # Reading Table (FR2)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS readings (
                    reading_id TEXT PRIMARY KEY,
                    area_id TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    discharge REAL NOT NULL,
                    precipitation REAL NOT NULL,
                    source TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    is_stale INTEGER DEFAULT 0,
                    FOREIGN KEY(area_id) REFERENCES areas(area_id)
                )
            """)

            # RiskState Table (FR3)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS risk_states (
                    risk_id TEXT PRIMARY KEY,
                    area_id TEXT NOT NULL,
                    level TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    rule_version TEXT NOT NULL,
                    calculated_at TEXT NOT NULL,
                    discharge_val REAL,
                    precipitation_val REAL,
                    FOREIGN KEY(area_id) REFERENCES areas(area_id)
                )
            """)

            # Telegram Bot Subscriptions Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    chat_id TEXT NOT NULL,
                    area_id TEXT NOT NULL,
                    subscribed_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, area_id),
                    FOREIGN KEY(area_id) REFERENCES areas(area_id)
                )
            """)

            # Audit & Historical Event Timeline Table (FR2 Provenance & Chronological Tracking)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_events (
                    event_id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    area_id TEXT,
                    timestamp_utc TEXT NOT NULL,
                    timestamp_ict TEXT NOT NULL,
                    timestamp_local TEXT,
                    details_json TEXT NOT NULL
                )
            """)
            cursor.execute("PRAGMA table_info(audit_events);")
            audit_cols = [row["name"] for row in cursor.fetchall()]
            if "timestamp_local" not in audit_cols:
                cursor.execute("ALTER TABLE audit_events ADD COLUMN timestamp_local TEXT;")

            # User Alert Dispatch & Rate Limit Cooldown Log (Anti-Spam per User / Channel)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_alert_logs (
                    alert_id TEXT PRIMARY KEY,
                    recipient_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    area_id TEXT NOT NULL,
                    alert_level TEXT NOT NULL,
                    dispatched_at_utc TEXT NOT NULL,
                    dispatched_at_ict TEXT NOT NULL,
                    dispatched_at_local TEXT,
                    status TEXT NOT NULL,
                    details_json TEXT
                )
            """)
            cursor.execute("PRAGMA table_info(user_alert_logs);")
            alert_cols = [row["name"] for row in cursor.fetchall()]
            if "dispatched_at_local" not in alert_cols:
                cursor.execute("ALTER TABLE user_alert_logs ADD COLUMN dispatched_at_local TEXT;")

            # Soil Moisture Saturation Table (Production Hydrometeorological Telemetry)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS soil_moisture_readings (
                    area_id TEXT PRIMARY KEY,
                    observed_at TEXT NOT NULL,
                    moisture_surface REAL NOT NULL,
                    moisture_rootzone REAL NOT NULL,
                    saturation_percent REAL NOT NULL,
                    runoff_coefficient REAL NOT NULL,
                    source TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(area_id) REFERENCES areas(area_id)
                )
            """)

            # High-Performance Indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_readings_area_fetched ON readings(area_id, fetched_at DESC);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_risk_states_area_calc ON risk_states(area_id, calculated_at DESC);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_subs_area ON subscriptions(area_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_areas_country ON areas(country, active);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_events(timestamp_utc DESC);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_area ON audit_events(area_id, timestamp_utc DESC);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_alert_recip ON user_alert_logs(recipient_id, area_id, dispatched_at_utc DESC);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_alert_time ON user_alert_logs(dispatched_at_utc DESC);")

            # All 63 Monitored Stations (25 Cambodia + 18 Laos + 20 Pan-Asian Flagship Basins across 12 Countries)
            # Tuple: (area_id, name_en, name_km, latitude, longitude, country, basin_category, language, utc_offset_hours, timezone_name, active)
            areas_seed = [
                # === CAMBODIA (25 PROVINCES & MUNICIPALITIES) ===
                # 1. Mekong & Major Confluence Stations
                ('kratie-central', 'Kratie (Mekong Station)', 'ក្រុងក្រចេះ (ទន្លេមេគង្គ)', 12.4888, 106.0188, 'KH', 'mekong', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('stung-treng', 'Stung Treng (Mekong-Sekong Confluence)', 'ស្ទឹងត្រែង (មេគង្គ-សេកុង)', 13.5259, 105.9683, 'KH', 'mekong', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('kampong-cham', 'Kampong Cham (Mekong Station)', 'កំពង់ចាម (ទន្លេមេគង្គ)', 11.9924, 105.4645, 'KH', 'mekong', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('phnom-penh', 'Phnom Penh (Chaktomuk Confluence)', 'រាជធានីភ្នំពេញ (ទន្លេចតុមុខ)', 11.5564, 104.9282, 'KH', 'mekong', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('kandal', 'Kandal (Koh Khel - Bassac River)', 'កណ្ដាល (កោះខែល - ទន្លេបាសាក់)', 11.2667, 105.0333, 'KH', 'mekong', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('prey-veng', 'Prey Veng (Neak Loeung - Mekong)', 'ព្រៃវែង (អ្នកលឿង - មេគង្គ)', 11.2581, 105.2819, 'KH', 'mekong', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('tbong-khmum', 'Tbong Khmum (Suong Station)', 'ត្បូងឃ្មុំ (ក្រុងសួង)', 11.9167, 105.6500, 'KH', 'mekong', 'km', 7.0, 'Asia/Phnom_Penh', 1),

                # 2. Tonle Sap Lake & Surrounding Basin
                ('siem-reap', 'Siem Reap (Tonle Sap Basin)', 'សៀមរាប (បឹងទន្លេសាប)', 13.3671, 103.8448, 'KH', 'tonle-sap', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('battambang', 'Battambang (Sangkae River)', 'បាត់ដំបង (ស្ទឹងសង្កែ)', 13.0957, 103.2022, 'KH', 'tonle-sap', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('kampong-chhnang', 'Kampong Chhnang (Tonle Sap River)', 'កំពង់ឆ្នាំង (ទន្លេសាប)', 12.2500, 104.6667, 'KH', 'tonle-sap', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('kampong-thom', 'Kampong Thom (Stung Sen River)', 'កំពង់ធំ (ស្ទឹងសែន)', 12.7111, 104.8887, 'KH', 'tonle-sap', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('pursat', 'Pursat (Stung Pursat Basin)', 'ពោធិ៍សាត់ (ស្ទឹងពោធិ៍សាត់)', 12.5388, 103.9192, 'KH', 'tonle-sap', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('banteay-meanchey', 'Banteay Meanchey (Stung Sisophon)', 'បន្ទាយមានជ័យ (ស្ទឹងសិរីសោភ័ណ)', 13.5859, 102.9737, 'KH', 'tonle-sap', 'km', 7.0, 'Asia/Phnom_Penh', 1),

                # 3. Southern & Coastal Estuaries
                ('kampot', 'Kampot (Prek Tuek Chhu)', 'កំពត (ព្រែកទឹកឈូ)', 10.6104, 104.1815, 'KH', 'coastal', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('koh-kong', 'Koh Kong (Prek Kaoh Pao Estuary)', 'កោះកុង (ព្រែកកោះប៉ោ)', 11.6153, 102.9838, 'KH', 'coastal', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('preah-sihanouk', 'Preah Sihanouk (Coastal Watershed)', 'ព្រះសីហនុ (តំបន់ឆ្នេរ)', 10.6253, 103.5234, 'KH', 'coastal', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('kep', 'Kep (Coastal Bay Basin)', 'កែប (ឆ្នេរសមុទ្រកែប)', 10.4829, 104.2944, 'KH', 'coastal', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('takeo', 'Takeo (Angkor Borey Floodplain)', 'តាកែវ (អង្គរបុរី)', 10.9908, 104.7848, 'KH', 'highland', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('svay-rieng', 'Svay Rieng (Waiko River Basin)', 'ស្វាយរៀង (ស្ទឹងវ៉ៃកូ)', 11.0879, 105.7993, 'KH', 'highland', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('kampong-speu', 'Kampong Speu (Stung Prek Thnot)', 'កំពង់ស្ពឺ (ស្ទឹងព្រែកត្នោត)', 11.4533, 104.5209, 'KH', 'highland', 'km', 7.0, 'Asia/Phnom_Penh', 1),

                # 4. Northern & Highland Tributaries
                ('preah-vihear', 'Preah Vihear (Stung Sen Upper)', 'ព្រះវិហារ (ស្ទឹងសែនលើ)', 13.8073, 104.9805, 'KH', 'highland', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('oddar-meanchey', 'Oddar Meanchey (Stung Sreng Basin)', 'ឧត្តរមានជ័យ (ស្ទឹងស្រែង)', 14.1818, 103.5176, 'KH', 'highland', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('ratanakiri', 'Ratanakiri (Sesan River Basin)', 'រតនគិរី (ទន្លេសេសាន)', 13.7394, 106.9873, 'KH', 'highland', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('mondulkiri', 'Mondulkiri (Srepok River Basin)', 'មណ្ឌលគិរី (ទន្លេស្រែពក)', 12.4558, 107.1881, 'KH', 'highland', 'km', 7.0, 'Asia/Phnom_Penh', 1),
                ('pailin', 'Pailin (Cardamom Headwaters)', 'ប៉ៃលិន (ជួរភ្នំក្រវាញ)', 12.8489, 102.6093, 'KH', 'highland', 'km', 7.0, 'Asia/Phnom_Penh', 1),

                # === LAOS TRANSBOUNDARY UPSTREAM INFLOW (18 PROVINCES) ===
                # 5. Southern Laos (Direct Inflow Corridor to Cambodia)
                ('laos-champasak', 'Champasak (Pakse - Border Inflow)', 'ប៉ាក់សេ (ចំប៉ាសាក់ ឡាវ)', 15.1202, 105.7821, 'LA', 'laos-southern', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-attapeu', 'Attapeu (Sekong River Inflow)', 'អាត្តាពឺ (ទន្លេសេកុង ឡាវ)', 14.8107, 106.8294, 'LA', 'laos-southern', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-sekong', 'Sekong (Sekong Catchment)', 'សេកុង (ស្ទឹងសេកុង ឡាវ)', 15.3444, 106.7214, 'LA', 'laos-southern', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-salavan', 'Salavan (Sedone River Basin)', 'សាឡាវ៉ាន់ (ទន្លេសេដូន ឡាវ)', 15.7167, 106.4167, 'LA', 'laos-southern', 'en', 7.0, 'Asia/Vientiane', 1),

                # 6. Central Laos Mekong Corridor
                ('laos-savannakhet', 'Savannakhet (Mekong Corridor)', 'សាវ៉ាន់ណាខេត (មេគង្គ ឡាវ)', 16.5564, 104.7525, 'LA', 'laos-central', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-khammouane', 'Khammouane (Thakhek Station)', 'ខាំមួន (ថាខេក ឡាវ)', 17.4042, 104.8306, 'LA', 'laos-central', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-bolikhamsai', 'Bolikhamsai (Paksan Station)', 'បូលីខាំសៃ (ប៉ាកសាន ឡាវ)', 18.3972, 103.6578, 'LA', 'laos-central', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-vientiane-cap', 'Vientiane Capital (Mekong River)', 'រដ្ឋធានីវៀងចន្ទន៍ (មេគង្គ ឡាវ)', 17.9757, 102.6331, 'LA', 'laos-central', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-vientiane-prov', 'Vientiane Province (Nam Ngum)', 'ខេត្តវៀងចន្ទន៍ (ណាំង៉ុម ឡាវ)', 18.5000, 102.4167, 'LA', 'laos-central', 'en', 7.0, 'Asia/Vientiane', 1),

                # 7. Northern & Upper Mekong Catchment
                ('laos-luang-prabang', 'Luang Prabang (Upper Mekong)', 'ហ្លួងព្រះបាង (មេគង្គលើ ឡាវ)', 19.8893, 102.1350, 'LA', 'laos-upper', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-sayaboury', 'Sayaboury (Mekong West Bank)', 'សាយ៉ាបូរី (មេគង្គខាងលិច ឡាវ)', 19.2500, 101.7500, 'LA', 'laos-upper', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-bokeo', 'Bokeo (Golden Triangle Corridor)', 'បូកែវ (ត្រីកោណមាស ឡាវ)', 20.2764, 100.4167, 'LA', 'laos-upper', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-luang-namtha', 'Luang Namtha (Northern Watershed)', 'ហ្លួងណាំថា (តំបន់ភ្នំ ឡាវ)', 20.9500, 101.4000, 'LA', 'laos-upper', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-oudomxay', 'Oudomxay (Muang Xai Basin)', 'ឧត្តមជ័យ (មឿងសៃ ឡាវ)', 20.6833, 101.9833, 'LA', 'laos-upper', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-phongsaly', 'Phongsaly (Nam Ou Headwaters)', 'ផុងសាលី (ណាំអ៊ូ ឡាវ)', 21.6833, 102.1000, 'LA', 'laos-upper', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-houaphanh', 'Houaphanh (Sam Neua Basin)', 'ហួផាន់ (សាំណឿ ឡាវ)', 20.4167, 104.0500, 'LA', 'laos-upper', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-xiangkhouang', 'Xiangkhouang (Plain of Jars)', 'សៀងខ្វាង (វាលពាង ឡាវ)', 19.4500, 103.1833, 'LA', 'laos-upper', 'en', 7.0, 'Asia/Vientiane', 1),
                ('laos-xaisomboun', 'Xaisomboun (Annamite Catchment)', 'សាយសំប៊ុន (ជួរភ្នំអណ្ណាម ឡាវ)', 18.8833, 103.0833, 'LA', 'laos-upper', 'en', 7.0, 'Asia/Vientiane', 1),

                # === PAN-ASIAN FLAGSHIP BASINS (SOUTH, EAST & SE ASIA) ===
                # 8. South Asia (Ganges, Brahmaputra, Meghna, Indus)
                ('in-assam-guwahati', 'Guwahati (Brahmaputra Basin, India)', 'ហ្គូវ៉ាហាទី (ទន្លេព្រហ្មបុត្រ ឥណ្ឌា)', 26.1856, 91.7483, 'IN', 'brahmaputra', 'en', 5.5, 'Asia/Kolkata', 1),
                ('in-patna-ganga', 'Patna (Ganges River Basin, India)', 'ប៉ាត់ណា (ទន្លេគង្គា ឥណ្ឌា)', 25.5941, 85.1376, 'IN', 'ganges', 'en', 5.5, 'Asia/Kolkata', 1),
                ('bd-sylhet-surma', 'Sylhet (Surma-Meghna Flash Basin, Bangladesh)', 'ស៊ីលហេត (ទន្លេមេឃ្នា បង់ក្លាដែស)', 24.8949, 91.8687, 'BD', 'meghna', 'en', 6.0, 'Asia/Dhaka', 1),
                ('bd-sirajganj-jamuna', 'Sirajganj (Jamuna Confluence, Bangladesh)', 'ស៊ីរ៉ាចហ្កាន (ទន្លេយមុនា បង់ក្លាដែស)', 24.4534, 89.7006, 'BD', 'brahmaputra', 'en', 6.0, 'Asia/Dhaka', 1),
                ('pk-sukkur-indus', 'Sukkur Barrage (Indus River Basin, Pakistan)', 'ស៊ូកួរ (ទន្លេឥណ្ឌូ ប៉ាគីស្ថាន)', 27.7052, 68.8574, 'PK', 'indus', 'en', 5.0, 'Asia/Karachi', 1),
                ('pk-nowshera-kabul', 'Nowshera (Kabul/Indus Confluence, Pakistan)', 'ណូវសេរ៉ា (ទន្លេកាប៊ុល/ឥណ្ឌូ)', 34.0150, 71.9747, 'PK', 'indus', 'en', 5.0, 'Asia/Karachi', 1),
                ('np-narayani-chitwan', 'Chitwan (Narayani/Gandaki, Nepal Himalayas)', 'ឈីតវ៉ាន់ (ទន្លេណារ៉ាយ៉ានី នេប៉ាល់)', 27.6833, 84.4333, 'NP', 'himalayas', 'en', 5.75, 'Asia/Kathmandu', 1),

                # 9. East Asia (Yangtze & Lancang Headwaters)
                ('cn-yichang-yangtze', 'Yichang (Three Gorges Dam Outlet, China)', 'យីឆាង (ទំនប់បីជ្រលង ទន្លេយ៉ាងសេ ចិន)', 30.6919, 111.2865, 'CN', 'yangtze', 'en', 8.0, 'Asia/Shanghai', 1),
                ('cn-wuhan-yangtze', 'Wuhan (Middle Yangtze Inundation Hub, China)', 'វូហាន (ទន្លេយ៉ាងសេកណ្តាល ចិន)', 30.5928, 114.3055, 'CN', 'yangtze', 'en', 8.0, 'Asia/Shanghai', 1),
                ('cn-xishuangbanna-lancang', 'Jinghong (Lancang Headwaters Cascade, Yunnan)', 'ជីងហុង (ទន្លេឡានឆាង យូណាន ចិន)', 22.0017, 100.7979, 'CN', 'lancang', 'en', 8.0, 'Asia/Shanghai', 1),

                # 10. Southeast Asia (Chao Phraya, Red River, Irrawaddy, Pasig, Ciliwung)
                ('th-nakhon-sawan', 'Nakhon Sawan (Chao Phraya Origin, Thailand)', 'នគរសួគ៌ (ទន្លេចៅប្រាយ៉ា ថៃ)', 15.7047, 100.1372, 'TH', 'chao-phraya', 'en', 7.0, 'Asia/Bangkok', 1),
                ('th-ayutthaya-chao-phraya', 'Ayutthaya (Lower Chao Phraya Floodplain, Thailand)', 'អយុធ្យា (ទំនាបចៅប្រាយ៉ា ថៃ)', 14.3532, 100.5684, 'TH', 'chao-phraya', 'en', 7.0, 'Asia/Bangkok', 1),
                ('vn-hanoi-red-river', 'Hanoi (Red River / Song Hong Levee System, Vietnam)', 'ហាណូយ (ទន្លេក្រហម វៀតណាម)', 21.0285, 105.8542, 'VN', 'red-river', 'en', 7.0, 'Asia/Ho_Chi_Minh', 1),
                ('vn-can-tho-mekong-delta', 'Can Tho (Mekong Delta Hau River, Vietnam)', 'កឹងធើ (តំបន់ដីសណ្ដមេគង្គ វៀតណាម)', 10.0452, 105.7469, 'VN', 'mekong-delta', 'en', 7.0, 'Asia/Ho_Chi_Minh', 1),
                ('mm-mandalay-irrawaddy', 'Mandalay (Irrawaddy River Basin, Myanmar)', 'ម៉ាន់ដាឡាយ (ទន្លេឥរ៉ាវតី មីយ៉ាន់ម៉ា)', 21.9588, 96.0891, 'MM', 'irrawaddy', 'en', 6.5, 'Asia/Yangon', 1),
                ('mm-hinthada-delta', 'Hinthada (Irrawaddy Delta Corridor, Myanmar)', 'ហ៊ីនថាដា (ដីសណ្ដឥរ៉ាវតី មីយ៉ាន់ម៉ា)', 17.6500, 95.4500, 'MM', 'irrawaddy', 'en', 6.5, 'Asia/Yangon', 1),
                ('ph-manila-marikina', 'Metro Manila (Pasig-Marikina Flood Basin, Philippines)', 'ម៉ានីល (ទន្លេប៉ាស៊ីក-ម៉ារីគីណា ហ្វីលីពីន)', 14.6507, 121.1029, 'PH', 'philippines', 'en', 8.0, 'Asia/Manila', 1),
                ('ph-cagayan-tuguegarao', 'Tuguegarao (Cagayan River Basin, Philippines)', 'ទូហ្កេហ្គារ៉ាវ (ទន្លេកាហ្កាយ៉ាន ហ្វីលីពីន)', 17.6132, 121.7270, 'PH', 'philippines', 'en', 8.0, 'Asia/Manila', 1),
                ('id-jakarta-ciliwung', 'Jakarta (Ciliwung River Basin, Indonesia)', 'ហ្សាការតា (ទន្លេស៊ីលីវុង ឥណ្ឌូនេស៊ី)', -6.2088, 106.8456, 'ID', 'indonesia', 'en', 7.0, 'Asia/Jakarta', 1),
                ('id-solo-bengawan', 'Surakarta Solo (Bengawan Solo River, Central Java)', 'សូឡូ (ទន្លេបេងហ្កាវ៉ាន់សូឡូ ជ្វា ឥណ្ឌូនេស៊ី)', -7.5755, 110.8243, 'ID', 'indonesia', 'en', 7.0, 'Asia/Jakarta', 1),

                # === PAN-ASIAN COMPLETE COVERAGE (remaining UN Asian states) ===
                # 11. Rest of South Asia
                ('lk-colombo-kelani', 'Colombo (Kelani River, Sri Lanka)', 'Colombo · Kelani', 6.9271, 79.8612, 'LK', 'kelani', 'en', 5.5, 'Asia/Colombo', 1),
                ('lk-ratnapura-kalu', 'Ratnapura (Kalu River, Sri Lanka)', 'Ratnapura · Kalu', 6.6828, 80.3992, 'LK', 'kalu', 'en', 5.5, 'Asia/Colombo', 1),
                ('bt-thimphu-wangchhu', 'Thimphu (Wang Chhu, Bhutan)', 'Thimphu · Wang Chhu', 27.4712, 89.6339, 'BT', 'wang-chhu', 'en', 6.0, 'Asia/Thimphu', 1),
                ('bt-punakha-punatsangchhu', 'Punakha (Puna Tsang Chhu, Bhutan)', 'Punakha · Puna Tsang', 27.5921, 89.8797, 'BT', 'punatsang', 'en', 6.0, 'Asia/Thimphu', 1),
                ('mv-male-atoll', 'Malé (North Malé Atoll Tidal Basin, Maldives)', 'Malé · Atoll', 4.1755, 73.5093, 'MV', 'maldives-atoll', 'en', 5.0, 'Indian/Maldives', 1),
                ('af-kabul-river', 'Kabul (Kabul River, Afghanistan)', 'Kabul · Kabul River', 34.5553, 69.2075, 'AF', 'kabul', 'en', 4.5, 'Asia/Kabul', 1),
                ('af-jalalabad-kabul', 'Jalalabad (Lower Kabul River, Afghanistan)', 'Jalalabad · Kabul River', 34.4265, 70.4515, 'AF', 'kabul', 'en', 4.5, 'Asia/Kabul', 1),

                # 12. Rest of Southeast Asia
                ('my-kualalumpur-klang', 'Kuala Lumpur (Klang River, Malaysia)', 'Kuala Lumpur · Klang', 3.1390, 101.6869, 'MY', 'klang', 'en', 8.0, 'Asia/Kuala_Lumpur', 1),
                ('my-kotabharu-kelantan', 'Kota Bharu (Kelantan River, Malaysia)', 'Kota Bharu · Kelantan', 6.1254, 102.2381, 'MY', 'kelantan', 'en', 8.0, 'Asia/Kuala_Lumpur', 1),
                ('my-kuching-sarawak', 'Kuching (Sarawak River, Borneo, Malaysia)', 'Kuching · Sarawak', 1.5533, 110.3592, 'MY', 'sarawak', 'en', 8.0, 'Asia/Kuching', 1),
                ('sg-singapore-kallang', 'Singapore (Kallang / Bukit Timah Basin)', 'Singapore · Kallang', 1.3521, 103.8198, 'SG', 'singapore', 'en', 8.0, 'Asia/Singapore', 1),
                ('bn-bsb-brunei', 'Bandar Seri Begawan (Brunei River, Brunei)', 'BSB · Brunei River', 4.9031, 114.9398, 'BN', 'brunei', 'en', 8.0, 'Asia/Brunei', 1),
                ('tl-dili-comoro', 'Dili (Comoro River, Timor-Leste)', 'Dili · Comoro', -8.5569, 125.5603, 'TL', 'timor', 'en', 9.0, 'Asia/Dili', 1),

                # 13. Rest of East Asia
                ('jp-tokyo-arakawa', 'Tokyo (Arakawa / Edogawa System, Japan)', 'Tokyo · Arakawa', 35.6762, 139.6503, 'JP', 'arakawa', 'en', 9.0, 'Asia/Tokyo', 1),
                ('jp-osaka-yodo', 'Osaka (Yodo River, Japan)', 'Osaka · Yodo', 34.6937, 135.5023, 'JP', 'yodo', 'en', 9.0, 'Asia/Tokyo', 1),
                ('jp-fukuoka-chikugo', 'Fukuoka (Chikugo River, Kyushu, Japan)', 'Fukuoka · Chikugo', 33.5904, 130.4017, 'JP', 'chikugo', 'en', 9.0, 'Asia/Tokyo', 1),
                ('kr-seoul-han', 'Seoul (Han River, South Korea)', 'Seoul · Han', 37.5665, 126.9780, 'KR', 'han', 'en', 9.0, 'Asia/Seoul', 1),
                ('kr-busan-nakdong', 'Busan (Nakdong River, South Korea)', 'Busan · Nakdong', 35.1796, 129.0756, 'KR', 'nakdong', 'en', 9.0, 'Asia/Seoul', 1),
                ('kp-pyongyang-taedong', 'Pyongyang (Taedong River, North Korea)', 'Pyongyang · Taedong', 39.0392, 125.7625, 'KP', 'taedong', 'en', 9.0, 'Asia/Pyongyang', 1),
                ('mn-ulaanbaatar-tuul', 'Ulaanbaatar (Tuul River, Mongolia)', 'Ulaanbaatar · Tuul', 47.8864, 106.9057, 'MN', 'tuul', 'en', 8.0, 'Asia/Ulaanbaatar', 1),

                # 14. Central Asia
                ('kz-almaty-ili', 'Almaty (Ili-Balkhash Basin, Kazakhstan)', 'Almaty · Ili', 43.2220, 76.8512, 'KZ', 'ili', 'en', 6.0, 'Asia/Almaty', 1),
                ('kz-atyrau-ural', 'Atyrau (Ural River Delta, Kazakhstan)', 'Atyrau · Ural', 47.0945, 51.9238, 'KZ', 'ural', 'en', 5.0, 'Asia/Atyrau', 1),
                ('uz-tashkent-chirchiq', 'Tashkent (Chirchiq River, Uzbekistan)', 'Tashkent · Chirchiq', 41.2995, 69.2401, 'UZ', 'chirchiq', 'en', 5.0, 'Asia/Tashkent', 1),
                ('uz-nukus-amudarya', 'Nukus (Lower Amu Darya, Uzbekistan)', 'Nukus · Amu Darya', 42.4611, 59.6266, 'UZ', 'amu-darya', 'en', 5.0, 'Asia/Samarkand', 1),
                ('kg-bishkek-chuy', 'Bishkek (Chüy River, Kyrgyzstan)', 'Bishkek · Chüy', 42.8746, 74.5698, 'KG', 'chuy', 'en', 6.0, 'Asia/Bishkek', 1),
                ('tj-dushanbe-kofarnihon', 'Dushanbe (Kofarnihon / Amu Darya, Tajikistan)', 'Dushanbe · Kofarnihon', 38.5598, 68.7870, 'TJ', 'amu-darya', 'en', 5.0, 'Asia/Dushanbe', 1),
                ('tm-ashgabat-karakum', 'Ashgabat (Karakum Canal, Turkmenistan)', 'Ashgabat · Karakum', 37.9601, 58.3261, 'TM', 'karakum', 'en', 5.0, 'Asia/Ashgabat', 1),

                # 15. Western Asia / Middle East
                ('ir-tehran-basin', 'Tehran (Alborz Foothill Wadi Basin, Iran)', 'Tehran · Alborz', 35.6892, 51.3890, 'IR', 'alborz', 'en', 3.5, 'Asia/Tehran', 1),
                ('ir-ahvaz-karun', 'Ahvaz (Karun River, Iran)', 'Ahvaz · Karun', 31.3183, 48.6706, 'IR', 'karun', 'en', 3.5, 'Asia/Tehran', 1),
                ('iq-baghdad-tigris', 'Baghdad (Tigris River, Iraq)', 'Baghdad · Tigris', 33.3152, 44.3661, 'IQ', 'tigris', 'en', 3.0, 'Asia/Baghdad', 1),
                ('iq-basra-shattalarab', 'Basra (Shatt al-Arab, Iraq)', 'Basra · Shatt al-Arab', 30.5085, 47.7804, 'IQ', 'shatt-al-arab', 'en', 3.0, 'Asia/Baghdad', 1),
                ('tr-istanbul-marmara', 'Istanbul (Marmara Urban Basin, Türkiye)', 'Istanbul · Marmara', 41.0082, 28.9784, 'TR', 'marmara', 'en', 3.0, 'Europe/Istanbul', 1),
                ('tr-ankara-sakarya', 'Ankara (Sakarya Headwaters, Türkiye)', 'Ankara · Sakarya', 39.9334, 32.8597, 'TR', 'sakarya', 'en', 3.0, 'Europe/Istanbul', 1),
                ('sy-damascus-barada', 'Damascus (Barada River, Syria)', 'Damascus · Barada', 33.5138, 36.2765, 'SY', 'barada', 'en', 3.0, 'Asia/Damascus', 1),
                ('lb-beirut-river', 'Beirut (Beirut / Litani Basin, Lebanon)', 'Beirut · Litani', 33.8938, 35.5018, 'LB', 'litani', 'en', 2.0, 'Asia/Beirut', 1),
                ('jo-amman-zarqa', 'Amman (Zarqa River, Jordan)', 'Amman · Zarqa', 31.9454, 35.9284, 'JO', 'zarqa', 'en', 3.0, 'Asia/Amman', 1),
                ('il-telaviv-yarkon', 'Tel Aviv (Yarkon River, Israel)', 'Tel Aviv · Yarkon', 32.0853, 34.7818, 'IL', 'yarkon', 'en', 2.0, 'Asia/Jerusalem', 1),
                ('sa-jeddah-wadi', 'Jeddah (Wadi Flash-Flood Basin, Saudi Arabia)', 'Jeddah · Wadi', 21.4858, 39.1925, 'SA', 'arabian-wadi', 'en', 3.0, 'Asia/Riyadh', 1),
                ('sa-riyadh-wadihanifa', 'Riyadh (Wadi Hanifa, Saudi Arabia)', 'Riyadh · Wadi Hanifa', 24.7136, 46.6753, 'SA', 'arabian-wadi', 'en', 3.0, 'Asia/Riyadh', 1),
                ('ye-sanaa-wadi', 'Sanaá (Highland Wadi Basin, Yemen)', 'Sanaá · Wadi', 15.3694, 44.1910, 'YE', 'arabian-wadi', 'en', 3.0, 'Asia/Aden', 1),
                ('om-muscat-wadi', 'Muscat (Coastal Wadi Basin, Oman)', 'Muscat · Wadi', 23.5880, 58.3829, 'OM', 'arabian-wadi', 'en', 4.0, 'Asia/Muscat', 1),
                ('ae-dubai-urban', 'Dubai (Urban Storm-Drainage Basin, UAE)', 'Dubai · Urban', 25.2048, 55.2708, 'AE', 'gulf-urban', 'en', 4.0, 'Asia/Dubai', 1),
                ('qa-doha-urban', 'Doha (Urban Storm-Drainage Basin, Qatar)', 'Doha · Urban', 25.2854, 51.5310, 'QA', 'gulf-urban', 'en', 3.0, 'Asia/Qatar', 1),
                ('bh-manama-urban', 'Manama (Urban Coastal Basin, Bahrain)', 'Manama · Urban', 26.2285, 50.5860, 'BH', 'gulf-urban', 'en', 3.0, 'Asia/Bahrain', 1),
                ('kw-kuwaitcity-urban', 'Kuwait City (Urban Wadi Basin, Kuwait)', 'Kuwait City · Wadi', 29.3759, 47.9774, 'KW', 'gulf-urban', 'en', 3.0, 'Asia/Kuwait', 1),

                # 16. South Caucasus & East Mediterranean (transcontinental Asia)
                ('ge-tbilisi-kura', 'Tbilisi (Kura / Mtkvari River, Georgia)', 'Tbilisi · Kura', 41.7151, 44.8271, 'GE', 'kura', 'en', 4.0, 'Asia/Tbilisi', 1),
                ('am-yerevan-hrazdan', 'Yerevan (Hrazdan River, Armenia)', 'Yerevan · Hrazdan', 40.1792, 44.4991, 'AM', 'kura', 'en', 4.0, 'Asia/Yerevan', 1),
                ('az-baku-kura', 'Baku (Kura Delta / Caspian, Azerbaijan)', 'Baku · Kura', 40.4093, 49.8671, 'AZ', 'kura', 'en', 4.0, 'Asia/Baku', 1),
                ('cy-nicosia-pedieos', 'Nicosia (Pedieos River, Cyprus)', 'Nicosia · Pedieos', 35.1856, 33.3823, 'CY', 'pedieos', 'en', 2.0, 'Asia/Nicosia', 1)
            ]

            cursor.executemany("""
                INSERT OR REPLACE INTO areas (area_id, name_en, name_km, latitude, longitude, country, basin_category, language, utc_offset_hours, timezone_name, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, areas_seed)

            def get_plausible_seed_discharge(bcat: str, aid: str) -> float:
                """Calculates realistic, plausible baseline hydrological discharge per river basin."""
                if bcat == "yangtze":
                    return 24500.0 if "wuhan" in aid else 18500.0
                elif bcat == "brahmaputra":
                    return 21000.0 if "sirajganj" in aid else 19200.0
                elif bcat == "ganges":
                    return 14500.0
                elif bcat == "indus":
                    return 7800.0 if "sukkur" in aid else 3200.0
                elif bcat == "irrawaddy":
                    return 12800.0 if "hinthada" in aid else 9600.0
                elif bcat == "mekong-delta":
                    return 15200.0
                elif bcat == "red-river":
                    return 2650.0
                elif bcat == "chao-phraya":
                    return 1950.0 if "ayutthaya" in aid else 1650.0
                elif bcat == "meghna":
                    return 2300.0
                elif bcat == "himalayas":
                    return 1150.0
                elif bcat == "lancang":
                    return 1850.0
                elif bcat == "philippines":
                    return 1250.0 if "cagayan" in aid else 420.0
                elif bcat == "indonesia":
                    return 680.0 if "solo" in aid else 340.0
                elif bcat in ("laos-southern", "laos-central", "laos-upper"):
                    if "champasak" in aid or "savannakhet" in aid:
                        return 15200.0
                    elif "vientiane" in aid or "luang-prabang" in aid:
                        return 8200.0
                    return 2400.0
                elif bcat == "tonle-sap":
                    return 950.0
                elif bcat == "coastal":
                    return 260.0
                elif bcat == "highland":
                    return 480.0
                else:
                    # Default Mekong Mainstream / Kratie
                    return 15200.0 if ("kratie" in aid or "stung-treng" in aid or "kampong-cham" in aid or "phnom-penh" in aid) else 1500.0

            # Pre-seed initial telemetry readings and risk states if empty
            cursor.execute("SELECT COUNT(*) as count FROM readings")
            if cursor.fetchone()["count"] == 0:
                seed_iso = "2026-01-01T00:00:00+00:00"
                seed_engine = RiskClassificationEngine()
                for aid, nen, nkm, lat, lon, ccode, bcat, lang, utc_off, tz_n, act in areas_seed:
                    rid = f"seed-reading-{aid}"
                    base_dis = get_plausible_seed_discharge(bcat, aid)
                    cursor.execute("""
                        INSERT OR REPLACE INTO readings
                        (reading_id, area_id, observed_at, discharge, precipitation, source, fetched_at, is_stale)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                    """, (rid, aid, seed_iso, base_dis, 0.0, "Open-Meteo GloFAS / Seed", seed_iso))

                    seed_area = Area(
                        area_id=aid,
                        name_en=nen,
                        name_km=nkm,
                        latitude=lat,
                        longitude=lon,
                        country=ccode,
                        basin_category=bcat,
                        language=lang,
                        utc_offset_hours=utc_off,
                        timezone_name=tz_n,
                        active=bool(act)
                    )

                    seed_reading = Reading(
                        reading_id=rid,
                        area_id=aid,
                        observed_at=datetime.fromisoformat(seed_iso),
                        discharge=base_dis,
                        precipitation=0.0,
                        source="Open-Meteo GloFAS / Seed",
                        fetched_at=datetime.fromisoformat(seed_iso),
                        is_stale=False
                    )
                    seed_risk = seed_engine.evaluate(seed_reading, seed_area)

                    cursor.execute("""
                        INSERT OR REPLACE INTO risk_states
                        (risk_id, area_id, level, reason, rule_version, calculated_at, discharge_val, precipitation_val)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (f"seed-risk-{aid}", aid, seed_risk.level, seed_risk.reason, seed_risk.rule_version, seed_iso, base_dis, 0.0))

            conn.commit()

    # --- Cache Initialization & Helpers ---
    def _load_hot_cache(self):
        """Pre-populates in-memory hot caches with latest readings and risk states for all stations."""
        try:
            with self._connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT r.* FROM readings r
                    INNER JOIN (
                        SELECT area_id, MAX(fetched_at) as max_fetched
                        FROM readings GROUP BY area_id
                    ) latest ON r.area_id = latest.area_id AND r.fetched_at = latest.max_fetched
                """)
                for row in cursor.fetchall():
                    self._latest_reading_cache[row["area_id"]] = Reading(
                        reading_id=row["reading_id"],
                        area_id=row["area_id"],
                        observed_at=datetime.fromisoformat(row["observed_at"]),
                        discharge=row["discharge"],
                        precipitation=row["precipitation"],
                        source=row["source"],
                        fetched_at=datetime.fromisoformat(row["fetched_at"]),
                        is_stale=bool(row["is_stale"])
                    )

                cursor.execute("""
                    SELECT k.* FROM risk_states k
                    INNER JOIN (
                        SELECT area_id, MAX(calculated_at) as max_calc
                        FROM risk_states GROUP BY area_id
                    ) latest ON k.area_id = latest.area_id AND k.calculated_at = latest.max_calc
                """)
                for row in cursor.fetchall():
                    self._latest_risk_cache[row["area_id"]] = RiskState(
                        risk_id=row["risk_id"],
                        area_id=row["area_id"],
                        level=row["level"],
                        reason=row["reason"],
                        rule_version=row["rule_version"],
                        calculated_at=datetime.fromisoformat(row["calculated_at"]),
                        discharge_val=row["discharge_val"],
                        precipitation_val=row["precipitation_val"]
                    )
        except Exception:
            pass

    def _get_ttl_cached(self, key: str, ttl_seconds: float, fetcher: Callable[[], Any]) -> Any:
        now = time.time()
        if key in self._ttl_cache:
            cached_time, val = self._ttl_cache[key]
            if now - cached_time < ttl_seconds:
                return val
        result = fetcher()
        self._ttl_cache[key] = (now, result)
        return result

    def _invalidate_ttl(self, prefix: Optional[str] = None):
        if prefix:
            self._ttl_cache = {k: v for k, v in self._ttl_cache.items() if not k.startswith(prefix)}
        else:
            self._ttl_cache.clear()

    # --- Area Operations ---
    def get_all_areas(self, country: Optional[str] = None) -> List[Area]:
        if self._areas_cache is None:
            with self._connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM areas WHERE active = 1 ORDER BY country, area_id")
                rows = cursor.fetchall()
                self._areas_cache = [
                    Area(
                        area_id=r["area_id"],
                        name_en=r["name_en"],
                        name_km=r["name_km"],
                        latitude=r["latitude"],
                        longitude=r["longitude"],
                        country=r["country"] if "country" in r.keys() else "KH",
                        basin_category=r["basin_category"] if "basin_category" in r.keys() else "mekong",
                        language=r["language"] if "language" in r.keys() and r["language"] is not None else ("km" if (r["country"] if "country" in r.keys() else "KH") == "KH" else "en"),
                        utc_offset_hours=float(r["utc_offset_hours"]) if "utc_offset_hours" in r.keys() and r["utc_offset_hours"] is not None else 7.0,
                        timezone_name=r["timezone_name"] if "timezone_name" in r.keys() and r["timezone_name"] is not None else "Asia/Phnom_Penh",
                        active=bool(r["active"])
                    ) for r in rows
                ]
        if country:
            return [a for a in self._areas_cache if a.country == country]
        return list(self._areas_cache)

    def get_area(self, area_id: str) -> Optional[Area]:
        areas = self.get_all_areas()
        for a in areas:
            if a.area_id == area_id:
                return a
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM areas WHERE area_id = ?", (area_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return Area(
                area_id=row["area_id"],
                name_en=row["name_en"],
                name_km=row["name_km"],
                latitude=row["latitude"],
                longitude=row["longitude"],
                country=row["country"] if "country" in row.keys() else "KH",
                basin_category=row["basin_category"] if "basin_category" in row.keys() else "mekong",
                language=row["language"] if "language" in row.keys() and row["language"] is not None else ("km" if (row["country"] if "country" in row.keys() else "KH") == "KH" else "en"),
                utc_offset_hours=float(row["utc_offset_hours"]) if "utc_offset_hours" in row.keys() and row["utc_offset_hours"] is not None else 7.0,
                timezone_name=row["timezone_name"] if "timezone_name" in row.keys() and row["timezone_name"] is not None else "Asia/Phnom_Penh",
                active=bool(row["active"])
            )

    # --- Readings Operations (FR2 with Smart Write De-Duplication & Hot Cache) ---
    def save_reading(self, reading: Reading) -> None:
        """
        Smart Write Optimization + Hot Cache Update:
        Updates in-memory hot cache immediately, and updates existing row if values are identical.
        """
        self._latest_reading_cache[reading.area_id] = reading
        self._invalidate_ttl("ts_")
        self._invalidate_ttl("stats")

        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT reading_id, discharge, precipitation, is_stale FROM readings WHERE area_id = ? ORDER BY fetched_at DESC LIMIT 1", (reading.area_id,))
            row = cursor.fetchone()
            if (
                row is not None
                and abs(row["discharge"] - reading.discharge) < 0.01
                and abs(row["precipitation"] - reading.precipitation) < 0.01
                and bool(row["is_stale"]) == reading.is_stale
            ):
                conn.execute(
                    "UPDATE readings SET fetched_at = ? WHERE reading_id = ?",
                    (reading.fetched_at.isoformat(), row["reading_id"])
                )
                conn.commit()
                return

            conn.execute("""
                INSERT OR REPLACE INTO readings 
                (reading_id, area_id, observed_at, discharge, precipitation, source, fetched_at, is_stale)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                reading.reading_id,
                reading.area_id,
                reading.observed_at.isoformat(),
                reading.discharge,
                reading.precipitation,
                reading.source,
                reading.fetched_at.isoformat(),
                1 if reading.is_stale else 0
            ))
            conn.commit()

    def save_readings_bulk(self, readings: List[Reading]) -> None:
        """
        Optimization #4: Batch Bulk Ingestion Writes.
        Saves multiple readings inside a single atomic SQLite transaction with executemany.
        """
        if not readings:
            return
        for r in readings:
            self._latest_reading_cache[r.area_id] = r
        self._invalidate_ttl("ts_")
        self._invalidate_ttl("stats")

        data_to_insert = [
            (
                r.reading_id,
                r.area_id,
                r.observed_at.isoformat(),
                r.discharge,
                r.precipitation,
                r.source,
                r.fetched_at.isoformat(),
                1 if r.is_stale else 0
            )
            for r in readings
        ]

        with self._connection() as conn:
            conn.executemany("""
                INSERT OR REPLACE INTO readings 
                (reading_id, area_id, observed_at, discharge, precipitation, source, fetched_at, is_stale)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, data_to_insert)
            conn.commit()

    def get_latest_reading(self, area_id: str = "kratie-central") -> Optional[Reading]:
        if area_id in self._latest_reading_cache:
            return self._latest_reading_cache[area_id]

        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM readings 
                WHERE area_id = ? 
                ORDER BY fetched_at DESC LIMIT 1
            """, (area_id,))
            row = cursor.fetchone()
            if not row:
                return None
            reading = Reading(
                reading_id=row["reading_id"],
                area_id=row["area_id"],
                observed_at=datetime.fromisoformat(row["observed_at"]),
                discharge=row["discharge"],
                precipitation=row["precipitation"],
                source=row["source"],
                fetched_at=datetime.fromisoformat(row["fetched_at"]),
                is_stale=bool(row["is_stale"])
            )
            self._latest_reading_cache[area_id] = reading
            return reading

    def get_readings_history(self, area_id: str = "kratie-central", limit: int = 15) -> List[Reading]:
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM readings 
                WHERE area_id = ? 
                ORDER BY fetched_at DESC LIMIT ?
            """, (area_id, limit))
            rows = cursor.fetchall()
            return [
                Reading(
                    reading_id=r["reading_id"],
                    area_id=r["area_id"],
                    observed_at=datetime.fromisoformat(r["observed_at"]),
                    discharge=r["discharge"],
                    precipitation=r["precipitation"],
                    source=r["source"],
                    fetched_at=datetime.fromisoformat(r["fetched_at"]),
                    is_stale=bool(r["is_stale"])
                ) for r in rows
            ]

    # --- RiskState Operations (FR3 with Hot Caching & Bulk Updates) ---
    def save_risk_state(self, state: RiskState) -> None:
        self._latest_risk_cache[state.area_id] = state
        self._invalidate_ttl("ts_")
        self._invalidate_ttl("stats")
        with self._connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO risk_states 
                (risk_id, area_id, level, reason, rule_version, calculated_at, discharge_val, precipitation_val)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                state.risk_id,
                state.area_id,
                state.level,
                state.reason,
                state.rule_version,
                state.calculated_at.isoformat(),
                state.discharge_val,
                state.precipitation_val
            ))
            conn.commit()

    def save_risk_states_bulk(self, states: List[RiskState]) -> None:
        """
        Optimization #4: Batch Bulk RiskState Writes.
        Saves multiple risk evaluations inside a single atomic SQLite transaction with executemany.
        """
        if not states:
            return
        for s in states:
            self._latest_risk_cache[s.area_id] = s
        self._invalidate_ttl("ts_")
        self._invalidate_ttl("stats")

        data_to_insert = [
            (
                s.risk_id,
                s.area_id,
                s.level,
                s.reason,
                s.rule_version,
                s.calculated_at.isoformat(),
                s.discharge_val,
                s.precipitation_val
            )
            for s in states
        ]

        with self._connection() as conn:
            conn.executemany("""
                INSERT OR REPLACE INTO risk_states 
                (risk_id, area_id, level, reason, rule_version, calculated_at, discharge_val, precipitation_val)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, data_to_insert)
            conn.commit()

    def get_latest_risk_state(self, area_id: str = "kratie-central") -> Optional[RiskState]:
        if area_id in self._latest_risk_cache:
            return self._latest_risk_cache[area_id]

        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM risk_states 
                WHERE area_id = ? 
                ORDER BY calculated_at DESC LIMIT 1
            """, (area_id,))
            row = cursor.fetchone()
            if not row:
                return None
            risk = RiskState(
                risk_id=row["risk_id"],
                area_id=row["area_id"],
                level=row["level"],
                reason=row["reason"],
                rule_version=row["rule_version"],
                calculated_at=datetime.fromisoformat(row["calculated_at"]),
                discharge_val=row["discharge_val"],
                precipitation_val=row["precipitation_val"]
            )
            self._latest_risk_cache[area_id] = risk
            return risk

    def get_risk_history(self, area_id: str = "kratie-central", limit: int = 15) -> List[RiskState]:
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM risk_states 
                WHERE area_id = ? 
                ORDER BY calculated_at DESC LIMIT ?
            """, (area_id, limit))
            rows = cursor.fetchall()
            return [
                RiskState(
                    risk_id=r["risk_id"],
                    area_id=r["area_id"],
                    level=r["level"],
                    reason=r["reason"],
                    rule_version=r["rule_version"],
                    calculated_at=datetime.fromisoformat(r["calculated_at"]),
                    discharge_val=r["discharge_val"],
                    precipitation_val=r["precipitation_val"]
                ) for r in rows
            ]

    # --- Telegram Bot Subscriptions ---
    def add_subscription(self, chat_id: str, area_id: str) -> bool:
        with self._connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO subscriptions (chat_id, area_id, subscribed_at)
                VALUES (?, ?, ?)
            """, (str(chat_id), area_id, datetime.now(timezone.utc).isoformat()))
            conn.commit()
            return True

    def remove_subscription(self, chat_id: str, area_id: str) -> bool:
        with self._connection() as conn:
            conn.execute("""
                DELETE FROM subscriptions WHERE chat_id = ? AND area_id = ?
            """, (str(chat_id), area_id))
            conn.commit()
            return True

    def get_user_subscriptions(self, chat_id: str) -> List[str]:
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT area_id FROM subscriptions WHERE chat_id = ?", (str(chat_id),))
            return [row["area_id"] for row in cursor.fetchall()]

    def get_subscribers_for_area(self, area_id: str) -> List[str]:
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT chat_id FROM subscriptions WHERE area_id = ?", (str(area_id),))
            return [row["chat_id"] for row in cursor.fetchall()]

    # --- Storage Health & Pruning Optimization ---
    def prune_historical_readings(self, keep_days: int = 30) -> int:
        """Prunes historical telemetry readings older than keep_days to prevent database bloat."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM readings 
                WHERE datetime(fetched_at) < datetime('now', '-' || ? || ' days')
            """, (keep_days,))
            deleted = cursor.rowcount
            cursor.execute("PRAGMA optimize;")
            conn.commit()
            self._invalidate_ttl()
            return deleted

    def get_storage_stats(self) -> dict:
        """Returns database storage and record count statistics (Cached for 10s via TTL)."""
        def _fetch_stats():
            with self._connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) as c FROM areas")
                total_areas = cursor.fetchone()["c"]
                cursor.execute("SELECT COUNT(*) as c FROM readings")
                total_readings = cursor.fetchone()["c"]
                cursor.execute("SELECT COUNT(*) as c FROM risk_states")
                total_risks = cursor.fetchone()["c"]
                cursor.execute("SELECT COUNT(*) as c FROM subscriptions")
                total_subs = cursor.fetchone()["c"]
                cursor.execute("SELECT COUNT(*) as c FROM audit_events")
                total_audits = cursor.fetchone()["c"]
                return {
                    "total_stations": total_areas,
                    "total_readings": total_readings,
                    "total_risk_evaluations": total_risks,
                    "total_subscriptions": total_subs,
                    "total_audit_events": total_audits,
                    "wal_mode": True,
                    "hot_cache_entries": len(self._latest_reading_cache)
                }
        return self._get_ttl_cached("storage_stats", 10.0, _fetch_stats)

    # --- Time-Series Chronological Audit Logging (FR2 Provenance) ---
    def log_audit_event(self, event_type: str, area_id: Optional[str] = None, details: Optional[dict] = None) -> str:
        """Records an immutable, timestamped chronological event in UTC and local station timezone."""
        import uuid, json
        from datetime import datetime, timezone, timedelta
        
        now_utc = datetime.now(timezone.utc)
        
        # Calculate local offset
        utc_offset = 7.0
        tz_label = "ICT"
        if area_id:
            area = self.get_area(area_id)
            if area:
                utc_offset = area.utc_offset_hours
                tz_label = area.timezone_name.split('/')[-1] if '/' in area.timezone_name else "LOCAL"
        
        local_tz = timezone(timedelta(hours=utc_offset))
        now_local = now_utc.astimezone(local_tz)
        
        event_id = f"evt-{uuid.uuid4().hex[:12]}"
        details_json = json.dumps(details or {}, ensure_ascii=False)
        ts_utc_str = now_utc.isoformat()
        ts_local_str = now_local.strftime(f"%Y-%m-%d %H:%M:%S UTC{'+' if utc_offset >= 0 else ''}{utc_offset:g}")
        ts_ict_str = now_utc.astimezone(timezone(timedelta(hours=7))).strftime("%Y-%m-%d %H:%M:%S ICT")
        
        with self._connection() as conn:
            conn.execute("""
                INSERT INTO audit_events 
                (event_id, event_type, area_id, timestamp_utc, timestamp_ict, timestamp_local, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (event_id, event_type, area_id, ts_utc_str, ts_ict_str, ts_local_str, details_json))
            conn.commit()
            
        self._invalidate_ttl("storage_stats")
        return event_id

    def get_audit_events(self, limit: int = 50, area_id: Optional[str] = None) -> List[dict]:
        """Retrieves recent chronological audit events."""
        import json
        with self._connection() as conn:
            cursor = conn.cursor()
            if area_id:
                cursor.execute("""
                    SELECT * FROM audit_events 
                    WHERE area_id = ? 
                    ORDER BY timestamp_utc DESC LIMIT ?
                """, (area_id, limit))
            else:
                cursor.execute("""
                    SELECT * FROM audit_events 
                    ORDER BY timestamp_utc DESC LIMIT ?
                """, (limit,))
                
            rows = cursor.fetchall()
            events = []
            for r in rows:
                try:
                    det = json.loads(r["details_json"])
                except Exception:
                    det = {}
                events.append({
                    "event_id": r["event_id"],
                    "event_type": r["event_type"],
                    "area_id": r["area_id"],
                    "timestamp_utc": r["timestamp_utc"],
                    "timestamp_ict": r["timestamp_ict"],
                    "timestamp_local": r["timestamp_local"] if "timestamp_local" in r.keys() else r["timestamp_ict"],
                    "details": det
                })
            return events

    def get_time_series_history(self, area_id: str, limit: int = 24) -> List[dict]:
        """Returns time-indexed readings and risk state history for a given station (Cached for 5s)."""
        def _fetch_ts():
            with self._connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT r.reading_id, r.observed_at, r.discharge, r.precipitation, r.source, r.fetched_at, r.is_stale,
                           k.level as risk_level, k.reason as risk_reason, k.rule_version
                    FROM readings r
                    LEFT JOIN risk_states k ON r.area_id = k.area_id
                    WHERE r.area_id = ?
                    ORDER BY r.fetched_at DESC LIMIT ?
                """, (area_id, limit))
                rows = cursor.fetchall()
                return [dict(r) for r in rows]
        return self._get_ttl_cached(f"ts_{area_id}_{limit}", 5.0, _fetch_ts)

    # --- User-Level Alert Rate Limiting & Dispatch Logs (Anti-Spam per User) ---
    def can_dispatch_alert_to_user(
        self,
        recipient_id: str,
        area_id: str,
        alert_level: str = "CAUTION",
        max_per_hour: int = 3,
        cooldown_minutes: int = 15
    ) -> Tuple[bool, str]:
        """
        Evaluates whether a user can receive an emergency alert without notification spam:
        - Allows maximum `max_per_hour` alerts per recipient per 60-minute window for the same province.
        - Enforces a `cooldown_minutes` pause between non-critical repeat alerts.
        - LIFE SAFETY EXCEPTION: 'DANGER' alerts always bypass cooldown if previous alert was not DANGER.
        """
        with self._connection() as conn:
            cursor = conn.cursor()
            
            # 1. Count alerts sent to this user in the last 60 minutes
            cursor.execute("""
                SELECT COUNT(*) as recent_count, MAX(dispatched_at_utc) as last_sent,
                       (SELECT alert_level FROM user_alert_logs WHERE recipient_id = ? AND area_id = ? ORDER BY dispatched_at_utc DESC, rowid DESC LIMIT 1) as last_level
                FROM user_alert_logs
                WHERE recipient_id = ? AND area_id = ? AND status = 'DELIVERED'
                AND datetime(dispatched_at_utc) >= datetime('now', '-60 minutes')
            """, (recipient_id, area_id, recipient_id, area_id))
            
            row = cursor.fetchone()
            recent_count = row["recent_count"] or 0
            last_sent_str = row["last_sent"]
            last_level = row["last_level"]
            
            # Life-Safety Exception: Danger alert escalation immediately passes
            if alert_level.upper() == "DANGER" and last_level != "DANGER":
                return True, "Life-safety escalation: DANGER level bypasses cooldown."
                
            # Quota Check (Max alerts per hour)
            if recent_count >= max_per_hour:
                return False, f"User alert quota exceeded: {recent_count}/{max_per_hour} alerts in last 60m."
                
            # Cooldown Check
            if last_sent_str:
                from datetime import datetime, timezone, timedelta
                try:
                    last_sent = datetime.fromisoformat(last_sent_str)
                    now_utc = datetime.now(timezone.utc)
                    elapsed = (now_utc - last_sent).total_seconds() / 60.0
                    if elapsed < cooldown_minutes and alert_level.upper() == last_level:
                        return False, f"In cooldown: Last alert sent {elapsed:.1f}m ago (Cooldown: {cooldown_minutes}m)."
                except Exception:
                    pass
                    
            return True, "OK"

    def log_user_alert_dispatch(
        self,
        recipient_id: str,
        channel: str,
        area_id: str,
        alert_level: str,
        status: str = "DELIVERED",
        details: Optional[dict] = None
    ) -> str:
        """Records an alert dispatch record to a specific recipient with exact timestamps."""
        import uuid, json
        from datetime import datetime, timezone, timedelta
        
        now_utc = datetime.now(timezone.utc)
        
        utc_offset = 7.0
        if area_id:
            area = self.get_area(area_id)
            if area:
                utc_offset = area.utc_offset_hours
                
        local_tz = timezone(timedelta(hours=utc_offset))
        now_local = now_utc.astimezone(local_tz)
        
        alert_id = f"alt-{uuid.uuid4().hex[:12]}"
        details_json = json.dumps(details or {}, ensure_ascii=False)
        ts_utc = now_utc.isoformat()
        ts_local = now_local.strftime(f"%Y-%m-%d %H:%M:%S UTC{'+' if utc_offset >= 0 else ''}{utc_offset:g}")
        ts_ict = now_utc.astimezone(timezone(timedelta(hours=7))).strftime("%Y-%m-%d %H:%M:%S ICT")
        
        with self._connection() as conn:
            conn.execute("""
                INSERT INTO user_alert_logs
                (alert_id, recipient_id, channel, area_id, alert_level, dispatched_at_utc, dispatched_at_ict, dispatched_at_local, status, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (alert_id, recipient_id, channel.upper(), area_id, alert_level.upper(), ts_utc, ts_ict, ts_local, status.upper(), details_json))
            conn.commit()
            
        return alert_id

    def get_user_alert_history(self, recipient_id: Optional[str] = None, limit: int = 50) -> List[dict]:
        """Returns recent user alert logs."""
        import json
        with self._connection() as conn:
            cursor = conn.cursor()
            if recipient_id:
                cursor.execute("""
                    SELECT * FROM user_alert_logs
                    WHERE recipient_id = ?
                    ORDER BY dispatched_at_utc DESC, rowid DESC LIMIT ?
                """, (recipient_id, limit))
            else:
                cursor.execute("""
                    SELECT * FROM user_alert_logs
                    ORDER BY dispatched_at_utc DESC, rowid DESC LIMIT ?
                """, (limit,))
                
            rows = cursor.fetchall()
            alerts = []
            for r in rows:
                try:
                    det = json.loads(r["details_json"])
                except Exception:
                    det = {}
                alerts.append({
                    "alert_id": r["alert_id"],
                    "recipient_id": r["recipient_id"],
                    "channel": r["channel"],
                    "area_id": r["area_id"],
                    "alert_level": r["alert_level"],
                    "dispatched_at_utc": r["dispatched_at_utc"],
                    "dispatched_at_ict": r["dispatched_at_ict"],
                    "dispatched_at_local": r["dispatched_at_local"] if "dispatched_at_local" in r.keys() else r["dispatched_at_ict"],
                    "status": r["status"],
                    "details": det
                })
            return alerts

    # ==========================================================================
    # PRODUCTION GIS & SOIL MOISTURE PERSISTENCE
    # ==========================================================================

    def save_soil_moisture(self, soil: SoilMoistureReading) -> None:
        """Stores or updates the latest volumetric soil moisture reading for an area."""
        now_str = datetime.now(timezone.utc).isoformat()
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO soil_moisture_readings (
                    area_id, observed_at, moisture_surface, moisture_rootzone,
                    saturation_percent, runoff_coefficient, source, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(area_id) DO UPDATE SET
                    observed_at = excluded.observed_at,
                    moisture_surface = excluded.moisture_surface,
                    moisture_rootzone = excluded.moisture_rootzone,
                    saturation_percent = excluded.saturation_percent,
                    runoff_coefficient = excluded.runoff_coefficient,
                    source = excluded.source,
                    updated_at = excluded.updated_at
            """, (
                soil.area_id,
                soil.observed_at.isoformat(),
                soil.moisture_surface_m3m3,
                soil.moisture_rootzone_m3m3,
                soil.saturation_percent,
                soil.runoff_coefficient,
                soil.source,
                now_str
            ))

    def get_latest_soil_moisture(self, area_id: str) -> Optional[SoilMoistureReading]:
        """Retrieves the latest stored soil moisture reading for a station."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM soil_moisture_readings WHERE area_id = ?
            """, (area_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return SoilMoistureReading(
                area_id=row["area_id"],
                observed_at=datetime.fromisoformat(row["observed_at"]),
                moisture_surface_m3m3=float(row["moisture_surface"]),
                moisture_rootzone_m3m3=float(row["moisture_rootzone"]),
                saturation_percent=float(row["saturation_percent"]),
                runoff_coefficient=float(row["runoff_coefficient"]),
                source=row["source"]
            )

    def get_stations_geojson(self) -> Dict[str, Any]:
        """
        Generates an RFC 7946 compliant GeoJSON FeatureCollection of all 43 stations.
        Includes real-time hydrometeorological properties for direct ingestion into
        Leaflet, Mapbox, QGIS, or ESRI ArcGIS platforms.
        """
        areas = self.get_all_areas()
        features = []

        for a in areas:
            reading = self.get_latest_reading(a.area_id)
            risk = self.get_latest_risk_state(a.area_id)
            soil = self.get_latest_soil_moisture(a.area_id)

            level = (risk.level if risk else "Normal").capitalize()
            q = reading.discharge if reading else 0.0
            rain = reading.precipitation if reading else 0.0
            sat = soil.saturation_percent if soil else 50.0

            # Map marker color coding by danger severity
            color_map = {
                "Danger": "#ef4444",
                "Caution": "#f59e0b",
                "Advisory": "#3b82f6",
                "Normal": "#10b981"
            }

            feature = {
                "type": "Feature",
                "id": a.area_id,
                "geometry": {
                    "type": "Point",
                    "coordinates": [a.longitude, a.latitude]
                },
                "properties": {
                    "area_id": a.area_id,
                    "name_en": a.name_en,
                    "name_km": a.name_km,
                    "country": a.country,
                    "basin_category": a.basin_category,
                    "risk_level": level,
                    "marker_color": color_map.get(level, "#10b981"),
                    "discharge_m3s": q,
                    "precipitation_mm": rain,
                    "soil_saturation_pct": sat,
                    "is_stale": reading.is_stale if reading else False,
                    "updated_at": reading.fetched_at.isoformat() if reading else None
                }
            }
            features.append(feature)

        return {
            "type": "FeatureCollection",
            "crs": {
                "type": "name",
                "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}
            },
            "features": features
        }

    def get_risk_heatmap_geojson(self) -> Dict[str, Any]:
        """
        Generates polygon buffer geometries representing disaster risk influence radii
        around affected stations (e.g. 25km buffer around Caution/Danger zones).
        """
        import math
        areas = self.get_all_areas()
        polygons = []

        for a in areas:
            risk = self.get_latest_risk_state(a.area_id)
            level = (risk.level if risk else "Normal").capitalize()
            if level in ("Danger", "Caution"):
                radius_km = 30.0 if level == "Danger" else 18.0
                # Generate 24-point circle polygon approximation
                coords = []
                for i in range(25):
                    angle = (i / 24.0) * 2 * math.pi
                    d_lat = (radius_km / 111.0) * math.sin(angle)
                    d_lon = (radius_km / (111.0 * math.cos(math.radians(a.latitude)))) * math.cos(angle)
                    coords.append([round(a.longitude + d_lon, 5), round(a.latitude + d_lat, 5)])

                polygons.append({
                    "type": "Feature",
                    "properties": {
                        "area_id": a.area_id,
                        "name_en": a.name_en,
                        "level": level,
                        "fill_color": "#ef4444" if level == "Danger" else "#f59e0b",
                        "radius_km": radius_km
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [coords]
                    }
                })

        return {
            "type": "FeatureCollection",
            "features": polygons
        }





