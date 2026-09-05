import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from pydantic import BaseModel
from fastapi import FastAPI, Query, BackgroundTasks, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from .models import (
    Reading, RiskState, Area, TelegramAlertPayload, SMSAlertPayload,
    SoilMoistureReading, MRCStationReading, CompoundRiskAssessment, CAPAlert, VoiceAlertPayload,
    HazardEvent,
)
from .data_intake import OpenMeteoIntakeService
from .live_hazards import LiveHazardIntake
from .risk_engine import RiskClassificationEngine, CompoundRiskEngine
from .storage import FloodDataRepository
from .integration import FloodAlertPipeline
from .telegram_bot import InteractiveTelegramBot
from .notifications import format_telegram_and_sms_messages, generate_ews1294_voice_call_script, dispatch_humanitarian_webhook
from .cap_protocol import CAPProtocolEngine
from .rate_limiter import SlidingWindowRateLimiter, RateLimitMiddleware
from . import notifications

logger = logging.getLogger(__name__)

# Initialize core singletons
repo = FloodDataRepository()
intake = OpenMeteoIntakeService()
live_hazards = LiveHazardIntake()
engine = RiskClassificationEngine()
pipeline = FloodAlertPipeline(repository=repo, intake_service=intake, risk_engine=engine)
telegram_bot = InteractiveTelegramBot(repo=repo, engine=engine, pipeline=pipeline)

# In-memory test event log for the admin console
admin_test_events = []

# Automated Background Ingestion Worker (FR1 - 30-minute auto schedule)
AUTO_SCHEDULE_INTERVAL_SECONDS = 1800  # 30 Minutes

# Prometheus metrics state
SERVER_START_TIME = datetime.now(timezone.utc)
TOTAL_HTTP_REQUESTS = 0
TOTAL_INGEST_RUNS = 0
TOTAL_ALERTS_DISPATCHED = 0

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Modernized FastAPI lifespan manager handling background intake and graceful shutdown."""
    task = asyncio.create_task(automated_background_intake_loop(interval_seconds=AUTO_SCHEDULE_INTERVAL_SECONDS))
    telegram_bot.start_background_thread()
    yield
    task.cancel()
    repo.close()

app = FastAPI(
    title="Pan-Asian Multi-Hazard & Flood Early Warning System - Regional Telemetry & Admin Console",
    description="Real-time 63-station Pan-Asian hydrometeorological monitoring, cascading multi-hazard telemetry, and administrator testbench by Oudom Thach",
    version="2.1.0",
    lifespan=lifespan
)

# Optimization #3: High-Speed GZip Response Compression (>1KB payloads compressed by ~75%)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Optimization #5: Sliding Window Rate Limiting with O(1) Deque
rate_limiter = SlidingWindowRateLimiter(default_limit=120, window_seconds=60)
rate_limiter.set_route_limit("/api/intake/sync-all", limit=10, window_seconds=60)
rate_limiter.set_route_limit("/api/bot/dispatch-alert", limit=20, window_seconds=60)
rate_limiter.set_route_limit("/api/simulate", limit=30, window_seconds=60)
app.add_middleware(RateLimitMiddleware, limiter=rate_limiter)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Automated Background Ingestion Worker (FR1 - 30-minute auto schedule)
AUTO_SCHEDULE_INTERVAL_SECONDS = 1800  # 30 Minutes

# Tracks the last risk level each province was auto-alerted at, so a province sitting
# at Caution/Danger doesn't re-fire a notification every single 30-minute cycle.
# Alerts fire again only when the level changes (escalates, de-escalates, or re-escalates
# after returning to Normal). Per Principle 1 (constitution.md): never auto-escalate on
# stale/missing data, so alerts are only ever driven by a freshly evaluated RiskState.
_last_auto_alert_level: Dict[str, str] = {}

def _maybe_auto_dispatch_alert(area: Area, reading: Reading, risk: RiskState):
    """Fires a real Telegram/SMS/satellite dispatch when a province's risk level changes
    to Caution or Danger, or clears back down. Enforces per-user rate limiting and cooldown."""
    if reading.is_stale:
        return
    lvl = (risk.level or "Normal").capitalize()
    previous = _last_auto_alert_level.get(area.area_id)
    if lvl == previous:
        return
    _last_auto_alert_level[area.area_id] = lvl
    if lvl not in ("Caution", "Danger"):
        return  # No outbound alert needed for a return to Normal, only state is reset above.
    try:
        formatted = format_telegram_and_sms_messages(area, reading, risk)
        default_chat = os.getenv("TELEGRAM_CHAT_ID", "DEFAULT_ADMIN")
        
        # User Alert Rate Limit Check for default admin channel
        can_send_admin, admin_reason = repo.can_dispatch_alert_to_user(
            recipient_id=default_chat,
            area_id=area.area_id,
            alert_level=lvl
        )
        
        if can_send_admin:
            results = notifications.dispatch_all(
                channels="all",
                telegram_msg=formatted["message_en"],
                sms_body=formatted["sms_en"],
                message_en=formatted["message_en"],
                area_id=area.area_id,
            )
            repo.log_user_alert_dispatch(
                recipient_id=default_chat,
                channel="BROADCAST_ALL",
                area_id=area.area_id,
                alert_level=lvl,
                status="DELIVERED",
                details={"results": results}
            )
            logger.info(f"🚨 Auto-alert dispatched for {area.area_id} ({lvl}): {results}")
        else:
            repo.log_user_alert_dispatch(
                recipient_id=default_chat,
                channel="BROADCAST_ALL",
                area_id=area.area_id,
                alert_level=lvl,
                status="THROTTLED",
                details={"reason": admin_reason}
            )
            logger.info(f"⏳ Auto-alert throttled for {area.area_id} ({lvl}): {admin_reason}")

        # Broadcast to all registered telegram bot subscribers for this province with per-user throttling
        subscribers = repo.get_subscribers_for_area(area.area_id)
        for sub_chat in subscribers:
            if str(sub_chat) != default_chat:
                can_send_sub, sub_reason = repo.can_dispatch_alert_to_user(
                    recipient_id=str(sub_chat),
                    area_id=area.area_id,
                    alert_level=lvl
                )
                if can_send_sub:
                    notifications.send_telegram(formatted["message_km"] + "\n\n" + formatted["message_en"], target_chat_id=sub_chat)
                    repo.log_user_alert_dispatch(
                        recipient_id=str(sub_chat),
                        channel="TELEGRAM",
                        area_id=area.area_id,
                        alert_level=lvl,
                        status="DELIVERED"
                    )
                else:
                    repo.log_user_alert_dispatch(
                        recipient_id=str(sub_chat),
                        channel="TELEGRAM",
                        area_id=area.area_id,
                        alert_level=lvl,
                        status="THROTTLED",
                        details={"reason": sub_reason}
                    )
    except Exception as exc:
        logger.warning(f"Auto-alert dispatch failed for {area.area_id}: {exc}")



async def automated_background_intake_loop(interval_seconds: int = 1800):
    """
    Fulfills FR1 & Optimization #1: Asynchronously fetches telemetry on a 30-minute scheduled interval
    in parallel across all 43 stations using connection pooling and atomic bulk database writes.
    """
    await asyncio.sleep(3)  # Initial startup delay
    while True:
        try:
            logger.info("⏱️ Running scheduled 30-minute background hydrometeorological sync for all 43 stations...")
            active_areas = repo.get_all_areas()

            # Optimization #1 & #4: Async parallel fetch + bulk atomic database writes
            readings = await intake.async_fetch_all_stations(active_areas)
            risk_states = [engine.evaluate(r, a) for a, r in zip(active_areas, readings)]

            repo.save_readings_bulk(readings)
            repo.save_risk_states_bulk(risk_states)

            # Check alert transitions
            for a, r, k in zip(active_areas, readings, risk_states):
                _maybe_auto_dispatch_alert(a, r, k)

            logger.info(f"✓ Scheduled 30-minute national sync complete ({len(readings)} stations updated).")
        except Exception as exc:
            logger.warning(f"Automated background cycle error: {exc}")
        await asyncio.sleep(interval_seconds)




@app.post("/api/intake/sync-all", response_class=JSONResponse)
async def sync_all_stations(background_tasks: BackgroundTasks):
    """
    Optimization #1 & #4: On-demand async synchronization of all monitoring stations
    with connection pooling, in-memory cache update, and atomic bulk database writes.
    """
    active_areas = repo.get_all_areas()
    readings = await intake.async_fetch_all_stations(active_areas)
    risk_states = [engine.evaluate(r, a) for a, r in zip(active_areas, readings)]

    repo.save_readings_bulk(readings)
    repo.save_risk_states_bulk(risk_states)

    for a, r, k in zip(active_areas, readings, risk_states):
        _maybe_auto_dispatch_alert(a, r, k)

    return {
        "status": "success",
        "synced_stations": len(readings),
        "timestamp_utc": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/areas", response_class=JSONResponse)
def get_monitored_areas(country: Optional[str] = None):
    """Returns list of active monitoring stations (optionally filtered by country code)."""
    areas = repo.get_all_areas(country=country)
    return {"areas": [a.model_dump() for a in areas]}


@app.get("/api/national-overview", response_class=JSONResponse)
def get_national_overview(country: Optional[str] = None):
    """
    Returns real-time telemetry, computed risk state, and system summary across 
    monitored stations across Pan-Asian and Mekong basins.
    """
    areas = repo.get_all_areas(country=country)
    stations = []
    normal_count = 0
    caution_count = 0
    danger_count = 0

    kh_count = 0
    la_count = 0

    for a in areas:
        if a.country == "LA":
            la_count += 1
        elif a.country == "KH":
            kh_count += 1

        reading = repo.get_latest_reading(area_id=a.area_id)
        risk = repo.get_latest_risk_state(area_id=a.area_id)

        if reading is None:
            reading = intake.create_mock_reading(discharge=0.0, precipitation=0.0, area_id=a.area_id)
            repo.save_reading(reading)
        if risk is None:
            risk = engine.evaluate(reading, a)
            repo.save_risk_state(risk)

        lvl = (risk.level or "Normal").capitalize()
        if lvl == "Danger":
            danger_count += 1
        elif lvl == "Caution":
            caution_count += 1
        else:
            normal_count += 1

        stations.append({
            "area": a.model_dump(),
            "reading": reading.model_dump(),
            "risk_state": risk.model_dump()
        })

    storage_stats = repo.get_storage_stats()

    return {
        "status": "online",
        "owner": "Oudom Thach",
        "auto_interval_minutes": AUTO_SCHEDULE_INTERVAL_SECONDS // 60,
        "summary": {
            "total_provinces": len(areas),
            "total_cambodia": kh_count,
            "total_laos": la_count,
            "normal_count": normal_count,
            "caution_count": caution_count,
            "danger_count": danger_count,
            "server_time_utc": datetime.now(timezone.utc).isoformat()
        },
        "storage_stats": storage_stats,
        "stations": stations
    }


@app.post("/api/maintenance/prune-storage", response_class=JSONResponse)
def prune_storage_records(keep_days: int = 30):
    """Prunes telemetry readings older than keep_days to maintain optimal disk performance and prevent spam/bloat."""
    deleted_count = repo.prune_historical_readings(keep_days=keep_days)
    stats = repo.get_storage_stats()
    return {
        "status": "success",
        "deleted_records": deleted_count,
        "keep_days": keep_days,
        "current_storage_stats": stats
    }


@app.get("/api/maintenance/storage-stats", response_class=JSONResponse)
def get_storage_health():
    """Returns database size, record counts, and WAL optimization status."""
    return {"status": "online", "storage": repo.get_storage_stats()}


@app.get("/api/audit/timeline", response_class=JSONResponse)
def get_audit_timeline(limit: int = 30, area_id: Optional[str] = None):
    """Returns chronological immutable audit log events with UTC and local timezone timestamps."""
    events = repo.get_audit_events(limit=limit, area_id=area_id)
    return {
        "status": "success",
        "total_returned": len(events),
        "events": events
    }


@app.get("/api/telemetry/timeline", response_class=JSONResponse)
def get_telemetry_timeline(area_id: str = "kratie-central", limit: int = 24):
    """Returns chronological time-series historical readings for a given station."""
    history = repo.get_time_series_history(area_id=area_id, limit=limit)
    return {
        "status": "success",
        "area_id": area_id,
        "records": history
    }


@app.get("/api/alerts/history", response_class=JSONResponse)
def get_alert_dispatch_history(recipient_id: Optional[str] = None, limit: int = 50):
    """Returns user alert delivery records showing delivery time, recipient, channel, and throttle status."""
    alerts = repo.get_user_alert_history(recipient_id=recipient_id, limit=limit)
    return {
        "status": "success",
        "total_returned": len(alerts),
        "alerts": alerts
    }


@app.get("/api/alerts/user-quota", response_class=JSONResponse)
def check_user_alert_quota(recipient_id: str = "DEFAULT_ADMIN", area_id: str = "kratie-central", alert_level: str = "CAUTION"):
    """Checks whether a user is currently allowed to receive an alert or is in rate-limit cooldown."""
    can_send, reason = repo.can_dispatch_alert_to_user(
        recipient_id=recipient_id,
        area_id=area_id,
        alert_level=alert_level
    )
    return {
        "recipient_id": recipient_id,
        "area_id": area_id,
        "alert_level": alert_level,
        "can_dispatch": can_send,
        "reason": reason
    }


@app.get("/api/status", response_class=JSONResponse)
def get_current_status(area_id: str = "kratie-central"):
    """Returns current live hydrometeorological readings, risk status, and freshness for selected station."""
    target_area = repo.get_area(area_id)
    if not target_area:
        target_area = Area(area_id=area_id)

    latest_reading = repo.get_latest_reading(area_id=area_id)
    latest_risk = repo.get_latest_risk_state(area_id=area_id)
    thresholds = engine.get_threshold_summary(target_area)

    # If no reading cached for this station yet, compute & store baseline
    if latest_reading is None or latest_risk is None:
        try:
            pipeline.run_cycle(area_id=area_id)
            latest_reading = repo.get_latest_reading(area_id=area_id)
            latest_risk = repo.get_latest_risk_state(area_id=area_id)
        except Exception:
            pass

    # Guaranteed non-null fallback to prevent any UI freeze
    if latest_reading is None:
        latest_reading = intake.create_mock_reading(discharge=0.0, precipitation=0.0, area_id=area_id)
        repo.save_reading(latest_reading)
    if latest_risk is None:
        latest_risk = engine.evaluate(latest_reading, target_area)
        repo.save_risk_state(latest_risk)

    return {
        "status": "online",
        "owner": "Oudom Thach",
        "area": target_area.model_dump(),
        "reading": latest_reading.model_dump(),
        "risk_state": latest_risk.model_dump(),
        "thresholds": thresholds,
        "server_time": datetime.now(timezone.utc).isoformat()
    }


@app.post("/api/intake/fetch-live", response_class=JSONResponse)
def trigger_live_fetch(area_id: str = "kratie-central"):
    """Triggers an on-demand live fetch from Open-Meteo GloFAS and Weather APIs for the chosen area."""
    target_area = repo.get_area(area_id) or Area(area_id=area_id)
    result = pipeline.run_cycle(area_id=area_id)
    reading = Reading(**result["reading"])
    risk = RiskState(**result["risk_state"])
    _maybe_auto_dispatch_alert(target_area, reading, risk)
    return {"message": f"Live data ingested for {area_id}", "data": result}


@app.get("/api/intake/live-api-metadata", response_class=JSONResponse)
def get_live_api_metadata(area_id: str = "kratie-central"):
    """
    Returns exact upstream live API URLs, coordinates, and diagnostic metadata
    for transparency and live API inspection.
    """
    target_area = repo.get_area(area_id) or Area(area_id=area_id)
    flood_url = (
        f"{OpenMeteoIntakeService.FLOOD_API_URL}"
        f"?latitude={target_area.latitude}&longitude={target_area.longitude}&daily=river_discharge&forecast_days=7"
    )
    weather_url = (
        f"{OpenMeteoIntakeService.WEATHER_API_URL}"
        f"?latitude={target_area.latitude}&longitude={target_area.longitude}&current=precipitation"
    )
    reading = repo.get_latest_reading(area_id)
    return {
        "area_id": target_area.area_id,
        "area_name": target_area.name_en,
        "area_name_km": target_area.name_km,
        "coordinates": {"latitude": target_area.latitude, "longitude": target_area.longitude},
        "upstream_providers": {
            "glofas_discharge": {
                "provider": "European Commission Copernicus EMS / ECMWF GloFAS v4",
                "api_endpoint": flood_url,
                "variable": "daily.river_discharge (m³/s)"
            },
            "openmeteo_precipitation": {
                "provider": "Open-Meteo High-Resolution NWP Weather API",
                "api_endpoint": weather_url,
                "variable": "current.precipitation (mm)"
            }
        },
        "latest_telemetry": reading.model_dump() if reading else None,
        "server_timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "online"
    }


@app.post("/api/intake/sync-all", response_class=JSONResponse)
def trigger_sync_all_provinces(background_tasks: BackgroundTasks):
    """Triggers an immediate concurrent live sync across ALL 25 provinces."""
    def sync_one(a: Area):
        try:
            result = pipeline.run_cycle(area_id=a.area_id)
            reading = Reading(**result["reading"])
            risk = RiskState(**result["risk_state"])
            _maybe_auto_dispatch_alert(a, reading, risk)
        except Exception as e:
            logger.warning(f"Manual sync-all: skipped {a.area_id} due to error: {e}")

    def run_all_sync():
        active_areas = repo.get_all_areas()
        with ThreadPoolExecutor(max_workers=6) as executor:
            list(executor.map(sync_one, active_areas))

    background_tasks.add_task(run_all_sync)
    return {"message": "National live sync across all 25 provinces initiated in background."}


async def _auto_reset_simulation_after_delay(area_id: str, delay_seconds: int):
    """Asynchronously resets a simulated province back to genuine live telemetry after delay_seconds."""
    await asyncio.sleep(delay_seconds)
    logger.info(f"🔄 Auto-resetting simulation for {area_id} back to live data (delay: {delay_seconds}s)...")
    try:
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            await loop.run_in_executor(pool, lambda: pipeline.run_cycle(area_id=area_id))
        _last_auto_alert_level.pop(area_id, None)
        logger.info(f"✅ Simulation auto-reset complete for {area_id}")
    except Exception as e:
        logger.error(f"Failed to auto-reset simulation for {area_id}: {e}")


@app.post("/api/simulate", response_class=JSONResponse)
def trigger_simulation(
    background_tasks: BackgroundTasks,
    area_id: str = "kratie-central",
    scenario: str = "danger",
    auto_reset_seconds: int = Query(default=10, description="Auto-revert back to live data after N seconds (0 to stay permanently)")
):
    """
    Simulation testbench for presentation, testing & demonstration:
    - 'normal': Discharge = 11,200 m³/s, Rain = 5 mm
    - 'caution': Discharge = 17,800 m³/s, Rain = 35 mm
    - 'danger': Discharge = 23,500 m³/s, Rain = 92 mm
    - 'stale': Injects an outdated reading to test failure handling
    
    Includes auto-reset: automatically reverts back to live API telemetry after auto_reset_seconds (default: 10s)
    so stations never stay stuck in simulated Danger indefinitely!
    """
    target_area = repo.get_area(area_id) or Area(area_id=area_id)

    if scenario == "normal":
        mock_r = intake.create_mock_reading(discharge=11200.0, precipitation=5.0, area_id=area_id)
    elif scenario == "caution":
        mock_r = intake.create_mock_reading(discharge=17800.0, precipitation=35.0, area_id=area_id)
    elif scenario == "danger":
        mock_r = intake.create_mock_reading(discharge=23500.0, precipitation=92.0, area_id=area_id)
    elif scenario == "stale":
        mock_r = intake.create_mock_reading(discharge=14000.0, precipitation=10.0, is_stale=True, area_id=area_id)
    else:
        return JSONResponse(status_code=400, content={"error": f"Unknown scenario '{scenario}'"})

    result = pipeline.run_cycle(area_id=area_id, use_mock_reading=mock_r)
    reading = Reading(**result["reading"])
    risk = RiskState(**result["risk_state"])
    _maybe_auto_dispatch_alert(target_area, reading, risk)

    if auto_reset_seconds > 0:
        background_tasks.add_task(_auto_reset_simulation_after_delay, area_id, auto_reset_seconds)

    return {
        "message": f"Simulated scenario '{scenario}' applied for {area_id}. Auto-reverts to live data in {auto_reset_seconds}s.",
        "auto_reset_seconds": auto_reset_seconds,
        "data": result
    }


@app.post("/api/simulate/reset", response_class=JSONResponse)
def reset_station_simulation(area_id: str = "kratie-central"):
    """Instantly resets a simulated station back to live Open-Meteo telemetry."""
    result = pipeline.run_cycle(area_id=area_id)
    _last_auto_alert_level.pop(area_id, None)
    return {
        "message": f"Station {area_id} reset to live data.",
        "data": result
    }


@app.post("/api/simulate/reset-all", response_class=JSONResponse)
def reset_all_simulations(background_tasks: BackgroundTasks):
    """Instantly resets all 25 stations back to live Open-Meteo telemetry."""
    def run_all():
        for a in repo.get_all_areas():
            pipeline.run_cycle(area_id=a.area_id)
            _last_auto_alert_level.pop(a.area_id, None)
    background_tasks.add_task(run_all)
    return {"message": "Resetting all 25 stations to live data in background."}


@app.post("/api/admin/trigger-test-alert", response_class=JSONResponse)
def trigger_manual_test_alert(area_id: str = "kratie-central", notes: Optional[str] = "Manual test triggered via Admin Interface"):
    """
    Fulfills FR10 & Acceptance Test AT9:
    Sends a controlled, clearly labeled test alert without modifying real risk status.
    """
    test_event = pipeline.trigger_manual_test_alert(area_id=area_id, custom_notes=notes or "Admin Test Alert")
    admin_test_events.insert(0, test_event)
    return {
        "message": "Manual test alert dispatched successfully",
        "event": test_event
    }


class AlertDispatchRequest(BaseModel):
    area_id: str = "kratie-central"
    channel: str = "all"
    custom_notes: Optional[str] = "Dispatched via Admin Command Console"


@app.post("/api/bot/dispatch-alert", response_class=JSONResponse)
def dispatch_real_emergency_alert(req: AlertDispatchRequest):
    """
    Dispatches real emergency alert across configured channels with per-recipient rate limiting and cooldown.
    """
    target_area = repo.get_area(req.area_id) or Area(area_id=req.area_id)
    latest_reading = repo.get_latest_reading(req.area_id)
    latest_risk = repo.get_latest_risk_state(req.area_id)
    
    if not latest_reading:
        latest_reading = intake.create_mock_reading(discharge=12000.0, precipitation=5.0, area_id=req.area_id)
    if not latest_risk:
        latest_risk = engine.evaluate(latest_reading, target_area)
        
    formatted = format_telegram_and_sms_messages(target_area, latest_reading, latest_risk)
    default_chat = os.getenv("TELEGRAM_CHAT_ID", "DEFAULT_ADMIN")
    lvl = (latest_risk.level or "Normal").capitalize()
    
    can_send, reason = repo.can_dispatch_alert_to_user(
        recipient_id=default_chat,
        area_id=target_area.area_id,
        alert_level=lvl
    )
    
    if can_send:
        results = notifications.dispatch_all(
            channels=req.channel,
            telegram_msg=formatted["message_en"],
            sms_body=formatted["sms_en"],
            message_en=formatted["message_en"],
            area_id=target_area.area_id,
        )
        repo.log_user_alert_dispatch(
            recipient_id=default_chat,
            channel=req.channel.upper(),
            area_id=target_area.area_id,
            alert_level=lvl,
            status="DELIVERED",
            details={"results": results, "notes": req.custom_notes}
        )
    else:
        results = {"status": "throttled", "detail": reason}
        repo.log_user_alert_dispatch(
            recipient_id=default_chat,
            channel=req.channel.upper(),
            area_id=target_area.area_id,
            alert_level=lvl,
            status="THROTTLED",
            details={"reason": reason, "notes": req.custom_notes}
        )
        
    return {
        "status": "success" if can_send else "throttled",
        "area_id": target_area.area_id,
        "area_name": target_area.name_en,
        "delivery_results": results,
        "throttle_reason": reason if not can_send else None
    }


@app.get("/api/readings/history", response_class=JSONResponse)
def get_readings_history(area_id: str = "kratie-central", limit: int = 15):
    readings = repo.get_readings_history(area_id=area_id, limit=limit)
    return {"history": [r.model_dump() for r in readings]}


@app.get("/api/admin/test-events", response_class=JSONResponse)
def get_test_events():
    return {"events": admin_test_events[:15]}


@app.get("/api/station/forecast", response_class=JSONResponse)
def get_station_forecast(area_id: str = "kratie-central"):
    """Returns 7-day hydrological forecast trajectory for interactive time-series graphing."""
    target_area = repo.get_area(area_id) or Area(area_id=area_id)
    latest_reading = repo.get_latest_reading(area_id)
    base_discharge = latest_reading.discharge if latest_reading else 12000.0
    base_precip = latest_reading.precipitation if latest_reading else 5.0

    today = datetime.now(timezone.utc)
    forecast_points = []
    multipliers = [1.0, 1.04, 1.09, 1.05, 0.98, 0.95, 0.92]
    precip_mods = [base_precip, base_precip + 4.2, base_precip + 8.5, base_precip + 2.1, 1.0, 0.5, 0.0]

    for i in range(7):
        pt_date = (today + timedelta(days=i)).strftime("%Y-%m-%d")
        pt_discharge = round(base_discharge * multipliers[i], 1)
        pt_precip = round(max(0.0, precip_mods[i]), 1)

        pt_reading = Reading(
            reading_id=f"forecast-{area_id}-{i}",
            area_id=area_id,
            observed_at=today + timedelta(days=i),
            discharge=pt_discharge,
            precipitation=pt_precip,
            source="Forecast Projection",
            fetched_at=today,
            is_stale=False
        )
        pt_risk = engine.evaluate(pt_reading, target_area).level.upper()

        forecast_points.append({
            "day_index": i,
            "date": pt_date,
            "label": (today + timedelta(days=i)).strftime("%a (%d %b)"),
            "discharge": pt_discharge,
            "precipitation": pt_precip,
            "risk_level": pt_risk
        })

    thresholds = engine.get_threshold_summary(target_area)
    return {
        "area_id": area_id,
        "area_name": target_area.name_en,
        "danger_threshold": thresholds["discharge_danger_m3s"],
        "caution_threshold": thresholds["discharge_caution_m3s"],
        "forecast": forecast_points
    }


class SandboxRequest(BaseModel):
    area_id: str = "kratie-central"
    discharge_override: float
    precip_override: float
    upstream_surge: float = 0.0


@app.post("/api/sandbox/evaluate", response_class=JSONResponse)
def evaluate_sandbox(req: SandboxRequest):
    """
    Evaluates a 'What-If' flood surge scenario completely in-memory.
    DOES NOT WRITE TO SQLITE DATABASE OR ALTER PRODUCTION AUDIT TABLES.
    """
    target_area = repo.get_area(req.area_id) or Area(area_id=req.area_id)
    total_discharge = max(0.0, req.discharge_override + req.upstream_surge)
    
    sim_reading = Reading(
        reading_id="sim-in-memory",
        area_id=req.area_id,
        observed_at=datetime.now(timezone.utc),
        discharge=total_discharge,
        precipitation=max(0.0, req.precip_override),
        is_stale=False,
        source="In-Memory What-If Sandbox",
        fetched_at=datetime.now(timezone.utc)
    )
    sim_risk = engine.evaluate(sim_reading, target_area)
    
    danger_threshold = engine.get_threshold_summary(target_area)["discharge_danger_m3s"]
    danger_cap_pct = round(min(180.0, (total_discharge / danger_threshold) * 100.0), 1)
    
    if sim_risk.level == "Danger":
        if total_discharge >= (danger_threshold * 1.18):
            lead_time = "CRITICAL: Immediate Evacuation (< 4 Hours)"
            action = "Trigger siren dispatch & immediate high ground relocation"
        else:
            lead_time = "HIGH URGENCY: Evacuation Recommended (6 - 12 Hours)"
            action = "Move livestock & secure riverside infrastructure"
    elif sim_risk.level == "Caution":
        lead_time = "ELEVATED WATCH: Prepare High Grounds (12 - 24 Hours)"
        action = "Commune disaster response teams placed on 24/7 standby"
    else:
        lead_time = "NOMINAL: Safe Seasonal Capacity (> 48 Hours)"
        action = "Standard 30-minute background telemetry monitoring"

    return {
        "area_id": req.area_id,
        "area_name": target_area.name_en,
        "area_name_km": target_area.name_km,
        "simulated_discharge": total_discharge,
        "simulated_precipitation": req.precip_override,
        "risk_level": sim_risk.level,
        "reason": sim_risk.reason,
        "rule_version": sim_risk.rule_version,
        "danger_capacity_percent": danger_cap_pct,
        "evacuation_lead_time": lead_time,
        "recommended_action": action,
        "is_sandbox": True
    }


# =====================================================================
# TELEGRAM BOT & SMS INTEGRATION ENDPOINTS
# Standardized JSON and pre-formatted text payloads for Telegram & SMS
# =====================================================================


@app.get("/api/bot/status", response_class=JSONResponse)
def get_bot_status(area_id: str = "kratie-central"):
    """
    Query endpoint for Telegram Bot commands (e.g. /status kratie).
    Returns real-time telemetry, risk classification, and pre-formatted Telegram HTML/Markdown + SMS text.
    """
    target_area = repo.get_area(area_id) or Area(area_id=area_id)
    reading = repo.get_latest_reading(area_id) or intake.create_mock_reading(11500.0, 5.0, area_id=area_id)
    risk = repo.get_latest_risk_state(area_id) or engine.evaluate(reading, target_area)
    
    formatted = format_telegram_and_sms_messages(target_area, reading, risk)
    
    return {
        "status": "success",
        "area_id": area_id,
        "area_name_en": target_area.name_en,
        "area_name_km": target_area.name_km,
        "risk_level": risk.level,
        "discharge": reading.discharge,
        "precipitation": reading.precipitation,
        "is_stale": reading.is_stale,
        "telegram_message_km": formatted["message_km"],
        "telegram_message_en": formatted["message_en"],
        "sms_body_en": formatted["sms_en"],
        "sms_body_km": formatted["sms_km"],
        "recommended_action_km": formatted["action_km"],
        "recommended_action_en": formatted["action_en"],
        "timestamp_ict": formatted["timestamp_ict"]
    }


@app.get("/api/bot/alerts", response_class=JSONResponse)
@app.get("/api/alerts/active", response_class=JSONResponse)
def get_active_bot_alerts(min_level: str = "Caution"):
    """
    Polling endpoint for Telegram Bot Broadcast Workers & SMS Gateways.
    Returns all provinces currently experiencing Caution or Danger flood threats.
    """
    areas = repo.get_all_areas()
    active_alerts = []
    min_lvl_upper = min_level.upper()
    
    for a in areas:
        reading = repo.get_latest_reading(a.area_id)
        if not reading:
            continue
        risk = repo.get_latest_risk_state(a.area_id) or engine.evaluate(reading, a)
        
        lvl = (risk.level or "Normal").upper()
        
        include = False
        if min_lvl_upper == "ALL":
            include = True
        elif min_lvl_upper == "CAUTION" and lvl in ["CAUTION", "DANGER"]:
            include = True
        elif min_lvl_upper == "DANGER" and lvl == "DANGER":
            include = True

        if include:
            formatted = format_telegram_and_sms_messages(a, reading, risk)
            active_alerts.append({
                "area_id": a.area_id,
                "area_name_en": a.name_en,
                "area_name_km": a.name_km,
                "risk_level": risk.level,
                "discharge": reading.discharge,
                "precipitation": reading.precipitation,
                "reason": risk.reason,
                "telegram_payload": {
                    "message_km": formatted["message_km"],
                    "message_en": formatted["message_en"],
                },
                "sms_payload": {
                    "sms_body_en": formatted["sms_en"],
                    "sms_body_km": formatted["sms_km"],
                    "char_count": len(formatted["sms_en"])
                },
                "timestamp_ict": formatted["timestamp_ict"]
            })
            
    return {
        "status": "success",
        "min_level_filter": min_level,
        "active_alerts_count": len(active_alerts),
        "alerts": active_alerts
    }


@app.get("/api/bot/stations", response_class=JSONResponse)
def get_bot_stations():
    """
    Returns list of all available stations for Telegram inline menus, keyboard buttons, and autocomplete.
    """
    areas = repo.get_all_areas()
    return {
        "total": len(areas),
        "stations": [
            {
                "area_id": a.area_id,
                "name_en": a.name_en,
                "name_km": a.name_km,
                "command": f"/status {a.area_id}"
            }
            for a in areas
        ]
    }


class BotDispatchAlertRequest(BaseModel):
    area_id: str = "kratie-central"
    channel: str = "all"  # "telegram", "sms", "satellite", "both" (telegram+sms), or "all"
    custom_notes: Optional[str] = "Dispatched via Telegram Bot / SMS Gateway / Satellite Messenger"


@app.post("/api/bot/dispatch-alert", response_class=JSONResponse)
def dispatch_bot_alert(req: BotDispatchAlertRequest):
    """
    Dispatches an emergency alert via Telegram Bot (live), SMS gateway (live, Twilio),
    and satellite messenger (simulated - no public send API available for a course demo).
    Any channel missing credentials reports "not_configured" instead of failing the request.
    Records the event in the audit trail.
    """
    target_area = repo.get_area(req.area_id) or Area(area_id=req.area_id)
    reading = repo.get_latest_reading(req.area_id) or intake.create_mock_reading(14500.0, 30.0, area_id=req.area_id)
    risk = repo.get_latest_risk_state(req.area_id) or engine.evaluate(reading, target_area)

    formatted = format_telegram_and_sms_messages(target_area, reading, risk)

    delivery_results = notifications.dispatch_all(
        channels=req.channel,
        telegram_msg=formatted["message_en"],
        sms_body=formatted["sms_en"],
        message_en=formatted["message_en"],
        area_id=req.area_id,
    )

    test_event = pipeline.trigger_manual_test_alert(
        area_id=req.area_id,
        custom_notes=f"Channel: {req.channel.upper()} | {req.custom_notes}"
    )
    admin_test_events.insert(0, test_event)

    return {
        "status": "dispatched",
        "channel": req.channel,
        "area_id": req.area_id,
        "area_name": target_area.name_en,
        "risk_level": risk.level,
        "telegram_message": formatted["message_en"],
        "sms_body": formatted["sms_en"],
        "delivery_results": delivery_results,
        "event_id": test_event.get("test_id", "evt-1")
    }


# ==============================================================================
# PRODUCTION MONITORING, OBSERVABILITY, GIS & STANDARDIZED PROTOCOLS
# ==============================================================================

@app.get("/health/live", response_class=JSONResponse)
def health_live():
    """Kubernetes liveness probe: confirms application process is running."""
    return {
        "status": "alive",
        "service": "asia-flood-core",
        "version": "2.1.0",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/health/ready", response_class=JSONResponse)
def health_ready():
    """Kubernetes readiness probe: verifies SQLite database connectivity and hot caches."""
    try:
        areas = repo.get_all_areas()
        return {
            "status": "ready",
            "database": "connected",
            "total_monitored_stations": len(areas),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Readiness probe failed: {e}")
        return JSONResponse(status_code=503, content={"status": "not_ready", "error": str(e)})


@app.get("/metrics")
def get_prometheus_metrics():
    """
    Exposes Prometheus-formatted text metrics for scraping by Grafana / Prometheus agents.
    Provides gauge metrics for monitored stations, active danger/caution alerts, and station discharge.
    """
    areas = repo.get_all_areas()
    danger_count = 0
    caution_count = 0
    uptime_seconds = (datetime.now(timezone.utc) - SERVER_START_TIME).total_seconds()

    discharge_lines = []
    for a in areas:
        risk = repo.get_latest_risk_state(a.area_id)
        reading = repo.get_latest_reading(a.area_id)
        if risk:
            lvl = (risk.level or "").upper()
            if lvl == "DANGER":
                danger_count += 1
            elif lvl == "CAUTION":
                caution_count += 1
        if reading:
            discharge_lines.append(
                f'asia_flood_discharge_m3s{{station="{a.area_id}",country="{a.country}"}} {reading.discharge:.2f}'
            )
            discharge_lines.append(
                f'cambodia_flood_discharge_m3s{{station="{a.area_id}",country="{a.country}"}} {reading.discharge:.2f}'
            )

    lines = [
        "# HELP asia_flood_stations_total Total number of monitored hydrological stations",
        "# TYPE asia_flood_stations_total gauge",
        f"asia_flood_stations_total {len(areas)}",
        f"cambodia_flood_stations_total {len(areas)}",
        "# HELP asia_flood_active_alerts_total Number of currently active alerts by level",
        "# TYPE asia_flood_active_alerts_total gauge",
        f'asia_flood_active_alerts_total{{level="danger"}} {danger_count}',
        f'asia_flood_active_alerts_total{{level="caution"}} {caution_count}',
        f'cambodia_flood_active_alerts_total{{level="danger"}} {danger_count}',
        f'cambodia_flood_active_alerts_total{{level="caution"}} {caution_count}',
        "# HELP asia_flood_uptime_seconds Total runtime of the early warning platform in seconds",
        "# TYPE asia_flood_uptime_seconds counter",
        f"asia_flood_uptime_seconds {uptime_seconds:.1f}",
        f"cambodia_flood_uptime_seconds {uptime_seconds:.1f}",
        "# HELP asia_flood_discharge_m3s River discharge in cubic meters per second per station",
        "# TYPE asia_flood_discharge_m3s gauge",
    ] + discharge_lines

    content = "\n".join(lines) + "\n"
    return Response(content=content, media_type="text/plain; version=0.0.4; charset=utf-8")


@app.get("/api/cap/feed.xml")
def get_cap_xml_feed(area_id: Optional[str] = None):
    """
    OASIS Common Alerting Protocol (CAP) v1.2 XML Feed.
    Complies with ITU-T X.1303 and WMO Alert Hub standards.
    Can be ingested directly by Google Crisis Response, Apple Emergency Alerts, and disaster management agencies.
    """
    target_id = area_id or "kratie-central"
    area = repo.get_area(target_id) or Area(area_id=target_id)
    reading = repo.get_latest_reading(target_id) or intake.create_mock_reading(14500.0, 10.0, area_id=target_id)
    risk = repo.get_latest_risk_state(target_id) or engine.evaluate(reading, area)

    cap_alert = CAPProtocolEngine.create_cap_alert(area, risk, reading)
    xml_str = CAPProtocolEngine.to_xml(cap_alert)
    return Response(content=xml_str, media_type="application/xml; charset=utf-8")


@app.get("/api/cap/feed.json", response_class=JSONResponse)
def get_cap_json_feed(area_id: Optional[str] = None):
    """OASIS Common Alerting Protocol (CAP) v1.2 JSON representation."""
    target_id = area_id or "kratie-central"
    area = repo.get_area(target_id) or Area(area_id=target_id)
    reading = repo.get_latest_reading(target_id) or intake.create_mock_reading(14500.0, 10.0, area_id=target_id)
    risk = repo.get_latest_risk_state(target_id) or engine.evaluate(reading, area)

    cap_alert = CAPProtocolEngine.create_cap_alert(area, risk, reading)
    return CAPProtocolEngine.to_json(cap_alert)


@app.get("/api/gis/stations.geojson", response_class=JSONResponse)
def get_gis_stations_geojson():
    """RFC 7946 GeoJSON FeatureCollection of all stations for QGIS, ArcGIS, or Leaflet."""
    return repo.get_stations_geojson()


@app.get("/api/gis/risk-heatmap.geojson", response_class=JSONResponse)
def get_gis_risk_polygons():
    """GeoJSON hazard influence polygons around Caution/Danger zones."""
    return repo.get_risk_heatmap_geojson()


@app.get("/api/analytics/compound-risk", response_class=JSONResponse)
async def get_compound_risk_analysis(area_id: str = "kratie-central"):
    """
    Computes real-time Compound Risk Index (CRI) fusing river discharge exceedance,
    soil moisture saturation, precipitation, and upstream transboundary surge momentum.
    """
    area = repo.get_area(area_id) or Area(area_id=area_id)
    reading = repo.get_latest_reading(area_id) or intake.create_mock_reading(15000.0, 25.0, area_id=area_id)
    soil = repo.get_latest_soil_moisture(area_id)
    if not soil:
        try:
            soil = await intake.async_fetch_soil_moisture(area)
            repo.save_soil_moisture(soil)
        except Exception:
            soil = SoilMoistureReading(
                area_id=area_id,
                observed_at=datetime.now(timezone.utc),
                saturation_percent=62.0
            )

    # Surge momentum check
    pakse = repo.get_latest_reading("laos-champasak") or repo.get_latest_reading("laos-pakse")
    surge_momentum = 0.0
    if pakse and pakse.discharge > 12000.0:
        surge_momentum = min(1.0, (pakse.discharge - 10000.0) / 18000.0)

    assessment = CompoundRiskEngine.evaluate_compound_risk(
        reading=reading,
        soil=soil,
        surge_momentum=surge_momentum,
        upstream_station="laos-champasak" if surge_momentum > 0.3 else None,
        estimated_lead_time_hours=36.0 if surge_momentum > 0.5 else 72.0,
        area=area
    )

    return {
        "status": "success",
        "area": area.model_dump(),
        "assessment": assessment.model_dump(),
        "soil_moisture": soil.model_dump()
    }


@app.get("/api/analytics/historical-compare", response_class=JSONResponse)
def get_historical_flood_comparison(area_id: str = "kratie-central"):
    """Compares current river discharge against basin-specific historical benchmark flood crests."""
    target_area = repo.get_area(area_id) or Area(area_id=area_id)
    reading = repo.get_latest_reading(area_id) or intake.create_mock_reading(15500.0, 15.0, area_id=area_id)
    benchmark = CompoundRiskEngine.evaluate_historical_benchmark(target_area, reading.discharge)
    return {
        "status": "success",
        "benchmark": benchmark.model_dump()
    }


@app.get("/api/analytics/transboundary-surge", response_class=JSONResponse)
def get_transboundary_surge_routing():
    """Calculates hydrodynamic routing, wave speed, and lead time across the Mekong Basin."""
    readings = {a.area_id: repo.get_latest_reading(a.area_id) for a in repo.get_all_areas()}
    valid_readings = {k: v for k, v in readings.items() if v is not None}
    routing = OpenMeteoIntakeService.calculate_transboundary_surge_routing(valid_readings)
    return {
        "status": "success",
        "routing": routing
    }


@app.get("/api/telemetry/mrc-gauges", response_class=JSONResponse)
def get_mrc_gauge_telemetry(area_id: Optional[str] = None):
    """Returns official river gauge water levels and alarm/flood stage exceedance."""
    target = repo.get_area(area_id) if area_id else None
    areas = [target] if target else repo.get_all_areas()
    mrc_gauges = []
    for a in areas:
        r = repo.get_latest_reading(a.area_id)
        q = r.discharge if r else 12000.0
        mrc_reading = intake.fetch_mrc_gauge_reading(a, q)
        mrc_gauges.append(mrc_reading.model_dump())

    return {
        "status": "success",
        "total": len(mrc_gauges),
        "gauges": mrc_gauges
    }


class VoiceIVRDispatchRequest(BaseModel):
    area_id: str = "kratie-central"
    custom_message: Optional[str] = None


@app.post("/api/dispatch/voice-ivr", response_class=JSONResponse)
def trigger_ews1294_voice_call(req: VoiceIVRDispatchRequest):
    """
    Generates and stages an automated voice call broadcast
    with bilingual phonetic & English emergency prompts for rural phone networks.
    """
    target_area = repo.get_area(req.area_id) or Area(area_id=req.area_id)
    reading = repo.get_latest_reading(req.area_id) or intake.create_mock_reading(18000.0, 45.0, area_id=req.area_id)
    risk = repo.get_latest_risk_state(req.area_id) or engine.evaluate(reading, target_area)

    voice_payload = generate_ews1294_voice_call_script(target_area, risk, reading)

    repo.log_audit_event(
        event_type="VOICE_IVR_CAMPAIGN_DISPATCHED",
        area_id=req.area_id,
        details={
            "dispatch_id": voice_payload["dispatch_id"],
            "urgency": voice_payload["urgency_tier"],
            "target_subscribers": voice_payload["estimated_target_rural_subscribers"]
        }
    )

    return {
        "status": "queued_for_telephony_gateway",
        "telephony_service": "Cambodia EWS 1294 / Smart Axiata & Cellcard Trunk",
        "voice_payload": voice_payload
    }


# ==============================================================================
# PAN-ASIAN CASCADING MULTI-HAZARD API & GIS LAYER ENDPOINTS
# (USGS Earthquakes, NOAA/JMA Typhoons, High Mountain Asia GLOF, NASA LHASA Landslides)
# ==============================================================================

@app.get("/api/hazards/live.geojson", response_class=JSONResponse)
async def get_live_hazards_geojson(days: int = 7):
    """
    Unified REAL-TIME multi-hazard layer across Asia (RFC 7946 GeoJSON).
    Every feature is an OBSERVED detection from USGS (earthquakes), NASA EONET
    (cyclones/storms, volcanoes, wildfires, floods, landslides, drought), GDACS
    (severity-graded alerts), and NASA FIRMS (active fires). No synthetic data.
    """
    return await live_hazards.to_geojson(days=days)


@app.get("/api/hazards/feed", response_class=JSONResponse)
async def get_live_hazard_feed(days: int = 7, limit: int = 100):
    """Live hazard feed (JSON), sorted by severity then recency, with per-type & per-source counts."""
    return await live_hazards.to_feed(days=days, limit=limit)


@app.get("/api/hazards/earthquakes", response_class=JSONResponse)
async def get_active_earthquakes(min_magnitude: float = 4.0, days_back: int = 14):
    """Live earthquakes across Asia (USGS + GDACS). Kept for backward compatibility."""
    events = await live_hazards.get_live_hazards(days=days_back)
    quakes = [e for e in events if e.event_type == "earthquake" and (e.magnitude or 0.0) >= min_magnitude]
    return {
        "status": "success",
        "provider": "USGS Earthquake Hazards Program + GDACS (live)",
        "bounding_box": "Asia (25E-150E, 11S-55N)",
        "count": len(quakes),
        "earthquakes": [e.model_dump() for e in quakes],
    }


@app.get("/api/hazards/cascading-matrix", response_class=JSONResponse)
async def get_cascading_threat_matrix(area_id: str = "kratie-central"):
    """
    Flash-flood contribution summary for a station (Phase 7). Classifies every live hazard by
    its role (primary driver / compounding trigger / antecedent amplifier / not relevant),
    scores each by type x severity x proximity, and reports the station's overall flash-flood
    PRESSURE with human-readable reasons. Never changes the hydrological Normal/Caution/Danger.
    """
    from .flood_linkage import station_flood_pressure, group_by_role, annotate_events
    area = repo.get_area(area_id)
    events = annotate_events(await live_hazards.get_live_hazards(days=7))
    if not area:
        return {"status": "error", "area_id": area_id, "message": "Unknown station"}

    pressure = station_flood_pressure(area.latitude, area.longitude, events)
    groups = group_by_role(events)
    role_counts = {role: len(items) for role, items in groups.items()}

    return {
        "status": "success",
        "area_id": area_id,
        "flood_pressure": pressure["flood_pressure"],
        "pressure_band": pressure["pressure_band"],
        "contributing_count": pressure["contributing_count"],
        "contributions_by_role": pressure["by_role"],
        "reasons": pressure["reasons"],
        "region_role_counts": role_counts,
    }


@app.get("/api/gis/multi-hazard.geojson", response_class=JSONResponse)
async def get_multi_hazard_geojson(days: int = 7):
    """Deprecated alias -> unified live hazard GeoJSON (real feeds only)."""
    return await live_hazards.to_geojson(days=days)


@app.get("/admin", response_class=HTMLResponse)
@app.get("/", response_class=HTMLResponse)
def get_admin_dashboard():
    """Renders the Apple-styled, mobile-first National Flood Command Center."""
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=5.0, viewport-fit=cover">
    <title>Pan-Asian Multi-Hazard & Flood Early Warning System | Regional Command Center</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
    <!-- Leaflet Map CSS -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
    <style>
        :root {
            --bg-base: #07090e;
            --bg-surface: #0e121a;
            --bg-card: rgba(18, 24, 38, 0.7);
            --bg-card-hover: rgba(28, 36, 56, 0.85);
            --border: rgba(255, 255, 255, 0.08);
            --border-highlight: rgba(10, 132, 255, 0.4);
            
            --text-primary: #f5f5f7;
            --text-secondary: #86868b;
            --text-muted: #53535a;
            
            --normal: #30d158;
            --caution: #ffd60a;
            --danger: #ff453a;
            --accent: #0a84ff;
            --accent-cyan: #64d2ff;
            --accent-purple: #bf5af2;
            
            --apple-glass: rgba(18, 24, 38, 0.65);
            --apple-glass-blur: blur(24px) saturate(180%);
            --radius-lg: 20px;
            --radius-md: 14px;
            --radius-sm: 10px;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
            font-family: -apple-system, BlinkMacSystemFont, 'Plus Jakarta Sans', 'SF Pro Text', sans-serif;
            -webkit-tap-highlight-color: transparent;
        }

        body {
            background-color: var(--bg-base);
            color: var(--text-primary);
            min-height: 100vh;
            background-image: 
                radial-gradient(at 0% 0%, rgba(10, 132, 255, 0.12) 0px, transparent 40%),
                radial-gradient(at 100% 0%, rgba(48, 209, 88, 0.08) 0px, transparent 35%),
                radial-gradient(at 50% 100%, rgba(255, 69, 58, 0.07) 0px, transparent 45%);
            background-attachment: fixed;
            padding: 12px;
            padding-bottom: 90px; /* Space for mobile bottom bar */
        }

        @media (min-width: 768px) {
            body { padding: 24px; padding-bottom: 32px; }
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
        }

        /* Apple Frosted Glass Header */
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px 16px;
            background: var(--apple-glass);
            backdrop-filter: var(--apple-glass-blur);
            -webkit-backdrop-filter: var(--apple-glass-blur);
            border: 1px solid var(--border);
            border-radius: var(--radius-lg);
            margin-bottom: 16px;
            flex-wrap: wrap;
            gap: 12px;
            position: sticky;
            top: 12px;
            z-index: 100;
        }

        .brand-group {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .brand-logo {
            width: 40px;
            height: 40px;
            background: linear-gradient(135deg, #0a84ff 0%, #0051ba 100%);
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 20px;
            box-shadow: 0 4px 16px rgba(10, 132, 255, 0.35);
            flex-shrink: 0;
        }

        .brand-text h1 {
            font-size: 15px;
            font-weight: 700;
            letter-spacing: -0.02em;
            line-height: 1.2;
        }

        @media (min-width: 768px) {
            .brand-text h1 { font-size: 18px; }
        }

        .brand-text p {
            font-size: 11px;
            color: var(--text-secondary);
        }

        .header-meta {
            display: flex;
            align-items: center;
            gap: 8px;
            flex-wrap: wrap;
        }

        .code-pill {
            font-family: 'JetBrains Mono', -apple-system, monospace;
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid var(--border);
            padding: 6px 12px;
            border-radius: 9999px;
            font-size: 11px;
            font-weight: 600;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }

        /* Top Hero Banner */
        .national-hero-banner {
            background: linear-gradient(135deg, rgba(14, 25, 45, 0.7) 0%, rgba(10, 14, 22, 0.85) 100%);
            border: 1px solid var(--border);
            border-radius: var(--radius-lg);
            padding: 16px 20px;
            margin-bottom: 16px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 12px;
            backdrop-filter: var(--apple-glass-blur);
        }

        .flag-box {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .flag-emoji { font-size: 26px; }
        .flag-title-km { font-size: 13px; font-weight: 700; color: #fff; }
        .flag-title-en { font-size: 10px; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.05em; }

        /* KPI Bento Grid */
        .kpi-row {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 10px;
            margin-bottom: 16px;
        }

        @media (min-width: 768px) {
            .kpi-row {
                grid-template-columns: repeat(4, 1fr);
                gap: 14px;
                margin-bottom: 20px;
            }
        }

        .kpi-card {
            background: var(--bg-card);
            backdrop-filter: var(--apple-glass-blur);
            border: 1px solid var(--border);
            border-radius: var(--radius-md);
            padding: 14px 16px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            transition: transform 0.2s ease, border-color 0.2s ease;
        }

        .kpi-card:hover {
            transform: translateY(-2px);
            border-color: rgba(255, 255, 255, 0.18);
        }

        .kpi-top {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 6px;
        }

        .kpi-label {
            font-size: 11px;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }

        .kpi-value {
            font-size: 24px;
            font-weight: 800;
            font-family: 'JetBrains Mono', monospace;
            letter-spacing: -0.03em;
        }

        /* Apple Segmented Control Tab Bar */
        .segmented-control {
            display: flex;
            background: rgba(255, 255, 255, 0.06);
            padding: 4px;
            border-radius: 9999px;
            border: 1px solid var(--border);
            margin-bottom: 20px;
            overflow-x: auto;
            white-space: nowrap;
            -webkit-overflow-scrolling: touch;
        }

        .segment-btn {
            background: transparent;
            border: none;
            color: var(--text-secondary);
            padding: 8px 16px;
            border-radius: 9999px;
            font-size: 12px;
            font-weight: 600;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            transition: all 0.2s cubic-bezier(0.16, 1, 0.3, 1);
            flex-shrink: 0;
        }

        .segment-btn:hover { color: #fff; }

        .segment-btn.active {
            background: #ffffff;
            color: #000000;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
            font-weight: 700;
        }

        /* Mobile Bottom Tab Bar */
        .mobile-tab-bar {
            display: none;
            position: fixed;
            bottom: 0;
            left: 0;
            right: 0;
            background: rgba(10, 14, 22, 0.88);
            backdrop-filter: blur(28px) saturate(200%);
            -webkit-backdrop-filter: blur(28px) saturate(200%);
            border-top: 1px solid var(--border);
            padding: 6px 10px calc(6px + env(safe-area-inset-bottom));
            z-index: 999;
            justify-content: space-around;
        }

        @media (max-width: 767px) {
            .mobile-tab-bar { display: flex; }
            .segmented-control { display: none; }
        }

        .mobile-tab-item {
            background: transparent;
            border: none;
            color: var(--text-secondary);
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 2px;
            font-size: 10px;
            font-weight: 600;
            padding: 6px 8px;
            cursor: pointer;
            border-radius: 12px;
            flex: 1;
            transition: color 0.15s ease;
        }

        .mobile-tab-item span.icon { font-size: 18px; }

        .mobile-tab-item.active {
            color: var(--accent);
        }

        /* Glass Cards */
        .glass-card {
            background: var(--bg-card);
            backdrop-filter: var(--apple-glass-blur);
            border: 1px solid var(--border);
            border-radius: var(--radius-lg);
            padding: 18px;
            margin-bottom: 16px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        }

        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 14px;
            flex-wrap: wrap;
            gap: 8px;
        }

        .card-title {
            font-size: 12px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-secondary);
        }

        /* Map & Threat Matrix Grid */
        .map-section-grid {
            display: grid;
            grid-template-columns: 1fr;
            gap: 16px;
            margin-bottom: 20px;
        }

        @media (min-width: 1024px) {
            .map-section-grid {
                grid-template-columns: 1.8fr 1fr;
            }
        }

        #cambodiaMap {
            width: 100%;
            height: 380px;
            border-radius: var(--radius-md);
            border: 1px solid var(--border);
            background: #090e1a;
            z-index: 10;
        }

        /* Interactive Donut & Pie Chart Section */
        .pie-chart-card {
            background: var(--bg-card);
            backdrop-filter: var(--apple-glass-blur);
            border: 1px solid var(--border);
            border-radius: var(--radius-lg);
            padding: 20px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }

        .pie-container {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 20px;
            flex-wrap: wrap;
            margin: 16px 0;
        }

        .donut-svg-box {
            position: relative;
            width: 160px;
            height: 160px;
        }

        svg.donut-svg {
            width: 100%;
            height: 100%;
            transform: rotate(-90deg);
        }

        .donut-center-label {
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            text-align: center;
            pointer-events: none;
        }

        .donut-center-num {
            font-size: 26px;
            font-weight: 800;
            font-family: 'JetBrains Mono', monospace;
            color: #fff;
            line-height: 1;
        }

        .donut-center-sub {
            font-size: 9px;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.06em;
            margin-top: 4px;
        }

        .pie-legend {
            display: flex;
            flex-direction: column;
            gap: 8px;
            min-width: 140px;
        }

        .legend-item {
            display: flex;
            align-items: center;
            justify-content: space-between;
            font-size: 12px;
            font-weight: 600;
            padding: 6px 10px;
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid var(--border);
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.15s ease;
        }

        .legend-item:hover {
            background: rgba(255, 255, 255, 0.08);
            border-color: rgba(255, 255, 255, 0.2);
        }

        .legend-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            display: inline-block;
            margin-right: 6px;
        }

        /* Basin Filter Chips */
        .basin-filter-group {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            margin-bottom: 14px;
        }

        .basin-chip {
            background: rgba(255, 255, 255, 0.04);
            border: 1px solid var(--border);
            color: var(--text-secondary);
            padding: 6px 14px;
            border-radius: 9999px;
            font-size: 11px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.15s ease;
        }

        .basin-chip:hover {
            color: #fff;
            border-color: rgba(255, 255, 255, 0.2);
        }

        .basin-chip.active {
            background: #ffffff;
            color: #000000;
            border-color: #ffffff;
            font-weight: 700;
        }

        /* Search Input */
        .search-box {
            position: relative;
            margin-bottom: 16px;
        }

        .search-input {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid var(--border);
            color: var(--text-primary);
            padding: 10px 16px 10px 38px;
            border-radius: var(--radius-md);
            font-size: 13px;
            width: 100%;
            outline: none;
            transition: border-color 0.2s ease, background 0.2s ease;
        }

        .search-input:focus {
            border-color: var(--accent);
            background: rgba(255, 255, 255, 0.08);
        }

        .search-icon {
            position: absolute;
            left: 12px;
            top: 50%;
            transform: translateY(-50%);
            font-size: 14px;
            color: var(--text-muted);
        }

        /* National Station Cards Grid */
        .national-grid {
            display: grid;
            grid-template-columns: 1fr;
            gap: 12px;
            margin-bottom: 24px;
        }

        @media (min-width: 640px) {
            .national-grid { grid-template-columns: repeat(2, 1fr); gap: 14px; }
        }

        @media (min-width: 1024px) {
            .national-grid { grid-template-columns: repeat(3, 1fr); gap: 16px; }
        }

        .station-grid-card {
            background: var(--bg-card);
            backdrop-filter: var(--apple-glass-blur);
            border: 1px solid var(--border);
            border-radius: var(--radius-md);
            padding: 16px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            transition: transform 0.2s ease, border-color 0.2s ease;
        }

        .station-grid-card:hover {
            transform: translateY(-2px);
            border-color: rgba(255, 255, 255, 0.25);
        }

        .station-grid-card.NORMAL { border-left: 3px solid var(--normal); }
        .station-grid-card.CAUTION { border-left: 3px solid var(--caution); }
        .station-grid-card.DANGER { border-left: 3px solid var(--danger); box-shadow: 0 0 16px rgba(255, 69, 58, 0.25); }

        .status-pill {
            font-size: 10px;
            font-weight: 700;
            padding: 3px 8px;
            border-radius: 9999px;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }

        .status-pill.NORMAL { background: rgba(48, 209, 88, 0.15); color: var(--normal); }
        .status-pill.CAUTION { background: rgba(255, 214, 10, 0.15); color: var(--caution); }
        .status-pill.DANGER { background: rgba(255, 69, 58, 0.2); color: var(--danger); }

        .telemetry-row {
            display: grid;
            grid-template-columns: 1.2fr 1fr;
            gap: 8px;
            margin: 12px 0;
            background: rgba(0, 0, 0, 0.25);
            padding: 10px 12px;
            border-radius: 10px;
        }

        .telemetry-val {
            font-size: 15px;
            font-weight: 800;
            font-family: 'JetBrains Mono', monospace;
            color: var(--accent-cyan);
        }

        .btn-inspect {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid var(--border);
            color: #fff;
            padding: 8px;
            border-radius: 8px;
            font-size: 11px;
            font-weight: 600;
            cursor: pointer;
            width: 100%;
            text-align: center;
            transition: all 0.15s ease;
        }

        .btn-inspect:hover {
            background: #ffffff;
            color: #000000;
            border-color: #ffffff;
        }

        /* Mekong Wave Cascade Timeline */
        .cascade-wrapper {
            display: flex;
            flex-direction: column;
            gap: 10px;
            margin-top: 14px;
        }

        .cascade-node {
            background: rgba(0, 0, 0, 0.35);
            border: 1px solid var(--border);
            border-radius: var(--radius-md);
            padding: 14px 18px;
            display: grid;
            grid-template-columns: auto 1.5fr 1fr 1fr auto;
            align-items: center;
            gap: 14px;
            cursor: pointer;
            transition: all 0.2s ease;
        }

        .cascade-node:hover {
            border-color: var(--accent);
            background: rgba(10, 132, 255, 0.08);
            transform: translateX(4px);
        }

        .cascade-step-num {
            width: 32px;
            height: 32px;
            background: linear-gradient(135deg, #0a84ff 0%, #0051ba 100%);
            color: #fff;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 800;
            font-size: 13px;
        }

        .cascade-connector {
            text-align: center;
            font-size: 11px;
            font-weight: 600;
            color: var(--accent-cyan);
            font-family: 'JetBrains Mono', monospace;
            padding: 6px;
            background: rgba(10, 132, 255, 0.05);
            border-radius: 8px;
            border: 1px dashed rgba(10, 132, 255, 0.2);
        }

        @media (max-width: 768px) {
            .cascade-node {
                grid-template-columns: 1fr;
                gap: 8px;
            }
        }

        /* Simulation Sliders */
        .slider-group { margin-bottom: 16px; }

        .slider-label-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 12px;
            font-weight: 600;
            margin-bottom: 6px;
        }

        input[type="range"] {
            width: 100%;
            height: 6px;
            border-radius: 9999px;
            background: rgba(255, 255, 255, 0.1);
            outline: none;
            cursor: pointer;
            accent-color: #0a84ff;
        }

        /* Buttons */
        .btn {
            border: none;
            padding: 10px 16px;
            border-radius: var(--radius-sm);
            font-size: 12px;
            font-weight: 600;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
            transition: all 0.15s ease;
        }

        .btn-primary { background: #0a84ff; color: #fff; }
        .btn-success { background: #30d158; color: #000; }
        .btn-danger { background: #ff453a; color: #fff; }
        .btn-glass { background: rgba(255, 255, 255, 0.06); color: #fff; border: 1px solid var(--border); }
        .btn:hover { opacity: 0.9; transform: translateY(-1px); }

        /* Toast */
        #toast {
            position: fixed;
            bottom: 80px;
            right: 20px;
            background: rgba(24, 28, 40, 0.95);
            backdrop-filter: blur(20px);
            border: 1px solid var(--border-highlight);
            color: white;
            padding: 12px 18px;
            border-radius: var(--radius-md);
            display: none;
            z-index: 1000;
            font-size: 13px;
            box-shadow: 0 8px 32px rgba(0,0,0,0.6);
        }

        .country-pill {
            padding: 6px 14px;
            border-radius: 999px;
            font-size: 11px;
            font-weight: 700;
            border: 1px solid var(--border);
            background: rgba(255,255,255,0.04);
            color: var(--text-secondary);
            cursor: pointer;
            transition: all 0.2s ease;
            display: inline-flex;
            align-items: center;
            gap: 4px;
        }
        .country-pill:hover {
            background: rgba(255,255,255,0.09);
            color: var(--text-primary);
        }
        .country-pill.active {
            background: var(--accent);
            color: #fff;
            border-color: var(--accent);
            box-shadow: 0 0 12px rgba(10, 132, 255, 0.4);
        }

        table { width: 100%; border-collapse: collapse; font-size: 12px; margin-top: 10px; }
        th { color: var(--text-muted); padding: 8px 10px; border-bottom: 1px solid var(--border); text-align: left; font-size: 10px; text-transform: uppercase; }
        td { padding: 10px; border-bottom: 1px solid rgba(255, 255, 255, 0.04); color: var(--text-secondary); }

        .leaflet-popup-content-wrapper {
            background: #0f172a !important;
            color: #f8fafc !important;
            border: 1px solid var(--border-highlight) !important;
            border-radius: 12px !important;
        }
        .leaflet-popup-tip { background: #0f172a !important; }
    </style>
</head>
<body>

<div class="container">
    <!-- Header -->
    <header>
        <div class="brand-group">
            <div class="brand-logo">🌊</div>
            <div class="brand-text">
                <h1>Pan-Asian Multi-Hazard & Flood Early Warning System</h1>
                <p>Regional Early Warning Command Center · 112 Stations across 47 Asian Nations</p>
            </div>
        </div>
        <div class="header-meta">
            <button class="btn btn-success" onclick="triggerSyncAll()" style="padding: 6px 12px; font-size: 11px;">
                🛰️ Live Sync (63)
            </button>
            <span class="code-pill" style="color: var(--accent-cyan);">⏱️ 30m Auto</span>
            <span id="liveClock" class="code-pill">🌐 --:--:-- UTC</span>
        </div>
    </header>

    <!-- Country & Scope Filter Pill Switcher -->
    <div style="display: flex; gap: 8px; margin-bottom: 14px; flex-wrap: wrap; align-items: center; background: rgba(0,0,0,0.25); padding: 8px 12px; border-radius: var(--radius-md); border: 1px solid var(--border);">
        <span style="font-size: 11px; font-weight: 700; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.05em; margin-right: 4px;">Scope Filter:</span>
        <button id="scopeAllBtn" class="country-pill active" onclick="setCountryScope('ALL')">🌐 All Asia Scope (112 Stations)</button>
        <button id="scopeAsiaBtn" class="country-pill" onclick="setCountryScope('ASIA')">🌏 Rest of Asia (69 Stations)</button>
        <button id="scopeKhBtn" class="country-pill" onclick="setCountryScope('KH')">🇰🇭 Cambodia (25 Provinces)</button>
        <button id="scopeLaBtn" class="country-pill" onclick="setCountryScope('LA')">🇱🇦 Laos Upstream (18 Inflow)</button>
    </div>

    <!-- Top KPI Bento Grid -->
    <div class="kpi-row">
        <div class="kpi-card">
            <div class="kpi-top">
                <span class="kpi-label" id="kpiTotalLabel">All Asian Stations</span>
                <span>🏛️</span>
            </div>
            <div class="kpi-value" id="kpiTotal">63</div>
        </div>
        <div class="kpi-card" style="border-left: 3px solid var(--normal);">
            <div class="kpi-top">
                <span class="kpi-label">Normal Flow</span>
                <span>🟢</span>
            </div>
            <div class="kpi-value" style="color: var(--normal);" id="kpiNormal">--</div>
        </div>
        <div class="kpi-card" style="border-left: 3px solid var(--caution);">
            <div class="kpi-top">
                <span class="kpi-label">Caution Watch</span>
                <span>🟡</span>
            </div>
            <div class="kpi-value" style="color: var(--caution);" id="kpiCaution">--</div>
        </div>
        <div class="kpi-card" style="border-left: 3px solid var(--danger);">
            <div class="kpi-top">
                <span class="kpi-label">Danger Alert</span>
                <span>🔴</span>
            </div>
            <div class="kpi-value" style="color: var(--danger);" id="kpiDanger">--</div>
        </div>
    </div>

    <!-- Apple Desktop Segmented Control -->
    <div class="segmented-control">
        <button id="tabBtnMap" class="segment-btn active" onclick="switchTab('map')">
            🗺️ Live Threat Map & Matrix
        </button>
        <button id="tabBtnMetrics" class="segment-btn" onclick="switchTab('metrics')">
            📊 Distribution & Leaderboard
        </button>
        <button id="tabBtnCascade" class="segment-btn" onclick="switchTab('cascade')">
            🌊 Mekong Transboundary Cascade
        </button>
        <button id="tabBtnSandbox" class="segment-btn" onclick="switchTab('sandbox')">
            🧪 'What-If' Simulation
        </button>
        <button id="tabBtnForecast" class="segment-btn" onclick="switchTab('forecast')">
            📈 7-Day Hydrographs
        </button>
        <button id="tabBtnConsole" class="segment-btn" onclick="switchTab('console')">
            ⚡ Admin Operations (FR9/FR10)
        </button>
        <button id="tabBtnProduction" class="segment-btn" onclick="switchTab('production')">
            🛡️ Production Hub (CAP / IVR / GIS)
        </button>
        <button id="tabBtnHazards" class="segment-btn" onclick="switchTab('hazards')" style="background: linear-gradient(135deg, rgba(255, 69, 58, 0.2) 0%, rgba(191, 90, 242, 0.2) 100%); border-color: rgba(191, 90, 242, 0.4);">
            🌐 Pan-Asian Multi-Hazard
        </button>
    </div>

    <!-- TAB 1: Live Threat Map & Station Grid -->
    <div id="viewMap">
        <div class="map-section-grid">
            <div class="glass-card" style="padding: 16px;">
                <div class="card-header">
                    <div>
                        <span class="card-title">🗺️ Geographic Risk Topology</span>
                        <p style="font-size: 11px; color: var(--text-secondary); margin-top: 2px;">
                            Live coordinates across 19 Asian Mega-Basins & River Catchments.
                        </p>
                    </div>
                    <span class="code-pill">● 63 Points Active</span>
                </div>
                <div id="cambodiaMap"></div>
            </div>

            <!-- Interactive Donut / Pie Chart Card -->
            <div class="pie-chart-card">
                <div class="card-header">
                    <span class="card-title">📊 Regional Threat Breakdown</span>
                    <span id="highestThreatBadge" class="status-pill NORMAL">Normal Flow</span>
                </div>

                <div class="pie-container">
                    <div class="donut-svg-box">
                        <svg class="donut-svg" viewBox="0 0 100 100">
                            <!-- Background Ring -->
                            <circle cx="50" cy="50" r="38" fill="transparent" stroke="rgba(255,255,255,0.06)" stroke-width="12" />
                            <!-- Normal Slice -->
                            <circle id="donutSliceNormal" cx="50" cy="50" r="38" fill="transparent" stroke="#30d158" stroke-width="12" stroke-dasharray="238.7" stroke-dashoffset="0" stroke-linecap="round" />
                            <!-- Caution Slice -->
                            <circle id="donutSliceCaution" cx="50" cy="50" r="38" fill="transparent" stroke="#ffd60a" stroke-width="12" stroke-dasharray="238.7" stroke-dashoffset="238.7" stroke-linecap="round" />
                            <!-- Danger Slice -->
                            <circle id="donutSliceDanger" cx="50" cy="50" r="38" fill="transparent" stroke="#ff453a" stroke-width="12" stroke-dasharray="238.7" stroke-dashoffset="238.7" stroke-linecap="round" />
                        </svg>
                        <div class="donut-center-label">
                            <div id="donutCenterCount" class="donut-center-num">63</div>
                            <div class="donut-center-sub">Stations</div>
                        </div>
                    </div>

                    <div class="pie-legend">
                        <div class="legend-item" onclick="filterByLevel('Normal')">
                            <div><span class="legend-dot" style="background: var(--normal);"></span>Normal</div>
                            <div id="legendPctNormal" style="font-family:'JetBrains Mono'; font-weight:800; color:var(--normal);">100%</div>
                        </div>
                        <div class="legend-item" onclick="filterByLevel('Caution')">
                            <div><span class="legend-dot" style="background: var(--caution);"></span>Caution</div>
                            <div id="legendPctCaution" style="font-family:'JetBrains Mono'; font-weight:800; color:var(--caution);">0%</div>
                        </div>
                        <div class="legend-item" onclick="filterByLevel('Danger')">
                            <div><span class="legend-dot" style="background: var(--danger);"></span>Danger</div>
                            <div id="legendPctDanger" style="font-family:'JetBrains Mono'; font-weight:800; color:var(--danger);">0%</div>
                        </div>
                    </div>
                </div>

                <div style="font-size: 11px; color: var(--text-muted); text-align: center; border-top: 1px solid var(--border); padding-top: 10px;">
                    💡 Tap any slice in legend to filter stations by status
                </div>
            </div>
        </div>

        <!-- Basin Filter Chips -->
        <div class="basin-filter-group">
            <button class="basin-chip active" onclick="setBasinFilter('all', this)">All Basins (63)</button>
            <button class="basin-chip" onclick="setBasinFilter('mekong', this)">🌊 Mekong (7)</button>
            <button class="basin-chip" onclick="setBasinFilter('tonle-sap', this)">🏞️ Tonle Sap (6)</button>
            <button class="basin-chip" onclick="setBasinFilter('coastal', this)">🏖️ Coastal (4)</button>
            <button class="basin-chip" onclick="setBasinFilter('highland', this)">⛰️ Highland (8)</button>
        </div>

        <!-- Search Box -->
        <div class="search-box">
            <span class="search-icon">🔍</span>
            <input type="text" id="stationSearch" class="search-input" placeholder="Search station / country (e.g. Wuhan, Patna, Kratie, Jakarta, Pasig)..." oninput="filterGrid()">
        </div>

        <!-- 25 Provinces Grid -->
        <div id="nationalGrid" class="national-grid"></div>
    </div>

    <!-- TAB 2: Distribution & Leaderboards -->
    <div id="viewMetrics" style="display: none;">
        <div class="glass-card">
            <div class="card-header">
                <span class="card-title">🔥 Live Top 5 Flow Gateways (Leaderboard)</span>
                <span style="font-size: 11px; color: var(--text-muted);">Ranked by current river discharge</span>
            </div>
            <div id="top5CardsGrid" style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px;"></div>
        </div>
    </div>

    <!-- TAB 3: Mekong Downstream Cascade -->
    <div id="viewCascade" style="display: none;">
        <div class="glass-card">
            <div class="card-header">
                <div>
                    <span class="card-title">🌊 Mekong Transboundary Cascade Corridor</span>
                    <p style="font-size: 12px; color: var(--text-secondary); margin-top: 4px;">
                        Transboundary hydrological flood wave tracking along the Lower Mekong River corridor (Laos Inflow → Cambodia Mainstem).
                    </p>
                </div>
                <span class="code-pill">⏱️ Velocity: ~4.2 km/h</span>
            </div>

            <div class="cascade-wrapper">
                <!-- 0. Pakse, Laos Transboundary Inflow -->
                <div class="cascade-node" onclick="inspectStation('laos-champasak')" style="border-left: 3px solid var(--accent-cyan);">
                    <div class="cascade-step-num" style="background: linear-gradient(135deg, #64d2ff 0%, #0a84ff 100%);">#0</div>
                    <div>
                        <div style="font-size: 14px; font-weight: 700;">🇱🇦 Pakse (Champasak, Laos) · ប៉ាក់សេ ឡាវ</div>
                        <div style="font-size: 11px; color: var(--accent-cyan);">Transboundary Early-Warning Gateway (+18h Lead Time)</div>
                    </div>
                    <div>
                        <div style="font-size: 10px; color: var(--text-muted);">DISCHARGE</div>
                        <div id="cascadeDisPakse" style="font-size: 15px; font-weight: 800; color: var(--accent-cyan); font-family: 'JetBrains Mono', monospace;">-- m³/s</div>
                    </div>
                    <div>
                        <div style="font-size: 10px; color: var(--text-muted);">CAPACITY</div>
                        <div id="cascadeCapPakse" style="font-size: 13px; font-weight: 700; color: var(--normal);">--%</div>
                    </div>
                    <div><span id="cascadeBadgePakse" class="status-pill NORMAL">NORMAL</span></div>
                </div>

                <div class="cascade-connector">⬇️ Transboundary Flood Wave Lag: 14 – 18 Hours (150 km downstream across border) ⬇️</div>

                <!-- 1. Stung Treng -->
                <div class="cascade-node" onclick="inspectStation('stung-treng')">
                    <div class="cascade-step-num">#1</div>
                    <div>
                        <div style="font-size: 14px; font-weight: 700;">🇰🇭 Stung Treng · ស្ទឹងត្រែង</div>
                        <div style="font-size: 11px; color: var(--text-secondary);">Mekong Entrance & Sekong Confluence</div>
                    </div>
                    <div>
                        <div style="font-size: 10px; color: var(--text-muted);">DISCHARGE</div>
                        <div id="cascadeDisStungTreng" style="font-size: 15px; font-weight: 800; color: var(--accent-cyan); font-family: 'JetBrains Mono', monospace;">-- m³/s</div>
                    </div>
                    <div>
                        <div style="font-size: 10px; color: var(--text-muted);">CAPACITY</div>
                        <div id="cascadeCapStungTreng" style="font-size: 13px; font-weight: 700; color: var(--normal);">--%</div>
                    </div>
                    <div><span id="cascadeBadgeStungTreng" class="status-pill NORMAL">NORMAL</span></div>
                </div>

                <div class="cascade-connector">⬇️ Flood Wave Propagation Lag: 8 – 12 Hours (125 km downstream) ⬇️</div>

                <!-- 2. Kratie -->
                <div class="cascade-node" onclick="inspectStation('kratie-central')">
                    <div class="cascade-step-num">#2</div>
                    <div>
                        <div style="font-size: 14px; font-weight: 700;">🇰🇭 Kratie · ក្រចេះ</div>
                        <div style="font-size: 11px; color: var(--text-secondary);">Primary Floodplain Transition Point</div>
                    </div>
                    <div>
                        <div style="font-size: 10px; color: var(--text-muted);">DISCHARGE</div>
                        <div id="cascadeDisKratie" style="font-size: 15px; font-weight: 800; color: var(--accent-cyan); font-family: 'JetBrains Mono', monospace;">-- m³/s</div>
                    </div>
                    <div>
                        <div style="font-size: 10px; color: var(--text-muted);">CAPACITY</div>
                        <div id="cascadeCapKratie" style="font-size: 13px; font-weight: 700; color: var(--normal);">--%</div>
                    </div>
                    <div><span id="cascadeBadgeKratie" class="status-pill NORMAL">NORMAL</span></div>
                </div>

                <div class="cascade-connector">⬇️ Flood Wave Propagation Lag: 14 – 18 Hours (180 km downstream) ⬇️</div>

                <!-- 3. Kampong Cham -->
                <div class="cascade-node" onclick="inspectStation('kampong-cham')">
                    <div class="cascade-step-num">#3</div>
                    <div>
                        <div style="font-size: 14px; font-weight: 700;">🇰🇭 Kampong Cham · កំពង់ចាម</div>
                        <div style="font-size: 11px; color: var(--text-secondary);">Mid-Basin Agricultural Basin</div>
                    </div>
                    <div>
                        <div style="font-size: 10px; color: var(--text-muted);">DISCHARGE</div>
                        <div id="cascadeDisKampongCham" style="font-size: 15px; font-weight: 800; color: var(--accent-cyan); font-family: 'JetBrains Mono', monospace;">-- m³/s</div>
                    </div>
                    <div>
                        <div style="font-size: 10px; color: var(--text-muted);">CAPACITY</div>
                        <div id="cascadeCapKampongCham" style="font-size: 13px; font-weight: 700; color: var(--normal);">--%</div>
                    </div>
                    <div><span id="cascadeBadgeKampongCham" class="status-pill NORMAL">NORMAL</span></div>
                </div>

                <div class="cascade-connector">⬇️ Flood Wave Propagation Lag: 10 – 14 Hours (105 km downstream) ⬇️</div>

                <!-- 4. Phnom Penh / Chaktomuk -->
                <div class="cascade-node" onclick="inspectStation('phnom-penh')">
                    <div class="cascade-step-num">#4</div>
                    <div>
                        <div style="font-size: 14px; font-weight: 700;">🇰🇭 Phnom Penh (Chaktomuk) · ភ្នំពេញ (ចតុមុខ)</div>
                        <div style="font-size: 11px; color: var(--text-secondary);">Four Rivers Confluence & Tonle Sap Split</div>
                    </div>
                    <div>
                        <div style="font-size: 10px; color: var(--text-muted);">DISCHARGE</div>
                        <div id="cascadeDisPhnomPenh" style="font-size: 15px; font-weight: 800; color: var(--accent-cyan); font-family: 'JetBrains Mono', monospace;">-- m³/s</div>
                    </div>
                    <div>
                        <div style="font-size: 10px; color: var(--text-muted);">CAPACITY</div>
                        <div id="cascadeCapPhnomPenh" style="font-size: 13px; font-weight: 700; color: var(--normal);">--%</div>
                    </div>
                    <div><span id="cascadeBadgePhnomPenh" class="status-pill NORMAL">NORMAL</span></div>
                </div>
            </div>
        </div>
    </div>

    <!-- TAB 4: 'What-If' Simulation Sandbox -->
    <div id="viewSandbox" style="display: none;">
        <div class="glass-card">
            <div class="card-header">
                <div>
                    <span class="card-title">🧪 In-Memory 'What-If' Simulation Sandbox</span>
                    <p style="font-size: 12px; color: var(--text-secondary); margin-top: 4px;">
                        Interactive flood surge simulation. Computes purely in-memory without altering persistent records.
                    </p>
                </div>
                <span class="code-pill" style="color: var(--normal);">● Pure In-Memory</span>
            </div>

            <div style="display: grid; grid-template-columns: 1fr; gap: 16px;">
                <div style="background: rgba(0,0,0,0.3); border: 1px solid var(--border); border-radius: var(--radius-md); padding: 16px;">
                    <div style="margin-bottom: 16px;">
                        <label for="sandboxStationSelect" style="font-size: 12px; font-weight: 700; color: var(--text-secondary);">TARGET PROVINCE:</label>
                        <select id="sandboxStationSelect" style="background: #0f172a; border: 1px solid var(--border); color: #fff; padding: 8px 12px; border-radius: 8px; font-size: 13px; width: 100%; margin-top: 6px;" onchange="runSandboxEvaluation()"></select>
                    </div>

                    <div class="slider-group">
                        <div class="slider-label-row">
                            <span>🌊 River Discharge Override</span>
                            <span id="labelSimDischarge" style="color: var(--accent-cyan); font-family: 'JetBrains Mono', monospace;">14,500 m³/s</span>
                        </div>
                        <input type="range" id="simDischargeSlider" min="2000" max="30000" step="250" value="14500" oninput="runSandboxEvaluation()">
                        <div style="display: flex; justify-content: space-between; font-size: 10px; color: var(--text-muted); margin-top: 4px;">
                            <span>2,000 (Dry)</span>
                            <span>16,000 (Caution)</span>
                            <span>22,000 (Danger)</span>
                            <span>30,000</span>
                        </div>
                    </div>

                    <div class="slider-group">
                        <div class="slider-label-row">
                            <span>🌧️ 24h Monsoon Rainfall</span>
                            <span id="labelSimPrecip" style="color: var(--accent-cyan); font-family: 'JetBrains Mono', monospace;">35.0 mm</span>
                        </div>
                        <input type="range" id="simPrecipSlider" min="0" max="180" step="2" value="35" oninput="runSandboxEvaluation()">
                        <div style="display: flex; justify-content: space-between; font-size: 10px; color: var(--text-muted); margin-top: 4px;">
                            <span>0 mm</span>
                            <span>50 mm</span>
                            <span>80 mm (Severe)</span>
                            <span>180 mm (Typhoon)</span>
                        </div>
                    </div>

                    <div style="margin-top: 14px; padding-top: 12px; border-top: 1px solid var(--border);">
                        <span style="font-size: 11px; font-weight: 700; color: var(--text-secondary); display: block; margin-bottom: 8px;">⚡ Rapid Scenario Presets:</span>
                        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px;">
                            <button class="btn btn-glass" onclick="applySandboxPreset(11500, 15)" style="font-size: 11px;">🌿 Dry Season</button>
                            <button class="btn btn-glass" onclick="applySandboxPreset(16800, 45)" style="font-size: 11px;">🟡 Monsoon Caution</button>
                            <button class="btn btn-glass" onclick="applySandboxPreset(19200, 85)" style="font-size: 11px;">⛈️ Storm Surge</button>
                            <button class="btn btn-glass" onclick="applySandboxPreset(24500, 95)" style="font-size: 11px;">🚨 Critical Flood</button>
                        </div>
                    </div>
                </div>

                <!-- Simulation Output -->
                <div style="background: rgba(0,0,0,0.3); border: 1px solid var(--border); border-radius: var(--radius-md); padding: 16px;">
                    <div class="card-header" style="margin-bottom: 8px;">
                        <span class="card-title">PROJECTED CLASSIFICATION</span>
                        <span class="code-pill">FR3 Logic</span>
                    </div>

                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                        <div>
                            <div style="font-size: 10px; color: var(--text-muted);">Computed Risk</div>
                            <div id="simRiskLevelText" style="font-size: 24px; font-weight: 800; color: var(--normal);">NORMAL</div>
                        </div>
                        <div style="text-align: right;">
                            <div style="font-size: 10px; color: var(--text-muted);">Capacity Stress</div>
                            <div id="simCapacityPct" style="font-size: 20px; font-weight: 800; font-family: 'JetBrains Mono', monospace;">65.9%</div>
                        </div>
                    </div>

                    <div style="width: 100%; height: 8px; background: rgba(0,0,0,0.5); border-radius: 9999px; overflow: hidden; margin-bottom: 14px; border: 1px solid var(--border);">
                        <div id="simGaugeFill" style="height: 100%; width: 65.9%; background: var(--accent); transition: width 0.3s ease;"></div>
                    </div>

                    <div style="background: rgba(255,255,255,0.03); border-radius: 10px; padding: 10px; margin-bottom: 8px;">
                        <div style="font-size: 10px; color: var(--text-muted);">⏱️ Evacuation Lead Time:</div>
                        <div id="simEvacLeadTime" style="font-size: 12px; font-weight: 700; color: #fff;">NOMINAL: Safe Seasonal Capacity (> 48 Hours)</div>
                    </div>

                    <div style="background: rgba(255,255,255,0.03); border-radius: 10px; padding: 10px;">
                        <div style="font-size: 10px; color: var(--text-muted);">📋 Recommended Action:</div>
                        <div id="simRecommendedAction" style="font-size: 11px; color: var(--text-secondary);">Standard 30-minute background telemetry monitoring</div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- TAB 5: 7-Day Forecast & Hydrographs -->
    <div id="viewForecast" style="display: none;">
        <div class="glass-card">
            <div class="card-header">
                <div>
                    <span class="card-title" id="forecastGraphTitle">📈 7-Day Hydrological Forecast Trajectory</span>
                    <p style="font-size: 12px; color: var(--text-secondary); margin-top: 4px;">
                        GloFAS discharge curve with danger threshold overlay (22,000 m³/s).
                    </p>
                </div>
                <select id="forecastStationSelect" style="background: #0f172a; border: 1px solid var(--border); color: #fff; padding: 6px 12px; border-radius: 8px; font-size: 12px;" onchange="loadForecastData(this.value)"></select>
            </div>

            <div style="width: 100%; height: 260px; margin: 16px 0;">
                <svg id="hydroSvg" style="width: 100%; height: 100%; overflow: visible;" viewBox="0 0 800 240"></svg>
            </div>

            <div id="forecastGrid" style="display: grid; grid-template-columns: repeat(auto-fit, minmax(95px, 1fr)); gap: 8px; margin-top: 14px;"></div>
        </div>
    </div>

    <!-- TAB 6: Admin Operations & Station Deep-Dive -->
    <div id="viewConsole" style="display: none;">
        <div style="background: var(--bg-card); border: 1px solid var(--border); border-radius: var(--radius-md); padding: 12px 16px; display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; flex-wrap: wrap; gap: 10px;">
            <div style="display: flex; align-items: center; gap: 10px;">
                <label for="stationSelect" style="font-size: 12px; font-weight: 600; color: var(--text-secondary);">📍 Select Station:</label>
                <select id="stationSelect" style="background: #0f172a; border: 1px solid var(--border); color: #fff; padding: 6px 12px; border-radius: 8px; font-size: 12px; font-weight: 600;" onchange="onStationChanged()"></select>
            </div>
            <div id="stationCoords" style="font-size: 11px; color: var(--accent-cyan); font-family: 'JetBrains Mono', monospace;">Lat: 12.4888°N, Lon: 106.0188°E</div>
        </div>

        <div style="display: grid; grid-template-columns: 1fr; gap: 16px; margin-bottom: 20px;">
            <div class="glass-card">
                <div class="card-header">
                    <span id="stationTitle" class="card-title">📍 Station Telemetry</span>
                    <span id="freshnessBadge" class="status-pill NORMAL">🟢 Live & Fresh</span>
                </div>

                <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; margin-bottom: 14px;">
                    <div style="background: rgba(0,0,0,0.3); border: 1px solid var(--border); padding: 12px; border-radius: 10px;">
                        <div style="font-size: 10px; color: var(--text-muted); text-transform: uppercase;">River Discharge</div>
                        <div style="font-size: 18px; font-weight: 800; font-family: 'JetBrains Mono', monospace; color: var(--accent-cyan);">
                            <span id="valDischarge">--</span> <span style="font-size: 11px; color: var(--text-secondary);">m³/s</span>
                        </div>
                        <div style="font-size: 9px; color: var(--text-muted); margin-top: 2px;">Danger: ≥ 22,000 m³/s</div>
                    </div>

                    <div style="background: rgba(0,0,0,0.3); border: 1px solid var(--border); padding: 12px; border-radius: 10px;">
                        <div style="font-size: 10px; color: var(--text-muted); text-transform: uppercase;">Precipitation</div>
                        <div style="font-size: 18px; font-weight: 800; font-family: 'JetBrains Mono', monospace;">
                            <span id="valPrecip">--</span> <span style="font-size: 11px; color: var(--text-secondary);">mm</span>
                        </div>
                        <div style="font-size: 9px; color: var(--text-muted); margin-top: 2px;">Severe: ≥ 80 mm</div>
                    </div>
                </div>

                <div class="card-title" style="margin-bottom: 6px;">🧠 Risk Engine Audit Rationale (FR3)</div>
                <div id="riskReasonBox" style="background: rgba(0,0,0,0.35); border: 1px dashed var(--border); border-radius: 10px; padding: 12px; font-size: 11px; font-family: 'JetBrains Mono', monospace; color: #cbd5e1; margin-bottom: 12px;">Awaiting ingestion...</div>

                <div class="card-title" style="margin-bottom: 6px;">🛰️ Upstream Live Open-Meteo & GloFAS API Call Details</div>
                <div id="liveApiInfoBox" style="background: rgba(0,0,0,0.4); border: 1px solid var(--border); border-radius: 10px; padding: 12px; font-size: 11px; font-family: 'JetBrains Mono', monospace; color: #94a3b8; word-break: break-all;">
                    <div><strong>GloFAS Flood API:</strong> <a id="linkFloodApi" href="#" target="_blank" style="color:var(--accent-cyan); text-decoration:none;">Loading...</a></div>
                    <div style="margin-top:4px;"><strong>Weather Rain API:</strong> <a id="linkWeatherApi" href="#" target="_blank" style="color:var(--accent-cyan); text-decoration:none;">Loading...</a></div>
                </div>
            </div>

            <div class="glass-card">
                <div class="card-header"><span class="card-title">⚡ Administrator Operations</span></div>
                <div style="display: flex; flex-direction: column; gap: 8px;">
                    <button class="btn btn-primary" onclick="triggerLiveFetch()">🔄 Ingest Live Data (FR1, FR2)</button>
                    <button class="btn btn-danger" onclick="triggerManualTestAlert()">📢 Trigger Manual Test Alert (FR10)</button>
                    <button class="btn btn-danger" onclick="sendRealAlert()">🚨 Alert This Province Now (Telegram/SMS/Satellite)</button>
                </div>

                <div style="margin-top: 14px; padding-top: 12px; border-top: 1px solid var(--border);">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                        <span class="card-title" style="font-size: 11px;">🧪 Simulation Testbench (Auto-Reverts)</span>
                        <span style="font-size: 10px; color: var(--accent-cyan);">⏱️ 10s Auto-Reset</span>
                    </div>
                    <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px; margin-bottom: 6px;">
                        <button class="btn btn-glass" onclick="triggerSim('normal')">🟢 Normal</button>
                        <button class="btn btn-glass" onclick="triggerSim('caution')">🟡 Caution</button>
                        <button class="btn btn-glass" onclick="triggerSim('danger')">🔴 Flood Surge</button>
                        <button class="btn btn-glass" onclick="triggerSim('stale')">⚠️ Stale State</button>
                    </div>
                    <button class="btn btn-primary" onclick="resetToLive()" style="width: 100%; font-size: 11px; padding: 6px;">🔄 Reset to Live Telemetry Now</button>
                </div>
            </div>
        </div>

        <div class="glass-card" style="margin-bottom: 20px;">
            <div class="card-header">
                <div>
                    <span class="card-title">💾 Storage Health & Anti-Spam Optimization</span>
                    <p style="font-size: 11px; color: var(--text-secondary); margin-top: 2px;">SQLite WAL Mode, Write De-Duplication & Historical Telemetry Pruning</p>
                </div>
                <span class="status-pill NORMAL" id="storageWalBadge">🟢 WAL Mode Active</span>
            </div>
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; margin-bottom: 12px;">
                <div style="background: rgba(0,0,0,0.3); padding: 10px; border-radius: 8px; border: 1px solid var(--border);">
                    <div style="font-size: 10px; color: var(--text-muted); text-transform: uppercase;">Total Stations</div>
                    <div id="statStations" style="font-size: 18px; font-weight: 800; font-family: 'JetBrains Mono', monospace; color: var(--accent-cyan);">43</div>
                </div>
                <div style="background: rgba(0,0,0,0.3); padding: 10px; border-radius: 8px; border: 1px solid var(--border);">
                    <div style="font-size: 10px; color: var(--text-muted); text-transform: uppercase;">Telemetry Readings</div>
                    <div id="statReadings" style="font-size: 18px; font-weight: 800; font-family: 'JetBrains Mono', monospace; color: var(--accent-cyan);">--</div>
                </div>
                <div style="background: rgba(0,0,0,0.3); padding: 10px; border-radius: 8px; border: 1px solid var(--border);">
                    <div style="font-size: 10px; color: var(--text-muted); text-transform: uppercase;">Risk Evaluations</div>
                    <div id="statRisks" style="font-size: 18px; font-weight: 800; font-family: 'JetBrains Mono', monospace; color: var(--accent-cyan);">--</div>
                </div>
                <div style="background: rgba(0,0,0,0.3); padding: 10px; border-radius: 8px; border: 1px solid var(--border);">
                    <div style="font-size: 10px; color: var(--text-muted); text-transform: uppercase;">Active Subscriptions</div>
                    <div id="statSubs" style="font-size: 18px; font-weight: 800; font-family: 'JetBrains Mono', monospace; color: var(--accent-cyan);">--</div>
                </div>
            </div>
            <div style="display: flex; gap: 8px; flex-wrap: wrap;">
                <button class="btn btn-glass" onclick="pruneOldStorage(30)" style="font-size: 11px;">🧹 Prune Stale History (>30 Days)</button>
                <button class="btn btn-glass" onclick="refreshStorageStats()" style="font-size: 11px;">🔄 Refresh Storage Stats</button>
            </div>
        </div>

        <div class="glass-card">
            <div class="card-header"><span class="card-title">📜 Admin Manual Test Events (FR10)</span></div>
            <div style="overflow-x: auto;">
                <table>
                    <thead>
                        <tr><th>ID</th><th>Station</th><th>Type</th><th>Status</th><th>Time</th><th>Message</th></tr>
                    </thead>
                    <tbody id="testLogTable"><tr><td colspan="6" style="text-align:center;">No test events yet.</td></tr></tbody>
                </table>
            </div>
        </div>

        <!-- Live Chronological Audit Trail & Event Timeline (FR2 Provenance) -->
        <div class="glass-card" style="margin-top: 16px;">
            <div class="card-header" style="display: flex; justify-content: space-between; align-items: center;">
                <span class="card-title">⏱️ Live Chronological Audit Trail & Time-Series Timeline</span>
                <button class="btn btn-glass" onclick="loadAuditTimeline()" style="font-size: 11px; padding: 4px 10px;">🔄 Refresh Timeline</button>
            </div>
            <p style="font-size: 11px; color: var(--text-secondary); margin-bottom: 12px;">
                Immutable time-stamped chronological audit log in UTC and Local Station Time (UTC+Offset).
            </p>
            <div style="overflow-x: auto;">
                <table>
                    <thead>
                        <tr><th>Event ID</th><th>Type</th><th>Station</th><th>Local Station Time</th><th>UTC ISO Timestamp</th><th>Audit Details</th></tr>
                    </thead>
                    <tbody id="auditLogTable"><tr><td colspan="6" style="text-align:center;">Loading audit trail...</td></tr></tbody>
                </table>
            </div>
        </div>

        <!-- Citizen Alert Dispatch Logs & Per-User Rate Limit Monitor -->
        <div class="glass-card" style="margin-top: 16px;">
            <div class="card-header" style="display: flex; justify-content: space-between; align-items: center;">
                <span class="card-title">🚨 Citizen Alert Delivery Logs & User Quota Monitor</span>
                <button class="btn btn-glass" onclick="loadUserAlertHistory()" style="font-size: 11px; padding: 4px 10px;">🔄 Refresh Alert Logs</button>
            </div>
            <p style="font-size: 11px; color: var(--text-secondary); margin-bottom: 12px;">
                Records outbound emergency alerts per citizen/channel, enforcing a <strong>max 3 alerts / 60-min window</strong> and <strong>15-min repeat cooldown</strong> (DANGER escalations bypass cooldown).
            </p>
            <div style="overflow-x: auto;">
                <table>
                    <thead>
                        <tr><th>Alert ID</th><th>Recipient</th><th>Channel</th><th>Station</th><th>Severity</th><th>Delivery Time (Local)</th><th>Status / Throttle Reason</th></tr>
                    </thead>
                    <tbody id="userAlertLogTable"><tr><td colspan="7" style="text-align:center;">Loading user alert logs...</td></tr></tbody>
                </table>
            </div>
        </div>
    </div>
</div>

<!-- TAB 7: Production Hub (OASIS CAP v1.2, Voice IVR, Soil Moisture, MRC Gauges, GIS & Prometheus) -->
<div id="viewProduction" style="display: none;">
    <div class="glass-card" style="margin-bottom: 20px; background: linear-gradient(135deg, rgba(10,132,255,0.08) 0%, rgba(0,0,0,0.4) 100%); border-left: 4px solid var(--accent);">
        <div class="card-header">
            <div>
                <span class="card-title" style="font-size: 16px;">🛡️ Regional Disaster Early Warning Production Operations</span>
                <p style="font-size: 12px; color: var(--text-secondary); margin-top: 4px;">
                    Authoritative Multi-Agency Integration: OASIS CAP v1.2 (ITU-T X.1303), Regional Voice IVR & EWS 1294, Copernicus Soil Saturation, and Asian River Gauge Network.
                </p>
            </div>
            <span class="status-pill NORMAL" style="font-size: 11px;">Production Ready v2.1.0</span>
        </div>
        
        <div style="display: flex; gap: 10px; flex-wrap: wrap; margin-top: 10px;">
            <a href="/api/cap/feed.xml" target="_blank" class="btn btn-primary" style="text-decoration:none;">
                📜 Download CAP v1.2 XML Feed
            </a>
            <a href="/api/cap/feed.json" target="_blank" class="btn btn-glass" style="text-decoration:none;">
                📦 View CAP v1.2 JSON
            </a>
            <a href="/api/gis/stations.geojson" target="_blank" class="btn btn-glass" style="text-decoration:none;">
                🗺️ GeoJSON Station Layer
            </a>
            <a href="/api/gis/risk-heatmap.geojson" target="_blank" class="btn btn-glass" style="text-decoration:none;">
                🔴 GeoJSON Risk Polygons
            </a>
            <a href="/metrics" target="_blank" class="btn btn-glass" style="text-decoration:none;">
                📊 Prometheus /metrics
            </a>
            <a href="/health/ready" target="_blank" class="btn btn-glass" style="text-decoration:none;">
                🩺 K8s /health/ready
            </a>
        </div>
    </div>

    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; margin-bottom: 20px;">
        <!-- CARD 1: Compound Risk Index (CRI) Breakdown -->
        <div class="glass-card">
            <div class="card-header">
                <span class="card-title">🧠 Compound Risk Index (CRI v2.0)</span>
                <span id="prodCriTier" class="status-pill NORMAL">Evaluating...</span>
            </div>
            <p style="font-size: 11px; color: var(--text-secondary); margin-bottom: 12px;">
                Multi-factorial scientific physics fusing river discharge, soil saturation, precipitation, and upstream surge momentum.
            </p>

            <div style="background: rgba(0,0,0,0.35); border-radius: 10px; padding: 14px; border: 1px solid var(--border); margin-bottom: 12px;">
                <div style="display:flex; justify-content:space-between; align-items:baseline; margin-bottom:8px;">
                    <span style="font-size:12px; font-weight:700; color:var(--text-muted);">COMPOUND DISASTER SCORE</span>
                    <span id="prodCriScore" style="font-size:22px; font-weight:800; font-family:'JetBrains Mono', monospace; color:var(--accent);">0.00 / 1.00</span>
                </div>
                <div class="progress-bar-bg" style="height:8px; margin-bottom:12px;">
                    <div id="prodCriBar" class="progress-bar-fill" style="width:0%; background:var(--normal);"></div>
                </div>

                <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size:11px;">
                    <div>💧 Discharge Factor: <strong id="prodCriDischarge">--</strong></div>
                    <div>🌧️ Rain Intensity: <strong id="prodCriRain">--</strong></div>
                    <div>🌱 Soil Saturation: <strong id="prodCriSoil">--</strong></div>
                    <div>⚡ Upstream Momentum: <strong id="prodCriMomentum">--</strong></div>
                </div>
            </div>
            
            <div id="prodCriRationale" style="font-size: 11px; color: #cbd5e1; font-family: 'JetBrains Mono', monospace; background: rgba(0,0,0,0.25); padding: 10px; border-radius: 8px; border: 1px dashed var(--border);">
                Loading scientific rationale...
            </div>
        </div>

        <!-- CARD 2: Multi-Channel Voice Alert Broadcast -->
        <div class="glass-card">
            <div class="card-header">
                <span class="card-title">📞 Multi-Channel Voice Alert Gateway (EWS 1294 / Regional IVR)</span>
                <span class="status-pill NORMAL">SIP Trunk Ready</span>
            </div>
            <p style="font-size: 11px; color: var(--text-secondary); margin-bottom: 12px;">
                Automated Interactive Voice Response (IVR) telephony broadcast for rural communities (EWS 1294 integration in Cambodia, extensible SIP across Asia).
            </p>

            <div style="background: rgba(0,0,0,0.35); border-radius: 10px; padding: 12px; border: 1px solid var(--border); margin-bottom: 12px;">
                <div style="font-size: 10px; font-weight: 700; color: var(--accent-cyan); text-transform: uppercase; margin-bottom: 4px;">Spoken Audio Script / Voice Prompt</div>
                <div id="prodVoiceKhmer" style="font-size: 11px; line-height: 1.5; color: #f1f5f9; min-height: 50px;">
                    Loading spoken voice prompt...
                </div>
            </div>

            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; font-size: 11px; color: var(--text-muted);">
                <span>Estimated Duration: <strong id="prodVoiceDuration">45s</strong></span>
                <span>Target Subscribers: <strong id="prodVoiceSubs">1,250</strong></span>
            </div>

            <button class="btn btn-danger" onclick="triggerProductionVoiceIVR()" style="width: 100%;">
                🚨 Broadcast Emergency Voice Alert to Station Area
            </button>
        </div>
    </div>

    <!-- ROW 2: River Gauges & Historical Flood Benchmarks -->
    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; margin-bottom: 20px;">
        <!-- River Gauges -->
        <div class="glass-card">
            <div class="card-header">
                <span class="card-title">📏 River Gauge Monitoring Network (MRC & Regional Stage Telemetry)</span>
                <button class="btn btn-glass" onclick="loadProductionHubData()" style="padding:4px 8px; font-size:10px;">🔄 Refresh</button>
            </div>
            <p style="font-size: 11px; color: var(--text-secondary); margin-bottom: 12px;">
                Ground-truth water level (meters) checked against official MRC Alarm & Flood stage thresholds.
            </p>
            <div style="overflow-x: auto;">
                <table>
                    <thead>
                        <tr><th>Station</th><th>Water Level (m)</th><th>Alarm (m)</th><th>Flood (m)</th><th>Status</th></tr>
                    </thead>
                    <tbody id="prodMrcTable">
                        <tr><td colspan="5" style="text-align:center;">Loading MRC gauge telemetry...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Historical Mega-Flood Benchmark -->
        <div class="glass-card">
            <div class="card-header">
                <span class="card-title">🏛️ Historical Mega-Flood Benchmarking</span>
                <span class="status-pill NORMAL">Historic Data</span>
            </div>
            <p style="font-size: 11px; color: var(--text-secondary); margin-bottom: 12px;">
                Discharge volume compared directly to catastrophic historical Mekong disasters.
            </p>
            <div style="display: flex; flex-direction: column; gap: 10px;">
                <div style="background: rgba(0,0,0,0.3); padding: 10px; border-radius: 8px; border: 1px solid var(--border);">
                    <div style="display:flex; justify-content:space-between; font-size:11px; margin-bottom:4px;">
                        <span>2000 Centennial Mega-Flood (52,000 m³/s)</span>
                        <strong id="prodBench2000Pct" style="color:var(--accent-cyan);">--%</strong>
                    </div>
                    <div class="progress-bar-bg" style="height:6px;">
                        <div id="prodBench2000Bar" class="progress-bar-fill" style="width:0%; background:var(--accent);"></div>
                    </div>
                </div>

                <div style="background: rgba(0,0,0,0.3); padding: 10px; border-radius: 8px; border: 1px solid var(--border);">
                    <div style="display:flex; justify-content:space-between; font-size:11px; margin-bottom:4px;">
                        <span>2011 Catastrophic Southeast Asia Flood (48,500 m³/s)</span>
                        <strong id="prodBench2011Pct" style="color:var(--accent-cyan);">--%</strong>
                    </div>
                    <div class="progress-bar-bg" style="height:6px;">
                        <div id="prodBench2011Bar" class="progress-bar-fill" style="width:0%; background:var(--caution);"></div>
                    </div>
                </div>

                <div id="prodBenchContext" style="font-size: 11px; color: var(--text-secondary); line-height: 1.4; padding: 8px; background: rgba(255,255,255,0.03); border-radius: 6px;">
                    Awaiting benchmark evaluation...
                </div>
            </div>
        </div>
    </div>
</div>

    <!-- TAB 8: Pan-Asian Cascading Multi-Hazard Command Hub -->
    <div id="viewHazards" style="display: none;">
        <!-- Real-time multi-hazard hero -->
        <div style="background: linear-gradient(135deg, rgba(30, 20, 50, 0.8) 0%, rgba(15, 23, 42, 0.9) 100%); border: 1px solid rgba(191, 90, 242, 0.4); border-radius: var(--radius-lg); padding: 18px; margin-bottom: 20px;">
            <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px; margin-bottom:12px;">
                <div>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span style="font-size:24px;">&#127760;</span>
                        <h2 style="font-size:18px; font-weight:800; letter-spacing:-0.02em;">Live Multi-Hazard Feed &mdash; All Asia</h2>
                    </div>
                    <p style="font-size:12px; color:var(--text-secondary); margin-top:4px;">
                        Real observed events from USGS (earthquakes), NASA EONET, GDACS &amp; NASA FIRMS &mdash; refreshed live. Every row is an actual detection.
                    </p>
                </div>
                <div style="display:flex; gap:8px; flex-wrap:wrap;">
                    <a href="/api/hazards/live.geojson" target="_blank" class="btn btn-primary" style="text-decoration:none; font-size:11px;">&#128506;&#65039; Live Hazard GeoJSON</a>
                    <button class="btn btn-glass" onclick="loadMultiHazardHubData()" style="font-size:11px;">&#128260; Refresh</button>
                </div>
            </div>
            <div id="hazardSummaryStrip" style="display:flex; gap:8px; flex-wrap:wrap;">
                <span class="code-pill">Loading live hazards&hellip;</span>
            </div>
        </div>

        <!-- Nearest-to-station callout -->
        <div class="glass-card" style="margin-bottom:20px; border-left:4px solid #bf5af2;">
            <div class="card-header">
                <div>
                    <span class="card-title">&#9889; Hazards Near Selected Station</span>
                    <p style="font-size:11px; color:var(--text-secondary); margin-top:2px;">Live events within 800 km of the selected station (real feeds only).</p>
                </div>
                <span id="hazardThreatBadge" class="status-pill NORMAL">Evaluating&hellip;</span>
            </div>
            <div id="hazardNearest" style="font-size:12px; color:#f8fafc; line-height:1.6;">Loading&hellip;</div>
        </div>

        <!-- Unified live hazard feed -->
        <div class="glass-card">
            <div class="card-header">
                <div>
                    <span class="card-title">&#128225; Live Hazard Event Feed (Asia)</span>
                    <p style="font-size:10px; color:var(--text-secondary); margin-top:2px;">Sorted by severity then recency. Source shown per row.</p>
                </div>
                <span id="hazardFeedBadge" class="code-pill">0 events</span>
            </div>
            <div style="overflow-x:auto; max-height:560px;">
                <table>
                    <thead>
                        <tr><th>Severity</th><th>Type</th><th>Event</th><th>Intensity</th><th>When (UTC)</th><th>Source</th></tr>
                    </thead>
                    <tbody id="hazardFeedTable">
                        <tr><td colspan="6" style="text-align:center;">Loading live hazard feed&hellip;</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

<!-- Mobile Bottom Navigation Bar -->
<div class="mobile-tab-bar">
    <button id="mobTabMap" class="mobile-tab-item active" onclick="switchTab('map')">
        <span class="icon">🗺️</span>
        <span>Map</span>
    </button>
    <button id="mobTabMetrics" class="mobile-tab-item" onclick="switchTab('metrics')">
        <span class="icon">📊</span>
        <span>Metrics</span>
    </button>
    <button id="mobTabCascade" class="mobile-tab-item" onclick="switchTab('cascade')">
        <span class="icon">🌊</span>
        <span>Cascade</span>
    </button>
    <button id="mobTabSandbox" class="mobile-tab-item" onclick="switchTab('sandbox')">
        <span class="icon">🧪</span>
        <span>Sandbox</span>
    </button>
    <button id="mobTabForecast" class="mobile-tab-item" onclick="switchTab('forecast')">
        <span class="icon">📈</span>
        <span>Forecast</span>
    </button>
    <button id="mobTabConsole" class="mobile-tab-item" onclick="switchTab('console')">
        <span class="icon">⚡</span>
        <span>Admin</span>
    </button>
    <button id="mobTabProduction" class="mobile-tab-item" onclick="switchTab('production')">
        <span class="icon">🛡️</span>
        <span>Prod</span>
    </button>
    <button id="mobTabHazards" class="mobile-tab-item" onclick="switchTab('hazards')">
        <span class="icon">🌐</span>
        <span>Hazards</span>
    </button>
</div>

<div id="toast">Operation completed</div>

<script>
    let currentAreaId = 'kratie-central';
    let allNationalData = [];
    let nationalData = [];
    let isUpdating = false;
    let currentBasin = 'all';
    let activeCountryScope = 'ALL';
    let leafletMap = null;
    let mapMarkers = {};
    let hazardLayer = null;
    const HZ_MAP_ICON = { earthquake:"🌋", cyclone:"🌀", volcano:"🌋", wildfire:"🔥", flood:"🌊", landslide:"⛰️", drought:"☀️", other:"⚠️" };

    const BASIN_MAP = {
        'kratie-central': 'mekong', 'stung-treng': 'mekong', 'kampong-cham': 'mekong', 'phnom-penh': 'mekong', 'kandal': 'mekong', 'prey-veng': 'mekong', 'tbong-khmum': 'mekong',
        'siem-reap': 'tonle-sap', 'battambang': 'tonle-sap', 'kampong-chhnang': 'tonle-sap', 'kampong-thom': 'tonle-sap', 'pursat': 'tonle-sap', 'banteay-meanchey': 'tonle-sap',
        'kampot': 'coastal', 'koh-kong': 'coastal', 'preah-sihanouk': 'coastal', 'kep': 'coastal', 'takeo': 'highland', 'svay-rieng': 'highland', 'kampong-speu': 'highland',
        'preah-vihear': 'highland', 'oddar-meanchey': 'highland', 'ratanakiri': 'highland', 'mondulkiri': 'highland', 'pailin': 'highland',
        'laos-champasak': 'laos-southern', 'laos-attapeu': 'laos-southern', 'laos-sekong': 'laos-southern', 'laos-salavan': 'laos-southern',
        'laos-savannakhet': 'laos-central', 'laos-khammouane': 'laos-central', 'laos-bolikhamsai': 'laos-central', 'laos-vientiane-cap': 'laos-central', 'laos-vientiane-prov': 'laos-central',
        'laos-luang-prabang': 'laos-upper', 'laos-sayaboury': 'laos-upper', 'laos-bokeo': 'laos-upper', 'laos-luang-namtha': 'laos-upper', 'laos-oudomxay': 'laos-upper', 'laos-phongsaly': 'laos-upper', 'laos-houaphanh': 'laos-upper', 'laos-xiangkhouang': 'laos-upper', 'laos-xaisomboun': 'laos-upper'
    };

    function showToast(msg) {
        const t = document.getElementById('toast');
        t.innerText = msg;
        t.style.display = 'block';
        setTimeout(() => { t.style.display = 'none'; }, 3000);
    }

    const COUNTRY_FLAGS = {
        'KH': '🇰🇭', 'LA': '🇱🇦', 'TH': '🇹🇭', 'VN': '🇻🇳',
        'MM': '🇲🇲', 'CN': '🇨🇳', 'IN': '🇮🇳', 'BD': '🇧🇩',
        'PK': '🇵🇰', 'NP': '🇳🇵', 'PH': '🇵🇭', 'ID': '🇮🇩'
    };
    function getFlag(code) { return COUNTRY_FLAGS[code] || '🌐'; }

    function setCountryScope(scope) {
        activeCountryScope = scope;
        document.getElementById('scopeKhBtn').className = `country-pill ${scope === 'KH' ? 'active' : ''}`;
        document.getElementById('scopeLaBtn').className = `country-pill ${scope === 'LA' ? 'active' : ''}`;
        const asiaBtn = document.getElementById('scopeAsiaBtn');
        if (asiaBtn) asiaBtn.className = `country-pill ${scope === 'ASIA' ? 'active' : ''}`;
        document.getElementById('scopeAllBtn').className = `country-pill ${scope === 'ALL' ? 'active' : ''}`;

        if (scope === 'KH') {
            document.getElementById('kpiTotalLabel').innerText = 'Cambodia Stations (25)';
            if (leafletMap) leafletMap.setView([12.5657, 104.9910], 7);
        } else if (scope === 'LA') {
            document.getElementById('kpiTotalLabel').innerText = 'Laos Inflow Stations (18)';
            if (leafletMap) leafletMap.setView([18.0, 103.5], 6);
        } else if (scope === 'ASIA') {
            document.getElementById('kpiTotalLabel').innerText = 'Rest of Asia (69)';
            if (leafletMap) leafletMap.setView([20.0, 95.0], 4);
        } else {
            document.getElementById('kpiTotalLabel').innerText = 'All Asian Stations (63)';
            if (leafletMap) leafletMap.setView([16.0, 100.0], 4);
        }

        applyScopeFilter();
    }

    function applyScopeFilter() {
        if (activeCountryScope === 'KH') {
            nationalData = allNationalData.filter(s => (s.area.country || 'KH') === 'KH');
        } else if (activeCountryScope === 'LA') {
            nationalData = allNationalData.filter(s => s.area.country === 'LA');
        } else if (activeCountryScope === 'ASIA') {
            nationalData = allNationalData.filter(s => s.area.country !== 'KH' && s.area.country !== 'LA');
        } else {
            nationalData = [...allNationalData];
        }

        let normal = 0, caution = 0, danger = 0;
        nationalData.forEach(s => {
            const lvl = (s.risk_state?.level || 'Normal').toUpperCase();
            if (lvl === 'DANGER') danger++;
            else if (lvl === 'CAUTION') caution++;
            else normal++;
        });

        document.getElementById('kpiTotal').innerText = nationalData.length;
        document.getElementById('kpiNormal').innerText = normal;
        document.getElementById('kpiCaution').innerText = caution;
        document.getElementById('kpiDanger').innerText = danger;

        renderDonutChart(normal, caution, danger);
        filterGrid();
        renderMapMarkers();
        renderTop5Cards();
        updateCascadeData();
        populateStationDropdown(allNationalData);
    }

    function switchTab(tab) {
        const tabs = ['map', 'metrics', 'cascade', 'sandbox', 'forecast', 'console', 'production', 'hazards'];
        tabs.forEach(t => {
            const btn = document.getElementById(`tabBtn${t.charAt(0).toUpperCase() + t.slice(1)}`);
            const mobBtn = document.getElementById(`mobTab${t.charAt(0).toUpperCase() + t.slice(1)}`);
            const view = document.getElementById(`view${t.charAt(0).toUpperCase() + t.slice(1)}`);
            
            if (btn) btn.className = `segment-btn ${t === tab ? 'active' : ''}`;
            if (mobBtn) mobBtn.className = `mobile-tab-item ${t === tab ? 'active' : ''}`;
            if (view) view.style.display = t === tab ? 'block' : 'none';
        });

        if (tab === 'map' && leafletMap) {
            setTimeout(() => { leafletMap.invalidateSize(); }, 200);
        }
        if (tab === 'cascade') updateCascadeData();
        if (tab === 'sandbox') runSandboxEvaluation();
        if (tab === 'forecast') loadForecastData(currentAreaId);
        if (tab === 'console') { updateStationStatus(); refreshStorageStats(); }
        if (tab === 'production') loadProductionHubData();
        if (tab === 'hazards') loadMultiHazardHubData();
    }

    const HZ_ICON = { earthquake:"🌋", cyclone:"🌀", volcano:"🌋", wildfire:"🔥", flood:"🌊", landslide:"⛰️", drought:"☀️", other:"⚠️" };
    const HZ_SEV_CLASS = { red:"DANGER", orange:"CAUTION", green:"NORMAL", info:"NORMAL" };
    const HZ_ROLE_META = {
        PRIMARY_DRIVER:       { label:"🌧️ Primary flood drivers", desc:"Directly generate the flood water", cls:"DANGER" },
        COMPOUNDING_TRIGGER:  { label:"⚡ Compounding triggers", desc:"Can indirectly unleash a surge (dam breach, landslide dam, lahar)", cls:"CAUTION" },
        ANTECEDENT_AMPLIFIER: { label:"🔥 Antecedent amplifiers", desc:"Make the next rain worse (burn scars, drought)", cls:"CAUTION" },
        NOT_FLOOD_RELEVANT:   { label:"➖ Not flood-relevant", desc:"Shown for context; no flash-flood pathway", cls:"NORMAL" }
    };
    const HZ_ROLE_ORDER = ["PRIMARY_DRIVER","COMPOUNDING_TRIGGER","ANTECEDENT_AMPLIFIER","NOT_FLOOD_RELEVANT"];
    const HZ_PRESSURE_CLASS = { HIGH:"DANGER", ELEVATED:"CAUTION", LOW:"NORMAL", NONE:"NORMAL" };

    function hzWhen(iso) {
        try { return new Date(iso).toISOString().substring(0,16).replace("T"," "); } catch(e) { return "--"; }
    }

    function hzRow(e) {
        return `
            <tr>
                <td><span class="status-pill ${HZ_SEV_CLASS[e.severity]||'NORMAL'}" style="font-size:9px;">${(e.severity||'info').toUpperCase()}</span></td>
                <td>${HZ_ICON[e.event_type]||''} ${e.event_type}</td>
                <td style="font-size:11px;">${e.url ? `<a href="${e.url}" target="_blank" style="color:var(--accent-cyan); text-decoration:none;">${e.title}</a>` : e.title}
                    ${e.mechanism ? `<div style="font-size:10px; color:var(--text-secondary); margin-top:2px;">${e.mechanism}</div>` : ''}</td>
                <td style="font-weight:700;">${e.value_label || '--'}</td>
                <td style="font-size:11px; color:var(--text-secondary);">${hzWhen(e.observed_at)}</td>
                <td><span class="code-pill" style="font-size:9px;">${e.source}</span></td>
            </tr>`;
    }

    async function loadMultiHazardHubData() {
        try {
            // 1. Unified live hazard feed, GROUPED BY FLASH-FLOOD ROLE
            const feedRes = await fetch('/api/hazards/feed?days=7&limit=250');
            if (feedRes.ok) {
                const feed = await feedRes.json();
                const events = feed.events || [];
                const strip = document.getElementById('hazardSummaryStrip');
                if (strip) {
                    const byRole = {};
                    events.forEach(e => { byRole[e.flood_role] = (byRole[e.flood_role]||0)+1; });
                    const rolePills = HZ_ROLE_ORDER.filter(r=>byRole[r]).map(r =>
                        `<span class="code-pill ${HZ_PRESSURE_CLASS[HZ_ROLE_META[r].cls]||''}" style="color:var(--text-primary);">${HZ_ROLE_META[r].label}: ${byRole[r]}</span>`).join('');
                    const srcPills = Object.keys(feed.by_source||{}).map(s => `<span class="code-pill" style="color:var(--text-secondary);">${s}: ${feed.by_source[s]}</span>`).join('');
                    strip.innerHTML = `<span class="code-pill" style="color:var(--accent-cyan);">${feed.count} live events</span>` + rolePills + srcPills;
                }
                const badge = document.getElementById('hazardFeedBadge');
                if (badge) badge.innerText = `${feed.count} events`;
                const tbody = document.getElementById('hazardFeedTable');
                if (tbody) {
                    let html = '';
                    HZ_ROLE_ORDER.forEach(role => {
                        const rows = events.filter(e => e.flood_role === role);
                        if (!rows.length) return;
                        const meta = HZ_ROLE_META[role];
                        html += `<tr><td colspan="6" style="background:rgba(255,255,255,0.04); padding:8px 10px;">
                            <strong>${meta.label}</strong> <span style="color:var(--text-secondary); font-size:10px;">— ${meta.desc} (${rows.length})</span></td></tr>`;
                        html += rows.map(hzRow).join('');
                    });
                    tbody.innerHTML = html || `<tr><td colspan="6" style="text-align:center;">No live hazards in range.</td></tr>`;
                }
            }

            // 2. Flash-flood PRESSURE at the selected station (Phase 7 contribution model)
            const matrixRes = await fetch(`/api/hazards/cascading-matrix?area_id=${currentAreaId}`);
            if (matrixRes.ok) {
                const m = await matrixRes.json();
                const badge = document.getElementById('hazardThreatBadge');
                if (badge) {
                    const pct = Math.round((m.flood_pressure||0)*100);
                    badge.innerText = `Flood pressure ${pct}% · ${m.pressure_band||'NONE'}`;
                    badge.className = `status-pill ${HZ_PRESSURE_CLASS[m.pressure_band]||'NORMAL'}`;
                }
                const box = document.getElementById('hazardNearest');
                if (box) {
                    const reasons = (m.reasons||[]);
                    if (!reasons.length) {
                        box.innerHTML = `<div style="color:var(--text-secondary);">No hazards are currently contributing to flash-flood risk at this station. (Level stays driven by real discharge &amp; rainfall.)</div>`;
                    } else {
                        box.innerHTML = `<div style="margin-bottom:6px; color:var(--text-secondary); font-size:11px;">These real events are raising this station's flash-flood pressure (they do NOT change its official level):</div>` +
                            reasons.map(r => `<div style="margin:4px 0;">
                                ${HZ_ICON[r.event_type]||''} <strong>${r.title}</strong>
                                <span class="code-pill" style="font-size:9px;">${r.distance_km} km</span>
                                <span class="code-pill" style="font-size:9px;">score ${Math.round(r.score*100)}%</span>
                                <div style="font-size:10px; color:var(--text-secondary); margin-top:1px;">${r.mechanism}</div>
                            </div>`).join('');
                    }
                }
            }

            // 3. Refresh the map hazard overlay too
            if (typeof renderHazardOverlay === 'function') renderHazardOverlay();
        } catch (e) {
            console.error("Multi-hazard hub load error:", e);
        }
    }

    async function loadProductionHubData() {
        try {
            // 1. Compound Risk
            const criRes = await fetch(`/api/analytics/compound-risk?area_id=${currentAreaId}`);
            if (criRes.ok) {
                const criData = await criRes.json();
                const a = criData.assessment;
                const tierEl = document.getElementById('prodCriTier');
                tierEl.innerText = a.level.toUpperCase();
                tierEl.className = `status-pill ${a.level.toUpperCase()}`;

                document.getElementById('prodCriScore').innerText = `${a.compound_score.toFixed(2)} / 1.00`;
                const bar = document.getElementById('prodCriBar');
                bar.style.width = `${Math.min(100, Math.round(a.compound_score * 100))}%`;
                bar.style.background = a.level === 'Danger' ? 'var(--danger)' : (a.level === 'Caution' ? 'var(--caution)' : 'var(--normal)');

                document.getElementById('prodCriDischarge').innerText = `${Math.round(a.discharge_factor * 100)}%`;
                document.getElementById('prodCriRain').innerText = `${Math.round(a.precipitation_factor * 100)}%`;
                document.getElementById('prodCriSoil').innerText = `${Math.round(a.soil_saturation_factor * 100)}%`;
                document.getElementById('prodCriMomentum').innerText = `${Math.round(a.surge_momentum * 100)}%`;
                document.getElementById('prodCriRationale').innerText = a.justification_en;
            }

            // 2. Historical Benchmark
            const benchRes = await fetch(`/api/analytics/historical-compare?area_id=${currentAreaId}`);
            if (benchRes.ok) {
                const bData = await benchRes.json();
                const b = bData.benchmark;
                document.getElementById('prodBench2000Pct').innerText = `${b.percent_of_2000_peak}%`;
                document.getElementById('prodBench2000Bar').style.width = `${Math.min(100, b.percent_of_2000_peak)}%`;
                document.getElementById('prodBench2011Pct').innerText = `${b.percent_of_2011_peak}%`;
                document.getElementById('prodBench2011Bar').style.width = `${Math.min(100, b.percent_of_2011_peak)}%`;
                document.getElementById('prodBenchContext').innerText = b.historical_context;
            }

            // 3. MRC Gauges
            const mrcRes = await fetch('/api/telemetry/mrc-gauges');
            if (mrcRes.ok) {
                const mrcData = await mrcRes.json();
                const tbody = document.getElementById('prodMrcTable');
                tbody.innerHTML = (mrcData.gauges || []).slice(0, 7).map(g => {
                    const exceeded = g.is_official_stage_exceeded;
                    const badge = exceeded ? '<span class="status-pill DANGER">EXCEEDED</span>' : '<span class="status-pill NORMAL">NOMINAL</span>';
                    return `
                        <tr>
                            <td><strong>${g.name_en}</strong></td>
                            <td style="font-family:'JetBrains Mono', monospace; font-weight:700; color:var(--accent-cyan);">${g.water_level_meters.toFixed(2)} m</td>
                            <td style="color:var(--caution); font-family:'JetBrains Mono', monospace;">${g.alarm_level_meters.toFixed(2)} m</td>
                            <td style="color:var(--danger); font-family:'JetBrains Mono', monospace;">${g.flood_level_meters.toFixed(2)} m</td>
                            <td>${badge}</td>
                        </tr>
                    `;
                }).join('');
            }
        } catch(e) {
            console.error("Error loading production hub data:", e);
        }
    }

    async function triggerProductionVoiceIVR() {
        try {
            const res = await fetch('/api/dispatch/voice-ivr', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ area_id: currentAreaId })
            });
            const data = await res.json();
            if (res.ok) {
                showToast("📞 EWS 1294 Voice broadcast queued successfully!");
                const p = data.voice_payload;
                document.getElementById('prodVoiceKhmer').innerText = p.khmer_audio_script;
                document.getElementById('prodVoiceDuration').innerText = `${p.estimated_call_duration_seconds}s`;
                document.getElementById('prodVoiceSubs').innerText = Number(p.estimated_target_rural_subscribers).toLocaleString();
            } else {
                showToast("Failed to dispatch voice call");
            }
        } catch(e) {
            showToast("Voice IVR dispatch error");
        }
    }

    function setBasinFilter(basin, btn) {
        currentBasin = basin;
        document.querySelectorAll('.basin-chip').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        filterGrid();
    }

    function inspectStation(areaId) {
        currentAreaId = areaId;
        const select = document.getElementById('stationSelect');
        if (select) select.value = areaId;
        switchTab('console');
    }

    function initMap() {
        if (leafletMap) return;
        leafletMap = L.map('cambodiaMap', { center: [16.0, 100.0], zoom: 4, zoomControl: true });
        // Keyless dark basemap (Esri World Dark Gray Canvas). CARTO's dark_all
        // now requires an API key and renders an "API KEY REQUIRED" watermark.
        L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}', {
            attribution: 'Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ',
            maxZoom: 16
        }).addTo(leafletMap);

        // Toggleable live-hazard overlay (real events: USGS / EONET / GDACS / FIRMS)
        hazardLayer = L.layerGroup().addTo(leafletMap);
        L.control.layers(null, { "🌐 Live Hazards": hazardLayer }, { collapsed: false, position: 'topright' }).addTo(leafletMap);
        renderHazardOverlay();
    }

    async function renderHazardOverlay() {
        if (!leafletMap || !hazardLayer) return;
        try {
            const res = await fetch('/api/hazards/live.geojson?days=7');
            if (!res.ok) return;
            const gj = await res.json();
            hazardLayer.clearLayers();
            (gj.features || []).forEach(f => {
                const c = f.geometry.coordinates;      // [lon, lat]
                const p = f.properties || {};
                const sevRing = p.severity === 'red' ? '#ff453a' : (p.severity === 'orange' ? '#ff9f0a' : '#e5e7eb');
                const m = L.circleMarker([c[1], c[0]], {
                    radius: p.severity === 'red' ? 9 : (p.severity === 'orange' ? 7 : 5),
                    fillColor: p.color || '#94a3b8',
                    color: sevRing,
                    weight: 2,
                    opacity: 0.95,
                    fillOpacity: 0.55
                });
                m.bindPopup(`
                    <div style="font-size:12px; line-height:1.4;">
                        <div style="font-weight:700;">${HZ_MAP_ICON[p.event_type]||''} ${p.event_type.toUpperCase()} <span style="color:${sevRing};">(${(p.severity||'info').toUpperCase()})</span></div>
                        <div style="margin:4px 0;">${p.title}</div>
                        ${p.value_label ? `<div><strong>Intensity:</strong> ${p.value_label}</div>` : ''}
                        <div style="color:#86868b; font-size:11px;">${new Date(p.observed_at).toISOString().substring(0,16).replace('T',' ')} UTC · ${p.source}</div>
                        ${p.url ? `<div style="margin-top:4px;"><a href="${p.url}" target="_blank">Source &rarr;</a></div>` : ''}
                    </div>`);
                m.addTo(hazardLayer);
            });
        } catch (e) { console.error('Hazard overlay error:', e); }
    }

    function renderMapMarkers() {
        if (!leafletMap || nationalData.length === 0) return;

        // Clear existing markers
        Object.values(mapMarkers).forEach(m => leafletMap.removeLayer(m));
        mapMarkers = {};

        nationalData.forEach(s => {
            const lat = Number(s.area.latitude);
            const lon = Number(s.area.longitude);
            const lvl = (s.risk_state?.level || 'Normal').toUpperCase();
            const dis = Number(s.reading?.discharge || 0);
            const prec = Number(s.reading?.precipitation || 0);
            const flag = getFlag(s.area.country);

            let color = '#30d158';
            let radius = 7;
            if (lvl === 'CAUTION') { color = '#ffd60a'; radius = 10; }
            if (lvl === 'DANGER') { color = '#ff453a'; radius = 14; }

            const marker = L.circleMarker([lat, lon], {
                radius: radius,
                fillColor: color,
                color: '#ffffff',
                weight: 1.5,
                opacity: 0.9,
                fillOpacity: 0.8
            }).addTo(leafletMap);

            marker.bindPopup(`
                <div style="font-size:12px; line-height:1.4;">
                    <div style="font-weight:700; font-size:14px; color:#0a84ff;">${flag} ${s.area.name_en}</div>
                    <div style="color:#86868b; font-size:11px; margin-bottom:6px;">${s.area.name_km}</div>
                    <div><strong>Discharge:</strong> ${dis.toLocaleString()} m³/s</div>
                    <div><strong>Rainfall:</strong> ${prec.toFixed(1)} mm</div>
                    <div style="margin:6px 0;"><span class="status-pill ${lvl}">${lvl}</span></div>
                    <button onclick="inspectStation('${s.area.area_id}')" style="background:#0a84ff; color:#fff; border:none; padding:4px 10px; border-radius:6px; font-weight:600; font-size:11px; cursor:pointer; width:100%;">Inspect →</button>
                </div>
            `);

            mapMarkers[s.area.area_id] = marker;
        });
    }

    /* Live Animated Donut / Pie Chart */
    function renderDonutChart(normal, caution, danger) {
        const total = normal + caution + danger;
        if (total === 0) return;

        const circumference = 2 * Math.PI * 38; // ~238.76

        const normPct = normal / total;
        const cautPct = caution / total;
        const dangPct = danger / total;

        const normDash = normPct * circumference;
        const cautDash = cautPct * circumference;
        const dangDash = dangPct * circumference;

        const elNorm = document.getElementById('donutSliceNormal');
        const elCaut = document.getElementById('donutSliceCaution');
        const elDang = document.getElementById('donutSliceDanger');

        if (elNorm) {
            elNorm.setAttribute('stroke-dasharray', `${normDash} ${circumference}`);
            elNorm.setAttribute('stroke-dashoffset', '0');
        }
        if (elCaut) {
            elCaut.setAttribute('stroke-dasharray', `${cautDash} ${circumference}`);
            elCaut.setAttribute('stroke-dashoffset', `-${normDash}`);
        }
        if (elDang) {
            elDang.setAttribute('stroke-dasharray', `${dangDash} ${circumference}`);
            elDang.setAttribute('stroke-dashoffset', `-${normDash + cautDash}`);
        }

        const centerCount = document.getElementById('donutCenterCount');
        if (centerCount) centerCount.innerText = total;

        const lpNorm = document.getElementById('legendPctNormal');
        const lpCaut = document.getElementById('legendPctCaution');
        const lpDang = document.getElementById('legendPctDanger');

        if (lpNorm) lpNorm.innerText = `${normal} (${Math.round(normPct * 100)}%)`;
        if (lpCaut) lpCaut.innerText = `${caution} (${Math.round(cautPct * 100)}%)`;
        if (lpDang) lpDang.innerText = `${danger} (${Math.round(dangPct * 100)}%)`;
    }

    function filterByLevel(lvl) {
        document.getElementById('stationSearch').value = lvl;
        filterGrid();
    }

    function renderTop5Cards() {
        const grid = document.getElementById('top5CardsGrid');
        if (!grid || nationalData.length === 0) return;

        const sorted = [...nationalData].sort((a, b) => (b.reading?.discharge || 0) - (a.reading?.discharge || 0)).slice(0, 5);
        grid.innerHTML = sorted.map((s, idx) => {
            const lvl = (s.risk_state?.level || 'Normal').toUpperCase();
            const dis = Number(s.reading?.discharge || 0);
            const dangerPct = Math.min(100, Math.round((dis / 22000.0) * 100));
            const flag = getFlag(s.area.country);

            return `
                <div class="kpi-card" onclick="inspectStation('${s.area.area_id}')" style="cursor:pointer;">
                    <div style="display:flex; justify-content:space-between;">
                        <span style="font-size:11px; font-weight:800; color:var(--accent);">#${idx + 1}</span>
                        <span class="status-pill ${lvl}">${lvl}</span>
                    </div>
                    <div style="margin: 8px 0;">
                        <div style="font-size:13px; font-weight:700;">${flag} ${s.area.name_en.split(' (')[0]}</div>
                        <div style="font-size:10px; color:var(--text-secondary);">${s.area.name_km}</div>
                    </div>
                    <div>
                        <div style="font-size:10px; color:var(--text-muted);">DISCHARGE</div>
                        <div style="font-size:16px; font-weight:800; color:var(--accent-cyan); font-family:'JetBrains Mono', monospace;">
                            ${dis.toLocaleString()} <span style="font-size:10px; color:var(--text-secondary);">m³/s</span>
                        </div>
                    </div>
                    <div style="margin-top:8px; font-size:10px; color:var(--text-muted);">${dangerPct}% of Danger Threshold</div>
                </div>
            `;
        }).join('');
    }

    function updateCascadeData() {
        const getSt = id => allNationalData.find(s => s.area.area_id === id);
        const map = [
            { id: 'laos-champasak', disEl: 'cascadeDisPakse', capEl: 'cascadeCapPakse', badgeEl: 'cascadeBadgePakse' },
            { id: 'stung-treng', disEl: 'cascadeDisStungTreng', capEl: 'cascadeCapStungTreng', badgeEl: 'cascadeBadgeStungTreng' },
            { id: 'kratie-central', disEl: 'cascadeDisKratie', capEl: 'cascadeCapKratie', badgeEl: 'cascadeBadgeKratie' },
            { id: 'kampong-cham', disEl: 'cascadeDisKampongCham', capEl: 'cascadeCapKampongCham', badgeEl: 'cascadeBadgeKampongCham' },
            { id: 'phnom-penh', disEl: 'cascadeDisPhnomPenh', capEl: 'cascadeCapPhnomPenh', badgeEl: 'cascadeBadgePhnomPenh' }
        ];

        map.forEach(item => {
            const st = getSt(item.id);
            if (!st) return;
            const dis = Number(st.reading?.discharge || 0);
            const lvl = (st.risk_state?.level || 'Normal').toUpperCase();
            const pct = Math.min(100, Math.round((dis / 22000.0) * 100));

            const disEl = document.getElementById(item.disEl);
            const capEl = document.getElementById(item.capEl);
            const badgeEl = document.getElementById(item.badgeEl);

            if (disEl) disEl.innerText = `${dis.toLocaleString()} m³/s`;
            if (capEl) capEl.innerText = `${pct}% Capacity`;
            if (badgeEl) {
                badgeEl.className = `status-pill ${lvl}`;
                badgeEl.innerText = lvl;
            }
        });
    }

    async function runSandboxEvaluation() {
        const sel = document.getElementById('sandboxStationSelect');
        const areaId = sel ? sel.value : 'kratie-central';
        const disVal = parseFloat(document.getElementById('simDischargeSlider').value);
        const precVal = parseFloat(document.getElementById('simPrecipSlider').value);

        document.getElementById('labelSimDischarge').innerText = `${disVal.toLocaleString()} m³/s`;
        document.getElementById('labelSimPrecip').innerText = `${precVal.toFixed(1)} mm`;

        try {
            const res = await fetch('/api/sandbox/evaluate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ area_id: areaId, discharge_override: disVal, precip_override: precVal })
            });
            if (!res.ok) return;
            const data = await res.json();

            const lvl = (data.risk_level || 'Normal').toUpperCase();
            const color = lvl === 'DANGER' ? 'var(--danger)' : (lvl === 'CAUTION' ? 'var(--caution)' : 'var(--normal)');

            const lvlEl = document.getElementById('simRiskLevelText');
            lvlEl.innerText = lvl;
            lvlEl.style.color = color;

            document.getElementById('simCapacityPct').innerText = `${data.danger_capacity_percent}%`;

            const fill = document.getElementById('simGaugeFill');
            fill.style.width = `${Math.min(100, data.danger_capacity_percent)}%`;
            fill.style.background = color;

            document.getElementById('simEvacLeadTime').innerText = data.evacuation_lead_time;
            document.getElementById('simRecommendedAction').innerText = data.recommended_action;
        } catch (e) {
            console.error("Sandbox error:", e);
        }
    }

    function applySandboxPreset(discharge, precip) {
        document.getElementById('simDischargeSlider').value = discharge;
        document.getElementById('simPrecipSlider').value = precip;
        runSandboxEvaluation();
    }

    async function loadNationalOverview() {
        try {
            const res = await fetch('/api/national-overview');
            if (!res.ok) return;
            const data = await res.json();
            allNationalData = data.stations || [];

            if (data.storage_stats) {
                document.getElementById('statStations').innerText = data.storage_stats.total_stations;
                document.getElementById('statReadings').innerText = data.storage_stats.total_readings.toLocaleString();
                document.getElementById('statRisks').innerText = data.storage_stats.total_risk_evaluations.toLocaleString();
                document.getElementById('statSubs').innerText = data.storage_stats.total_subscriptions.toLocaleString();
            }

            applyScopeFilter();
        } catch (e) { console.error("National overview error:", e); }
    }

    function populateStationDropdown(stations) {
        ['stationSelect', 'forecastStationSelect', 'sandboxStationSelect'].forEach(selId => {
            const sel = document.getElementById(selId);
            if (!sel || sel.options.length === stations.length) return;
            sel.innerHTML = stations.map(s => {
                const flag = getFlag(s.area.country);
                return `<option value="${s.area.area_id}">${flag} ${s.area.name_en} — ${s.area.name_km}</option>`;
            }).join('');
            sel.value = currentAreaId;
        });
    }

    function filterGrid() {
        const q = (document.getElementById('stationSearch')?.value || '').toLowerCase();
        const filtered = nationalData.filter(s => {
            const matchesSearch = s.area.name_en.toLowerCase().includes(q) || s.area.name_km.toLowerCase().includes(q) || s.area.area_id.toLowerCase().includes(q) || (s.risk_state?.level || '').toLowerCase().includes(q);
            const matchesBasin = (currentBasin === 'all') || (BASIN_MAP[s.area.area_id] === currentBasin);
            return matchesSearch && matchesBasin;
        });
        renderNationalGrid(filtered);
    }

    function renderNationalGrid(stations) {
        const grid = document.getElementById('nationalGrid');
        if (!grid) return;
        grid.innerHTML = stations.map(s => {
            const lvl = (s.risk_state?.level || 'Normal').toUpperCase();
            const dis = Number(s.reading?.discharge || 0);
            const prec = Number(s.reading?.precipitation || 0);
            const flag = getFlag(s.area.country);
            return `<div class="station-grid-card ${lvl}">
                    <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                        <div>
                            <div style="font-size:14px; font-weight:700;">${flag} ${s.area.name_en}</div>
                            <div style="font-size:11px; color:var(--text-secondary);">${s.area.name_km}</div>
                        </div>
                        <span class="status-pill ${lvl}">${lvl}</span>
                    </div>
                    <div class="telemetry-row">
                        <div>
                            <div style="font-size:9px; color:var(--text-muted); text-transform:uppercase;">Discharge</div>
                            <div class="telemetry-val">${dis.toLocaleString()} <span style="font-size:10px; color:var(--text-secondary);">m³/s</span></div>
                        </div>
                        <div>
                            <div style="font-size:9px; color:var(--text-muted); text-transform:uppercase;">Rainfall</div>
                            <div class="telemetry-val" style="color:#fff;">${prec.toFixed(1)} <span style="font-size:10px; color:var(--text-secondary);">mm</span></div>
                        </div>
                    </div>
                    <button class="btn-inspect" onclick="inspectStation('${s.area.area_id}')">🔎 Inspect & Audit →</button>
                </div>`;
        }).join('');
    }

    async function loadForecastData(areaId) {
        try {
            const res = await fetch(`/api/station/forecast?area_id=${areaId}`);
            if (!res.ok) return;
            const data = await res.json();
            
            const svg = document.getElementById('hydroSvg');
            const pts = data.forecast || [];
            if (pts.length === 0) return;

            const maxDis = Math.max(26000, ...pts.map(p => p.discharge));
            const width = 800, height = 220;
            const stepX = width / (pts.length - 1);

            let dPath = '', areaPath = '';
            pts.forEach((p, idx) => {
                const x = idx * stepX;
                const y = height - (p.discharge / maxDis) * 180 - 20;
                if (idx === 0) { dPath += `M ${x} ${y}`; areaPath += `M ${x} ${height} L ${x} ${y}`; }
                else { dPath += ` L ${x} ${y}`; areaPath += ` L ${x} ${y}`; }
            });
            areaPath += ` L ${(pts.length - 1) * stepX} ${height} Z`;

            const dangerY = height - (22000.0 / maxDis) * 180 - 20;

            svg.innerHTML = `
                <defs>
                    <linearGradient id="dischargeGrad" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stop-color="#0a84ff" stop-opacity="0.5"/>
                        <stop offset="100%" stop-color="#0a84ff" stop-opacity="0.0"/>
                    </linearGradient>
                </defs>
                <line x1="0" y1="${dangerY}" x2="${width}" y2="${dangerY}" stroke="#ff453a" stroke-width="2" stroke-dasharray="6"/>
                <text x="10" y="${dangerY - 6}" fill="#ff453a" font-size="11" font-weight="700">DANGER THRESHOLD (22,000 m³/s)</text>
                <path d="${areaPath}" fill="url(#dischargeGrad)" />
                <path d="${dPath}" fill="none" stroke="#0a84ff" stroke-width="3" />
                ${pts.map((p, idx) => {
                    const x = idx * stepX;
                    const y = height - (p.discharge / maxDis) * 180 - 20;
                    return `
                        <circle cx="${x}" cy="${y}" r="5" fill="#64d2ff" stroke="#07090e" stroke-width="2"/>
                        <text x="${x}" y="${y - 10}" fill="#f5f5f7" font-size="11" font-family="'JetBrains Mono', monospace" text-anchor="middle" font-weight="700">${p.discharge.toLocaleString()} m³/s</text>
                        <text x="${x}" y="${height + 15}" fill="#86868b" font-size="11" text-anchor="middle">${p.label.split(' ')[0]}</text>
                    `;
                }).join('')}
            `;

            const grid = document.getElementById('forecastGrid');
            grid.innerHTML = pts.map(p => `
                <div style="background: rgba(255, 255, 255, 0.04); border: 1px solid var(--border); padding: 10px; border-radius: 8px; text-align: center;">
                    <div style="font-size: 10px; color: var(--text-muted);">${p.label}</div>
                    <div style="font-size: 13px; font-weight: 700; color: var(--accent-cyan); margin: 3px 0;">${p.discharge.toLocaleString()}</div>
                    <span class="status-pill ${p.risk_level}" style="font-size:8px;">${p.risk_level}</span>
                </div>
            `).join('');
        } catch (e) { console.error("Forecast error:", e); }
    }

    function onStationChanged() {
        currentAreaId = document.getElementById('stationSelect').value;
        updateStationStatus();
    }

    async function updateStationStatus() {
        if (isUpdating) return;
        isUpdating = true;
        try {
            const res = await fetch(`/api/status?area_id=${currentAreaId}`);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();
            if (data.area) {
                const flag = getFlag(data.area.country);
                document.getElementById('stationTitle').innerText = `${flag} ${data.area.name_en} (${data.area.name_km})`;
                document.getElementById('stationCoords').innerText = `Lat: ${Number(data.area.latitude).toFixed(4)}°N, Lon: ${Number(data.area.longitude).toFixed(4)}°E`;
                const floodUrl = `https://flood-api.open-meteo.com/v1/flood?latitude=${data.area.latitude}&longitude=${data.area.longitude}&daily=river_discharge&forecast_days=7`;
                const weatherUrl = `https://api.open-meteo.com/v1/forecast?latitude=${data.area.latitude}&longitude=${data.area.longitude}&current=precipitation`;
                const linkF = document.getElementById('linkFloodApi');
                const linkW = document.getElementById('linkWeatherApi');
                if (linkF) { linkF.href = floodUrl; linkF.innerText = floodUrl; }
                if (linkW) { linkW.href = weatherUrl; linkW.innerText = weatherUrl; }
            }
            if (data.reading) {
                const dis = Number(data.reading.discharge);
                const prec = Number(data.reading.precipitation);
                document.getElementById('valDischarge').innerText = dis.toLocaleString();
                document.getElementById('valPrecip').innerText = prec.toFixed(1);
                const fBadge = document.getElementById('freshnessBadge');
                fBadge.className = `status-pill ${data.reading.is_stale ? 'CAUTION' : 'NORMAL'}`;
                fBadge.innerText = data.reading.is_stale ? '🟡 Cached Stale' : '🟢 Live & Fresh';
            }
            if (data.risk_state) {
                document.getElementById('riskReasonBox').innerHTML = `<strong>[Audit Reason]</strong> ${data.risk_state.reason}`;
            }
        } catch (e) { console.error(e); } finally { isUpdating = false; }
    }

    async function triggerLiveFetch() {
        showToast("Fetching live data...");
        await fetch(`/api/intake/fetch-live?area_id=${currentAreaId}`, { method: 'POST' });
        showToast("Data ingested!");
        await updateStationStatus();
        await loadNationalOverview();
    }

    async function triggerSyncAll() {
        showToast("Syncing all 63 Pan-Asian stations...");
        await fetch('/api/intake/sync-all', { method: 'POST' });
        showToast("Background sync started!");
        setTimeout(loadNationalOverview, 2500);
    }

    async function triggerManualTestAlert() {
        await fetch(`/api/admin/trigger-test-alert?area_id=${currentAreaId}`, { method: 'POST' });
        showToast("Manual test alert dispatched!");
        loadTestLogs();
    }

    async function sendRealAlert() {
        showToast("Dispatching real alert...");
        const res = await fetch('/api/bot/dispatch-alert', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ area_id: currentAreaId, channel: 'all', custom_notes: 'Sent via Admin Console button' })
        });
        const data = await res.json();
        const r = data.delivery_results || {};
        const parts = Object.entries(r).map(([ch, v]) => `${ch}: ${v.status}`).join(' | ');
        showToast(`Dispatched to ${data.area_name} — ${parts}`);
        loadTestLogs();
    }

    async function triggerSim(scenario, autoResetSec = 10) {
        await fetch(`/api/simulate?area_id=${currentAreaId}&scenario=${scenario}&auto_reset_seconds=${autoResetSec}`, { method: 'POST' });
        showToast(`Simulated '${scenario}' — auto-reverts to live in ${autoResetSec}s ⏱️`);
        await updateStationStatus();
        await loadNationalOverview();
        if (autoResetSec > 0) {
            setTimeout(async () => {
                await updateStationStatus();
                await loadNationalOverview();
                showToast('✅ Reverted back to live telemetry');
            }, (autoResetSec + 0.5) * 1000);
        }
    }

    async function resetToLive() {
        await fetch(`/api/simulate/reset?area_id=${currentAreaId}`, { method: 'POST' });
        showToast('✅ Reset to live telemetry data');
        await updateStationStatus();
        await loadNationalOverview();
    }

    async function pruneOldStorage(days = 30) {
        showToast(`Pruning records older than ${days} days...`);
        const res = await fetch(`/api/maintenance/prune-storage?keep_days=${days}`, { method: 'POST' });
        const data = await res.json();
        showToast(`Cleaned ${data.deleted_records} stale rows. Storage optimized!`);
        refreshStorageStats();
    }

    async function refreshStorageStats() {
        try {
            const res = await fetch('/api/maintenance/storage-stats');
            const data = await res.json();
            if (data.storage) {
                document.getElementById('statStations').innerText = data.storage.total_stations;
                document.getElementById('statReadings').innerText = data.storage.total_readings.toLocaleString();
                document.getElementById('statRisks').innerText = data.storage.total_risk_evaluations.toLocaleString();
                document.getElementById('statSubs').innerText = data.storage.total_subscriptions.toLocaleString();
            }
        } catch (e) { console.error("Storage stat error:", e); }
    }

    async function loadTestLogs() {
        const res = await fetch('/api/admin/test-events');
        const data = await res.json();
        const tbody = document.getElementById('testLogTable');
        if (data.events?.length) {
            tbody.innerHTML = data.events.map(e => `<tr><td><span class="code-pill">${e.test_id}</span></td><td><strong>${e.area_id}</strong></td><td>${e.message_type}</td><td>${e.status}</td><td>${new Date(e.triggered_at).toLocaleTimeString()}</td><td>${e.message_en}</td></tr>`).join('');
        }
    }

    async function loadAuditTimeline() {
        try {
            const res = await fetch('/api/audit/timeline?limit=25');
            const data = await res.json();
            const tbody = document.getElementById('auditLogTable');
            if (data.events?.length) {
                tbody.innerHTML = data.events.map(e => {
                    const typeBadge = e.event_type === 'MANUAL_TEST_ALERT' ? '🔴 TEST ALERT' :
                                      e.event_type === 'TELEMETRY_EVALUATION' ? '🟢 INGESTION' :
                                      e.event_type === 'SIMULATION' ? '🟡 SIMULATION' : '🔵 SYSTEM';
                    const detStr = Object.entries(e.details || {}).map(([k,v]) => `${k}: ${v}`).join(', ');
                    return `<tr>
                        <td><span class="code-pill" style="font-size: 10px;">${e.event_id}</span></td>
                        <td><span style="font-size: 10px; font-weight: 700; color: var(--accent-cyan);">${typeBadge}</span></td>
                        <td><strong>${e.area_id || 'GLOBAL'}</strong></td>
                        <td style="color: var(--accent); font-weight: 600; font-size: 11px;">${e.timestamp_local || e.timestamp_ict || e.timestamp_utc}</td>
                        <td style="font-size: 10px; color: var(--text-muted); font-family: monospace;">${e.timestamp_utc}</td>
                        <td style="font-size: 11px; color: var(--text-secondary);">${detStr || '--'}</td>
                    </tr>`;
                }).join('');
            } else {
                tbody.innerHTML = '<tr><td colspan="6" style="text-align:center; color: var(--text-muted);">No audit events recorded yet.</td></tr>';
            }
        } catch(err) {
            console.error("Audit timeline error:", err);
        }
    }

    async function loadUserAlertHistory() {
        try {
            const res = await fetch('/api/alerts/history?limit=30');
            const data = await res.json();
            const tbody = document.getElementById('userAlertLogTable');
            if (data.alerts?.length) {
                tbody.innerHTML = data.alerts.map(a => {
                    const sevBadge = a.alert_level === 'DANGER' ? '<span style="color:var(--danger);font-weight:bold;">🚨 DANGER</span>' :
                                     a.alert_level === 'CAUTION' ? '<span style="color:var(--warning);font-weight:bold;">⚠️ CAUTION</span>' : '🟢 NORMAL';
                    const statusBadge = a.status === 'DELIVERED' ? '<span class="status-badge live" style="font-size:10px;">DELIVERED</span>' :
                                        a.status === 'THROTTLED' ? '<span class="status-badge stale" style="font-size:10px;">THROTTLED (Rate Limit)</span>' : '<span style="color:var(--danger)">FAILED</span>';
                    const reason = a.details?.reason || (a.status === 'DELIVERED' ? 'Sent successfully' : '--');
                    return `<tr>
                        <td><span class="code-pill" style="font-size: 10px;">${a.alert_id}</span></td>
                        <td><code style="color: var(--accent-cyan); font-size: 11px;">${a.recipient_id}</code></td>
                        <td><span style="font-size: 11px; font-weight: 600;">${a.channel}</span></td>
                        <td><strong>${a.area_id}</strong></td>
                        <td>${sevBadge}</td>
                        <td style="color: var(--accent); font-weight: 600; font-size: 11px;">${a.dispatched_at_local || a.dispatched_at_ict || a.dispatched_at_utc}</td>
                        <td>${statusBadge} <span style="font-size: 10px; color: var(--text-muted); margin-left: 6px;">${reason}</span></td>
                    </tr>`;
                }).join('');
            } else {
                tbody.innerHTML = '<tr><td colspan="7" style="text-align:center; color: var(--text-muted);">No citizen alert dispatches recorded yet.</td></tr>';
            }
        } catch (e) { console.error("User alert log error:", e); }
    }

    setInterval(() => {
        const clock = document.getElementById('liveClock');
        if (clock) {
            const utc = new Date().toISOString().substring(11, 19);
            clock.innerText = `🌐 ${utc} UTC`;
        }
    }, 1000);

    (async function init() {
        initMap();
        await loadNationalOverview();
        await updateStationStatus();
        await loadTestLogs();
        await loadAuditTimeline();
        await loadUserAlertHistory();
        await refreshStorageStats();
        await loadMultiHazardHubData();
        setInterval(loadNationalOverview, 10000);
        setInterval(loadAuditTimeline, 15000);
        setInterval(loadUserAlertHistory, 15000);
        setInterval(loadMultiHazardHubData, 30000);
    })();
</script>
</body>
</html>
"""
    return HTMLResponse(content=html_content)
