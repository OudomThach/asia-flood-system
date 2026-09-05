"""
Unit tests for Storage Performance, Anti-Spam Write Deduplication, and Laos Transboundary Expansion.
Lead & Author: Oudom Thach
"""

from datetime import datetime, timezone
import pytest
from asia_flood_core.models import Reading, Area
from asia_flood_core.storage import FloodDataRepository


class TestStorageOptimization:
    def test_repository_seeds_pan_asian_stations(self):
        repo = FloodDataRepository(db_path=":memory:")
        all_areas = repo.get_all_areas()
        # 25 KH + 18 LA + 69 rest-of-Asia stations = 112 stations across 47 countries
        assert len(all_areas) == 112

        cambodia_areas = repo.get_all_areas(country="KH")
        assert len(cambodia_areas) == 25

        laos_areas = repo.get_all_areas(country="LA")
        assert len(laos_areas) == 18

        # Pan-Asian expansion stations across ~45 other Asian countries.
        pan_asian_areas = [a for a in all_areas if a.country not in ["KH", "LA"]]
        assert len(pan_asian_areas) == 69
        # Coverage spans the whole continent, not just the Lower Mekong.
        assert len({a.country for a in all_areas}) == 47

    def test_smart_write_deduplication_prevents_spam(self):
        repo = FloodDataRepository(db_path=":memory:")
        aid = "kratie-central"
        now1 = datetime.now(timezone.utc)
        
        r1 = Reading(
            reading_id="test-r1",
            area_id=aid,
            observed_at=now1,
            discharge=12500.0,
            precipitation=15.0,
            source="Test Engine",
            fetched_at=now1,
            is_stale=False
        )
        repo.save_reading(r1)
        history_before = repo.get_readings_history(area_id=aid, limit=50)
        count_before = len(history_before)
        
        # Second identical reading within same period (anti-spam delta gate)
        now2 = datetime.now(timezone.utc)
        r2 = Reading(
            reading_id="test-r2",
            area_id=aid,
            observed_at=now1,
            discharge=12500.0,
            precipitation=15.0,
            source="Test Engine",
            fetched_at=now2,
            is_stale=False
        )
        repo.save_reading(r2)
        
        history_after = repo.get_readings_history(area_id=aid, limit=50)
        # Should not append an extra duplicate row, preserving memory and disk
        assert len(history_after) == count_before
        
        # Third reading with different discharge should be saved
        r3 = Reading(
            reading_id="test-r3",
            area_id=aid,
            observed_at=now2,
            discharge=18000.0,
            precipitation=35.0,
            source="Test Engine",
            fetched_at=now2,
            is_stale=False
        )
        repo.save_reading(r3)
        history_updated = repo.get_readings_history(area_id=aid, limit=50)
        assert len(history_updated) == count_before + 1

    def test_storage_stats_and_pruning(self):
        repo = FloodDataRepository(db_path=":memory:")
        stats = repo.get_storage_stats()
        assert stats["total_stations"] == 112
        assert stats["wal_mode"] is True
        
        deleted = repo.prune_historical_readings(keep_days=30)
        assert isinstance(deleted, int)

    def test_audit_event_logging_by_time(self):
        repo = FloodDataRepository(db_path=":memory:")
        evt_id = repo.log_audit_event(
            event_type="TEST_RECORDING",
            area_id="kratie-central",
            details={"discharge": 15400.0, "status": "VERIFIED"}
        )
        assert evt_id.startswith("evt-")
        
        events = repo.get_audit_events(limit=10)
        assert len(events) >= 1
        latest = events[0]
        assert latest["event_type"] == "TEST_RECORDING"
        assert "ICT" in latest["timestamp_ict"]
        assert latest["details"]["discharge"] == 15400.0

    def test_in_memory_hot_cache_and_bulk_operations(self):
        repo = FloodDataRepository(db_path=":memory:")
        now = datetime.now(timezone.utc)
        
        readings = [
            Reading(
                reading_id=f"bulk-r-{i}",
                area_id=f"station-{i}",
                observed_at=now,
                discharge=1000.0 * i,
                precipitation=5.0 * i,
                source="Bulk Ingestion Test",
                fetched_at=now,
                is_stale=False
            )
            for i in range(1, 11)
        ]
        
        # Test Optimization #4: Bulk Save
        repo.save_readings_bulk(readings)
        
        # Test Optimization #2: In-Memory Hot Cache Lookups
        for i in range(1, 11):
            latest = repo.get_latest_reading(f"station-{i}")
            assert latest is not None
            assert latest.reading_id == f"bulk-r-{i}"
            assert latest.discharge == 1000.0 * i
            
        # Test TTL Caching for Storage Stats
        stats1 = repo.get_storage_stats()
        assert stats1["hot_cache_entries"] >= 10
        stats2 = repo.get_storage_stats()
        assert stats1 == stats2


