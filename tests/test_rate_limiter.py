"""
Unit tests for Production Sliding-Window Rate Limiter.
Lead & Author: Oudom Thach
"""

import time
import pytest
from asia_flood_core.rate_limiter import SlidingWindowRateLimiter


class TestRateLimiter:
    def test_default_rate_limit_allows_under_quota(self):
        limiter = SlidingWindowRateLimiter(default_limit=5, window_seconds=1)
        ip = "192.168.1.100"
        
        for i in range(5):
            allowed, limit, remaining, reset_sec = limiter.is_allowed(client_ip=ip, path="/api/status")
            assert allowed is True
            assert remaining == 4 - i
            assert limit == 5

    def test_rate_limit_blocks_when_quota_exceeded(self):
        limiter = SlidingWindowRateLimiter(default_limit=3, window_seconds=1)
        ip = "192.168.1.101"
        
        for _ in range(3):
            allowed, _, _, _ = limiter.is_allowed(client_ip=ip, path="/api/status")
            assert allowed is True
            
        allowed, limit, remaining, reset_sec = limiter.is_allowed(client_ip=ip, path="/api/status")
        assert allowed is False
        assert remaining == 0
        assert reset_sec > 0

    def test_quota_resets_after_window(self):
        limiter = SlidingWindowRateLimiter(default_limit=2, window_seconds=0.2)
        ip = "192.168.1.102"
        
        assert limiter.is_allowed(client_ip=ip, path="/api/status")[0] is True
        assert limiter.is_allowed(client_ip=ip, path="/api/status")[0] is True
        assert limiter.is_allowed(client_ip=ip, path="/api/status")[0] is False
        
        time.sleep(0.25)
        
        assert limiter.is_allowed(client_ip=ip, path="/api/status")[0] is True

    def test_custom_route_limits(self):
        limiter = SlidingWindowRateLimiter(default_limit=10, window_seconds=1)
        limiter.set_route_limit("/api/simulate", limit=2, window_seconds=1)
        ip = "192.168.1.103"
        
        assert limiter.is_allowed(client_ip=ip, path="/api/simulate")[0] is True
        assert limiter.is_allowed(client_ip=ip, path="/api/simulate")[0] is True
        assert limiter.is_allowed(client_ip=ip, path="/api/simulate")[0] is False
        
        # Other paths still have default limit
        assert limiter.is_allowed(client_ip=ip, path="/api/status")[0] is True


class TestUserAlertRateLimiter:
    def test_user_alert_quota_and_cooldown(self):
        from asia_flood_core.storage import FloodDataRepository
        repo = FloodDataRepository(db_path=":memory:")
        chat_id = "user-12345"
        area = "kratie-central"
        
        # 1. First alert allowed
        allowed, reason = repo.can_dispatch_alert_to_user(chat_id, area, alert_level="CAUTION", max_per_hour=3, cooldown_minutes=15)
        assert allowed is True
        
        # Log dispatch
        alt_id = repo.log_user_alert_dispatch(chat_id, "TELEGRAM", area, "CAUTION", status="DELIVERED")
        assert alt_id.startswith("alt-")
        
        # 2. Immediate second CAUTION alert blocked by 15-minute cooldown
        allowed, reason = repo.can_dispatch_alert_to_user(chat_id, area, alert_level="CAUTION", max_per_hour=3, cooldown_minutes=15)
        assert allowed is False
        assert "cooldown" in reason.lower()
        
        # 3. Escalation to DANGER bypasses cooldown for life safety!
        allowed, reason = repo.can_dispatch_alert_to_user(chat_id, area, alert_level="DANGER", max_per_hour=3, cooldown_minutes=15)
        assert allowed is True
        assert "life-safety" in reason.lower()
        
        # Log danger alert
        repo.log_user_alert_dispatch(chat_id, "TELEGRAM", area, "DANGER", status="DELIVERED")
        
        # Check history
        history = repo.get_user_alert_history(recipient_id=chat_id)
        assert len(history) == 2
        assert history[0]["alert_level"] == "DANGER"
        assert "ICT" in history[0]["dispatched_at_ict"]

