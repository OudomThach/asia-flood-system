"""
Rate Limiting Middleware for Cambodia Flood Alert Platform.
Author: Oudom Thach

Implements in-memory sliding-window token bucket rate limiting per client IP.
Provides protection against denial-of-service, brute force, and upstream API quota exhaustion.
"""

import time
import logging
from collections import defaultdict, deque
from typing import Dict, Tuple, Optional
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)


class SlidingWindowRateLimiter:
    """
    High-performance in-memory sliding window rate limiter tracking request timestamps per client IP.
    Uses collections.deque for O(1) amortized timestamp push and pop operations.
    """

    def __init__(self, default_limit: int = 120, window_seconds: int = 60):
        self.default_limit = default_limit
        self.window_seconds = window_seconds
        # client_ip -> deque of epoch timestamps
        self._requests: Dict[str, deque] = defaultdict(deque)
        # Custom route rate limits: path_prefix -> (limit, window_seconds)
        self._route_limits: Dict[str, Tuple[int, int]] = {}

    def set_route_limit(self, path_prefix: str, limit: int, window_seconds: int = 60):
        """Sets a stricter rate limit for specific endpoints (e.g. sync-all, dispatch-alert)."""
        self._route_limits[path_prefix] = (limit, window_seconds)

    def is_allowed(self, client_ip: str, path: str) -> Tuple[bool, int, int, int]:
        """
        Checks if a request from client_ip to path is allowed with O(1) amortized queue operations.
        Returns: (allowed: bool, limit: int, remaining: int, reset_seconds: int)
        """
        now = time.time()
        
        # Determine applicable limit and window for this route
        limit = self.default_limit
        window = self.window_seconds
        for prefix, (r_limit, r_window) in self._route_limits.items():
            if path.startswith(prefix):
                limit = r_limit
                window = r_window
                break

        key = f"{client_ip}:{path.split('?')[0]}"
        queue = self._requests[key]

        # Purge entries older than current window using O(1) left pops
        cutoff = now - window
        while queue and queue[0] <= cutoff:
            queue.popleft()

        remaining = max(0, limit - len(queue))
        oldest = queue[0] if queue else now
        reset_seconds = max(1, int(window - (now - oldest)))

        if len(queue) < limit:
            queue.append(now)
            return True, limit, remaining - 1, reset_seconds
        else:
            return False, limit, 0, reset_seconds


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    FastAPI / Starlette middleware enforcing sliding window rate limits.
    Injects standard RFC rate limit headers (X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset).
    """

    def __init__(self, app, limiter: Optional[SlidingWindowRateLimiter] = None):
        super().__init__(app)
        self.limiter = limiter or SlidingWindowRateLimiter(default_limit=120, window_seconds=60)

    async def dispatch(self, request: Request, call_next) -> Response:
        # Bypass rate limiting for static assets or health checks if needed
        path = request.url.path
        if path in ("/favicon.ico", "/docs", "/openapi.json", "/redoc"):
            return await call_next(request)

        # Extract client IP (handling X-Forwarded-For if behind a reverse proxy)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            client_ip = forwarded_for.split(",")[0].strip()
        else:
            client_ip = request.client.host if request.client else "127.0.0.1"

        allowed, limit, remaining, reset_sec = self.limiter.is_allowed(client_ip, path)

        if not allowed:
            logger.warning(f"⚠️ Rate limit exceeded for IP {client_ip} on path {path} (limit={limit}/{self.limiter.window_seconds}s)")
            response = JSONResponse(
                status_code=429,
                content={
                    "error": "Too Many Requests",
                    "message": f"Rate limit of {limit} requests per minute exceeded. Please retry after {reset_sec} seconds.",
                    "retry_after_seconds": reset_sec
                },
                headers={
                    "Retry-After": str(reset_sec),
                    "X-RateLimit-Limit": str(limit),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(reset_sec)
                }
            )
            return response

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(limit)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(reset_sec)
        return response
