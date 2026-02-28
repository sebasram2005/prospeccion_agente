"""
Humanized rate limiters for scraping and Gemini API calls.
"""

import asyncio
import logging
import random
import time

import structlog

logger = structlog.get_logger(__name__)


class HumanizedRateLimiter:
    """Rate limiter that simulates human browsing patterns."""

    def __init__(
        self,
        min_delay: float = 5.0,
        max_delay: float = 10.0,
        pause_every: int = 30,
        pause_duration: float = 90.0,
    ):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.pause_every = pause_every
        self.pause_duration = pause_duration
        self.request_count = 0
        self.failure_count = 0
        self.MAX_FAILURES = 5

    async def wait(self) -> None:
        self.request_count += 1
        if self.request_count % self.pause_every == 0:
            logger.info(
                "extended_pause",
                duration=self.pause_duration,
                request_count=self.request_count,
            )
            await asyncio.sleep(self.pause_duration)
        else:
            delay = random.uniform(self.min_delay, self.max_delay)
            await asyncio.sleep(delay)

    async def on_rate_limit(self) -> None:
        self.failure_count += 1
        if self.failure_count >= self.MAX_FAILURES:
            logger.warning(
                "circuit_breaker_activated",
                failure_count=self.failure_count,
                pause_minutes=10,
            )
            await asyncio.sleep(600)
            self.failure_count = 0
        else:
            wait = 30 * (2 ** self.failure_count)
            logger.warning(
                "rate_limit_backoff",
                failure_count=self.failure_count,
                wait_seconds=wait,
            )
            await asyncio.sleep(wait)

    def reset(self) -> None:
        self.request_count = 0
        self.failure_count = 0


class GeminiRateLimiter:
    """Semaphore-based limiter to stay under Gemini free tier (15 RPM).

    Configured to allow max 12 req/min to keep a safety margin.
    """

    def __init__(self, max_per_minute: int = 12):
        self.max_per_minute = max_per_minute
        self._lock = asyncio.Lock()
        self._timestamps: list[float] = []

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            # Purge timestamps older than 60s
            self._timestamps = [t for t in self._timestamps if now - t < 60.0]

            if len(self._timestamps) >= self.max_per_minute:
                oldest = self._timestamps[0]
                sleep_time = 60.0 - (now - oldest) + 0.1
                if sleep_time > 0:
                    logger.info(
                        "gemini_rate_limit_wait",
                        sleep_seconds=round(sleep_time, 1),
                        requests_in_window=len(self._timestamps),
                    )
                    await asyncio.sleep(sleep_time)
                    now = time.monotonic()
                    self._timestamps = [
                        t for t in self._timestamps if now - t < 60.0
                    ]

            self._timestamps.append(time.monotonic())
