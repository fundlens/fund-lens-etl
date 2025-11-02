"""Rate limiter for FEC API requests."""

import logging
import time
from collections import deque
from datetime import datetime, timedelta

from prefect import get_run_logger
from prefect.exceptions import MissingContextError

from fund_lens_etl.config import get_settings


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


class FECRateLimiter:
    """
    Rate limiter to enforce FEC API limits: 60 requests/minute, 1000 requests/hour.

    Uses sliding window algorithm to track request timestamps.
    """

    def __init__(self) -> None:
        """Initialize rate limiter with empty request queues."""
        self.settings = get_settings()

        # Deques to track request timestamps
        self.minute_requests: deque[datetime] = deque()
        self.hour_requests: deque[datetime] = deque()

        # Limits from config
        self.max_per_minute = self.settings.max_requests_per_minute
        self.max_per_hour = self.settings.max_requests_per_hour
        self.min_delay = self.settings.api_rate_limit_delay

    def _clean_old_requests(self, now: datetime) -> None:
        """
        Remove request timestamps outside the current sliding windows.

        Args:
            now: Current timestamp
        """
        minute_ago = now - timedelta(minutes=1)
        hour_ago = now - timedelta(hours=1)

        # Remove requests older than 1 minute
        while self.minute_requests and self.minute_requests[0] < minute_ago:
            self.minute_requests.popleft()

        # Remove requests older than 1 hour
        while self.hour_requests and self.hour_requests[0] < hour_ago:
            self.hour_requests.popleft()

    def wait_if_needed(self) -> None:
        """
        Wait if necessary to stay within rate limits.

        Blocks execution until it's safe to make another request.
        """
        logger = get_logger()
        now = datetime.now()

        # Clean up old requests
        self._clean_old_requests(now)

        # Check minute limit
        if len(self.minute_requests) >= self.max_per_minute:
            oldest_in_minute = self.minute_requests[0]
            wait_until = oldest_in_minute + timedelta(minutes=1)
            wait_seconds = (wait_until - now).total_seconds()

            if wait_seconds > 0:
                logger.warning(
                    f"Rate limit reached ({self.max_per_minute}/min). "
                    f"Waiting {wait_seconds:.1f}s..."
                )
                time.sleep(wait_seconds + 0.1)  # Small buffer
                now = datetime.now()
                self._clean_old_requests(now)

        # Check hour limit
        if len(self.hour_requests) >= self.max_per_hour:
            oldest_in_hour = self.hour_requests[0]
            wait_until = oldest_in_hour + timedelta(hours=1)
            wait_seconds = (wait_until - now).total_seconds()

            if wait_seconds > 0:
                logger.warning(
                    f"Hourly rate limit reached ({self.max_per_hour}/hour). "
                    f"Waiting {wait_seconds:.1f}s..."
                )
                time.sleep(wait_seconds + 0.1)  # Small buffer
                now = datetime.now()
                self._clean_old_requests(now)

        # Enforce minimum delay between requests
        if self.minute_requests:
            last_request = self.minute_requests[-1]
            time_since_last = (now - last_request).total_seconds()

            if time_since_last < self.min_delay:
                wait_time = self.min_delay - time_since_last
                time.sleep(wait_time)
                now = datetime.now()

        # Record this request
        self.minute_requests.append(now)
        self.hour_requests.append(now)

    def get_stats(self) -> dict[str, int]:
        """
        Get current rate limiter statistics.

        Returns:
            Dictionary with request counts
        """
        now = datetime.now()
        self._clean_old_requests(now)

        return {
            "requests_last_minute": len(self.minute_requests),
            "requests_last_hour": len(self.hour_requests),
            "remaining_minute": self.max_per_minute - len(self.minute_requests),
            "remaining_hour": self.max_per_hour - len(self.hour_requests),
        }

    def reset(self) -> None:
        """Clear all tracked requests (useful for testing)."""
        self.minute_requests.clear()
        self.hour_requests.clear()
