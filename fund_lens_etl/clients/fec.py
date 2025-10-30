"""FEC API client for making requests with rate limiting."""

import logging
from typing import Any

import requests
from prefect import get_run_logger
from prefect.exceptions import MissingContextError

from fund_lens_etl.config import get_settings
from fund_lens_etl.utils import FECRateLimiter


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


class FECAPIClient:
    """
    Client for making requests to the FEC API.

    Handles authentication, rate limiting, and request execution.
    Shared by all FEC extractors.
    """

    BASE_URL = "https://api.open.fec.gov/v1"

    def __init__(self):
        """Initialize FEC API client."""
        self.settings = get_settings()
        self.api_key = self.settings.fec_api_key
        self.rate_limiter = FECRateLimiter()

    def get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        """
        Make a GET request to the FEC API with rate limiting.

        Args:
            endpoint: API endpoint path (e.g., '/schedules/schedule_a/')
            params: Query parameters (api_key added automatically)

        Returns:
            JSON response as dictionary

        Raises:
            requests.HTTPError: If request fails
        """
        logger = get_logger()

        # Wait if needed to respect rate limits
        self.rate_limiter.wait_if_needed()

        url = f"{self.BASE_URL}{endpoint}"
        params["api_key"] = self.api_key

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            logger.error(f"API request failed for {endpoint}: {e}")
            raise

    def get_rate_limiter_stats(self) -> dict[str, Any]:
        """Get current rate limiter statistics."""
        return self.rate_limiter.get_stats()
