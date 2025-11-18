import logging
import time
from typing import Any

import requests
from prefect import get_run_logger
from prefect.exceptions import MissingContextError

from fund_lens_etl.config import get_settings
from fund_lens_etl.utils.rate_limiter import get_rate_limiter


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


class FECAPIClient:
    """
    Client for making requests to the FEC API.

    Handles authentication, rate limiting, retries, and request execution.
    Shared by all FEC extractors.

    Note: All instances share a singleton rate limiter to enforce global
    rate limits across multiple subflows/tasks.
    """

    BASE_URL = "https://api.open.fec.gov/v1"

    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0):
        """
        Initialize FEC API client.

        Args:
            max_retries: Maximum number of retry attempts for failed requests
            retry_delay: Initial delay between retries (seconds), doubles each retry
        """
        self.settings = get_settings()
        self.api_key = self.settings.fec_api_key
        self.rate_limiter = get_rate_limiter()  # Use singleton rate limiter
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        """
        Make a GET request to the FEC API with rate limiting and retries.

        Implements exponential backoff for transient failures (timeouts, 5xx errors).
        Does NOT retry on 4xx errors (client errors like invalid parameters).

        Args:
            endpoint: API endpoint path (e.g., '/schedules/schedule_a/')
            params: Query parameters (api_key added automatically)

        Returns:
            JSON response as dictionary

        Raises:
            requests.HTTPError: If request fails after all retries
            requests.Timeout: If request times out after all retries
        """
        logger = get_logger()

        url = f"{self.BASE_URL}{endpoint}"
        params["api_key"] = self.api_key

        last_exception: Exception | None = None

        for attempt in range(self.max_retries + 1):  # +1 for initial attempt
            try:
                # Wait if needed to respect rate limits
                self.rate_limiter.wait_if_needed()

                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()

            except requests.HTTPError as e:
                last_exception = e

                # Special handling for rate limit errors (429)

                if e.response.status_code == 429:
                    if attempt < self.max_retries:
                        # Longer backoff for rate limits (start at 60s, not 1s)

                        delay = 60 * (2**attempt)  # 60s, 120s, 240s

                        logger.warning(
                            f"Rate limit exceeded (429) on attempt {attempt + 1}/{self.max_retries + 1} "
                            f"for {endpoint}. Waiting {delay}s before retry..."
                        )

                        time.sleep(delay)

                    else:
                        logger.error(
                            f"Rate limit exceeded after {self.max_retries + 1} attempts for {endpoint}"
                        )

                        raise

                    continue  # Skip to next retry attempt

                # Don't retry on other 4xx client errors (bad request, not found, etc.)

                if 400 <= e.response.status_code < 500:
                    logger.error(f"Client error ({e.response.status_code}) for {endpoint}: {e}")

                    raise

                # Retry on 5xx server errors

                if attempt < self.max_retries:
                    delay = self.retry_delay * (2**attempt)

                    logger.warning(
                        f"Server error ({e.response.status_code}) on attempt {attempt + 1}/{self.max_retries + 1} "
                        f"for {endpoint}. Retrying in {delay}s..."
                    )

                    time.sleep(delay)

                else:
                    logger.error(
                        f"Server error after {self.max_retries + 1} attempts for {endpoint}: {e}"
                    )

                    raise

            except requests.RequestException as e:
                # Catch-all for other network errors
                last_exception = e
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2**attempt)
                    logger.warning(
                        f"Request failed on attempt {attempt + 1}/{self.max_retries + 1} "
                        f"for {endpoint}: {e}. Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"Request failed after {self.max_retries + 1} attempts for {endpoint}: {e}"
                    )
                    raise

        # This should be unreachable, but satisfies mypy
        raise RuntimeError(f"Request failed after retries for {endpoint}: {last_exception}")

    def get_rate_limiter_stats(self) -> dict[str, Any]:
        """Get current rate limiter statistics."""
        return self.rate_limiter.get_stats()
