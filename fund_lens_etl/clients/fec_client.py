"""FEC API Client with rate limiting and retry logic."""

import time
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import requests

from fund_lens_etl import config

logger = logging.getLogger(__name__)


class FECAPIError(Exception):
    """Base exception for FEC API errors."""

    pass


class FECRateLimitError(FECAPIError):
    """Raised when rate limit is exceeded and cannot be retried."""

    pass


class FECClient:
    """
    Enterprise-grade FEC API client with rate limiting and retries.

    Features:
    - In-memory rate limiting (calls per hour)
    - Exponential backoff retries
    - Automatic pagination
    - Request logging
    """

    def __init__(self):
        self.base_url = config.FEC_API_BASE_URL
        self.api_key = config.FEC_API_KEY
        self.timeout = config.FEC_API_TIMEOUT
        self.max_retries = config.FEC_MAX_RETRIES
        self.rate_limit_calls = config.FEC_RATE_LIMIT_CALLS
        self.rate_limit_period = config.FEC_RATE_LIMIT_PERIOD
        self.results_per_page = config.FEC_RESULTS_PER_PAGE
        self.backoff_factor = config.FEC_RETRY_BACKOFF_FACTOR
        self.retry_statuses = config.FEC_RETRY_STATUSES

        # Rate limiting state
        self._call_times: List[datetime] = []

    def _check_rate_limit(self) -> None:
        """
        Check if we're within rate limits. Sleep if necessary.

        Uses a sliding window to track calls per hour.
        """
        now = datetime.now()
        cutoff = now - timedelta(seconds=self.rate_limit_period)

        # Remove calls outside the rate limit window
        self._call_times = [t for t in self._call_times if t > cutoff]

        if len(self._call_times) >= self.rate_limit_calls:
            # Calculate how long to wait
            oldest_call = self._call_times[0]
            wait_until = oldest_call + timedelta(seconds=self.rate_limit_period)
            wait_seconds = (wait_until - now).total_seconds()

            if wait_seconds > 0:
                logger.warning(
                    f"Rate limit reached ({len(self._call_times)}/{self.rate_limit_calls} calls). "
                    f"Sleeping for {wait_seconds:.2f} seconds..."
                )
                time.sleep(wait_seconds + 1)  # Add 1 second buffer
                # Clear old calls after sleeping
                self._call_times = []

    def _record_call(self) -> None:
        """Record that an API call was made."""
        self._call_times.append(datetime.now())

    def _make_request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make a single API request with retries.

        Args:
            endpoint: API endpoint (e.g., "/schedules/schedule_a/")
            params: Query parameters

        Returns:
            Response JSON as dictionary

        Raises:
            FECAPIError: If request fails after all retries
        """
        if params is None:
            params = {}

        # Add API key to params
        params["api_key"] = self.api_key

        url = f"{self.base_url}{endpoint}"

        for attempt in range(self.max_retries + 1):
            try:
                # Check rate limit before making request
                self._check_rate_limit()

                logger.debug(f"API request (attempt {attempt + 1}): {endpoint}")
                response = requests.get(url, params=params, timeout=self.timeout)

                # Record the call for rate limiting
                self._record_call()

                time.sleep(0.5)

                # Handle rate limit responses
                if response.status_code == 429:
                    if attempt < self.max_retries:
                        wait_time = self.backoff_factor**attempt
                        logger.warning(
                            f"Rate limited by API. Waiting {wait_time}s before retry..."
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        raise FECRateLimitError("Rate limit exceeded after all retries")

                # Handle server errors with retry
                if response.status_code in self.retry_statuses:
                    if attempt < self.max_retries:
                        wait_time = self.backoff_factor**attempt
                        logger.warning(
                            f"Server error {response.status_code}. "
                            f"Waiting {wait_time}s before retry..."
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        response.raise_for_status()

                # Raise for other HTTP errors
                response.raise_for_status()

                return response.json()

            except requests.exceptions.Timeout:
                if attempt < self.max_retries:
                    wait_time = self.backoff_factor**attempt
                    logger.warning(
                        f"Request timeout. Waiting {wait_time}s before retry..."
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    raise FECAPIError(
                        f"Request timeout after {self.max_retries + 1} attempts"
                    )

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries:
                    wait_time = self.backoff_factor**attempt
                    logger.warning(
                        f"Request failed: {e}. Waiting {wait_time}s before retry..."
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    raise FECAPIError(
                        f"Request failed after {self.max_retries + 1} attempts: {e}"
                    )

        raise FECAPIError("Unexpected error in retry loop")

    def get_contributions(
        self,
        contributor_state: Optional[str] = None,
        two_year_transaction_period: Optional[int] = None,
        committee_id: Optional[str] = None,
        max_results: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch individual contributions with automatic pagination.

        Args:
            contributor_state: Two-letter state code (e.g., "MD")
            two_year_transaction_period: Even-numbered year (e.g., 2024 for 2023-2024 cycle)
            committee_id: Specific committee ID to filter
            max_results: Maximum total results to return (None = all)

        Returns:
            List of contribution dictionaries
        """
        params = {
            "per_page": self.results_per_page,
            "sort": "-contribution_receipt_date",  # Negative for descending
            "sort_hide_null": "false",
        }

        if contributor_state:
            params["contributor_state"] = contributor_state
        if two_year_transaction_period:
            params["two_year_transaction_period"] = two_year_transaction_period
        if committee_id:
            params["committee_id"] = committee_id

        all_results = []
        page = 1

        while True:
            params["page"] = page

            logger.info(f"Fetching page {page} of contributions...")
            response = self._make_request("/schedules/schedule_a/", params)

            results = response.get("results", [])
            if not results:
                break

            all_results.extend(results)

            # Check if we've hit max_results
            if max_results and len(all_results) >= max_results:
                all_results = all_results[:max_results]
                break

            # Check if there are more pages
            pagination = response.get("pagination", {})
            if page >= pagination.get("pages", 1):
                break

            page += 1

        logger.info(f"Fetched {len(all_results)} total contributions")
        return all_results
