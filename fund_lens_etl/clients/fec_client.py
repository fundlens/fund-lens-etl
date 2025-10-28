"""FEC API Client with rate limiting and retry logic."""

import logging
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

import requests

from fund_lens_etl import config

logger = logging.getLogger(__name__)


class FECAPIError(Exception):
    """Base exception for FEC API errors."""

    pass


class FECRateLimitError(FECAPIError):
    """Raised when rate limit is exceeded and cannot be retried."""

    pass


# noinspection PyMethodMayBeStatic
class FECClient:
    """
    Enterprise-grade FEC API client with rate limiting and retries.

    Features:
    - In-memory rate limiting (calls per hour)
    - Exponential backoff retries
    - Automatic pagination
    - Request logging
    """

    def __init__(self) -> None:
        self.base_url = config.FEC_API_BASE_URL
        self.api_key = config.FEC_API_KEY
        self.timeout = config.FEC_API_TIMEOUT
        self.max_retries = config.FEC_MAX_RETRIES

        # Hourly rate limit
        self.rate_limit_calls = config.FEC_RATE_LIMIT_CALLS
        self.rate_limit_period = config.FEC_RATE_LIMIT_PERIOD

        # Minute rate limit
        self.rate_limit_calls_per_minute = config.FEC_RATE_LIMIT_CALLS_PER_MINUTE
        self.rate_limit_period_minute = config.FEC_RATE_LIMIT_PERIOD_MINUTE

        self.results_per_page = config.FEC_RESULTS_PER_PAGE
        self.backoff_factor = config.FEC_RETRY_BACKOFF_FACTOR
        self.retry_statuses = config.FEC_RETRY_STATUSES

        self._call_times: list[datetime] = []  # Add type hint
        self._call_times_minute: list[datetime] = []  # Add type hint

    def _check_rate_limit(self) -> None:
        """
        Check both hourly and per-minute rate limits before making a request.
        Sleeps if either limit is exceeded.
        """
        current_time = datetime.now()

        # Check hourly rate limit
        self._check_rate_limit_window(
            current_time=current_time,
            call_times=self._call_times,
            limit_calls=self.rate_limit_calls,
            limit_period=self.rate_limit_period,
            window_name="hourly",
        )

        # Check per-minute rate limit
        self._check_rate_limit_window(
            current_time=current_time,
            call_times=self._call_times_minute,
            limit_calls=self.rate_limit_calls_per_minute,
            limit_period=self.rate_limit_period_minute,
            window_name="per-minute",
        )

        # Record the call in both tracking lists
        self._call_times.append(current_time)
        self._call_times_minute.append(current_time)

    def _check_rate_limit_window(
        self,
        current_time: datetime,
        call_times: list[datetime],
        limit_calls: int,
        limit_period: int,
        window_name: str,
    ) -> None:
        """
        Check rate limit for a specific time window.

        Args:
            current_time: Current timestamp
            call_times: List of recent call timestamps for this window
            limit_calls: Maximum calls allowed in the period
            limit_period: Period in seconds
            window_name: Name of the window (for logging)
        """
        # Remove calls outside the current window
        cutoff_time = current_time - timedelta(seconds=limit_period)
        while call_times and call_times[0] < cutoff_time:
            call_times.pop(0)

        # Check if we've hit the limit
        if len(call_times) >= limit_calls:
            # Calculate sleep time until oldest call expires
            oldest_call = call_times[0]
            sleep_until = oldest_call + timedelta(seconds=limit_period)
            sleep_seconds = (sleep_until - current_time).total_seconds()

            if sleep_seconds > 0:
                logger.warning(
                    f"Rate limit reached for {window_name} window "
                    f"({len(call_times)}/{limit_calls} calls). "
                    f"Sleeping for {sleep_seconds:.2f} seconds..."
                )
                time.sleep(sleep_seconds)

    def _make_request(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
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

                time.sleep(4.0)

                # Handle rate limit responses
                if response.status_code == 429:
                    if attempt < self.max_retries:
                        wait_time = self.backoff_factor**attempt
                        logger.warning(f"Rate limited by API. Waiting {wait_time}s before retry...")
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

                    logger.warning(f"Request timeout. Waiting {wait_time}s before retry...")

                    time.sleep(wait_time)

                    continue

                else:
                    raise FECAPIError(
                        f"Request timeout after {self.max_retries + 1} attempts"
                    ) from None  # Add 'from None' to suppress original exception

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries:
                    wait_time = self.backoff_factor**attempt

                    logger.warning(f"Request failed: {e}. Waiting {wait_time}s before retry...")

                    time.sleep(wait_time)

                    continue

                else:
                    raise FECAPIError(
                        f"Request failed after {self.max_retries + 1} attempts: {e}"
                    ) from e  # Add 'from e' to chain the exception

        raise FECAPIError("Unexpected error in retry loop")

    def get_contributions(
        self,
        contributor_state: str,
        two_year_transaction_period: int,
        min_date: datetime | None = None,
        max_date: datetime | None = None,  # NEW: for backfilling
        max_results: int | None = None,
        batch_callback: Callable[[list[dict[str, Any]]], None] | None = None,
        batch_size: int = 1000,
        sort_order: str = "desc",  # NEW: 'desc' (newest first) or 'asc' (oldest first)
    ) -> list[dict[str, Any]]:
        """
        Get individual contributions by state and election cycle.

        Args:
            contributor_state: Two-letter state code
            two_year_transaction_period: Election cycle (e.g., 2024 for 2023-2024)
            min_date: Only fetch contributions on or after this date
            max_date: Only fetch contributions on or before this date (for backfilling)
            max_results: Maximum number of results to return (None for all)
            batch_callback: Optional callback function called with each batch of records
            batch_size: Number of records to accumulate before calling batch_callback
            sort_order: 'desc' for newest first (incremental), 'asc' for oldest first (backfill)

        Returns:
            List of contribution records (only if batch_callback is None, otherwise empty)
        """
        # Determine sort parameter based on order
        sort_param = (
            "-contribution_receipt_date" if sort_order == "desc" else "contribution_receipt_date"
        )

        params = {
            "contributor_state": contributor_state,
            "two_year_transaction_period": two_year_transaction_period,
            "sort": sort_param,
        }

        if min_date:
            params["min_contribution_receipt_date"] = min_date.strftime("%Y-%m-%d")
            logger.info(
                f"Filtering contributions with min_contribution_receipt_date >= "
                f"{params['min_contribution_receipt_date']}"
            )

        if max_date:
            params["max_contribution_receipt_date"] = max_date.strftime("%Y-%m-%d")
            logger.info(
                f"Filtering contributions with max_contribution_receipt_date <= "
                f"{params['max_contribution_receipt_date']}"
            )

        all_results: list[dict[str, Any]] = []
        batch_buffer: list[dict[str, Any]] = []
        page = 1
        total_processed = 0

        while True:
            params["page"] = page

            logger.info(
                f"Fetching page {page} ({sort_order}): "
                f"{total_processed:,} records processed so far "
                + (f"(target: {max_results:,} max)" if max_results else "")
            )

            # ... rest of the method stays the same

            response = self._make_request("/schedules/schedule_a/", params)

            results = response.get("results", [])
            if not results:
                logger.info("No more results available")
                break

            # Add to appropriate buffer
            if batch_callback:
                batch_buffer.extend(results)
                total_processed += len(results)

                # When buffer reaches batch_size, process it
                if len(batch_buffer) >= batch_size:
                    logger.info(f"Batch ready: {len(batch_buffer)} records, invoking callback")
                    batch_callback(batch_buffer)
                    batch_buffer = []
            else:
                all_results.extend(results)
                total_processed += len(results)

            # Check if we've hit max_results
            if max_results and total_processed >= max_results:
                if batch_callback and batch_buffer:
                    # Process remaining buffer
                    logger.info(f"Final batch: {len(batch_buffer)} records, invoking callback")
                    batch_callback(batch_buffer)
                elif not batch_callback:
                    all_results = all_results[:max_results]
                logger.info(f"Reached max_results limit: {total_processed:,} records")
                break

            # Check if there are more pages
            pagination = response.get("pagination", {})
            total_pages = pagination.get("pages", 1)

            if page >= total_pages:
                logger.info(f"Reached last page ({page} of {total_pages})")
                break

            # Log progress every 50 pages
            if page % 50 == 0:
                logger.info(f"Progress: Page {page}, {total_processed:,} records processed")

            page += 1

        # Process any remaining records in buffer
        if batch_callback and batch_buffer:
            logger.info(f"Final batch: {len(batch_buffer)} records, invoking callback")
            batch_callback(batch_buffer)

        logger.info(
            f"Completed: {len(all_results) if not batch_callback else total_processed} total contributions"
        )
        return all_results if not batch_callback else []
