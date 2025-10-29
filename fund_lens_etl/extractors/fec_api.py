"""FEC API extractor for campaign finance data."""

from collections.abc import Generator
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests
from prefect import get_run_logger

from fund_lens_etl.config import USState, get_settings, validate_election_cycle
from fund_lens_etl.extractors.base import BaseExtractor
from fund_lens_etl.utils import FECRateLimiter


class FECAPIExtractor(BaseExtractor):
    """Extractor for FEC API data with page-by-page streaming."""

    BASE_URL = "https://api.open.fec.gov/v1"

    def __init__(self):
        """Initialize FEC API extractor."""
        self.settings = get_settings()
        self.api_key = self.settings.fec_api_key
        self.rate_limiter = FECRateLimiter()

    def get_source_name(self) -> str:
        """Get source system name."""
        return "FEC"

    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Extract data - not implemented for page-by-page extractor.

        Use specific generator methods like extract_schedule_a_pages().
        """
        raise NotImplementedError("Use generator methods: extract_schedule_a_pages(), etc.")

    def _make_request(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        """
        Make a request to the FEC API with rate limiting.

        Args:
            endpoint: API endpoint path (e.g., '/schedules/schedule_a/')
            params: Query parameters

        Returns:
            JSON response as dictionary

        Raises:
            requests.HTTPError: If request fails
        """
        # Wait if needed to respect rate limits
        self.rate_limiter.wait_if_needed()

        url = f"{self.BASE_URL}{endpoint}"
        params["api_key"] = self.api_key

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        return response.json()

    def extract_schedule_a_pages(
        self,
        committee_id: str,
        election_cycle: int,
        start_date: date | None = None,
        end_date: date | None = None,
        starting_page: int = 1,
    ) -> Generator[tuple[pd.DataFrame, dict[str, Any]], None, None]:
        """
        Extract Schedule A (contributions) data page by page.

        Yields one page at a time for incremental processing and loading.

        Args:
            committee_id: FEC committee ID
            election_cycle: Two-year transaction period (validated)
            start_date: Start date for contributions (inclusive)
            end_date: End date for contributions (inclusive)
            starting_page: Page number to start from (for resuming)

        Yields:
            Tuple of (DataFrame with page data, pagination metadata)
        """
        logger = get_run_logger()
        election_cycle = validate_election_cycle(election_cycle)

        logger.info(
            f"Extracting Schedule A for committee {committee_id}, "
            f"cycle {election_cycle}, starting page {starting_page}"
        )

        if start_date:
            logger.info(f"Start date filter: {start_date}")
        if end_date:
            logger.info(f"End date filter: {end_date}")

        page = starting_page
        per_page = 100

        while True:
            params = {
                "committee_id": committee_id,
                "two_year_transaction_period": election_cycle,
                "per_page": per_page,
                "page": page,
                "sort": "contribution_receipt_date",  # Ascending - oldest first
            }

            # Add date filters if provided
            if start_date:
                params["min_date"] = start_date.isoformat()
            if end_date:
                params["max_date"] = end_date.isoformat()

            logger.info(f"Fetching page {page}...")

            try:
                data = self._make_request("/schedules/schedule_a/", params)
            except requests.HTTPError as e:
                logger.error(f"API request failed on page {page}: {e}")
                raise

            results = data.get("results", [])
            pagination = data.get("pagination", {})

            if not results:
                logger.info(f"No more results at page {page}")
                break

            # Convert to DataFrame
            df = pd.DataFrame(results)

            # Metadata for this page
            page_metadata = {
                "page": page,
                "total_pages": pagination.get("pages", 0),
                "total_count": pagination.get("count", 0),
                "records_in_page": len(results),
                "rate_limiter_stats": self.rate_limiter.get_stats(),
            }

            if page == 1:
                logger.info(
                    f"Total available: {page_metadata['total_count']:,} records, "
                    f"{page_metadata['total_pages']:,} pages"
                )

            logger.info(
                f"Page {page}/{page_metadata['total_pages']}: " f"{len(results)} records retrieved"
            )

            # Yield this page's data
            yield df, page_metadata

            # Check if we've reached the last page
            if page >= pagination.get("pages", 0):
                logger.info(f"Reached last page ({page})")
                break

            page += 1

        logger.info(f"Extraction complete for committee {committee_id}")

    def calculate_lookback_date(
        self, last_contribution_date: date, lookback_days: int | None = None
    ) -> date:
        """
        Calculate the start date for incremental extraction.

        Args:
            last_contribution_date: Date of last processed contribution
            lookback_days: Days to look back (defaults to config)

        Returns:
            Start date for extraction (last_date - lookback_days)
        """
        lookback_days = lookback_days or self.settings.lookback_days
        return last_contribution_date - timedelta(days=lookback_days)

    def extract_candidates(
        self,
        state: USState,
        office: str | None = None,
        election_cycle: int | None = None,
    ) -> pd.DataFrame:
        """
        Extract candidate data (single call, not paginated for MVP).

        Args:
            state: State code
            office: Office type ('H', 'S', 'P')
            election_cycle: Election cycle year

        Returns:
            DataFrame with candidate records
        """
        logger = get_run_logger()
        election_cycle = election_cycle or 2026
        election_cycle = validate_election_cycle(election_cycle)

        logger.info(
            f"Extracting candidates for {state.value}, " f"office {office}, cycle {election_cycle}"
        )

        all_records = []
        page = 1
        per_page = 100

        while True:
            params = {
                "state": state.value,
                "cycle": election_cycle,
                "per_page": per_page,
                "page": page,
            }

            if office:
                params["office"] = office

            try:
                data = self._make_request("/candidates/", params)
            except requests.HTTPError as e:
                logger.error(f"API request failed: {e}")
                raise

            results = data.get("results", [])

            if not results:
                break

            all_records.extend(results)
            logger.info(f"Retrieved {len(results)} candidates from page {page}")

            # Check pagination
            pagination = data.get("pagination", {})
            if page >= pagination.get("pages", 0):
                break

            page += 1

        df = pd.DataFrame(all_records)
        logger.info(f"Extraction complete: {len(df)} candidates")

        return df

    def extract_committees(
        self,
        state: USState,
        election_cycle: int | None = None,
    ) -> pd.DataFrame:
        """
        Extract committee data (single call, not paginated for MVP).

        Args:
            state: State code
            election_cycle: Election cycle year

        Returns:
            DataFrame with committee records
        """
        logger = get_run_logger()
        election_cycle = election_cycle or 2026
        election_cycle = validate_election_cycle(election_cycle)

        logger.info(f"Extracting committees for {state.value}, cycle {election_cycle}")

        all_records = []
        page = 1
        per_page = 100

        while True:
            params = {
                "state": state.value,
                "cycle": election_cycle,
                "per_page": per_page,
                "page": page,
            }

            try:
                data = self._make_request("/committees/", params)
            except requests.HTTPError as e:
                logger.error(f"API request failed: {e}")
                raise

            results = data.get("results", [])

            if not results:
                break

            all_records.extend(results)
            logger.info(f"Retrieved {len(results)} committees from page {page}")

            # Check pagination
            pagination = data.get("pagination", {})
            if page >= pagination.get("pages", 0):
                break

            page += 1

        df = pd.DataFrame(all_records)
        logger.info(f"Extraction complete: {len(df)} committees")

        return df

    def get_candidate_committees(self, state: USState, election_cycle: int) -> list[dict[str, Any]]:
        """
        Get all House and Senate candidate principal committees for a state.

        Args:
            state: State code
            election_cycle: Election cycle year

        Returns:
            List of dictionaries with committee and candidate info
        """
        logger = get_run_logger()
        election_cycle = validate_election_cycle(election_cycle)

        committees = []

        # Get House and Senate candidates
        for office in ["H", "S"]:
            candidates_df = self.extract_candidates(
                state=state, office=office, election_cycle=election_cycle
            )

            for _, candidate in candidates_df.iterrows():
                principal_committees = candidate.get("principal_committees", [])

                if principal_committees:
                    for committee in principal_committees:
                        committees.append(
                            {
                                "committee_id": committee.get("committee_id"),
                                "committee_name": committee.get("name"),
                                "candidate_id": candidate.get("candidate_id"),
                                "candidate_name": candidate.get("name"),
                                "office": candidate.get("office"),
                                "district": candidate.get("district"),
                                "party": candidate.get("party"),
                            }
                        )

        logger.info(f"Found {len(committees)} {state.value} candidate committees")
        return committees
