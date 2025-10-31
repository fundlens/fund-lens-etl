"""FEC Schedule A extractor for individual contributions."""

import logging
from collections.abc import Generator
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests
from prefect import get_run_logger
from prefect.exceptions import MissingContextError

from fund_lens_etl.clients.fec import FECAPIClient
from fund_lens_etl.config import USState, get_settings, validate_election_cycle
from fund_lens_etl.extractors.base import BaseExtractor


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


class FECScheduleAExtractor(BaseExtractor):
    """Extractor for FEC Schedule A (individual contributions) with page-by-page streaming."""

    def __init__(self, api_client: FECAPIClient | None = None):
        """
        Initialize Schedule A extractor.

        Args:
            api_client: FEC API client (creates default if None)
        """
        self.api_client = api_client or FECAPIClient()
        self.settings = get_settings()

    def get_source_name(self) -> str:
        """Get source system name."""
        return "FEC"

    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Extract data - not implemented for page-by-page extractor.

        Use specific generator methods like extract_schedule_a_pages().
        """
        raise NotImplementedError("Use generator methods: extract_schedule_a_pages(), etc.")

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
        logger = get_logger()
        election_cycle = validate_election_cycle(election_cycle)

        logger.info(
            f"Extracting Schedule A for committee {committee_id}, "
            f"cycle {election_cycle}, starting page {starting_page}"
        )

        if start_date:
            logger.info(f"Start date filter: {start_date}")
        if end_date:
            logger.info(f"End date filter: {end_date}")

        page = 1
        per_page = 100
        last_index = None
        last_date = None

        while True:
            params = {
                "committee_id": committee_id,
                "two_year_transaction_period": election_cycle,
                "per_page": per_page,
                "sort": "contribution_receipt_date",  # Ascending - oldest first
            }

            # Add cursor pagination if we have it
            if last_index and last_date:
                params["last_index"] = last_index
                params["last_contribution_receipt_date"] = last_date

            # Add date filters if provided
            if start_date:
                params["min_date"] = start_date.isoformat()
            if end_date:
                params["max_date"] = end_date.isoformat()

            logger.info(f"Fetching page {page}...")

            try:
                data = self.api_client.get("/schedules/schedule_a/", params)
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
                "rate_limiter_stats": self.api_client.get_rate_limiter_stats(),
            }

            if page == 1:
                logger.info(
                    f"Total available: {page_metadata['total_count']:,} records, "
                    f"{page_metadata['total_pages']:,} pages"
                )

            logger.info(
                f"Page {page}/{page_metadata['total_pages']}: {len(results)} records retrieved"
            )

            # Yield this page's data
            yield df, page_metadata

            # Get cursor for next page
            last_indexes = pagination.get("last_indexes", {})
            if not last_indexes:
                logger.info("No last_indexes in pagination - reached end")
                break

            last_index = last_indexes.get("last_index")
            last_date = last_indexes.get("last_contribution_receipt_date")

            if not last_index or not last_date:
                logger.info("Missing cursor values - reached end")
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

    def get_candidate_committees(self, state: USState, election_cycle: int) -> list[dict[str, Any]]:
        """
        Get all House and Senate candidate principal committees for a state.

        Args:
            state: State code
            election_cycle: Election cycle year

        Returns:
            List of dictionaries with committee and candidate info
        """
        logger = get_logger()
        election_cycle = validate_election_cycle(election_cycle)

        logger.info(f"Fetching committees for {state.value}, cycle {election_cycle}")

        all_committees = []
        page = 1
        per_page = 100

        # Get all committees for the state and cycle
        while True:
            params = {
                "state": state.value,
                "cycle": election_cycle,
                "per_page": per_page,
                "page": page,
            }

            try:
                data = self.api_client.get("/committees/", params)
            except requests.HTTPError as e:
                logger.error(f"API request failed: {e}")
                raise

            results = data.get("results", [])

            if not results:
                break

            for committee in results:
                committee_type = committee.get("committee_type")
                designation = committee.get("designation")

                # Filter for candidate committees:
                # - Committee type H (House) or S (Senate)
                # - Designation P (Principal) or A (Authorized)
                if committee_type in ["H", "S"] and designation in ["P", "A"]:
                    candidate_ids = committee.get("candidate_ids", [])
                    all_committees.append(
                        {
                            "committee_id": committee.get("committee_id"),
                            "committee_name": committee.get("name"),
                            "candidate_id": candidate_ids[0] if candidate_ids else None,
                            "candidate_name": None,  # Not available in committee endpoint
                            "office": committee_type,
                            "district": None,  # Not available in committee endpoint
                            "party": committee.get("party"),
                        }
                    )

            logger.info(f"Retrieved {len(results)} committees from page {page}")

            # Check pagination
            pagination = data.get("pagination", {})
            if page >= pagination.get("pages", 0):
                break

            page += 1

        logger.info(f"Found {len(all_committees)} {state.value} candidate committees")
        return all_committees
