"""FEC Candidate extractor."""

import logging
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import requests
from prefect import get_run_logger
from prefect.exceptions import MissingContextError

from fund_lens_etl.clients.fec import FECAPIClient
from fund_lens_etl.config import USState, validate_election_cycle
from fund_lens_etl.extractors.base import BaseExtractor


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


# noinspection PyMethodMayBeStatic
class FECCandidateExtractor(BaseExtractor):
    """
    Extract candidate data from FEC API.

    Fetches candidate master data including names, office, district,
    party affiliation, and election details.
    """

    def __init__(self, api_client: FECAPIClient | None = None):
        """
        Initialize the candidate extractor.

        Args:
            api_client: FEC API client (creates default if None)
        """
        self.api_client = api_client or FECAPIClient()

    def get_source_name(self) -> str:
        """Get the name of the data source."""
        return "FEC"

    def extract(
        self,
        candidate_ids: list[str] | None = None,
        state: USState | None = None,
        office: str | None = None,
        cycle: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Extract candidate data from FEC API.

        Args:
            candidate_ids: Specific candidate IDs to fetch (if None, fetches all matching filters)
            state: Filter by state (e.g., USState.MD)
            office: Filter by office type ('H' for House, 'S' for Senate, 'P' for President)
            cycle: Election cycle year (e.g., 2024)
            **kwargs: Additional parameters passed to API client

        Returns:
            DataFrame with candidate records
        """
        logger = get_logger()

        if candidate_ids:
            logger.info(f"Extracting {len(candidate_ids)} specific candidates")
            return self._extract_by_ids(candidate_ids)
        else:
            state_value = state.value if state else None
            logger.info(
                f"Extracting candidates with filters: state={state_value}, "
                f"office={office}, cycle={cycle}"
            )
            return self._extract_filtered(state_value, office, cycle, **kwargs)

    def _extract_by_ids(self, candidate_ids: list[str]) -> pd.DataFrame:
        """Extract specific candidates by ID."""
        logger = get_logger()
        all_candidates = []

        for candidate_id in candidate_ids:
            try:
                response = self.api_client.get(f"/candidate/{candidate_id}/", params={})

                if response.get("results"):
                    candidate = response["results"][0]
                    all_candidates.append(self._transform_candidate(candidate))
                else:
                    logger.warning(f"Candidate {candidate_id} not found")

            except requests.HTTPError as e:
                logger.error(f"Error fetching candidate {candidate_id}: {e}")
                continue

        if not all_candidates:
            return pd.DataFrame()

        return pd.DataFrame(all_candidates)

    def _extract_filtered(
        self,
        state: str | None,
        office: str | None,
        cycle: int | None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Extract candidates using filters with pagination."""
        logger = get_logger()

        # Validate cycle if provided
        if cycle:
            cycle = validate_election_cycle(cycle)

        params = {
            "per_page": kwargs.get("per_page", 100),
            "sort": "name",
        }

        if state:
            params["state"] = state
        if office:
            params["office"] = office
        if cycle:
            params["cycle"] = cycle

        all_candidates = []
        page = 1

        while True:
            params["page"] = page

            try:
                response = self.api_client.get("/candidates/", params=params)

                results = response.get("results", [])
                if not results:
                    break

                for candidate in results:
                    all_candidates.append(self._transform_candidate(candidate))

                # Check if there are more pages
                pagination = response.get("pagination", {})
                if page >= pagination.get("pages", 1):
                    break

                page += 1
                logger.info(f"Fetched page {page - 1}, total candidates: {len(all_candidates)}")

            except requests.HTTPError as e:
                logger.error(f"Error on page {page}: {e}")
                raise

        logger.info(f"Extraction complete: {len(all_candidates)} candidates")

        if not all_candidates:
            return pd.DataFrame()

        return pd.DataFrame(all_candidates)

    def _transform_candidate(self, candidate: dict[str, Any]) -> dict[str, Any]:
        """Transform API response to match bronze schema."""
        return {
            "candidate_id": candidate.get("candidate_id"),
            "name": candidate.get("name"),
            "candidate_first_name": candidate.get("candidate_first_name"),
            "candidate_last_name": candidate.get("candidate_last_name"),
            "candidate_middle_name": candidate.get("candidate_middle_name"),
            "party": candidate.get("party"),
            "party_full": candidate.get("party_full"),
            "office": candidate.get("office"),
            "office_full": candidate.get("office_full"),
            "state": candidate.get("state"),
            "district": candidate.get("district"),
            "district_number": candidate.get("district_number"),
            "incumbent_challenge": candidate.get("incumbent_challenge"),
            "incumbent_challenge_full": candidate.get("incumbent_challenge_full"),
            "candidate_status": candidate.get("candidate_status"),
            "candidate_inactive": candidate.get("candidate_inactive"),
            "election_years": candidate.get("election_years", []),
            "election_districts": candidate.get("election_districts", []),
            "cycles": candidate.get("cycles", []),
            "first_file_date": candidate.get("first_file_date"),
            "last_file_date": candidate.get("last_file_date"),
            "last_f2_date": candidate.get("last_f2_date"),
            "has_raised_funds": candidate.get("has_raised_funds"),
            "federal_funds_flag": candidate.get("federal_funds_flag"),
            "address_city": candidate.get("address_city"),
            "address_state": candidate.get("address_state"),
            "address_street_1": candidate.get("address_street_1"),
            "address_street_2": candidate.get("address_street_2"),
            "address_zip": candidate.get("address_zip"),
            "raw_json": candidate,  # Store complete response
            "extracted_at": datetime.now(UTC),
        }
