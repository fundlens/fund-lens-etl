"""FEC Committee extractor."""

import logging
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import requests
from fund_lens_models.enums import USState
from prefect import get_run_logger
from prefect.exceptions import MissingContextError

from fund_lens_etl.clients.fec import FECAPIClient
from fund_lens_etl.config import validate_election_cycle
from fund_lens_etl.extractors.base import BaseExtractor


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


# noinspection PyMethodMayBeStatic
class FECCommitteeExtractor(BaseExtractor):
    """
    Extract committee data from FEC API.

    Fetches committee master data including names, addresses, treasurers,
    party affiliations, and organizational details.
    """

    def __init__(self, api_client: FECAPIClient | None = None):
        """
        Initialize the committee extractor.

        Args:
            api_client: FEC API client (creates default if None)
        """
        self.api_client = api_client or FECAPIClient()

    def get_source_name(self) -> str:
        """Get the name of the data source."""
        return "FEC"

    def extract(
        self,
        committee_ids: list[str] | None = None,
        state: USState | None = None,
        committee_type: str | None = None,
        cycle: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Extract committee data from FEC API.

        Args:
            committee_ids: Specific committee IDs to fetch (if None, fetches all matching filters)
            state: Filter by state (e.g., USState.MD)
            committee_type: Filter by type (e.g., 'S' for Senate, 'H' for House)
            cycle: Election cycle year (e.g., 2024)
            **kwargs: Additional parameters passed to API client

        Returns:
            DataFrame with committee records
        """
        logger = get_logger()

        if committee_ids:
            logger.info(f"Extracting {len(committee_ids)} specific committees")
            return self._extract_by_ids(committee_ids)
        else:
            state_value = state.value if state else None
            logger.info(
                f"Extracting committees with filters: state={state_value}, "
                f"type={committee_type}, cycle={cycle}"
            )
            return self._extract_filtered(state_value, committee_type, cycle, **kwargs)

    def _extract_by_ids(self, committee_ids: list[str]) -> pd.DataFrame:
        """Extract specific committees by ID."""
        logger = get_logger()
        all_committees = []

        for committee_id in committee_ids:
            try:
                response = self.api_client.get(f"/committee/{committee_id}/", params={})

                if response.get("results"):
                    committee = response["results"][0]
                    all_committees.append(self._transform_committee(committee))
                else:
                    logger.warning(f"Committee {committee_id} not found")

            except requests.HTTPError as e:
                logger.error(f"Error fetching committee {committee_id}: {e}")
                continue

        if not all_committees:
            return pd.DataFrame()

        return pd.DataFrame(all_committees)

    def _extract_filtered(
        self,
        state: str | None,
        committee_type: str | None,
        cycle: int | None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Extract committees using filters with pagination."""
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
        if committee_type:
            params["committee_type"] = committee_type
        if cycle:
            params["cycle"] = cycle

        all_committees = []
        page = 1

        while True:
            params["page"] = page

            try:
                response = self.api_client.get("/committees/", params=params)

                results = response.get("results", [])
                if not results:
                    break

                for committee in results:
                    all_committees.append(self._transform_committee(committee))

                # Check if there are more pages
                pagination = response.get("pagination", {})
                if page >= pagination.get("pages", 1):
                    break

                page += 1
                logger.info(f"Fetched page {page - 1}, total committees: {len(all_committees)}")

            except requests.HTTPError as e:
                logger.error(f"Error on page {page}: {e}")
                raise

        logger.info(f"Extraction complete: {len(all_committees)} committees")

        if not all_committees:
            return pd.DataFrame()

        return pd.DataFrame(all_committees)

    def _transform_committee(self, committee: dict[str, Any]) -> dict[str, Any]:
        """Transform API response to match bronze schema."""
        return {
            "committee_id": committee.get("committee_id"),
            "name": committee.get("name"),
            "committee_type": committee.get("committee_type"),
            "committee_type_full": committee.get("committee_type_full"),
            "designation": committee.get("designation"),
            "designation_full": committee.get("designation_full"),
            "party": committee.get("party"),
            "party_full": committee.get("party_full"),
            "state": committee.get("state"),
            "city": committee.get("city"),
            "street_1": committee.get("street_1"),
            "street_2": committee.get("street_2"),
            "zip": committee.get("zip"),
            "treasurer_name": committee.get("treasurer_name"),
            "organization_type": committee.get("organization_type"),
            "organization_type_full": committee.get("organization_type_full"),
            "filing_frequency": committee.get("filing_frequency"),
            "first_file_date": committee.get("first_file_date"),
            "last_file_date": committee.get("last_file_date"),
            "candidate_ids": committee.get("candidate_ids", []),
            "cycles": committee.get("cycles", []),
            "raw_json": committee,  # Store complete response
            "extracted_at": datetime.now(UTC),
        }
