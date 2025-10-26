"""FEC extraction service - orchestrates client and repository operations."""

import logging
import hashlib
import json
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from fund_lens_etl.clients.fec_client import FECClient
from fund_lens_etl.repos.raw_filing_repo import RawFilingRepo
from fund_lens_etl.repos.fec_staging_repo import FECContributionStagingRepo
from fund_lens_etl.models.raw_filing import RawFiling
from fund_lens_etl.models.fec_contribution_staging import FECContributionStaging

logger = logging.getLogger(__name__)


# noinspection PyMethodMayBeStatic
class FECExtractionService:
    """
    Service for extracting FEC contribution data.

    Orchestrates FEC API client and database repositories to:
    1. Fetch contributions from FEC API
    2. Calculate file hashes for deduplication
    3. Store raw data in raw_filings
    4. Store parsed contributions in fec_contributions_staging

    Manages transactions and business logic.
    """

    def __init__(
        self,
        fec_client: Optional[FECClient] = None,
        raw_filing_repo: Optional[RawFilingRepo] = None,
        fec_staging_repo: Optional[FECContributionStagingRepo] = None,
    ):
        """
        Initialize the extraction service.

        Args:
            fec_client: FEC API client (creates default if None)
            raw_filing_repo: Raw filing repository (creates default if None)
            fec_staging_repo: Staging repository (creates default if None)
        """
        self.fec_client = fec_client or FECClient()
        self.raw_filing_repo = raw_filing_repo or RawFilingRepo()
        self.fec_staging_repo = fec_staging_repo or FECContributionStagingRepo()

    def extract_and_store_contributions(
        self,
        session: Session,
        contributor_state: str,
        two_year_transaction_period: int,
        source: str = "fec_api",
        max_results: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Extract contributions from FEC API and store in database.

        Args:
            session: SQLAlchemy session for transaction management
            contributor_state: Two-letter state code (e.g., "MD")
            two_year_transaction_period: Even-numbered year (e.g., 2024 for 2023-2024 cycle)
            source: Source identifier for raw_filings table
            max_results: Maximum number of contributions to fetch (None = all)

        Returns:
            Dictionary with extraction statistics

        Raises:
            Exception: If extraction or storage fails
        """
        logger.info(
            f"Starting extraction: state={contributor_state}, "
            f"cycle={two_year_transaction_period}, max_results={max_results}"
        )

        try:
            # Step 1: Fetch contributions from FEC API
            contributions = self.fec_client.get_contributions(
                contributor_state=contributor_state,
                two_year_transaction_period=two_year_transaction_period,
                max_results=max_results,  # Pass the limit through
            )

            # Step 2: Calculate file hash for deduplication
            file_hash = self._calculate_file_hash(contributions)
            logger.debug(f"Calculated file hash: {file_hash}")

            # Step 3: Check if we've already processed this data
            existing_filing = self.raw_filing_repo.get_by_file_hash(session, file_hash)
            if existing_filing:
                logger.info(
                    f"Data already processed (raw_filing_id={existing_filing.id}). Skipping."
                )
                return {
                    "contributions_fetched": len(contributions),
                    "raw_filing_id": existing_filing.id,
                    "contributions_stored": 0,
                    "file_hash": file_hash,
                    "skipped": True,
                }

            # Step 3b: Record-level deduplication (filter out existing sub_ids)
            all_sub_ids = [
                c["sub_id"] for c in contributions if c.get("sub_id") is not None
            ]
            existing_sub_ids = self.fec_staging_repo.get_existing_sub_ids(
                session, all_sub_ids
            )

            logger.info(f"DEBUG: Total contributions fetched: {len(contributions)}")
            logger.info(f"DEBUG: Sub_ids to check: {len(all_sub_ids)}")
            logger.info(f"DEBUG: Existing sub_ids found: {len(existing_sub_ids)}")
            logger.info(f"DEBUG: Type of existing_sub_ids: {type(existing_sub_ids)}")
            logger.info(
                f"DEBUG: Sample of existing_sub_ids: {list(existing_sub_ids)[:5] if existing_sub_ids else 'empty'}"
            )
            logger.info(f"DEBUG: Sample of all_sub_ids: {all_sub_ids[:5]}")

            # Add this check:
            if existing_sub_ids:
                overlap = set(all_sub_ids) & existing_sub_ids
                logger.info(
                    f"DEBUG: Overlap between all_sub_ids and existing_sub_ids: {len(overlap)}"
                )
                logger.info(
                    f"DEBUG: Sample overlap: {list(overlap)[:5] if overlap else 'NONE - THIS IS THE BUG!'}"
                )

            new_contributions = [
                c
                for c in contributions
                if c.get("sub_id") is not None and c["sub_id"] not in existing_sub_ids
            ]

            logger.info(
                f"DEBUG: Are all fetched sub_ids in existing? {set(all_sub_ids).issubset(existing_sub_ids)}"
            )
            logger.info(
                f"DEBUG: Total unique sub_ids in DB check: {len(set(all_sub_ids))}"
            )
            logger.info(
                f"DEBUG: New contributions after filter: {len(new_contributions)}"
            )

            duplicate_count = len(contributions) - len(new_contributions)
            if duplicate_count > 0:
                logger.info(
                    f"Found {duplicate_count} duplicate contributions (by sub_id). "
                    f"Storing {len(new_contributions)} new contributions."
                )

            # Update the contributions list to only process new ones
            original_count = len(contributions)
            contributions = new_contributions

            # Step 4: Store raw filing record
            raw_filing = self._create_raw_filing(
                contributions=contributions,
                source=source,
                file_hash=file_hash,
                file_metadata={
                    "contributor_state": contributor_state,
                    "two_year_transaction_period": two_year_transaction_period,
                    "record_count": len(contributions),
                },
            )
            raw_filing = self.raw_filing_repo.insert(session, raw_filing)
            logger.info(f"Stored raw filing with id={raw_filing.id}")

            session.flush()

            assert raw_filing.id is not None
            filing_id: int = int(raw_filing.id)

            # Step 5: Store individual contributions in staging
            stored_count = self._store_contributions_staging(
                session=session,
                raw_filing_id=filing_id,
                contributions=contributions,
            )

            # Step 6: Commit transaction
            session.commit()
            logger.info(
                f"Successfully extracted and stored {stored_count} contributions "
                f"(raw_filing_id={raw_filing.id})"
            )

            return {
                "contributions_fetched": original_count,  # Changed from len(contributions)
                "raw_filing_id": raw_filing.id,
                "contributions_stored": stored_count,
                "file_hash": file_hash,
            }

        except Exception as e:
            session.rollback()
            logger.error(f"Extraction failed: {e}", exc_info=True)
            raise

    def _calculate_file_hash(self, contributions: List[Dict[str, Any]]) -> str:
        """
        Calculate SHA-256 hash of contributions data for deduplication.

        Args:
            contributions: List of contribution dictionaries from FEC API

        Returns:
            SHA-256 hash as hexadecimal string
        """
        # Sort contributions by sub_id for consistent hashing
        sorted_contributions = sorted(contributions, key=lambda x: x.get("sub_id", ""))

        # Serialize to JSON with sorted keys for consistency
        content_json = json.dumps(sorted_contributions, sort_keys=True, default=str)

        # Calculate SHA-256 hash
        hash_obj = hashlib.sha256(content_json.encode())
        return hash_obj.hexdigest()

    def _create_raw_filing(
        self,
        contributions: List[Dict[str, Any]],
        source: str,
        file_hash: str,
        file_metadata: Dict[str, Any],
    ) -> RawFiling:
        """
        Create a RawFiling model instance from contributions data.

        Args:
            contributions: List of contribution dictionaries from FEC API
            source: Source identifier (e.g., 'fec_api')
            file_hash: SHA-256 hash of the data
            file_metadata: Additional metadata (state, date range, etc.)

        Returns:
            RawFiling model instance (not yet persisted)
        """
        # Construct a virtual "file URL" for API extractions
        contributor_state = file_metadata.get("contributor_state", "unknown")
        period = file_metadata.get("two_year_transaction_period", "unknown")
        file_url = f"fec_api://schedules/schedule_a?contributor_state={contributor_state}&two_year_transaction_period={period}"

        raw_filing = RawFiling(
            source=source,
            file_url=file_url,
            file_hash=file_hash,
            raw_content=contributions,  # JSONB column stores the list directly
            file_metadata=file_metadata,
        )
        return raw_filing

    def _store_contributions_staging(
        self, session: Session, raw_filing_id: int, contributions: List[Dict[str, Any]]
    ) -> int:
        """
        Store individual contributions in staging table.

        Args:
            session: SQLAlchemy session
            raw_filing_id: Foreign key to raw_filings table
            contributions: List of contribution dictionaries from FEC API

        Returns:
            Number of contributions stored
        """
        stored_count = 0

        for contrib in contributions:
            staging_record = self._map_contribution_to_staging(
                raw_filing_id=raw_filing_id, contribution=contrib
            )
            self.fec_staging_repo.insert(session, staging_record)
            stored_count += 1

        logger.info(f"Stored {stored_count} contributions in staging table")
        return stored_count

    def _map_contribution_to_staging(
        self, raw_filing_id: int, contribution: Dict[str, Any]
    ) -> FECContributionStaging:
        """
        Map FEC API contribution dictionary to staging model.

        Args:
            raw_filing_id: Foreign key to raw_filings table
            contribution: Single contribution dictionary from FEC API

        Returns:
            FECContributionStaging model instance (not yet persisted)
        """
        # Convert date from API format (YYYY-MM-DD) to FEC format (YYYYMMDD)
        transaction_date = contribution.get("contribution_receipt_date")
        if transaction_date and "-" in transaction_date:
            transaction_date = transaction_date.replace("-", "")[:8]

        # Convert amount to string if it's a number
        transaction_amt = contribution.get("contribution_receipt_amount")
        if transaction_amt is not None:
            transaction_amt = str(transaction_amt)

        return FECContributionStaging(
            raw_filing_id=raw_filing_id,
            cmte_id=contribution.get("committee_id"),
            amndt_ind=contribution.get("amendment_indicator"),
            rpt_tp=contribution.get("report_type"),
            transaction_pgi=contribution.get("election_type"),
            image_num=contribution.get("image_number"),
            transaction_tp=contribution.get("transaction_type"),
            entity_tp=contribution.get("entity_type"),
            name=contribution.get("contributor_name"),
            city=contribution.get("contributor_city"),
            state=contribution.get("contributor_state"),
            zip_code=contribution.get("contributor_zip"),
            employer=contribution.get("contributor_employer"),
            occupation=contribution.get("contributor_occupation"),
            transaction_dt=transaction_date,
            transaction_amt=transaction_amt,
            other_id=contribution.get("other_id"),
            tran_id=contribution.get("transaction_id"),
            file_num=contribution.get("file_number"),
            memo_cd=contribution.get("memo_code"),
            memo_text=contribution.get("memo_text"),
            sub_id=contribution.get("sub_id"),
        )
