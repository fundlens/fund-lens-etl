"""FEC extraction service - orchestrates client and repository operations."""

import logging
import hashlib
import json
from dateutil import parser  # type: ignore
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from fund_lens_etl.clients import FECClient
from fund_lens_etl.repos import RawFilingRepo
from fund_lens_etl.repos import FECContributionStagingRepo
from fund_lens_etl.repos import ExtractionMetadataRepo
from fund_lens_etl.models import RawFiling
from fund_lens_etl.models import FECContributionStaging

logger = logging.getLogger(__name__)


# noinspection PyMethodMayBeStatic,PyArgumentEqualDefault
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
        fec_client: FECClient,
        raw_filing_repo: RawFilingRepo,
        fec_staging_repo: FECContributionStagingRepo,
        metadata_repo: ExtractionMetadataRepo,  # Add this
    ):
        self.fec_client = fec_client
        self.raw_filing_repo = raw_filing_repo
        self.fec_staging_repo = fec_staging_repo
        self.metadata_repo = metadata_repo  # Add this

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

    def extract_and_store_contributions_incremental(
        self,
        session: Session,
        contributor_state: str,
        two_year_transaction_period: int,
        source: str = "fec_api",
        max_results: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Extract and store FEC contributions incrementally using date-based filtering.

        This method:
        1. Checks metadata for the last processed date
        2. Fetches only contributions after that date
        3. Stores new contributions
        4. Updates metadata with new last processed date

        Args:
            session: Database session
            contributor_state: Two-letter state code (e.g., "MD")
            two_year_transaction_period: Election cycle (e.g., 2026 for 2025-2026)
            source: Data source identifier (default: "fec_api")
            max_results: Maximum number of contributions to fetch (None for all)

        Returns:
            Dictionary with extraction statistics
        """
        logger.info(
            f"Starting incremental extraction: state={contributor_state}, "
            f"cycle={two_year_transaction_period}, max_results={max_results or 'ALL'}"
        )

        try:
            # Step 1: Get last processed date from metadata
            last_processed_date = self.metadata_repo.get_last_processed_date(
                session=session,
                source=source,
                entity_type="contributions",
                state=contributor_state,
                cycle=two_year_transaction_period,
            )

            if last_processed_date:
                logger.info(f"Resuming from last processed date: {last_processed_date}")
            else:
                logger.info("First extraction - fetching all contributions")

            # Step 2: Fetch contributions with date filter
            contributions = self.fec_client.get_contributions(
                contributor_state=contributor_state,
                two_year_transaction_period=two_year_transaction_period,
                min_date=last_processed_date,  # Only fetch contributions after this date
                max_results=max_results,
            )

            if not contributions:
                logger.info("No new contributions found")
                # Still update metadata to record the extraction attempt
                self.metadata_repo.upsert_metadata(
                    session=session,
                    source=source,
                    entity_type="contributions",
                    state=contributor_state,
                    cycle=two_year_transaction_period,
                    last_processed_date=last_processed_date,  # Keep same date
                    records_extracted=0,
                    status="active",
                )
                session.commit()

                return {
                    "contributions_fetched": 0,
                    "contributions_stored": 0,
                    "raw_filing_id": None,
                    "file_hash": None,
                    "last_processed_date": last_processed_date.isoformat()
                    if last_processed_date
                    else None,
                }

            logger.info(f"Fetched {len(contributions)} contributions from API")

            # Step 3: Calculate file hash for deduplication
            file_hash = self._calculate_file_hash(contributions)
            logger.debug(f"Calculated file hash: {file_hash}")

            # Step 4: Check if we've already processed this exact data
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
                    "last_processed_date": last_processed_date.isoformat()
                    if last_processed_date
                    else None,
                }

            # Step 5: Record-level deduplication (filter out existing sub_ids)
            all_sub_ids = [
                c["sub_id"] for c in contributions if c.get("sub_id") is not None
            ]
            existing_sub_ids = self.fec_staging_repo.get_existing_sub_ids(
                session, all_sub_ids
            )

            new_contributions = [
                c
                for c in contributions
                if c.get("sub_id") is not None and c["sub_id"] not in existing_sub_ids
            ]

            duplicate_count = len(contributions) - len(new_contributions)
            if duplicate_count > 0:
                logger.info(
                    f"Found {duplicate_count} duplicate contributions (by sub_id). "
                    f"Storing {len(new_contributions)} new contributions."
                )

            # Update contributions list to only process new ones
            original_contributions = contributions
            original_count = len(contributions)
            contributions = new_contributions

            # Step 6: Store raw filing record
            # Build metadata
            file_metadata = {
                "contributor_state": contributor_state,
                "two_year_transaction_period": two_year_transaction_period,
                "record_count": original_count,
                "extraction_type": "incremental",
                "min_date": last_processed_date.isoformat()
                if last_processed_date
                else None,
            }

            raw_filing = self._create_raw_filing(
                contributions=original_contributions,
                source=source,
                file_hash=file_hash,
                file_metadata=file_metadata,
            )
            session.add(raw_filing)
            # Flush to get raw_filing.id
            session.flush()

            # Type assertion for mypy
            assert raw_filing.id is not None
            filing_id: int = raw_filing.id

            logger.info(f"Stored raw filing with id={filing_id}")

            # Step 7: Store individual contributions in staging with batching
            from fund_lens_etl.config import FEC_BATCH_SIZE

            stored_count = 0
            total_to_store = len(contributions)

            for i in range(0, total_to_store, FEC_BATCH_SIZE):
                batch = contributions[i : i + FEC_BATCH_SIZE]
                batch_num = (i // FEC_BATCH_SIZE) + 1
                total_batches = (total_to_store + FEC_BATCH_SIZE - 1) // FEC_BATCH_SIZE

                logger.info(
                    f"Processing batch {batch_num}/{total_batches}: "
                    f"{len(batch)} contributions (records {i + 1}-{min(i + FEC_BATCH_SIZE, total_to_store)})"
                )

                batch_stored = self._store_contributions_staging(
                    session=session,
                    raw_filing_id=filing_id,
                    contributions=batch,
                )

                stored_count += batch_stored

                # Commit each batch
                session.commit()
                logger.info(
                    f"Batch {batch_num}/{total_batches} committed: "
                    f"{batch_stored} contributions stored "
                    f"(total so far: {stored_count}/{total_to_store})"
                )

            logger.info(
                f"All batches complete: {stored_count} total contributions stored"
            )

            # Step 8: Determine the new last processed date (max date from this batch)
            new_last_processed_date = None
            if contributions:
                # Get the maximum contribution_receipt_date from the batch
                dates = [
                    c.get("contribution_receipt_date")
                    for c in contributions
                    if c.get("contribution_receipt_date")
                ]
                if dates:
                    parsed_dates = [parser.parse(d) for d in dates]
                    new_last_processed_date = max(parsed_dates)
                    logger.info(f"New last processed date: {new_last_processed_date}")

            # Step 9: Update metadata
            self.metadata_repo.upsert_metadata(
                session=session,
                source=source,
                entity_type="contributions",
                state=contributor_state,
                cycle=two_year_transaction_period,
                last_processed_date=new_last_processed_date or last_processed_date,
                records_extracted=stored_count,
                status="active",
            )

            # Commit the transaction
            session.commit()
            logger.info("Metadata and raw filing committed")

            logger.info(
                f"Successfully extracted and stored {stored_count} contributions "
                f"(raw_filing_id={filing_id})"
            )

            return {
                "contributions_fetched": original_count,
                "raw_filing_id": filing_id,
                "contributions_stored": stored_count,
                "file_hash": file_hash,
                "last_processed_date": new_last_processed_date.isoformat()
                if new_last_processed_date
                else None,
            }

        except Exception as e:
            session.rollback()
            logger.error(f"Extraction failed: {e}")
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
            raw_content={"contributions": contributions},
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
