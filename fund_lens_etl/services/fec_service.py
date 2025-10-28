"""FEC extraction service - orchestrates client and repository operations."""

import logging
import hashlib
import json
from datetime import datetime
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
        max_results: int | None = None,
    ) -> dict[str, Any]:
        """
        Extract and store FEC contributions incrementally using date-based filtering
        with fetch-and-store batching for resilience.

        This method:
        1. Checks metadata for the last processed date
        2. Fetches contributions in batches, storing each batch immediately
        3. Updates metadata with new last processed date

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

            # Tracking variables for batch processing
            total_fetched = 0
            total_stored = 0
            all_contribution_dates = []
            current_raw_filing_id: int | None = None

            # Define batch processing callback
            def process_batch(batch_contributions: list[dict[str, Any]]) -> None:
                """
                Define batch processing callback
                """
                nonlocal \
                    total_fetched, \
                    total_stored, \
                    all_contribution_dates, \
                    current_raw_filing_id

                total_fetched += len(batch_contributions)
                logger.info(
                    f"Processing batch: {len(batch_contributions)} contributions (total fetched: {total_fetched:,})"
                )

                # Calculate file hash for this batch
                file_hash = self._calculate_file_hash(batch_contributions)

                # Check for file-level deduplication
                existing_filing = self.raw_filing_repo.get_by_file_hash(
                    session, file_hash
                )
                if existing_filing:
                    logger.info(
                        f"Batch already processed (raw_filing_id={existing_filing.id}). Skipping batch."
                    )
                    return

                # Record-level deduplication
                batch_sub_ids = [
                    c["sub_id"]
                    for c in batch_contributions
                    if c.get("sub_id") is not None
                ]
                existing_sub_ids = self.fec_staging_repo.get_existing_sub_ids(
                    session, batch_sub_ids
                )

                new_contributions = [
                    c
                    for c in batch_contributions
                    if c.get("sub_id") is not None
                    and c["sub_id"] not in existing_sub_ids
                ]

                duplicate_count = len(batch_contributions) - len(new_contributions)
                if duplicate_count > 0:
                    logger.info(
                        f"Batch has {duplicate_count} duplicates. Storing {len(new_contributions)} new contributions."
                    )

                if not new_contributions:
                    logger.info(
                        "No new contributions in this batch after deduplication"
                    )
                    return

                # Store raw filing for this batch
                file_metadata = {
                    "contributor_state": contributor_state,
                    "two_year_transaction_period": two_year_transaction_period,
                    "record_count": len(batch_contributions),
                    "extraction_type": "incremental_batch",
                    "min_date": last_processed_date.isoformat()
                    if last_processed_date
                    else None,
                }

                raw_filing = self._create_raw_filing(
                    contributions=batch_contributions,
                    source=source,
                    file_hash=file_hash,
                    file_metadata=file_metadata,
                )
                session.add(raw_filing)
                session.flush()

                assert raw_filing.id is not None
                filing_id: int = raw_filing.id
                current_raw_filing_id = filing_id

                # Store contributions in staging
                stored_count = self._store_contributions_staging(
                    session=session,
                    raw_filing_id=filing_id,
                    contributions=new_contributions,
                )

                total_stored += stored_count

                # Commit this batch
                session.commit()
                logger.info(
                    f"Batch committed: {stored_count} contributions stored "
                    f"(total stored: {total_stored:,}/{total_fetched:,})"
                )

                # Track contribution dates for metadata update
                batch_dates = [
                    c.get("contribution_receipt_date")
                    for c in batch_contributions
                    if c.get("contribution_receipt_date")
                ]
                all_contribution_dates.extend(batch_dates)

            # Step 2: Fetch contributions with batching callback
            logger.info("Starting fetch-and-store batching process...")
            self.fec_client.get_contributions(
                contributor_state=contributor_state,
                two_year_transaction_period=two_year_transaction_period,
                min_date=last_processed_date,
                max_results=max_results,
                batch_callback=process_batch,
                batch_size=1000,  # Process every 1000 records
            )

            logger.info(
                f"Fetch-and-store complete: {total_fetched:,} fetched, {total_stored:,} stored"
            )

            # Step 3: Determine new last processed date (capped at today)
            new_last_processed_date = None
            if all_contribution_dates:
                from dateutil import parser
                from datetime import timezone

                parsed_dates = [parser.parse(d) for d in all_contribution_dates if d]
                if parsed_dates:
                    max_date = max(parsed_dates)
                    today = datetime.now(timezone.utc)

                    # Cap at today to avoid future dates from bad data
                    new_last_processed_date = min(max_date, today)

                    if max_date > today:
                        logger.warning(
                            f"Found future-dated contributions (max: {max_date.date()}). "
                            f"Capping last_processed_date at today: {today.date()}"
                        )
                    else:
                        logger.info(
                            f"New last processed date: {new_last_processed_date}"
                        )

            # Step 4: Update metadata
            self.metadata_repo.upsert_metadata(
                session=session,
                source=source,
                entity_type="contributions",
                state=contributor_state,
                cycle=two_year_transaction_period,
                last_processed_date=new_last_processed_date or last_processed_date,
                records_extracted=total_stored,
                status="active",
            )

            session.commit()
            logger.info("Metadata updated and committed")

            return {
                "contributions_fetched": total_fetched,
                "raw_filing_id": current_raw_filing_id,
                "contributions_stored": total_stored,
                "file_hash": "multiple_batches",
                "last_processed_date": new_last_processed_date.isoformat()
                if new_last_processed_date
                else None,
            }

        except Exception as e:
            session.rollback()
            logger.error(f"Extraction failed: {e}")
            raise

    def backfill_contributions(
        self,
        session: Session,
        contributor_state: str,
        two_year_transaction_period: int,
        start_date: datetime,
        end_date: datetime,
        source: str = "fec_api",
        max_results: int | None = None,
    ) -> dict[str, Any]:
        """
        Backfill historical FEC contributions for a specific date range.

        Fetches contributions from oldest to newest (ascending order) to fill gaps
        in historical data.

        Args:
            session: Database session
            contributor_state: Two-letter state code (e.g., "MD")
            two_year_transaction_period: Election cycle (e.g., 2026 for 2025-2026)
            start_date: Earliest contribution date to fetch
            end_date: Latest contribution date to fetch
            source: Data source identifier (default: "fec_api")
            max_results: Maximum number of contributions to fetch (None for all)

        Returns:
            Dictionary with backfill statistics
        """
        logger.info(
            f"Starting backfill: state={contributor_state}, "
            f"cycle={two_year_transaction_period}, "
            f"date_range={start_date.date()} to {end_date.date()}, "
            f"max_results={max_results or 'ALL'}"
        )

        try:
            # Tracking variables for batch processing
            total_fetched = 0
            total_stored = 0
            all_contribution_dates = []
            current_raw_filing_id: int | None = None

            # Define batch processing callback (same as incremental)
            def process_batch(batch_contributions: list[dict[str, Any]]) -> None:
                """
                Define batch processing callback (same as incremental)
                """
                nonlocal \
                    total_fetched, \
                    total_stored, \
                    all_contribution_dates, \
                    current_raw_filing_id

                total_fetched += len(batch_contributions)
                logger.info(
                    f"Processing backfill batch: {len(batch_contributions)} contributions (total fetched: "
                    f"{total_fetched:,})"
                )

                # Calculate file hash for this batch
                file_hash = self._calculate_file_hash(batch_contributions)

                # Check for file-level deduplication
                existing_filing = self.raw_filing_repo.get_by_file_hash(
                    session, file_hash
                )
                if existing_filing:
                    logger.info(
                        f"Batch already processed (raw_filing_id={existing_filing.id}). Skipping batch."
                    )
                    return

                # Record-level deduplication
                batch_sub_ids = [
                    c["sub_id"]
                    for c in batch_contributions
                    if c.get("sub_id") is not None
                ]
                existing_sub_ids = self.fec_staging_repo.get_existing_sub_ids(
                    session, batch_sub_ids
                )

                new_contributions = [
                    c
                    for c in batch_contributions
                    if c.get("sub_id") is not None
                    and c["sub_id"] not in existing_sub_ids
                ]

                duplicate_count = len(batch_contributions) - len(new_contributions)
                if duplicate_count > 0:
                    logger.info(
                        f"Batch has {duplicate_count} duplicates. Storing {len(new_contributions)} new contributions."
                    )

                if not new_contributions:
                    logger.info(
                        "No new contributions in this batch after deduplication"
                    )
                    return

                # Store raw filing for this batch
                file_metadata = {
                    "contributor_state": contributor_state,
                    "two_year_transaction_period": two_year_transaction_period,
                    "record_count": len(batch_contributions),
                    "extraction_type": "backfill_batch",
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                }

                raw_filing = self._create_raw_filing(
                    contributions=batch_contributions,
                    source=source,
                    file_hash=file_hash,
                    file_metadata=file_metadata,
                )
                session.add(raw_filing)
                session.flush()

                assert raw_filing.id is not None
                filing_id: int = raw_filing.id
                current_raw_filing_id = filing_id

                # Store contributions in staging
                stored_count = self._store_contributions_staging(
                    session=session,
                    raw_filing_id=filing_id,
                    contributions=new_contributions,
                )

                total_stored += stored_count

                # Commit this batch
                session.commit()
                logger.info(
                    f"Backfill batch committed: {stored_count} contributions stored "
                    f"(total stored: {total_stored:,}/{total_fetched:,})"
                )

                # Track contribution dates
                batch_dates = [
                    c.get("contribution_receipt_date")
                    for c in batch_contributions
                    if c.get("contribution_receipt_date")
                ]
                all_contribution_dates.extend(batch_dates)

            # Fetch contributions with backfill parameters (oldest first)
            logger.info(
                "Starting backfill fetch-and-store process (oldest to newest)..."
            )
            self.fec_client.get_contributions(
                contributor_state=contributor_state,
                two_year_transaction_period=two_year_transaction_period,
                min_date=start_date,
                max_date=end_date,
                max_results=max_results,
                batch_callback=process_batch,
                batch_size=1000,
                sort_order="asc",  # OLDEST FIRST for backfill
            )

            logger.info(
                f"Backfill complete: {total_fetched:,} fetched, {total_stored:,} stored"
            )

            return {
                "contributions_fetched": total_fetched,
                "raw_filing_id": current_raw_filing_id,
                "contributions_stored": total_stored,
                "file_hash": "multiple_batches",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            }

        except Exception as e:
            session.rollback()
            logger.error(f"Backfill failed: {e}")
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
