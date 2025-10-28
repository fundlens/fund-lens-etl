"""
Prefect flow for extracting FEC contributions to Bronze layer.

This flow orchestrates the extraction of contribution data from the FEC API
and stores it in the raw_filings and fec_contributions_staging tables.

Usage:
    # Test with limited results
    python -m flows.extract_fec_contributions --state MD --cycle 2024 --max-results 500

    # Full extraction for MD
    python -m flows.extract_fec_contributions --state MD --cycle 2024

    # Future: Run for all states
    python -m flows.extract_fec_contributions --state CA --cycle 2024
"""

from datetime import datetime

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NONE
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from fund_lens_etl.clients.fec_client import FECClient
from fund_lens_etl.config import get_database_url
from fund_lens_etl.repos import (
    ExtractionMetadataRepo,
    FECContributionStagingRepo,
    RawFilingRepo,
)
from fund_lens_etl.services import FECExtractionService


@task(
    name="initialize_services",
    description="Initialize database session and service layer",
    retries=3,
    retry_delay_seconds=10,
    cache_policy=NONE,
)
def initialize_services() -> tuple[Session, FECExtractionService]:
    """
    Initialize database connection and service layer.

    Returns:
        Tuple of (Session, FECExtractionService) for use in flow

    Raises:
        Exception: If database connection or service initialization fails
    """
    logger = get_run_logger()
    logger.info("Initializing database connection and services")

    # Create database engine and session
    engine = create_engine(get_database_url(), pool_pre_ping=True)
    session = Session(engine)

    # Initialize repositories (no session needed in constructor)
    raw_filing_repo = RawFilingRepo()
    staging_repo = FECContributionStagingRepo()
    metadata_repo = ExtractionMetadataRepo()  # Add this line

    # Initialize FEC client (pulls config automatically)
    fec_client = FECClient()

    # Initialize service with dependency injection
    fec_service = FECExtractionService(
        fec_client=fec_client,
        raw_filing_repo=raw_filing_repo,
        fec_staging_repo=staging_repo,
        metadata_repo=metadata_repo,  # Add this line
    )

    logger.info("Services initialized successfully")
    return session, fec_service


@task(
    name="extract_state_contributions",
    description="Extract contributions for a specific state and cycle",
    retries=2,
    retry_delay_seconds=30,
    cache_policy=NONE,
)
def extract_state_contributions(
    session: Session,
    fec_service: FECExtractionService,
    state: str,
    two_year_transaction_period: int,
    max_results: int | None = None,
) -> dict:
    """
    Extract FEC contributions for a specific state and election cycle using incremental extraction.

    Args:
        session: Database session for transactions
        fec_service: Initialized FEC extraction service
        state: Two-letter state code (e.g., "MD", "CA")
        two_year_transaction_period: Election cycle (e.g., 2026 for 2025-2026)
        max_results: Maximum records to extract (None for full extraction)

    Returns:
        Dictionary with extraction statistics:
            - contributions_fetched: Number of contributions fetched from API
            - contributions_stored: Number of contributions stored (after deduplication)
            - raw_filing_id: ID of the raw filing record
            - last_processed_date: Latest contribution date processed
            - start_time: Extraction start timestamp
            - end_time: Extraction end timestamp
            - duration_seconds: Total extraction time
    """
    logger = get_run_logger()

    start_time = datetime.now()
    logger.info(
        f"Starting incremental extraction for state={state}, cycle={two_year_transaction_period}, "
        f"max_results={max_results or 'ALL'}"
    )

    # Call the incremental extraction service method
    result = fec_service.extract_and_store_contributions_incremental(
        session=session,
        contributor_state=state,
        two_year_transaction_period=two_year_transaction_period,
        max_results=max_results,
    )

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Build statistics dictionary
    stats = {
        "state": state,
        "cycle": two_year_transaction_period,
        "contributions_fetched": result["contributions_fetched"],
        "contributions_stored": result["contributions_stored"],
        "raw_filing_id": result.get("raw_filing_id"),
        "file_hash": result.get("file_hash"),
        "last_processed_date": result.get("last_processed_date"),
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": duration,
    }

    logger.info(
        f"Extraction complete: {stats['contributions_fetched']} fetched, "
        f"{stats['contributions_stored']} stored, "
        f"last_date={stats['last_processed_date']}, "
        f"took {duration:.2f}s"
    )

    return stats


@task(
    name="cleanup_resources",
    description="Clean up database session and resources",
    cache_policy=NONE,  # Add this line
)
def cleanup_resources(session: Session) -> None:
    """
    Clean up database session and release resources.

    Args:
        session: Database session to close
    """
    logger = get_run_logger()

    try:
        session.close()
        logger.info("Database session closed successfully")
    except Exception as e:
        logger.error(f"Error closing database session: {e}")
        # Don't raise - cleanup is best effort


@flow(
    name="extract-fec-contributions",
    description="Extract FEC contributions for a specific state and election cycle",
    log_prints=True,
)
def extract_fec_contributions_flow(
    state: str = "MD",
    two_year_transaction_period: int = 2024,
    max_results: int | None = None,
) -> dict:
    """
    Main flow to extract FEC contributions to Bronze layer.

    This flow:
    1. Initializes database connection and services
    2. Extracts contributions from FEC API
    3. Stores raw JSON and staging records
    4. Cleans up resources

    Args:
        state: Two-letter state code (default: "MD")
        two_year_transaction_period: Election cycle (default: 2024 for 2023-2024)
        max_results: Maximum records to extract (None for full extraction)

    Returns:
        Dictionary with extraction statistics

    Examples:
        # Test with 500 records
        extract_fec_contributions_flow(state="MD", two_year_transaction_period=2024, max_results=500)

        # Full extraction for Maryland
        extract_fec_contributions_flow(state="MD", two_year_transaction_period=2024)

        # Extract for California
        extract_fec_contributions_flow(state="CA", two_year_transaction_period=2024)
    """
    logger = get_run_logger()
    logger.info(
        f"Starting FEC extraction flow: state={state}, "
        f"cycle={two_year_transaction_period}, max_results={max_results or 'ALL'}"
    )

    session = None

    try:
        # Task 1: Initialize services
        session, fec_service = initialize_services()

        # Task 2: Extract contributions
        stats = extract_state_contributions(
            session=session,
            fec_service=fec_service,
            state=state,
            two_year_transaction_period=two_year_transaction_period,
            max_results=max_results,
        )

        logger.info(f"Flow completed successfully: {stats}")
        return stats

    except Exception as e:
        logger.error(f"Flow failed with error: {e}")
        raise

    finally:
        # Task 3: Always cleanup resources
        if session is not None:
            cleanup_resources(session)
