"""
Backfill flow for historical FEC contribution data.

This flow fetches contributions in ascending date order (oldest first) to fill
gaps in historical data without interfering with incremental extraction.
"""

from datetime import datetime
from typing import Any

from prefect import flow, task
from prefect.cache_policies import NONE
from prefect.logging import get_run_logger
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from fund_lens_etl.clients.fec_client import FECClient
from fund_lens_etl.config import get_database_url
from fund_lens_etl.repos import (
    FECContributionStagingRepo,
    RawFilingRepo,
    ExtractionMetadataRepo,
)
from fund_lens_etl.services.fec_service import FECExtractionService


# In initialize_backfill_services:
raw_filing_repo = RawFilingRepo()
staging_repo = FECContributionStagingRepo()
metadata_repo = ExtractionMetadataRepo()  # Add this
fec_client = FECClient()

fec_service = FECExtractionService(
    fec_client=fec_client,
    raw_filing_repo=raw_filing_repo,
    fec_staging_repo=staging_repo,
    metadata_repo=metadata_repo,  # Pass it (won't be used by backfill method)
)


@task(
    name="backfill_date_range",
    description="Backfill contributions for a specific date range",
    retries=2,
    retry_delay_seconds=30,
    cache_policy=NONE,
)
def backfill_date_range(
    session: Session,
    service: FECExtractionService,
    state: str,
    two_year_transaction_period: int,
    start_date: str,
    end_date: str,
    max_results: int | None = None,
) -> dict[str, Any]:
    """
    Backfill FEC contributions for a specific date range.

    Args:
        session: Database session
        service: FEC extraction service
        state: Two-letter state code
        two_year_transaction_period: Election cycle
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        max_results: Maximum records to fetch

    Returns:
        Dictionary with backfill statistics
    """
    logger = get_run_logger()

    # Parse dates
    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.fromisoformat(end_date)

    logger.info(
        f"Starting backfill: state={state}, cycle={two_year_transaction_period}, "
        f"date_range={start_date} to {end_date}"
    )

    result = service.backfill_contributions(
        session=session,
        contributor_state=state,
        two_year_transaction_period=two_year_transaction_period,
        start_date=start_dt,
        end_date=end_dt,
        max_results=max_results,
    )

    logger.info(
        f"Backfill complete: {result['contributions_fetched']} fetched, "
        f"{result['contributions_stored']} stored"
    )

    return result


@task(
    name="initialize_backfill_services",
    description="Initialize services for backfill",
    retries=3,
    retry_delay_seconds=10,
    cache_policy=NONE,
)
def initialize_backfill_services() -> tuple[Session, FECExtractionService]:
    """Initialize database session and service layer for backfill."""
    logger = get_run_logger()
    logger.info("Initializing services for backfill")

    engine = create_engine(get_database_url(), pool_pre_ping=True)
    session = Session(engine)

    # Use different variable names to avoid shadowing imports
    raw_filing_repository = RawFilingRepo()
    staging_repository = FECContributionStagingRepo()
    metadata_repository = ExtractionMetadataRepo()  # Create instance instead of None
    client = FECClient()

    service = FECExtractionService(
        fec_client=client,
        raw_filing_repo=raw_filing_repository,
        fec_staging_repo=staging_repository,
        metadata_repo=metadata_repository,  # Pass actual instance
    )

    logger.info("Backfill services initialized")
    return session, service


@task(
    name="cleanup_backfill_resources",
    description="Clean up database connections after backfill",
    cache_policy=NONE,
)
def cleanup_backfill_resources(session: Session) -> None:
    """Close database session."""
    logger = get_run_logger()
    session.close()
    logger.info("Database session closed")


@flow(
    name="backfill-fec-contributions",
    description="Backfill historical FEC contribution data",
    log_prints=True,
)
def backfill_fec_contributions_flow(
    state: str,
    two_year_transaction_period: int,
    start_date: str,  # "2025-01-01"
    end_date: str,  # "2025-09-16"
    max_results: int | None = None,
) -> dict[str, Any]:
    """
    Backfill FEC contributions for a historical date range.

    Args:
        state: Two-letter state code (e.g., "MD")
        two_year_transaction_period: Election cycle (e.g., 2026)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        max_results: Maximum records to fetch (None for all in range)

    Returns:
        Dictionary with backfill statistics
    """
    logger = get_run_logger()
    logger.info(
        f"Starting backfill flow: state={state}, cycle={two_year_transaction_period}, "
        f"range={start_date} to {end_date}"
    )

    # Initialize services
    db_session, service = initialize_backfill_services()  # Renamed to avoid shadowing

    try:
        # Backfill date range
        stats = backfill_date_range(
            session=db_session,  # Use renamed variable
            service=service,  # Use renamed variable
            state=state,
            two_year_transaction_period=two_year_transaction_period,
            start_date=start_date,
            end_date=end_date,
            max_results=max_results,
        )

        logger.info(f"Backfill flow completed: {stats}")
        return stats

    finally:
        # Always cleanup
        cleanup_backfill_resources(db_session)  # Use renamed variable
