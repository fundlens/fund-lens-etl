"""
Bronze Layer Ingestion Flow

Extracts data from FEC API and loads into Bronze layer tables.
Handles committees, candidates, and contributions (Schedule A).

Supports both full and incremental extraction:
- Full refresh: Extracts all data (use for initial load or monthly reconciliation)
- Incremental: Extracts only new data using lookback window (default 90 days)
"""

from datetime import date
from typing import Any

import pandas as pd
from fund_lens_models.enums import USState
from prefect import flow, task

from fund_lens_etl.clients.fec import FECAPIClient
from fund_lens_etl.database import get_session
from fund_lens_etl.extractors.fec import (
    FECCandidateExtractor,
    FECCommitteeExtractor,
    FECScheduleAExtractor,
)
from fund_lens_etl.loaders.bronze.fec import (
    BronzeFECCandidateLoader,
    BronzeFECCommitteeLoader,
    BronzeFECScheduleALoader,
)
from fund_lens_etl.utils.extraction_state import (
    calculate_incremental_start_date,
    get_last_contribution_info,
    update_extraction_state,
)

# Retry configuration for FEC API calls
# Note: FECAPIClient handles rate limiting but not retries
FEC_RETRY_CONFIG = {
    "retries": 3,
    "retry_delay_seconds": 30,  # Wait 30s between retries for transient failures
}

# Timeout configuration (in seconds)
FEC_EXTRACTION_TIMEOUT = 1800  # 30 minutes for extraction (can be paginated)
FEC_LOAD_TIMEOUT = 600  # 10 minutes per load task


# ============================================================================
# EXTRACTION TASKS
# ============================================================================


@task(
    name="extract_committees",
    retries=FEC_RETRY_CONFIG["retries"],
    retry_delay_seconds=FEC_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=FEC_EXTRACTION_TIMEOUT,
)
def extract_committees_task(
    state: USState | None = None,
    cycle: int | None = None,
    committee_type: str | None = None,
) -> pd.DataFrame:
    """
    Extract committee data from FEC API.

    Args:
        state: State filter (e.g., USState.MD)
        cycle: Election cycle year (e.g., 2026)
        committee_type: Committee type filter (e.g., 'S' for Senate)

    Returns:
        DataFrame with committee records
    """
    client = FECAPIClient()
    extractor = FECCommitteeExtractor(api_client=client)

    df = extractor.extract(
        state=state,
        cycle=cycle,
        committee_type=committee_type,
    )

    return df


@task(
    name="extract_candidates",
    retries=FEC_RETRY_CONFIG["retries"],
    retry_delay_seconds=FEC_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=FEC_EXTRACTION_TIMEOUT,
)
def extract_candidates_task(
    state: USState | None = None,
    cycle: int | None = None,
    office: str | None = None,
) -> pd.DataFrame:
    """
    Extract candidate data from FEC API.

    Args:
        state: State filter (e.g., USState.MD)
        cycle: Election cycle year (e.g., 2026)
        office: Office filter ('H' for House, 'S' for Senate, 'P' for President)

    Returns:
        DataFrame with candidate records
    """
    client = FECAPIClient()
    extractor = FECCandidateExtractor(api_client=client)

    df = extractor.extract(
        state=state,
        cycle=cycle,
        office=office,
    )

    return df


@task(
    name="extract_contributions_for_committee",
    retries=FEC_RETRY_CONFIG["retries"],
    retry_delay_seconds=FEC_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=FEC_EXTRACTION_TIMEOUT,
)
def extract_contributions_for_committee_task(
    committee_id: str,
    election_cycle: int,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    """
    Extract all contributions for a single committee.

    This task extracts ALL pages for one committee and returns the complete dataset.
    For incremental loading, use the flow-level orchestration.

    Args:
        committee_id: FEC committee ID
        election_cycle: Election cycle year
        start_date: Optional start date filter
        end_date: Optional end date filter

    Returns:
        DataFrame with all contribution records for this committee
    """
    client = FECAPIClient()
    extractor = FECScheduleAExtractor(api_client=client)

    all_contributions = []

    # Use the generator to fetch page by page
    for page_df, _metadata in extractor.extract_schedule_a_pages(
        committee_id=committee_id,
        election_cycle=election_cycle,
        start_date=start_date,
        end_date=end_date,
    ):
        all_contributions.append(page_df)

    # Concatenate all pages
    if all_contributions:
        return pd.concat(all_contributions, ignore_index=True)
    else:
        return pd.DataFrame()


# ============================================================================
# LOADING TASKS
# ============================================================================


@task(
    name="load_committees_to_bronze",
    retries=FEC_RETRY_CONFIG["retries"],
    retry_delay_seconds=FEC_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=FEC_LOAD_TIMEOUT,
)
def load_committees_task(committees_df: pd.DataFrame) -> int:
    """
    Load committee data to Bronze layer.

    Args:
        committees_df: DataFrame with committee records

    Returns:
        Number of records loaded
    """
    if committees_df.empty:
        return 0

    with get_session() as session:
        loader = BronzeFECCommitteeLoader()
        loader.load(session, committees_df)

    return len(committees_df)


@task(
    name="load_candidates_to_bronze",
    retries=FEC_RETRY_CONFIG["retries"],
    retry_delay_seconds=FEC_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=FEC_LOAD_TIMEOUT,
)
def load_candidates_task(candidates_df: pd.DataFrame) -> int:
    """
    Load candidate data to Bronze layer.

    Args:
        candidates_df: DataFrame with candidate records

    Returns:
        Number of records loaded
    """
    if candidates_df.empty:
        return 0

    with get_session() as session:
        loader = BronzeFECCandidateLoader()
        loader.load(session, candidates_df)

    return len(candidates_df)


@task(
    name="load_contributions_page_to_bronze",
    retries=FEC_RETRY_CONFIG["retries"],
    retry_delay_seconds=FEC_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=FEC_LOAD_TIMEOUT,
)
def load_contributions_page_task(
    contributions_df: pd.DataFrame,
    page_metadata: dict[str, Any],
) -> dict[str, Any]:
    """
    Load a single page of contribution data to Bronze layer.

    Args:
        contributions_df: DataFrame with contribution records (one page)
        page_metadata: Metadata about this page

    Returns:
        Dictionary with loading results
    """
    if contributions_df.empty:
        return {
            "records_loaded": 0,
            "page": page_metadata.get("page", 0),
        }

    with get_session() as session:
        loader = BronzeFECScheduleALoader()
        loader.load(session, contributions_df)

    return {
        "records_loaded": len(contributions_df),
        "page": page_metadata.get("page", 0),
        "total_pages": page_metadata.get("total_pages", 0),
    }


# ============================================================================
# SUB-FLOWS (Committee-Level Processing)
# ============================================================================


@flow(
    name="process_committee_contributions",
    retries=1,  # Retry the whole committee if it fails
    retry_delay_seconds=60,
)
def process_committee_contributions_flow(
    committee_id: str,
    committee_name: str,
    election_cycle: int,
    start_date: date | None = None,
    end_date: date | None = None,
    full_refresh: bool = False,
) -> dict[str, Any]:
    """
    Process all contributions for a single committee, page by page.

    Each page is extracted and loaded immediately before fetching the next page.
    This ensures no data loss even if the process fails midway.

    Supports incremental extraction using extraction state tracking.

    Args:
        committee_id: FEC committee ID
        committee_name: Committee name (for logging)
        election_cycle: Election cycle year
        start_date: Optional start date filter (overrides incremental calculation)
        end_date: Optional end date filter
        full_refresh: If True, ignores extraction state and does full load

    Returns:
        Summary statistics for this committee
    """
    from prefect import get_run_logger

    logger = get_run_logger()
    logger.info(f"Processing contributions for {committee_name} ({committee_id})")

    # Determine extraction start date
    calculated_start_date = start_date
    is_incremental = False

    if not full_refresh and start_date is None:
        # Calculate incremental start date if not doing full refresh
        with get_session() as session:
            calculated_start_date = calculate_incremental_start_date(
                session=session,
                committee_id=committee_id,
                election_cycle=election_cycle,
            )
            if calculated_start_date:
                is_incremental = True
                logger.info(
                    f"  Incremental extraction starting from {calculated_start_date} "
                    f"(lookback from last extraction)"
                )
            else:
                logger.info("  Full extraction (no previous state found)")
    elif full_refresh:
        logger.info("  Full refresh requested - extracting all data")
    else:
        logger.info(f"  Using provided start_date: {start_date}")

    client = FECAPIClient()
    extractor = FECScheduleAExtractor(api_client=client)

    total_records = 0
    pages_processed = 0
    is_empty = False

    # Extract and load page by page
    for page_df, page_metadata in extractor.extract_schedule_a_pages(
        committee_id=committee_id,
        election_cycle=election_cycle,
        start_date=calculated_start_date,
        end_date=end_date,
    ):
        # Check if first page is empty - skip this committee
        if pages_processed == 0 and page_df.empty:
            logger.info(f"Committee {committee_name} has no contributions - skipping")
            is_empty = True
            break

        # Load this page immediately
        result = load_contributions_page_task(
            contributions_df=page_df,
            page_metadata=page_metadata,
        )

        total_records += result["records_loaded"]
        pages_processed += 1

        # Log progress every 100 pages
        if pages_processed % 100 == 0:
            logger.info(
                f"  Progress: {pages_processed}/{result['total_pages']} pages, "
                f"{total_records:,} records loaded"
            )

    if not is_empty:
        logger.info(
            f"Completed {committee_name}: "
            f"{pages_processed} pages, {total_records:,} contributions"
        )

        # Update extraction state after successful completion
        with get_session() as session:
            last_contrib_info = get_last_contribution_info(
                session=session,
                committee_id=committee_id,
                election_cycle=election_cycle,
            )

            if last_contrib_info:
                last_date, last_sub_id = last_contrib_info
                update_extraction_state(
                    session=session,
                    committee_id=committee_id,
                    election_cycle=election_cycle,
                    last_contribution_date=last_date,
                    last_sub_id=last_sub_id,
                    total_records_extracted=total_records,
                    extraction_start_date=calculated_start_date,
                    extraction_end_date=end_date,
                    is_complete=True,
                )
                logger.info(
                    f"  Updated extraction state: last_date={last_date}, "
                    f"total_records={total_records}"
                )

    return {
        "committee_id": committee_id,
        "committee_name": committee_name,
        "pages_processed": pages_processed,
        "total_records": total_records,
        "is_empty": is_empty,
        "is_incremental": is_incremental,
        "extraction_start_date": calculated_start_date,
        "extraction_end_date": end_date,
    }


# ============================================================================
# MAIN BRONZE INGESTION FLOW
# ============================================================================


@flow(
    name="bronze_ingestion",
    description="Extract and load FEC data to Bronze layer with incremental support",
    retries=0,  # Don't retry the whole flow, let individual tasks/sub-flows retry
)
def bronze_ingestion_flow(
    state: USState,
    election_cycle: int,
    start_date: date | None = None,
    end_date: date | None = None,
    committee_ids: list[str] | None = None,
    full_refresh: bool = False,
) -> dict[str, Any]:
    """
    Main Bronze layer ingestion flow.

    Extracts and loads FEC data in this order:
    1. Committees (all at once - small dataset)
    2. Candidates (all at once - small dataset)
    3. Contributions (page-by-page per committee - large dataset)

    Supports both full and incremental extraction:
    - Full refresh (full_refresh=True): Extracts all data from the beginning
      Use for: Initial load, monthly reconciliation, data quality fixes
    - Incremental (full_refresh=False, default): Uses extraction state to determine
      start date with 90-day lookback window to catch amendments
      Use for: Daily/regular updates

    Args:
        state: State to extract data for (e.g., USState.MD)
        election_cycle: Election cycle year (e.g., 2026)
        start_date: Optional start date for contributions (overrides incremental calculation)
        end_date: Optional end date for contributions
        committee_ids: Optional list of specific committee IDs to process
                      (if None, processes all candidate committees for the state)
        full_refresh: If True, forces full extraction ignoring extraction state
                     Default False (incremental mode)

    Returns:
        Summary statistics for the entire ingestion
    """
    from prefect import get_run_logger

    logger = get_run_logger()
    logger.info(f"Starting Bronze ingestion for {state.value}, cycle {election_cycle}")
    logger.info(f"Mode: {'FULL REFRESH' if full_refresh else 'INCREMENTAL (with lookback)'}")

    # ========================================================================
    # STEP 1: Extract and Load Committees
    # ========================================================================
    logger.info("Step 1: Extracting committees...")
    committees_df = extract_committees_task(
        state=state,
        cycle=election_cycle,
    )

    committees_loaded = load_committees_task(committees_df)
    logger.info(f"Loaded {committees_loaded} committees")

    # ========================================================================
    # STEP 2: Extract and Load Candidates
    # ========================================================================
    logger.info("Step 2: Extracting candidates...")
    candidates_df = extract_candidates_task(
        state=state,
        cycle=election_cycle,
    )

    candidates_loaded = load_candidates_task(candidates_df)
    logger.info(f"Loaded {candidates_loaded} candidates")

    # ========================================================================
    # STEP 3: Process Contributions (Page-by-Page per Committee)
    # ========================================================================
    logger.info("Step 3: Processing contributions...")

    # Determine which committees to process
    if committee_ids:
        # Use provided committee IDs
        committees_to_process = committees_df[committees_df["committee_id"].isin(committee_ids)]
        logger.info(f"Processing {len(committees_to_process)} specified committees")
    else:
        # Filter for candidate committees (House/Senate, Principal/Authorized)
        committees_to_process = committees_df[
            (committees_df["committee_type"].isin(["H", "S"]))
            & (committees_df["designation"].isin(["P", "A"]))
        ]
        logger.info(
            f"Processing {len(committees_to_process)} candidate committees "
            f"(H/S with P/A designation)"
        )

    # Process each committee
    contribution_results = []

    for idx, (_, committee) in enumerate(committees_to_process.iterrows(), start=1):
        logger.info(
            f"\n[{idx}/{len(committees_to_process)}] "
            f"Processing {committee['name']} ({committee['committee_id']})..."
        )

        result = process_committee_contributions_flow(
            committee_id=committee["committee_id"],
            committee_name=committee["name"],
            election_cycle=election_cycle,
            start_date=start_date,
            end_date=end_date,
            full_refresh=full_refresh,
        )

        contribution_results.append(result)

    # ========================================================================
    # Summary
    # ========================================================================
    total_contributions = sum(r["total_records"] for r in contribution_results)
    total_pages = sum(r["pages_processed"] for r in contribution_results)
    incremental_count = sum(1 for r in contribution_results if r.get("is_incremental", False))

    logger.info("\n" + "=" * 70)
    logger.info("Bronze Ingestion Complete!")
    logger.info(f"  Committees: {committees_loaded}")
    logger.info(f"  Candidates: {candidates_loaded}")
    logger.info(f"  Committees Processed: {len(contribution_results)}")
    logger.info(f"    - Incremental: {incremental_count}")
    logger.info(f"    - Full: {len(contribution_results) - incremental_count}")
    logger.info(f"  Total Contributions: {total_contributions:,}")
    logger.info(f"  Total Pages: {total_pages:,}")
    logger.info("=" * 70)

    return {
        "state": state.value,
        "election_cycle": election_cycle,
        "full_refresh": full_refresh,
        "committees_loaded": committees_loaded,
        "candidates_loaded": candidates_loaded,
        "committees_processed": len(contribution_results),
        "committees_incremental": incremental_count,
        "committees_full": len(contribution_results) - incremental_count,
        "total_contributions": total_contributions,
        "total_pages": total_pages,
        "committee_details": contribution_results,
    }
