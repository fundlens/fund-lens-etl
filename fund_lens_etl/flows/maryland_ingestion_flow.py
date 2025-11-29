"""
Maryland Bronze Layer Ingestion Flow

Extracts data from Maryland campaign finance sources and loads into Bronze layer tables:
- Contributions from MDCRIS (campaignfinance.maryland.gov)
- Committees from MDCRIS (campaignfinance.maryland.gov)
- Candidates from MD SBE (elections.maryland.gov)

Supports both full and incremental extraction:
- Full refresh: Extracts all data for specified years
- Incremental: Extracts only recent data using date range filters
"""

from datetime import date, timedelta
from typing import Any

import pandas as pd
from prefect import flow, task

from fund_lens_etl.clients.maryland import MarylandCRISClient, MarylandSBEClient
from fund_lens_etl.database import get_session
from fund_lens_etl.extractors.maryland import (
    MarylandCandidateExtractor,
    MarylandCommitteeExtractor,
    MarylandContributionExtractor,
)
from fund_lens_etl.loaders.bronze.maryland import (
    BronzeMarylandCandidateLoader,
    BronzeMarylandCommitteeLoader,
    BronzeMarylandContributionLoader,
)

# Retry configuration
MD_RETRY_CONFIG = {
    "retries": 3,
    "retry_delay_seconds": 60,  # Longer delay for web scraping
}

# Timeout configuration (in seconds)
MD_EXTRACTION_TIMEOUT = 600  # 10 minutes for extraction
MD_LOAD_TIMEOUT = 300  # 5 minutes per load task


# ============================================================================
# EXTRACTION TASKS
# ============================================================================


@task(
    name="extract_md_contributions",
    retries=MD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_EXTRACTION_TIMEOUT,
)
def extract_contributions_task(
    filing_year: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    """
    Extract contribution data from MDCRIS.

    Args:
        filing_year: Filing year filter
        start_date: Optional start date filter
        end_date: Optional end date filter

    Returns:
        DataFrame with contribution records
    """
    client = MarylandCRISClient()
    extractor = MarylandContributionExtractor(client=client)

    df = extractor.extract(
        filing_year=filing_year,
        start_date=start_date,
        end_date=end_date,
    )

    return df


@task(
    name="extract_md_committees",
    retries=MD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_EXTRACTION_TIMEOUT,
)
def extract_committees_task(
    status: str | None = None,
    committee_type: str | None = None,
) -> pd.DataFrame:
    """
    Extract committee data from MDCRIS.

    Args:
        status: Committee status filter ('A' for Active, None for all)
        committee_type: Committee type filter

    Returns:
        DataFrame with committee records
    """
    client = MarylandCRISClient()
    extractor = MarylandCommitteeExtractor(client=client)

    df = extractor.extract(
        status=status,
        committee_type=committee_type,
    )

    return df


@task(
    name="extract_md_candidates",
    retries=MD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_EXTRACTION_TIMEOUT,
)
def extract_candidates_task(
    years: list[int],
) -> pd.DataFrame:
    """
    Extract candidate data from MD SBE for multiple years.

    Args:
        years: List of election years to extract

    Returns:
        DataFrame with candidate records
    """
    client = MarylandSBEClient()
    extractor = MarylandCandidateExtractor(client=client)

    df = extractor.extract_multiple_years(years=years)

    return df


# ============================================================================
# LOADING TASKS
# ============================================================================


@task(
    name="load_md_contributions_to_bronze",
    retries=MD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_LOAD_TIMEOUT,
)
def load_contributions_task(contributions_df: pd.DataFrame) -> int:
    """
    Load contribution data to Bronze layer.

    Args:
        contributions_df: DataFrame with contribution records

    Returns:
        Number of records loaded
    """
    if contributions_df.empty:
        return 0

    with get_session() as session:
        loader = BronzeMarylandContributionLoader()
        loader.load(session, contributions_df)

    return len(contributions_df)


@task(
    name="load_md_committees_to_bronze",
    retries=MD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_LOAD_TIMEOUT,
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
        loader = BronzeMarylandCommitteeLoader()
        loader.load(session, committees_df)

    return len(committees_df)


@task(
    name="load_md_candidates_to_bronze",
    retries=MD_RETRY_CONFIG["retries"],
    retry_delay_seconds=MD_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=MD_LOAD_TIMEOUT,
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
        loader = BronzeMarylandCandidateLoader()
        loader.load(session, candidates_df)

    return len(candidates_df)


# ============================================================================
# MAIN MARYLAND INGESTION FLOW
# ============================================================================


@flow(
    name="maryland_bronze_ingestion",
    description="Extract and load Maryland campaign finance data to Bronze layer",
    retries=0,
)
def maryland_bronze_ingestion_flow(
    filing_years: list[int],
    start_date: date | None = None,
    end_date: date | None = None,
    include_committees: bool = True,
    include_candidates: bool = True,
    include_contributions: bool = True,
    committee_status: str | None = None,
    lookback_days: int = 3,
    full_refresh: bool = False,
) -> dict[str, Any]:
    """
    Main Maryland Bronze layer ingestion flow.

    Extracts and loads Maryland campaign finance data in this order:
    1. Committees (all at once - master data)
    2. Candidates (for specified years - master data)
    3. Contributions (per filing year - transactional data)

    Args:
        filing_years: List of filing years to extract (e.g., [2025, 2026])
        start_date: Optional start date for contributions (overrides incremental)
        end_date: Optional end date for contributions
        include_committees: Whether to extract/load committees
        include_candidates: Whether to extract/load candidates
        include_contributions: Whether to extract/load contributions
        committee_status: Committee status filter ('A' for Active, None for all)
        lookback_days: Days to look back for incremental contribution extraction
        full_refresh: If True, extract all contributions without date filter

    Returns:
        Summary statistics for the entire ingestion
    """
    from prefect import get_run_logger

    logger = get_run_logger()
    logger.info(f"Starting Maryland Bronze ingestion for years: {filing_years}")
    logger.info(f"Mode: {'FULL REFRESH' if full_refresh else 'INCREMENTAL'}")

    results: dict[str, Any] = {
        "filing_years": filing_years,
        "full_refresh": full_refresh,
        "committees_loaded": 0,
        "candidates_loaded": 0,
        "contributions_loaded": 0,
        "contribution_details": [],
    }

    # ========================================================================
    # STEP 1: Extract and Load Committees
    # ========================================================================
    if include_committees:
        logger.info("Step 1: Extracting committees...")
        committees_df = extract_committees_task(status=committee_status)
        committees_loaded = load_committees_task(committees_df)
        results["committees_loaded"] = committees_loaded
        logger.info(f"Loaded {committees_loaded} committees")
    else:
        logger.info("Step 1: Skipping committees (include_committees=False)")

    # ========================================================================
    # STEP 2: Extract and Load Candidates
    # ========================================================================
    if include_candidates:
        logger.info(f"Step 2: Extracting candidates for years {filing_years}...")
        candidates_df = extract_candidates_task(years=filing_years)
        candidates_loaded = load_candidates_task(candidates_df)
        results["candidates_loaded"] = candidates_loaded
        logger.info(f"Loaded {candidates_loaded} candidates")
    else:
        logger.info("Step 2: Skipping candidates (include_candidates=False)")

    # ========================================================================
    # STEP 3: Extract and Load Contributions (per filing year)
    # ========================================================================
    if include_contributions:
        logger.info("Step 3: Extracting contributions...")

        for filing_year in filing_years:
            logger.info(f"\nProcessing contributions for filing year {filing_year}...")

            # Determine date range
            contrib_start_date = start_date
            contrib_end_date = end_date

            if not full_refresh and start_date is None:
                # Incremental mode: use lookback
                contrib_end_date = date.today()
                contrib_start_date = contrib_end_date - timedelta(days=lookback_days)
                logger.info(f"  Incremental extraction: {contrib_start_date} to {contrib_end_date}")
            elif full_refresh:
                logger.info("  Full refresh: extracting all contributions for year")
                contrib_start_date = None
                contrib_end_date = None

            contributions_df = extract_contributions_task(
                filing_year=filing_year,
                start_date=contrib_start_date,
                end_date=contrib_end_date,
            )

            contributions_loaded = load_contributions_task(contributions_df)

            results["contributions_loaded"] += contributions_loaded
            results["contribution_details"].append(
                {
                    "filing_year": filing_year,
                    "records_loaded": contributions_loaded,
                    "start_date": str(contrib_start_date) if contrib_start_date else None,
                    "end_date": str(contrib_end_date) if contrib_end_date else None,
                }
            )

            logger.info(f"  Loaded {contributions_loaded} contributions for {filing_year}")
    else:
        logger.info("Step 3: Skipping contributions (include_contributions=False)")

    # ========================================================================
    # Summary
    # ========================================================================
    logger.info("\n" + "=" * 70)
    logger.info("Maryland Bronze Ingestion Complete!")
    logger.info(f"  Filing Years: {filing_years}")
    logger.info(f"  Committees: {results['committees_loaded']}")
    logger.info(f"  Candidates: {results['candidates_loaded']}")
    logger.info(f"  Contributions: {results['contributions_loaded']}")
    for detail in results["contribution_details"]:
        logger.info(
            f"    - {detail['filing_year']}: {detail['records_loaded']} records "
            f"({detail['start_date']} to {detail['end_date']})"
        )
    logger.info("=" * 70)

    return results


# ============================================================================
# CONVENIENCE FLOWS
# ============================================================================


@flow(
    name="maryland_daily_ingestion",
    description="Daily incremental ingestion for Maryland campaign finance data",
)
def maryland_daily_ingestion_flow(
    filing_years: list[int] | None = None,
    lookback_days: int = 3,
) -> dict[str, Any]:
    """
    Daily incremental ingestion for Maryland data.

    Extracts recent contributions (last N days) for current filing years.
    Also refreshes committees weekly (on Sundays).

    Args:
        filing_years: Filing years to extract (defaults to current and next year)
        lookback_days: Days to look back for contributions

    Returns:
        Summary statistics
    """
    from datetime import datetime

    # Default to current year and next year
    if filing_years is None:
        current_year = datetime.now().year
        filing_years = [current_year, current_year + 1]

    # Include committees on Sundays only
    include_committees = datetime.now().weekday() == 6  # Sunday = 6

    return maryland_bronze_ingestion_flow(
        filing_years=filing_years,
        include_committees=include_committees,
        include_candidates=False,  # Candidates change less frequently
        include_contributions=True,
        lookback_days=lookback_days,
        full_refresh=False,
    )


@flow(
    name="maryland_full_refresh",
    description="Full refresh of Maryland campaign finance data",
)
def maryland_full_refresh_flow(
    filing_years: list[int],
) -> dict[str, Any]:
    """
    Full refresh for Maryland data.

    Extracts all data for specified filing years.
    Use for initial load or reconciliation.

    Args:
        filing_years: Filing years to extract

    Returns:
        Summary statistics
    """
    return maryland_bronze_ingestion_flow(
        filing_years=filing_years,
        include_committees=True,
        include_candidates=True,
        include_contributions=True,
        committee_status=None,  # All committees
        full_refresh=True,
    )
