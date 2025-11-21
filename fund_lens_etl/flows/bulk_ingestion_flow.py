"""
Bulk File Ingestion Flow

Loads FEC data from bulk files instead of API.
More reliable for historical backfills - no API timeouts or rate limiting.

Bulk files are downloaded from: https://www.fec.gov/data/browse-data/?tab=bulk-data
"""

from pathlib import Path
from typing import Any

from prefect import flow, task

from fund_lens_etl.database import get_session
from fund_lens_etl.extractors.bulk import (
    BulkFECCandidateExtractor,
    BulkFECCommitteeExtractor,
    BulkFECContributionExtractor,
)
from fund_lens_etl.loaders.bronze.fec import (
    BronzeFECCandidateLoader,
    BronzeFECCommitteeLoader,
    BronzeFECScheduleALoader,
)

# Configuration
BULK_RETRY_CONFIG = {
    "retries": 2,
    "retry_delay_seconds": 10,
}

BULK_TIMEOUT = 3600  # 1 hour for bulk operations


# ============================================================================
# EXTRACTION TASKS
# ============================================================================


@task(
    name="extract_committees_from_bulk",
    retries=BULK_RETRY_CONFIG["retries"],
    retry_delay_seconds=BULK_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=BULK_TIMEOUT,
)
def extract_committees_from_bulk_task(
    data_file: Path,
    header_file: Path,
) -> int:
    """
    Extract and load committees from bulk file.

    Args:
        data_file: Path to cm.txt
        header_file: Path to cm_header_file.csv

    Returns:
        Number of records loaded
    """
    from prefect import get_run_logger

    logger = get_run_logger()
    logger.info(f"Extracting committees from {data_file}")

    extractor = BulkFECCommitteeExtractor()
    loader = BronzeFECCommitteeLoader()

    # Extract all committees (small file, ~17K records)
    df = extractor.extract(
        file_path=data_file,
        header_file_path=header_file,
    )

    # Load to Bronze
    with get_session() as session:
        records_loaded = loader.load(session, df, source_system="FEC_BULK")

    logger.info(f"Loaded {records_loaded:,} committees from bulk file")
    return records_loaded


@task(
    name="extract_candidates_from_bulk",
    retries=BULK_RETRY_CONFIG["retries"],
    retry_delay_seconds=BULK_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=BULK_TIMEOUT,
)
def extract_candidates_from_bulk_task(
    data_file: Path,
    header_file: Path,
) -> int:
    """
    Extract and load candidates from bulk file.

    Args:
        data_file: Path to cn.txt
        header_file: Path to cn_header_file.csv

    Returns:
        Number of records loaded
    """
    from prefect import get_run_logger

    logger = get_run_logger()
    logger.info(f"Extracting candidates from {data_file}")

    extractor = BulkFECCandidateExtractor()
    loader = BronzeFECCandidateLoader()

    # Extract all candidates (small file, ~6K records)
    df = extractor.extract(
        file_path=data_file,
        header_file_path=header_file,
    )

    # Load to Bronze
    with get_session() as session:
        records_loaded = loader.load(session, df, source_system="FEC_BULK")

    logger.info(f"Loaded {records_loaded:,} candidates from bulk file")
    return records_loaded


@task(
    name="extract_contributions_from_bulk_chunked",
    retries=BULK_RETRY_CONFIG["retries"],
    retry_delay_seconds=BULK_RETRY_CONFIG["retry_delay_seconds"],
    timeout_seconds=BULK_TIMEOUT * 4,  # 4 hours for large file
)
def extract_contributions_from_bulk_task(
    data_file: Path,
    header_file: Path,
    chunksize: int = 10_000,
) -> dict[str, int]:
    """
    Extract and load individual contributions from bulk file in chunks.

    The contribution file is very large (~9.2M records, 1.6GB).
    Processes in chunks to avoid memory issues.

    OPTIMIZED: Checks which sub_id values already exist and only inserts new records.
    This avoids slow UPSERT conflicts on millions of existing records.

    Args:
        data_file: Path to itcont.txt
        header_file: Path to indiv_header_file.csv
        chunksize: Records per chunk (default 10K, smaller = faster with existing data)

    Returns:
        Dict with total_records, new_records, skipped_records, and chunks_processed
    """
    from fund_lens_models.bronze import BronzeFECScheduleA
    from prefect import get_run_logger
    from sqlalchemy import select

    logger = get_run_logger()
    logger.info(f"Extracting individual contributions from {data_file} in chunks of {chunksize:,}")
    logger.info("OPTIMIZED MODE: Will skip records that already exist (checking sub_id)")

    extractor = BulkFECContributionExtractor()
    loader = BronzeFECScheduleALoader()

    total_records = 0
    new_records = 0
    skipped_existing = 0
    chunks_processed = 0

    # Process in chunks
    for chunk_df in extractor.extract_chunked(
        file_path=data_file,
        header_file_path=header_file,
        chunksize=chunksize,
    ):
        chunks_processed += 1
        chunk_size = len(chunk_df)
        total_records += chunk_size

        # Check which sub_id values already exist in the database
        with get_session() as session:
            # Get sub_ids from this chunk
            chunk_sub_ids = chunk_df["sub_id"].tolist()

            # Query which ones already exist
            existing_sub_ids_query = select(BronzeFECScheduleA.sub_id).where(
                BronzeFECScheduleA.sub_id.in_(chunk_sub_ids)
            )
            existing_sub_ids = set(session.execute(existing_sub_ids_query).scalars().all())

            # Filter to only new records
            new_records_df = chunk_df[~chunk_df["sub_id"].isin(existing_sub_ids)]
            num_new = len(new_records_df)
            num_existing = chunk_size - num_new

            skipped_existing += num_existing

            # Only load new records (skip UPSERT on existing)
            if num_new > 0:
                records_loaded = loader.load(session, new_records_df, source_system="FEC_BULK")
                new_records += records_loaded
            else:
                records_loaded = 0

        # Log progress every 10 chunks
        if chunks_processed % 10 == 0:
            logger.info(
                f"Progress: {chunks_processed} chunks, "
                f"{total_records:,} processed, "
                f"{new_records:,} new, "
                f"{skipped_existing:,} skipped (already exist)"
            )

    logger.info(
        f"Completed: {chunks_processed} chunks, "
        f"{total_records:,} total processed, "
        f"{new_records:,} new records inserted, "
        f"{skipped_existing:,} existing records skipped"
    )

    return {
        "total_records": total_records,
        "new_records": new_records,
        "skipped_existing": skipped_existing,
        "chunks_processed": chunks_processed,
    }


# ============================================================================
# MAIN BULK INGESTION FLOW
# ============================================================================


@flow(
    name="bulk_data_ingestion",
    description="Load FEC data from bulk files to Bronze layer",
    retries=0,  # Don't retry entire flow, let individual tasks retry
)
def bulk_ingestion_flow(
    data_dir: Path | str,
    election_cycle: int,
    load_committees: bool = True,
    load_candidates: bool = True,
    load_contributions: bool = True,
    contribution_chunksize: int = 10_000,
) -> dict[str, Any]:
    """
    Main bulk file ingestion flow.

    Loads FEC data from bulk files instead of API.
    More reliable for historical backfills.

    Expected directory structure:
        data_dir/
        ├── cm.txt (committees)
        ├── cm_header_file.csv
        ├── cn.txt (candidates)
        ├── cn_header_file.csv
        └── indiv26/
            ├── itcont.txt (individual contributions)
            └── (indiv_header_file.csv should be in parent dir)

    Args:
        data_dir: Path to directory containing bulk files
        election_cycle: Election cycle year (e.g., 2026)
        load_committees: Whether to load committees (default: True)
        load_candidates: Whether to load candidates (default: True)
        load_contributions: Whether to load contributions (default: True)
        contribution_chunksize: Records per chunk for contributions (default: 10K, optimized for skipping existing records)

    Returns:
        Summary statistics for the ingestion

    Example:
        >>> bulk_ingestion_flow(
        ...     data_dir="/Users/trb74/projects/fundlens/fund-lens-etl/data/2025-2026",
        ...     election_cycle=2026,
        ... )
    """
    from prefect import get_run_logger

    logger = get_run_logger()
    data_path = Path(data_dir)

    logger.info("=" * 80)
    logger.info("BULK FILE INGESTION - BRONZE LAYER")
    logger.info("=" * 80)
    logger.info(f"Data directory: {data_path}")
    logger.info(f"Election cycle: {election_cycle}")
    logger.info(f"Load committees: {load_committees}")
    logger.info(f"Load candidates: {load_candidates}")
    logger.info(f"Load contributions: {load_contributions}")
    logger.info("=" * 80)

    results = {
        "election_cycle": election_cycle,
        "data_directory": str(data_path),
    }

    # ========================================================================
    # STEP 1: Load Committees
    # ========================================================================
    if load_committees:
        logger.info("\nStep 1: Loading committees from bulk file...")
        committee_file = data_path / "cm.txt"
        committee_header = data_path / "cm_header_file.csv"

        if not committee_file.exists():
            logger.error(f"Committee file not found: {committee_file}")
            results["committees_loaded"] = 0
        elif not committee_header.exists():
            logger.error(f"Committee header file not found: {committee_header}")
            results["committees_loaded"] = 0
        else:
            committees_loaded = extract_committees_from_bulk_task(
                data_file=committee_file,
                header_file=committee_header,
            )
            results["committees_loaded"] = committees_loaded
            logger.info(f"✓ Loaded {committees_loaded:,} committees")
    else:
        logger.info("Skipping committees (load_committees=False)")
        results["committees_loaded"] = 0

    # ========================================================================
    # STEP 2: Load Candidates
    # ========================================================================
    if load_candidates:
        logger.info("\nStep 2: Loading candidates from bulk file...")
        candidate_file = data_path / "cn.txt"
        candidate_header = data_path / "cn_header_file.csv"

        if not candidate_file.exists():
            logger.error(f"Candidate file not found: {candidate_file}")
            results["candidates_loaded"] = 0
        elif not candidate_header.exists():
            logger.error(f"Candidate header file not found: {candidate_header}")
            results["candidates_loaded"] = 0
        else:
            candidates_loaded = extract_candidates_from_bulk_task(
                data_file=candidate_file,
                header_file=candidate_header,
            )
            results["candidates_loaded"] = candidates_loaded
            logger.info(f"✓ Loaded {candidates_loaded:,} candidates")
    else:
        logger.info("Skipping candidates (load_candidates=False)")
        results["candidates_loaded"] = 0

    # ========================================================================
    # STEP 3: Load Individual Contributions
    # ========================================================================
    if load_contributions:
        logger.info("\nStep 3: Loading individual contributions from bulk file...")
        logger.info("This may take 1-2 hours for ~9.2M records...")

        # Contributions are in indiv26 subdirectory
        contribution_file = data_path / f"indiv{election_cycle % 100}" / "itcont.txt"
        # Header is in parent directory
        contribution_header = data_path / "indiv_header_file.csv"

        if not contribution_file.exists():
            logger.error(f"Contribution file not found: {contribution_file}")
            results["contributions_loaded"] = 0
            results["contribution_chunks"] = 0
        elif not contribution_header.exists():
            logger.error(f"Contribution header file not found: {contribution_header}")
            results["contributions_loaded"] = 0
            results["contribution_chunks"] = 0
        else:
            contribution_results = extract_contributions_from_bulk_task(
                data_file=contribution_file,
                header_file=contribution_header,
                chunksize=contribution_chunksize,
            )
            results["contributions_loaded"] = contribution_results["new_records"]
            results["contributions_skipped"] = contribution_results["skipped_existing"]
            results["contribution_chunks"] = contribution_results["chunks_processed"]
            logger.info(
                f"✓ Processed {contribution_results['total_records']:,} contributions: "
                f"{contribution_results['new_records']:,} new, "
                f"{contribution_results['skipped_existing']:,} skipped (already exist) "
                f"in {contribution_results['chunks_processed']} chunks"
            )
    else:
        logger.info("Skipping contributions (load_contributions=False)")
        results["contributions_loaded"] = 0
        results["contribution_chunks"] = 0

    # ========================================================================
    # Summary
    # ========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("BULK FILE INGESTION COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Committees loaded: {results.get('committees_loaded', 0):,}")
    logger.info(f"Candidates loaded: {results.get('candidates_loaded', 0):,}")
    logger.info(
        f"Contributions loaded: {results.get('contributions_loaded', 0):,} "
        f"({results.get('contribution_chunks', 0)} chunks)"
    )
    logger.info("=" * 80)

    return results
