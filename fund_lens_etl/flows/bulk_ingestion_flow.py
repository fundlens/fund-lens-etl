"""
Bulk File Ingestion Flow

Loads FEC data from bulk files instead of API.
More reliable for historical backfills - no API timeouts or rate limiting.

Bulk files are downloaded from: https://www.fec.gov/data/browse-data/?tab=bulk-data
"""

from pathlib import Path
from typing import Any, TypedDict

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


# Type definitions
class ContributionFileInfo(TypedDict):
    """Type definition for contribution file mapping."""

    file: Path
    header: Path
    description: str
    estimated_records: str


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
    election_cycle: int,
) -> int:
    """
    Extract and load committees from bulk file.

    Args:
        data_file: Path to cm.txt
        header_file: Path to cm_header_file.csv
        election_cycle: Election cycle year (e.g., 2026)

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
        election_cycle=election_cycle,
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
    election_cycle: int,
) -> int:
    """
    Extract and load candidates from bulk file.

    Args:
        data_file: Path to cn.txt
        header_file: Path to cn_header_file.csv
        election_cycle: Election cycle year (e.g., 2026)

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
        election_cycle=election_cycle,
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
    election_cycle: int,
    file_type: str = "indiv",
    chunksize: int = 10_000,
) -> dict[str, int]:
    """
    Extract and load contributions from bulk file in chunks.

    Supports all FEC contribution file types:
    - indiv (itcont.txt): Individual → Committee (~9M records, 1.6GB)
    - pas2 (itpas2.txt): Committee → Candidate (~100K records, 12MB)
    - oth (itoth.txt): Committee → Committee (~millions of records, 733MB)

    Contribution files can be very large. Processes in chunks to avoid memory issues.

    OPTIMIZED: Checks which sub_id values already exist and only inserts new records.
    This avoids slow UPSERT conflicts on millions of existing records.

    Args:
        data_file: Path to contribution file (itcont.txt, itpas2.txt, or itoth.txt)
        header_file: Path to corresponding header file
        election_cycle: Election cycle year (e.g., 2026) to populate two_year_transaction_period
        file_type: Type of contribution file (indiv, pas2, or oth)
        chunksize: Records per chunk (default 10K, smaller = faster with existing data)

    Returns:
        Dict with total_records, new_records, skipped_records, and chunks_processed
    """
    from fund_lens_models.bronze import BronzeFECScheduleA
    from prefect import get_run_logger
    from sqlalchemy import select

    logger = get_run_logger()
    logger.info(f"Extracting {file_type} contributions from {data_file} in chunks of {chunksize:,}")
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
        election_cycle=election_cycle,
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
    contribution_types: list[str] | None = None,
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
        ├── indiv26/
        │   └── itcont.txt (individual contributions)
        ├── indiv_header_file.csv
        ├── itpas2.txt (committee→candidate contributions)
        ├── pas2_header_file.csv
        ├── itoth.txt (committee→committee contributions)
        └── oth_header_file.csv

    Args:
        data_dir: Path to directory containing bulk files
        election_cycle: Election cycle year (e.g., 2026)
        load_committees: Whether to load committees (default: True)
        load_candidates: Whether to load candidates (default: True)
        load_contributions: Whether to load contributions (default: True)
        contribution_types: List of contribution types to load (default: ["indiv", "pas2", "oth"])
                           Options: "indiv" (individual), "pas2" (committee→candidate), "oth" (committee→committee)
        contribution_chunksize: Records per chunk for contributions (default: 10K, optimized for skipping existing records)

    Returns:
        Summary statistics for the ingestion

    Example:
        >>> # Load all contribution types
        >>> bulk_ingestion_flow(
        ...     data_dir="/Users/trb74/projects/fundlens/fund-lens-etl/data/2025-2026",
        ...     election_cycle=2026,
        ... )
        >>> # Load only individual contributions
        >>> bulk_ingestion_flow(
        ...     data_dir="/Users/trb74/projects/fundlens/fund-lens-etl/data/2025-2026",
        ...     election_cycle=2026,
        ...     contribution_types=["indiv"],
        ... )
    """
    from prefect import get_run_logger

    logger = get_run_logger()
    data_path = Path(data_dir)

    # Default to loading all contribution types
    if contribution_types is None:
        contribution_types = ["indiv", "pas2", "oth"]

    logger.info("=" * 80)
    logger.info("BULK FILE INGESTION - BRONZE LAYER")
    logger.info("=" * 80)
    logger.info(f"Data directory: {data_path}")
    logger.info(f"Election cycle: {election_cycle}")
    logger.info(f"Load committees: {load_committees}")
    logger.info(f"Load candidates: {load_candidates}")
    logger.info(f"Load contributions: {load_contributions}")
    if load_contributions:
        logger.info(f"Contribution types: {', '.join(contribution_types)}")
    logger.info("=" * 80)

    results: dict[str, Any] = {
        "election_cycle": election_cycle,
        "data_directory": str(data_path),
        "contribution_types_processed": [],
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
                election_cycle=election_cycle,
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
                election_cycle=election_cycle,
            )
            results["candidates_loaded"] = candidates_loaded
            logger.info(f"✓ Loaded {candidates_loaded:,} candidates")
    else:
        logger.info("Skipping candidates (load_candidates=False)")
        results["candidates_loaded"] = 0

    # ========================================================================
    # STEP 3: Load Contributions (All Types)
    # ========================================================================
    if load_contributions:
        logger.info("\nStep 3: Loading contributions from bulk files...")

        # Define contribution file mappings
        contribution_file_mapping: dict[str, ContributionFileInfo] = {
            "indiv": {
                "file": data_path / f"indiv{election_cycle % 100}" / "itcont.txt",
                "header": data_path / "indiv_header_file.csv",
                "description": "Individual → Committee",
                "estimated_records": "~9M",
            },
            "pas2": {
                "file": data_path / "itpas2.txt",
                "header": data_path / "pas2_header_file.csv",
                "description": "Committee → Candidate",
                "estimated_records": "~100K",
            },
            "oth": {
                "file": data_path / "itoth.txt",
                "header": data_path / "oth_header_file.csv",
                "description": "Committee → Committee",
                "estimated_records": "~millions",
            },
        }

        total_contributions_loaded = 0
        total_contributions_skipped = 0
        total_chunks = 0

        for contrib_type in contribution_types:
            if contrib_type not in contribution_file_mapping:
                logger.warning(f"Unknown contribution type: {contrib_type}. Skipping.")
                continue

            file_info = contribution_file_mapping[contrib_type]
            logger.info(
                f"\n  Processing {contrib_type} contributions "
                f"({file_info['description']}, {file_info['estimated_records']} records)..."
            )

            contribution_file = file_info["file"]
            contribution_header = file_info["header"]

            if not contribution_file.exists():
                logger.warning(f"  File not found: {contribution_file}. Skipping {contrib_type}.")
                continue
            elif not contribution_header.exists():
                logger.warning(
                    f"  Header not found: {contribution_header}. Skipping {contrib_type}."
                )
                continue

            contribution_results = extract_contributions_from_bulk_task(
                data_file=contribution_file,
                header_file=contribution_header,
                election_cycle=election_cycle,
                file_type=contrib_type,
                chunksize=contribution_chunksize,
            )

            total_contributions_loaded += contribution_results["new_records"]
            total_contributions_skipped += contribution_results["skipped_existing"]
            total_chunks += contribution_results["chunks_processed"]
            results["contribution_types_processed"].append(contrib_type)

            logger.info(
                f"  ✓ {contrib_type}: {contribution_results['total_records']:,} processed, "
                f"{contribution_results['new_records']:,} new, "
                f"{contribution_results['skipped_existing']:,} skipped, "
                f"{contribution_results['chunks_processed']} chunks"
            )

        results["contributions_loaded"] = total_contributions_loaded
        results["contributions_skipped"] = total_contributions_skipped
        results["contribution_chunks"] = total_chunks

        logger.info(
            f"\n✓ Total contributions: {total_contributions_loaded:,} loaded, "
            f"{total_contributions_skipped:,} skipped, "
            f"{total_chunks} chunks"
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
