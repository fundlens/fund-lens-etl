"""
Monthly Bulk Reconciliation Flow

Downloads fresh bulk data and reconciles with existing data to catch:
- Late amendments to previous filings
- Corrections and updates from FEC
- Any data that was missed in daily incremental runs

This should be run monthly as a reconciliation mechanism in addition to
daily incremental API extractions.
"""

import sys
from datetime import date
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task

from fund_lens_etl.flows.bulk_ingestion_flow import bulk_ingestion_flow

# Add scripts directory to path to import download functions
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from download_bulk_data import ESSENTIAL_FILES, download_bulk_file  # noqa: E402


@task(
    name="download_bulk_data",
    retries=2,
    retry_delay_seconds=300,  # 5 minutes between retries
    timeout_seconds=3600,  # 1 hour timeout for download
)
def download_bulk_data_task(
    cycle: int,
    output_dir: Path,
) -> dict[str, Any]:
    """
    Download FEC bulk data files for the specified cycle.

    Args:
        cycle: Election cycle year (e.g., 2026)
        output_dir: Directory to save downloaded files

    Returns:
        Dict with download results
    """
    logger = get_run_logger()
    logger.info(f"Downloading bulk data for cycle {cycle}...")
    logger.info(f"Output directory: {output_dir}")

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Download essential files (committees, candidates, contributions)
    success_count = 0
    failed_files = []

    for file_type in ESSENTIAL_FILES:
        logger.info(f"Downloading {file_type}...")
        try:
            success = download_bulk_file(
                file_type=file_type,
                cycle=cycle,
                output_dir=output_dir,
                extract=True,
                keep_zip=False,
            )
            if success:
                success_count += 1
            else:
                failed_files.append(file_type)
        except Exception as e:
            logger.error(f"Failed to download {file_type}: {e}")
            failed_files.append(file_type)

    if failed_files:
        raise RuntimeError(
            f"Failed to download {len(failed_files)} files: {', '.join(failed_files)}"
        )

    logger.info(f"Download completed successfully ({success_count} files)")

    return {
        "success": True,
        "cycle": cycle,
        "output_dir": str(output_dir),
        "files_downloaded": success_count,
    }


@task(
    name="cleanup_bulk_data",
    retries=0,
)
def cleanup_bulk_data_task(data_dir: Path) -> None:
    """
    Clean up downloaded bulk data files after ingestion.

    Args:
        data_dir: Directory containing bulk files to clean up
    """
    logger = get_run_logger()
    logger.info(f"Cleaning up bulk data in {data_dir}...")

    try:
        import shutil

        if data_dir.exists():
            shutil.rmtree(data_dir)
            logger.info(f"✓ Removed {data_dir}")
        else:
            logger.warning(f"Directory not found: {data_dir}")

    except Exception as e:
        logger.warning(f"Failed to clean up {data_dir}: {e}")
        # Don't fail the flow if cleanup fails


@flow(
    name="monthly-bulk-reconciliation",
    description="Monthly bulk data download and reconciliation",
    retries=0,
)
def monthly_bulk_reconciliation_flow(
    election_cycle: int = 2026,
    data_dir: str | None = None,
    cleanup_after: bool = True,
) -> dict[str, Any]:
    """
    Monthly bulk reconciliation flow.

    Downloads fresh bulk data from FEC and reconciles with existing data.
    This catches any amendments, corrections, or missed data from daily runs.

    Args:
        election_cycle: Election cycle year (default: 2026)
        data_dir: Directory for bulk files (default: /opt/fund-lens-etl/data/{year}-{year+1})
        cleanup_after: Whether to clean up downloaded files after ingestion (default: True)

    Returns:
        Summary of the reconciliation results
    """
    logger = get_run_logger()

    # Determine data directory
    if data_dir is None:
        data_dir = f"/opt/fund-lens-etl/data/{election_cycle-1}-{election_cycle}"

    data_path = Path(data_dir)

    logger.info("=" * 80)
    logger.info("MONTHLY BULK RECONCILIATION")
    logger.info("=" * 80)
    logger.info(f"Election cycle: {election_cycle}")
    logger.info(f"Data directory: {data_path}")
    logger.info(f"Date: {date.today()}")
    logger.info(f"Cleanup after: {cleanup_after}")
    logger.info("=" * 80)

    # Step 1: Download bulk data
    logger.info("\nStep 1: Downloading bulk data...")
    download_result = download_bulk_data_task(
        cycle=election_cycle,
        output_dir=data_path,
    )

    # Step 2: Ingest bulk data
    logger.info("\nStep 2: Ingesting bulk data...")
    ingestion_result = bulk_ingestion_flow(
        data_dir=data_path,
        election_cycle=election_cycle,
        load_committees=True,
        load_candidates=True,
        load_contributions=True,
        contribution_types=["indiv"],  # Only individual contributions for reconciliation
        contribution_chunksize=10_000,
    )

    # Step 3: Cleanup
    if cleanup_after:
        logger.info("\nStep 3: Cleaning up downloaded files...")
        cleanup_bulk_data_task(data_path)
    else:
        logger.info("\nSkipping cleanup (cleanup_after=False)")

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("MONTHLY RECONCILIATION COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Committees updated: {ingestion_result.get('committees_loaded', 0):,}")
    logger.info(f"Candidates updated: {ingestion_result.get('candidates_loaded', 0):,}")
    logger.info(
        f"Contributions processed: {ingestion_result.get('contributions_loaded', 0):,} new, "
        f"{ingestion_result.get('contributions_skipped', 0):,} skipped"
    )
    logger.info(
        f"Extraction states updated: {ingestion_result.get('extraction_states_updated', 0):,}"
    )
    logger.info("=" * 80)

    return {
        "election_cycle": election_cycle,
        "date": str(date.today()),
        "download": download_result,
        "ingestion": ingestion_result,
        "cleanup_completed": cleanup_after,
    }


if __name__ == "__main__":
    # For testing
    monthly_bulk_reconciliation_flow(election_cycle=2026, cleanup_after=False)
