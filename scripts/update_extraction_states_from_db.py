#!/usr/bin/env python3
"""
Update extraction states for committees based on existing data in the database.

This script is useful when:
1. You've already loaded bulk data but extraction states weren't updated
2. You need to rebuild extraction states after a data migration
3. Extraction states got out of sync with actual data

It queries bronze_fec_schedule_a for each committee's latest contribution
and updates bronze_fec_extraction_state accordingly.
"""

import argparse
import logging
import sys
from datetime import datetime

from fund_lens_etl.database import get_session
from fund_lens_etl.utils.extraction_state import (
    get_last_contribution_info,
    update_extraction_state,
)
from fund_lens_models.bronze import BronzeFECScheduleA
from sqlalchemy import distinct, select


def setup_logging() -> str:
    """Setup logging to both console and file in /tmp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"/tmp/update_extraction_states_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )

    return log_file


logger = logging.getLogger(__name__)


def get_all_committees_with_contributions(election_cycle: int) -> list[str]:
    """
    Get all unique committee IDs that have contributions in the database.

    Args:
        election_cycle: Election cycle year

    Returns:
        List of committee IDs
    """
    with get_session() as session:
        stmt = (
            select(distinct(BronzeFECScheduleA.committee_id))
            .where(BronzeFECScheduleA.two_year_transaction_period == election_cycle)
            .where(BronzeFECScheduleA.committee_id.isnot(None))
        )
        committee_ids = list(session.execute(stmt).scalars().all())

    logger.info(f"Found {len(committee_ids):,} unique committees with contributions")
    return committee_ids


def update_extraction_states(
    committee_ids: list[str],
    election_cycle: int,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Update extraction states for a list of committees.

    Args:
        committee_ids: List of committee IDs
        election_cycle: Election cycle year
        dry_run: If True, log what would be done but don't update

    Returns:
        Dict with statistics
    """
    updated_count = 0
    skipped_count = 0
    total = len(committee_ids)

    with get_session() as session:
        for i, committee_id in enumerate(committee_ids, 1):
            # Get the latest contribution info for this committee
            last_contrib_info = get_last_contribution_info(
                session=session,
                committee_id=committee_id,
                election_cycle=election_cycle,
            )

            if last_contrib_info:
                last_date, last_sub_id = last_contrib_info

                if dry_run:
                    logger.info(
                        f"[DRY RUN] Would update {committee_id}: "
                        f"last_date={last_date}, last_sub_id={last_sub_id}"
                    )
                else:
                    # Update extraction state
                    update_extraction_state(
                        session=session,
                        committee_id=committee_id,
                        election_cycle=election_cycle,
                        last_contribution_date=last_date,
                        last_sub_id=last_sub_id,
                        total_records_extracted=0,  # Don't count in extraction total
                        extraction_start_date=None,  # None indicates full load
                        extraction_end_date=None,
                        is_complete=True,
                        last_page_processed=0,
                    )

                updated_count += 1
            else:
                logger.warning(
                    f"No contributions found for {committee_id} (cycle {election_cycle})"
                )
                skipped_count += 1

            # Log progress every 100 committees
            if i % 100 == 0:
                logger.info(
                    f"Progress: {i}/{total} ({i*100//total}%) - "
                    f"{updated_count:,} updated, {skipped_count:,} skipped"
                )

    return {
        "updated": updated_count,
        "skipped": skipped_count,
        "total": total,
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Update extraction states based on existing database data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update all committees for 2026 cycle
  %(prog)s --cycle 2026

  # Dry run to see what would be updated
  %(prog)s --cycle 2026 --dry-run

  # Update specific committees only
  %(prog)s --cycle 2026 --committees C00784934 C00401224

  # Limit to first 10 committees (for testing)
  %(prog)s --cycle 2026 --limit 10
        """,
    )
    parser.add_argument(
        "--cycle",
        type=int,
        required=True,
        help="Election cycle year (e.g., 2026)",
    )
    parser.add_argument(
        "--committees",
        nargs="+",
        help="Specific committee IDs to update (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit to first N committees (for testing)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without making changes",
    )

    return parser.parse_args()


def main():
    # Setup logging
    log_file = setup_logging()
    logger.info(f"Logging to: {log_file}")
    logger.info(f"Stream logs with: tail -f {log_file}\n")

    args = parse_args()

    logger.info("=" * 80)
    logger.info("UPDATE EXTRACTION STATES FROM DATABASE")
    logger.info("=" * 80)
    logger.info(f"Election cycle: {args.cycle}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("=" * 80)
    logger.info("")

    # Get committee IDs to process
    if args.committees:
        committee_ids = args.committees
        logger.info(f"Processing {len(committee_ids)} specified committees")
    else:
        logger.info("Finding all committees with contributions...")
        committee_ids = get_all_committees_with_contributions(args.cycle)

    # Apply limit if specified
    if args.limit:
        committee_ids = committee_ids[: args.limit]
        logger.info(f"Limited to first {args.limit} committees")

    if not committee_ids:
        logger.warning("No committees found to process")
        return

    # Update extraction states
    logger.info(f"\nUpdating extraction states for {len(committee_ids):,} committees...")
    results = update_extraction_states(
        committee_ids=committee_ids,
        election_cycle=args.cycle,
        dry_run=args.dry_run,
    )

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total committees: {results['total']:,}")
    logger.info(f"Updated: {results['updated']:,}")
    logger.info(f"Skipped: {results['skipped']:,}")
    logger.info("=" * 80)

    if args.dry_run:
        logger.info("\nThis was a DRY RUN - no changes were made")
        logger.info("Run without --dry-run to actually update the database")
    else:
        logger.info("\n✅ Extraction states updated successfully!")
        logger.info(
            "\nNext incremental extraction will use these dates as the baseline "
            "(with 180-day lookback by default)"
        )

    logger.info(f"\nLog file: {log_file}")


if __name__ == "__main__":
    main()
