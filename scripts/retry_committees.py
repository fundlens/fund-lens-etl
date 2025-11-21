#!/usr/bin/env python3
"""
Retry script for specific committees with flexible targeting.

Usage:
    # Retry specific committees
    poetry run python scripts/retry_committees.py --committee-ids C00503052 C00213512 --state CA --cycle 2026

    # Retry with exact resume from checkpoint (0-day lookback, default)
    poetry run python scripts/retry_committees.py --committee-ids C00503052 --state CA --cycle 2026 --lookback-days 0

    # Retry with lookback window (e.g., 180 days)
    poetry run python scripts/retry_committees.py --committee-ids C00503052 --state CA --cycle 2026 --lookback-days 180

    # Retry with full refresh (ignores checkpoint)
    poetry run python scripts/retry_committees.py --committee-ids C00503052 --state CA --cycle 2026 --full-refresh

Examples:
    # Retry failed committees from warning message
    poetry run python scripts/retry_committees.py \
        --committee-ids C00503052 C00213512 \
        --state CA \
        --cycle 2026
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fund_lens_models.enums import USState
from fund_lens_etl.flows.bronze_ingestion_flow import bronze_ingestion_flow


# Setup logging
def setup_logging() -> str:
    """Setup logging to both console and file in /tmp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"/tmp/retry_committees_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return log_file


logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Retry bronze ingestion for specific committees",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Retry specific committees (will auto-resume from checkpoint)
  %(prog)s --committee-ids C00503052 C00213512 --state CA --cycle 2026

  # Retry with exact resume from last checkpoint (0-day lookback)
  %(prog)s --committee-ids C00503052 --state CA --cycle 2026 --lookback-days 0

  # Retry with lookback window to catch amendments
  %(prog)s --committee-ids C00503052 --state CA --cycle 2026 --lookback-days 180

  # Force full refresh (ignores checkpoint and extracts all data)
  %(prog)s --committee-ids C00503052 --state CA --cycle 2026 --full-refresh

Note: By default, uses incremental mode with 0-day lookback (exact resume from checkpoint).
        """,
    )
    parser.add_argument(
        "--committee-ids",
        nargs="+",
        required=True,
        help="Committee IDs to retry (e.g., C00503052 C00213512)",
    )
    parser.add_argument(
        "--state",
        required=True,
        help="State code (e.g., CA, NY, TX). Use two-letter state codes.",
    )
    parser.add_argument(
        "--cycle",
        type=int,
        required=True,
        help="Election cycle year (e.g., 2026)",
    )
    parser.add_argument(
        "--full-refresh",
        dest="full_refresh",
        action="store_true",
        default=False,
        help="Use full refresh mode (ignores checkpoint, extracts all data)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Days to look back from last extraction (default: 0 for exact retry from checkpoint)",
    )

    return parser.parse_args()


def parse_state(state_code: str) -> USState:
    """Convert state code string to USState enum."""
    try:
        # Handle both "CA" and "CALIFORNIA" formats
        state_code_upper = state_code.upper().strip()
        # Access enum member by name (e.g., "CA" -> USState.CA)
        return getattr(USState, state_code_upper)
    except AttributeError:
        logger.error(f"✗ Invalid state code: {state_code}")
        logger.error(f"Valid codes: {', '.join([s.name for s in USState])}")
        sys.exit(1)


def run_committee_retry(
    committee_ids: list[str],
    state: USState,
    election_cycle: int,
    full_refresh: bool,
    lookback_days: int,
):
    """Run bronze ingestion for specified committees."""
    logger.info("=" * 80)
    logger.info("BRONZE INGESTION - COMMITTEE RETRY")
    logger.info("=" * 80)
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"State: {state.value}")
    logger.info(f"Election Cycle: {election_cycle}")
    logger.info(f"Committees: {', '.join(committee_ids)} ({len(committee_ids)} total)")
    logger.info(f"Full Refresh: {full_refresh}")
    logger.info(f"Lookback Days: {lookback_days}")
    logger.info("=" * 80)

    try:
        result = bronze_ingestion_flow(
            state=state,
            election_cycle=election_cycle,
            committee_ids=committee_ids,
            full_refresh=full_refresh,
            lookback_days=lookback_days,
        )

        logger.info("\n" + "=" * 80)
        logger.info("COMMITTEE RETRY SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Committees Processed: {result['committees_processed']}")
        logger.info(f"Total Contributions: {result['total_contributions']:,}")
        logger.info(f"Total Pages: {result['total_pages']:,}")

        if result.get('retries_attempted', 0) > 0:
            logger.info(f"\nRetry Statistics:")
            logger.info(f"  - Retries Attempted: {result['retries_attempted']}")
            logger.info(f"  - Retries Successful: {result['retries_successful']}")
            logger.info(f"  - Still Failed: {result['retries_still_failed']}")

        if result.get('still_incomplete_committees'):
            logger.warning("\n⚠️  Some committees are still incomplete:")
            for committee in result['still_incomplete_committees']:
                logger.warning(f"  - {committee['committee_name']} ({committee['committee_id']})")
                logger.warning(f"    Records: {committee['total_records']:,}, Pages: {committee['pages_processed']}")

            logger.warning("\n💡 To retry again, run:")
            logger.warning(f"  poetry run python scripts/retry_committees.py \\")
            logger.warning(f"    --committee-ids {' '.join(c['committee_id'] for c in result['still_incomplete_committees'])} \\")
            logger.warning(f"    --state {state.name} \\")
            logger.warning(f"    --cycle {election_cycle}")

        logger.info("=" * 80)

        return result

    except Exception as e:
        logger.error(f"\n✗ RETRY FAILED: {e}")
        logger.exception("Full traceback:")
        raise


def main():
    # Setup logging first
    log_file = setup_logging()
    logger.info(f"Logging to: {log_file}")
    logger.info(f"Stream logs with: tail -f {log_file}\n")

    args = parse_args()

    # Parse state code to USState enum
    state = parse_state(args.state)

    # Run committee retry
    result = run_committee_retry(
        committee_ids=args.committee_ids,
        state=state,
        election_cycle=args.cycle,
        full_refresh=args.full_refresh,
        lookback_days=args.lookback_days,
    )

    # Final summary
    logger.info(f"\nLog file: {log_file}")

    # Exit with error code if any committees still incomplete
    if result.get('retries_still_failed', 0) > 0:
        logger.error(f"\n⚠️  {result['retries_still_failed']} committee(s) still incomplete")
        sys.exit(1)


if __name__ == "__main__":
    main()
