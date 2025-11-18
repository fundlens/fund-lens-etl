#!/usr/bin/env python3
"""
Retry script for specific states with flexible targeting.

Usage:
    # Retry specific states
    poetry run python scripts/retry_states.py --states AZ CA FL TX --cycle 2026

    # Retry with full refresh (default)
    poetry run python scripts/retry_states.py --states NY VA --cycle 2026 --full-refresh

    # Retry with incremental (90-day lookback)
    poetry run python scripts/retry_states.py --states CA --cycle 2026 --no-full-refresh

Examples:
    # Current failed states from Nov 18 log
    poetry run python scripts/retry_states.py \
        --states AZ CA CT FL GA LA MN MT NY SC TN TX VA DC \
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
    log_file = f"/tmp/retry_states_{timestamp}.log"

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
        description="Retry bronze ingestion for specific states",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Retry the 14 failed states from Nov 18
  %(prog)s --states AZ CA CT FL GA LA MN MT NY SC TN TX VA DC --cycle 2026

  # Retry just California
  %(prog)s --states CA --cycle 2026

  # Retry multiple states with incremental mode
  %(prog)s --states NY TX FL --cycle 2026 --no-full-refresh

Note: Silver and gold transformations will run separately on ALL states later.
        """,
    )
    parser.add_argument(
        "--states",
        nargs="+",
        required=True,
        help="State codes to retry (e.g., AZ CA FL TX). Use two-letter state codes.",
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
        default=True,
        help="Use full refresh mode (default: True)",
    )
    parser.add_argument(
        "--no-full-refresh",
        dest="full_refresh",
        action="store_false",
        help="Use incremental mode (90-day lookback) instead of full refresh",
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


def run_bronze_ingest(states: list[USState], election_cycle: int, full_refresh: bool):
    """Run bronze ingestion for specified states."""
    logger.info("=" * 80)
    logger.info("BRONZE INGESTION - STATE RETRY")
    logger.info("=" * 80)
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Election Cycle: {election_cycle}")
    logger.info(f"States: {', '.join([s.value for s in states])} ({len(states)} total)")
    logger.info(f"Full Refresh: {full_refresh}")
    logger.info("=" * 80)

    failed_states = []
    successful_states = []
    results = {}

    for i, state in enumerate(states, 1):
        logger.info(f"\n[{i}/{len(states)}] Processing {state.value} ({state.name})...")
        logger.info(f"Time: {datetime.now().strftime('%H:%M:%S')}")

        try:
            result = bronze_ingestion_flow(
                state=state,
                election_cycle=election_cycle,
                full_refresh=full_refresh,
            )

            results[state.value] = result
            successful_states.append(state.value)

            logger.info(f"✓ {state.value} COMPLETED:")
            logger.info(f"  - Committees: {result['committees_loaded']}")
            logger.info(f"  - Candidates: {result['candidates_loaded']}")
            logger.info(f"  - Contributions: {result['total_contributions']:,}")

        except Exception as e:
            logger.error(f"✗ {state.value} FAILED: {e}")
            failed_states.append(state.value)
            results[state.value] = {"error": str(e)}

    logger.info("\n" + "=" * 80)
    logger.info("BRONZE INGESTION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Successful: {len(successful_states)}/{len(states)}")
    logger.info(f"Failed: {len(failed_states)}/{len(states)}")

    if successful_states:
        logger.info(f"\n✓ Successful states: {', '.join(successful_states)}")
        total_contributions = sum(
            r['total_contributions']
            for s, r in results.items()
            if s in successful_states and 'total_contributions' in r
        )
        logger.info(f"  Total contributions loaded: {total_contributions:,}")

    if failed_states:
        logger.warning(f"\n✗ Failed states: {', '.join(failed_states)}")
        logger.info("\nRetry command for failed states:")
        logger.info(f"  poetry run python scripts/retry_states.py \\")
        logger.info(f"    --states {' '.join(failed_states)} \\")
        logger.info(f"    --cycle {election_cycle}")

    logger.info("=" * 80)

    return successful_states, failed_states


def main():
    # Setup logging first
    log_file = setup_logging()
    logger.info(f"Logging to: {log_file}")
    logger.info(f"Stream logs with: tail -f {log_file}\n")

    args = parse_args()

    # Parse state codes to USState enums
    states = [parse_state(code) for code in args.states]

    # Run bronze ingestion
    bronze_success, bronze_failed = run_bronze_ingest(
        states=states,
        election_cycle=args.cycle,
        full_refresh=args.full_refresh,
    )

    # Final summary
    logger.info("\n" + "=" * 80)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Bronze - Success: {len(bronze_success)}, Failed: {len(bronze_failed)}")
    logger.info("=" * 80)
    logger.info(f"\nLog file: {log_file}")

    # Exit with error code if any failures
    if bronze_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
