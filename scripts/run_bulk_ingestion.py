#!/usr/bin/env python3
"""
Run bulk file ingestion flow.

Loads FEC data from bulk files to Bronze layer.
More reliable than API for historical backfills (no timeouts).

Usage:
    # Load all data types
    poetry run python scripts/run_bulk_ingestion.py --data-dir data/2025-2026 --cycle 2026

    # Load only committees and candidates (skip contributions)
    poetry run python scripts/run_bulk_ingestion.py --data-dir data/2025-2026 --cycle 2026 --no-contributions

    # Load only contributions (if committees/candidates already loaded)
    poetry run python scripts/run_bulk_ingestion.py --data-dir data/2025-2026 --cycle 2026 --only-contributions

    # Use smaller chunk size (for memory-constrained systems)
    poetry run python scripts/run_bulk_ingestion.py --data-dir data/2025-2026 --cycle 2026 --chunksize 50000
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fund_lens_etl.flows.bulk_ingestion_flow import bulk_ingestion_flow


# Setup logging
def setup_logging() -> str:
    """Setup logging to both console and file in /tmp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"/tmp/bulk_ingestion_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )

    return log_file


logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load FEC bulk data files to Bronze layer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load all data types for 2026 cycle
  %(prog)s --data-dir data/2025-2026 --cycle 2026

  # Load only committees and candidates (skip large contribution file)
  %(prog)s --data-dir data/2025-2026 --cycle 2026 --no-contributions

  # Load only contributions (if committees/candidates already exist)
  %(prog)s --data-dir data/2025-2026 --cycle 2026 --only-contributions

  # Use smaller chunk size to reduce memory usage
  %(prog)s --data-dir data/2025-2026 --cycle 2026 --chunksize 50000

Expected directory structure:
  data_dir/
  ├── cm.txt (committees)
  ├── cm_header_file.csv
  ├── cn.txt (candidates)
  ├── cn_header_file.csv
  ├── indiv_header_file.csv
  └── indiv26/
      └── itcont.txt (individual contributions)
        """,
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Directory containing bulk data files",
    )
    parser.add_argument(
        "--cycle",
        type=int,
        required=True,
        help="Election cycle year (e.g., 2026)",
    )
    parser.add_argument(
        "--no-contributions",
        action="store_true",
        help="Skip loading contributions (only load committees and candidates)",
    )
    parser.add_argument(
        "--only-contributions",
        action="store_true",
        help="Only load contributions (skip committees and candidates)",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=100_000,
        help="Records per chunk for contributions (default: 100,000)",
    )

    return parser.parse_args()


def main():
    # Setup logging first
    log_file = setup_logging()
    logger.info(f"Logging to: {log_file}")
    logger.info(f"Stream logs with: tail -f {log_file}\n")

    args = parse_args()

    # Validate data directory exists
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        sys.exit(1)

    # Determine what to load
    if args.only_contributions and args.no_contributions:
        logger.error("Cannot use both --only-contributions and --no-contributions")
        sys.exit(1)

    load_committees = not args.only_contributions
    load_candidates = not args.only_contributions
    load_contributions = not args.no_contributions

    logger.info("=" * 80)
    logger.info("BULK FILE INGESTION")
    logger.info("=" * 80)
    logger.info(f"Data directory: {data_dir}")
    logger.info(f"Election cycle: {args.cycle}")
    logger.info(f"Load committees: {load_committees}")
    logger.info(f"Load candidates: {load_candidates}")
    logger.info(f"Load contributions: {load_contributions}")
    if load_contributions:
        logger.info(f"Contribution chunk size: {args.chunksize:,}")
    logger.info("=" * 80)
    logger.info("")

    # Estimate time
    if load_contributions:
        logger.info("⏱️  Estimated time: 1-3 hours (depending on chunk size and system)")
        logger.info("   You can monitor progress in the log file or Prefect UI")
        logger.info("")

    # Run the flow
    try:
        result = bulk_ingestion_flow(
            data_dir=data_dir,
            election_cycle=args.cycle,
            load_committees=load_committees,
            load_candidates=load_candidates,
            load_contributions=load_contributions,
            contribution_chunksize=args.chunksize,
        )

        logger.info("\n" + "=" * 80)
        logger.info("BULK INGESTION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Committees loaded: {result.get('committees_loaded', 0):,}")
        logger.info(f"Candidates loaded: {result.get('candidates_loaded', 0):,}")
        logger.info(f"Contributions loaded: {result.get('contributions_loaded', 0):,}")
        if result.get("contribution_chunks"):
            logger.info(f"Contribution chunks: {result.get('contribution_chunks'):,}")
        logger.info("=" * 80)
        logger.info(f"\nLog file: {log_file}")

    except Exception as e:
        logger.error(f"\n✗ BULK INGESTION FAILED: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
