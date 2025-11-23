#!/usr/bin/env python3
"""
Run complete bulk ingestion including ALL contribution types.

This script ingests:
- Committees (cm.txt)
- Candidates (cn.txt)
- Individual→Committee contributions (itcont.txt) - ~9M records
- Committee→Candidate contributions (itpas2.txt) - ~100K records
- Committee→Committee contributions (itoth.txt) - ~millions of records

Usage:
    poetry run python scripts/run_complete_bulk_ingestion.py \\
        --data-dir data/2025-2026 \\
        --cycle 2026

    # Only load missing contribution types (if you already loaded indiv)
    poetry run python scripts/run_complete_bulk_ingestion.py \\
        --data-dir data/2025-2026 \\
        --cycle 2026 \\
        --contribution-types pas2 oth
"""

import argparse
import sys
from pathlib import Path

from fund_lens_etl.flows.bulk_ingestion_flow import bulk_ingestion_flow


def main():
    parser = argparse.ArgumentParser(
        description="Run complete bulk ingestion with all contribution types",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load everything (committees, candidates, all contribution types)
  %(prog)s --data-dir data/2025-2026 --cycle 2026

  # Load only committee→candidate and committee→committee contributions
  # (useful if you already loaded individual contributions)
  %(prog)s --data-dir data/2025-2026 --cycle 2026 \\
    --skip-committees --skip-candidates \\
    --contribution-types pas2 oth

  # Load only individual contributions (backwards compatible)
  %(prog)s --data-dir data/2025-2026 --cycle 2026 \\
    --contribution-types indiv

Contribution types:
  indiv - Individual → Committee contributions (~9M records, 1.6GB)
  pas2  - Committee → Candidate contributions (~100K records, 12MB)
  oth   - Committee → Committee contributions (~millions of records, 733MB)
        """,
    )
    parser.add_argument(
        "--data-dir",
        type=str,
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
        "--skip-committees",
        action="store_true",
        help="Skip loading committees",
    )
    parser.add_argument(
        "--skip-candidates",
        action="store_true",
        help="Skip loading candidates",
    )
    parser.add_argument(
        "--skip-contributions",
        action="store_true",
        help="Skip loading contributions",
    )
    parser.add_argument(
        "--contribution-types",
        nargs="+",
        choices=["indiv", "pas2", "oth"],
        default=None,
        help="Contribution types to load (default: all - indiv, pas2, oth)",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=10_000,
        help="Records per chunk for contributions (default: 10000)",
    )

    args = parser.parse_args()
    data_dir = Path(args.data_dir)

    # Validate data directory
    if not data_dir.exists():
        print(f"Error: Data directory does not exist: {data_dir}", file=sys.stderr)
        sys.exit(1)

    print("=" * 80)
    print("COMPLETE BULK INGESTION")
    print("=" * 80)
    print(f"Data directory: {data_dir}")
    print(f"Election cycle: {args.cycle}")
    print(f"Load committees: {not args.skip_committees}")
    print(f"Load candidates: {not args.skip_candidates}")
    print(f"Load contributions: {not args.skip_contributions}")
    if not args.skip_contributions:
        contrib_types = args.contribution_types or ["indiv", "pas2", "oth"]
        print(f"Contribution types: {', '.join(contrib_types)}")
    print(f"Chunk size: {args.chunksize:,}")
    print("=" * 80)
    print()

    # Run the flow
    try:
        results = bulk_ingestion_flow(
            data_dir=data_dir,
            election_cycle=args.cycle,
            load_committees=not args.skip_committees,
            load_candidates=not args.skip_candidates,
            load_contributions=not args.skip_contributions,
            contribution_types=args.contribution_types,
            contribution_chunksize=args.chunksize,
        )

        print("\n" + "=" * 80)
        print("INGESTION COMPLETE!")
        print("=" * 80)
        print(f"Committees loaded: {results.get('committees_loaded', 0):,}")
        print(f"Candidates loaded: {results.get('candidates_loaded', 0):,}")
        print(f"Contributions loaded: {results.get('contributions_loaded', 0):,}")
        print(f"Contributions skipped: {results.get('contributions_skipped', 0):,}")
        print(
            f"Contribution types processed: {', '.join(results.get('contribution_types_processed', []))}"
        )
        print("=" * 80)

        sys.exit(0)

    except Exception as e:
        print(f"\n❌ Ingestion failed: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
