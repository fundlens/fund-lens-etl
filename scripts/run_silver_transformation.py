#!/usr/bin/env python3
"""
Run silver transformation flow.

This script runs the silver transformation without any filters,
processing all Bronze records.

Usage:
    # Transform all Bronze records
    poetry run python scripts/run_silver_transformation.py

    # Transform with filters
    poetry run python scripts/run_silver_transformation.py --state MD
    poetry run python scripts/run_silver_transformation.py --cycle 2026
    poetry run python scripts/run_silver_transformation.py --start-date 2024-01-01 --end-date 2024-12-31
"""

import argparse
import sys
from datetime import datetime

from fund_lens_etl.flows.silver_transformation_flow import silver_transformation_flow


def main():
    parser = argparse.ArgumentParser(
        description="Run silver transformation flow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--state",
        type=str,
        help="Filter by state code (e.g., MD)",
    )
    parser.add_argument(
        "--cycle",
        type=int,
        help="Filter by election cycle (e.g., 2026)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date filter (YYYY-MM-DD)",
    )

    args = parser.parse_args()

    # Parse dates if provided
    start_date = None
    end_date = None

    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        except ValueError:
            print(f"Error: Invalid start date format: {args.start_date}", file=sys.stderr)
            print("Expected format: YYYY-MM-DD", file=sys.stderr)
            sys.exit(1)

    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        except ValueError:
            print(f"Error: Invalid end date format: {args.end_date}", file=sys.stderr)
            print("Expected format: YYYY-MM-DD", file=sys.stderr)
            sys.exit(1)

    print("=" * 80)
    print("SILVER TRANSFORMATION")
    print("=" * 80)
    print(f"State filter: {args.state or 'None (all states)'}")
    print(f"Cycle filter: {args.cycle or 'None (all cycles)'}")
    print(f"Start date: {start_date or 'None (all dates)'}")
    print(f"End date: {end_date or 'None (all dates)'}")
    print("=" * 80)
    print()

    # Run the flow
    try:
        results = silver_transformation_flow(
            state=args.state,
            cycle=args.cycle,
            start_date=start_date,
            end_date=end_date,
        )

        print("\n" + "=" * 80)
        print("TRANSFORMATION COMPLETE!")
        print("=" * 80)

        # Extract counts from nested results structure
        transformations = results.get("transformations", {})
        summary = results.get("summary", {})

        committees = transformations.get("committees", {})
        candidates = transformations.get("candidates", {})
        contributions = transformations.get("contributions", {})

        print(f"Committees: {committees.get('bronze_records', 0):,} bronze → {committees.get('silver_records', 0):,} silver ({committees.get('skipped', 0):,} skipped)")
        print(f"Candidates: {candidates.get('bronze_records', 0):,} bronze → {candidates.get('silver_records', 0):,} silver ({candidates.get('skipped', 0):,} skipped)")
        print(f"Contributions: {contributions.get('bronze_records', 0):,} bronze → {contributions.get('silver_records', 0):,} silver ({contributions.get('skipped', 0):,} skipped)")
        print()
        print(f"Total Bronze processed: {summary.get('total_bronze_records', 0):,}")
        print(f"Total Silver created: {summary.get('total_silver_records', 0):,}")
        print(f"Total skipped (already exists): {summary.get('total_skipped', 0):,}")
        print("=" * 80)

        sys.exit(0)

    except Exception as e:
        print(f"\n❌ Transformation failed: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
