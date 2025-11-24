#!/usr/bin/env python3
"""
Run gold transformation flow.

This script runs the gold transformation, converting Silver layer data
into the Gold dimensional model for analytics.

Usage:
    # Transform all Silver records
    poetry run python scripts/run_gold_transformation.py

    # Transform with filters
    poetry run python scripts/run_gold_transformation.py --state MD
    poetry run python scripts/run_gold_transformation.py --cycle 2026
"""

import argparse
import sys

from fund_lens_etl.flows.gold_transformation_flow import gold_transformation_flow


def main():
    parser = argparse.ArgumentParser(
        description="Run gold transformation flow",
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

    args = parser.parse_args()

    print("=" * 80)
    print("GOLD TRANSFORMATION")
    print("=" * 80)
    print(f"State filter: {args.state or 'None (all states)'}")
    print(f"Cycle filter: {args.cycle or 'None (all cycles)'}")
    print("=" * 80)
    print()

    # Run the flow
    try:
        results = gold_transformation_flow(
            state=args.state,
            cycle=args.cycle,
        )

        print("\n" + "=" * 80)
        print("TRANSFORMATION COMPLETE!")
        print("=" * 80)

        # Extract stats from results
        contributor_stats = results.get("contributor_stats", {})
        committee_stats = results.get("committee_stats", {})
        candidate_stats = results.get("candidate_stats", {})
        contribution_stats = results.get("contribution_stats", {})
        validation_stats = results.get("validation_stats", {})

        # Display entity counts
        print(
            f"Contributors: {contributor_stats.get('total_gold_contributors', 0):,} unique "
            f"(from {contributor_stats.get('total_silver_contributors', 0):,} silver records, "
            f"{contributor_stats.get('loaded_count', 0):,} new this run)"
        )
        print(
            f"Committees: {committee_stats.get('loaded_count', 0):,} new"
        )
        print(
            f"Candidates: {candidate_stats.get('loaded_count', 0):,} new"
        )
        print(
            f"Contributions: {contribution_stats.get('loaded_count', 0):,} new"
        )

        # Display any warnings
        if contribution_stats.get("unresolved_contributors", 0) > 0:
            print(
                f"\n⚠️  Warning: {contribution_stats['unresolved_contributors']:,} contributions "
                "skipped due to unresolved contributors"
            )
        if contribution_stats.get("unresolved_committees", 0) > 0:
            print(
                f"⚠️  Warning: {contribution_stats['unresolved_committees']:,} contributions "
                "skipped due to unresolved committees"
            )

        # Validation status
        print()
        if validation_stats.get("validation_passed", False):
            print("✅ Validation: PASSED")
        else:
            print(
                f"❌ Validation: FAILED ({validation_stats.get('total_errors', 0)} errors)"
            )

        if validation_stats.get("total_warnings", 0) > 0:
            print(f"⚠️  Warnings: {validation_stats['total_warnings']}")

        print("=" * 80)

        # Exit with appropriate code
        sys.exit(0 if results.get("success", False) else 1)

    except Exception as e:
        print(f"\n❌ Transformation failed: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
