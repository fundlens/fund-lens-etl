#!/usr/bin/env python3
"""
One-time full load script for all US states.

This script runs the bronze ingestion flow for all 51 states (50 states + DC)
with full_refresh=True. This is intended to be run ONCE to populate the database
initially before switching to daily incremental processing.

Expected runtime: 3-7 days depending on FEC API rate limits and data volume.

Usage:
    poetry run python scripts/run_full_load_all_states.py --cycle 2026

    # Or run directly with Prefect
    poetry run python scripts/run_full_load_all_states.py --cycle 2026 --deploy

The --deploy flag will create a one-time deployment that you can monitor via
the Prefect UI. Without it, it runs locally (blocking).
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def run_full_load_local(election_cycle: int):
    """Run the full load locally (blocking)."""
    from fund_lens_etl.flows.bronze_all_states_flow import bronze_all_states_flow

    print("=" * 80)
    print("FULL LOAD - ALL STATES")
    print("=" * 80)
    print(f"Election Cycle: {election_cycle}")
    print(f"Full Refresh: True")
    print(f"States: All 51 (50 states + DC)")
    print()
    print("WARNING: This will take 3-7 DAYS to complete!")
    print("         Make sure you have:")
    print("         - Stable internet connection")
    print("         - Sufficient disk space (~100+ GB recommended)")
    print("         - FEC API key configured in .env")
    print("=" * 80)
    print()

    response = input("Continue? (yes/no): ")
    if response.lower() != "yes":
        print("Aborted.")
        return

    print("\nStarting full load...")
    print("You can safely Ctrl+C and it will resume from where it left off.")
    print("(Each state's data is committed independently)")
    print()

    result = bronze_all_states_flow(
        election_cycle=election_cycle,
        full_refresh=True,
    )

    print("\n" + "=" * 80)
    print("FULL LOAD COMPLETE")
    print("=" * 80)
    print(f"Total states: {result['total_states']}")
    print(f"Successful: {result['successful_states']}")
    print(f"Failed: {len(result['failed_states'])}")
    if result["failed_states"]:
        print(f"Failed states: {', '.join(result['failed_states'])}")
    print(f"Total committees: {result['total_committees']:,}")
    print(f"Total candidates: {result['total_candidates']:,}")
    print(f"Total contributions: {result['total_contributions']:,}")
    print("=" * 80)


def deploy_full_load(election_cycle: int):
    """Create a one-time Prefect deployment for the full load."""
    from fund_lens_etl.flows.bronze_all_states_flow import bronze_all_states_flow

    print("=" * 80)
    print("DEPLOYING FULL LOAD TO PREFECT")
    print("=" * 80)
    print(f"Election Cycle: {election_cycle}")
    print()

    # Deploy as a one-time run (no schedule)
    bronze_all_states_flow.from_source(
        source="/opt/fund-lens-etl",
        entrypoint="fund_lens_etl/flows/bronze_all_states_flow.py:bronze_all_states_flow",
    ).deploy(
        name="bronze-full-load-all-states-onetime",
        work_pool_name="default",
        parameters={
            "election_cycle": election_cycle,
            "full_refresh": True,
        },
        schedules=[],  # No schedule - run manually
        tags=["etl", "bronze", "full-load", "all-states", "one-time"],
        description=f"One-time full load for all 51 states (cycle {election_cycle})",
        version="1.0.0",
    )

    print("\n✓ Deployment created: bronze-full-load-all-states-onetime")
    print()
    print("To run this deployment:")
    print("  1. Go to Prefect UI at http://<your-vm-ip>:4200")
    print("  2. Find deployment: 'bronze-full-load-all-states-onetime'")
    print("  3. Click 'Run' -> 'Quick Run'")
    print()
    print("Or run via CLI:")
    print(f"  prefect deployment run bronze-all-states-flow/bronze-full-load-all-states-onetime")
    print()
    print("Expected runtime: 3-7 days")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Run one-time full load for all US states",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--cycle",
        type=int,
        required=True,
        help="Election cycle year (e.g., 2026)",
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Deploy to Prefect instead of running locally",
    )

    args = parser.parse_args()

    if args.deploy:
        deploy_full_load(args.cycle)
    else:
        run_full_load_local(args.cycle)


if __name__ == "__main__":
    main()
