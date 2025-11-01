#!/usr/bin/env python3
"""
Deploy FundLens ETL flows to Prefect.

Usage:
    python scripts/deploy.py --all              # Deploy all flows
    python scripts/deploy.py --bronze           # Deploy bronze flows only
    python scripts/deploy.py --silver           # Deploy silver flows only
    python scripts/deploy.py --gold             # Deploy gold flows only
    python scripts/deploy.py --summary          # Show schedule summary
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fund_lens_etl.deployments.schedules import (
    create_all_deployments,
    create_bronze_deployments,
    create_gold_deployments,
    create_silver_deployments,
    print_schedule_summary,
)


def deploy_flows(deploy_func, description: str):
    """
    Deploy flows using the provided deployment function.

    Args:
        deploy_func: Function that deploys flows and returns count
        description: Description of what's being deployed
    """
    print(f"\n{'=' * 80}")
    print(f"Deploying {description}")
    print(f"{'=' * 80}\n")

    try:
        count = deploy_func()
        print(f"\n✓ Successfully deployed {count} {description}")
        return True
    except Exception as e:
        print(f"\n✗ Failed to deploy {description}")
        print(f"  Error: {e}")
        return False


def main():
    """Main deployment script."""
    parser = argparse.ArgumentParser(
        description="Deploy FundLens ETL flows to Prefect",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Deploy all flows (bronze, silver, gold)",
    )
    parser.add_argument(
        "--bronze",
        action="store_true",
        help="Deploy bronze ingestion flows only",
    )
    parser.add_argument(
        "--silver",
        action="store_true",
        help="Deploy silver transformation flows only",
    )
    parser.add_argument(
        "--gold",
        action="store_true",
        help="Deploy gold transformation flows only",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show deployment schedule summary without deploying",
    )

    args = parser.parse_args()

    # Show summary if requested
    if args.summary:
        print_schedule_summary()
        return

    # Check if any deployment option was specified
    if not (args.all or args.bronze or args.silver or args.gold):
        parser.print_help()
        print("\nError: You must specify at least one deployment option.")
        print("Use --help for usage information.")
        sys.exit(1)

    # Deploy flows based on arguments
    success = True

    if args.all:
        print("\n🚀 Deploying all FundLens ETL flows...")
        print_schedule_summary()
        success = deploy_flows(create_all_deployments, "All Flows")
    else:
        if args.bronze:
            if not deploy_flows(create_bronze_deployments, "Bronze Ingestion Flows"):
                success = False

        if args.silver:
            if not deploy_flows(create_silver_deployments, "Silver Transformation Flows"):
                success = False

        if args.gold:
            if not deploy_flows(create_gold_deployments, "Gold Transformation Flows"):
                success = False

    # Final summary
    if success:
        print("\n✅ All deployments completed successfully!")
        print("\nNext steps:")
        print("  1. Start a Prefect worker: prefect worker start --pool default")
        print("  2. View deployments: prefect deployment ls")
        print("  3. Trigger a run: prefect deployment run '<flow-name>/<deployment-name>'")
        print("\nSchedules will run automatically based on the configured cron schedules.")
    else:
        print("\n❌ Some deployments failed. Check the errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
