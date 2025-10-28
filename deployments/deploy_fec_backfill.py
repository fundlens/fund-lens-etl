"""
Prefect deployment configuration for FEC backfill.

This creates a scheduled deployment for monthly sweeps to catch late-filed contributions.
"""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path

from fund_lens_etl.flows.backfill_fec_contributions import (
    backfill_fec_contributions_flow,
)


async def deploy():
    """Deploy the backfill flow with monthly schedule."""
    project_root = Path(__file__).parent.parent

    # Calculate 90 days ago for default parameters
    today = datetime.now()
    ninety_days_ago = today - timedelta(days=90)

    flow_with_source = await backfill_fec_contributions_flow.from_source(
        source=str(project_root),
        entrypoint="fund_lens_etl/flows/backfill_fec_contributions.py:backfill_fec_contributions_flow",
    )

    await flow_with_source.deploy(
        name="md-2026-backfill-sweep",
        version="1.0.0",
        tags=["fec", "bronze", "maryland", "backfill", "sweep"],
        description="Monthly 90-day backfill sweep to catch late-filed contributions",
        parameters={
            "state": "MD",
            "two_year_transaction_period": 2026,
            "start_date": ninety_days_ago.strftime("%Y-%m-%d"),  # 90 days ago
            "end_date": today.strftime("%Y-%m-%d"),  # Today
            "max_results": None,  # Check all in 90-day window
        },
        cron="0 3 1 * *",  # First day of each month at 3 AM
        work_pool_name="default",
    )

    print("✅ Backfill sweep deployment created: md-2026-backfill-sweep")
    print("   Schedule: Monthly on 1st at 3 AM")
    print("   State: MD, Cycle: 2026")
    print(
        f"   Window: Last 90 days ({ninety_days_ago.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')})"
    )
    print("   Catches late-filed backdated contributions")
    print(f"   Code location: {project_root}")


if __name__ == "__main__":
    asyncio.run(deploy())
