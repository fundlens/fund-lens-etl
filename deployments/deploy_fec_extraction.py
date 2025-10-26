"""
Prefect deployment configuration for FEC contribution extraction.

This script creates a deployment that can be scheduled to run automatically.
"""

import asyncio
from pathlib import Path
from fund_lens_etl.flows.extract_fec_contributions import extract_fec_contributions_flow


async def deploy():
    """Deploy the FEC extraction flow with schedule."""
    # Get the project root directory
    project_root = Path(__file__).parent.parent

    # Load flow from source
    flow_with_source = await extract_fec_contributions_flow.from_source(
        source=str(project_root),
        entrypoint="fund_lens_etl/flows/extract_fec_contributions.py:extract_fec_contributions_flow"
    )

    # Deploy the flow
    await flow_with_source.deploy(
        name="md-2026-daily-extraction",  # Updated name
        version="2.0.0",  # Bumped version for incremental extraction
        tags=["fec", "bronze", "maryland", "production", "incremental"],  # Added incremental tag
        description="Daily incremental extraction of Maryland FEC contributions for 2025-2026 cycle",
        parameters={
            "state": "MD",
            "two_year_transaction_period": 2026,  # Changed from 2024 to 2026
            "max_results": 50000  # Set reasonable daily limit to avoid rate limits
        },
        cron="0 2 * * *",  # Run daily at 2 AM EST
        work_pool_name="default",
    )

    print("✅ Deployment created: md-2026-daily-extraction")
    print("   Schedule: Daily at 2 AM EST")
    print("   State: MD, Cycle: 2026 (active cycle)")
    print("   Mode: Incremental extraction (date-based)")
    print("   Max per run: 50,000 records")
    print(f"   Code location: {project_root}")


if __name__ == "__main__":
    asyncio.run(deploy())
