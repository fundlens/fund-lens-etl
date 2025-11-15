"""
Prefect deployment schedules for FundLens ETL pipeline.

This module defines scheduled deployments for:
- Bronze ingestion (daily incremental for ALL states)
- Silver transformation (triggered after bronze completes)
- Gold transformation (triggered after silver completes)
"""

from prefect.client.schemas.schedules import CronSchedule

# ============================================================================
# Deployment Parameters
# ============================================================================

# Default parameters for all deployments
DEFAULT_CYCLE = 2026

# Timezone for scheduling (FEC operates on US Eastern Time)
TIMEZONE = "America/New_York"


# ============================================================================
# Bronze Ingestion Deployments
# ============================================================================


def create_bronze_deployments():
    """
    Create and deploy bronze ingestion flows.

    Returns the number of deployments created.
    """
    from fund_lens_etl.flows.bronze_all_states_flow import bronze_all_states_flow

    # Daily incremental extraction for ALL states (weekdays only)
    bronze_all_states_flow.from_source(
        source="/opt/fund-lens-etl",
        entrypoint="fund_lens_etl/flows/bronze_all_states_flow.py:bronze_all_states_flow",
    ).deploy(
        name="bronze-ingestion-all-states-daily",
        work_pool_name="default",
        parameters={
            "election_cycle": DEFAULT_CYCLE,
            "full_refresh": False,
        },
        schedules=[
            CronSchedule(cron="0 2 * * 1-5", timezone=TIMEZONE)  # 2 AM ET, Monday-Friday
        ],
        tags=["etl", "bronze", "incremental", "daily", "all-states"],
        description="Daily incremental extraction for all 51 states with 90-day lookback",
        version="2.0.0",
    )

    return 1


# ============================================================================
# Silver Transformation Deployments
# ============================================================================


def create_silver_deployments():
    """
    Create and deploy silver transformation flows.

    Returns the number of deployments created.
    """
    from fund_lens_etl.flows.silver_transformation_flow import silver_transformation_flow

    # Daily silver transformation for all states (runs after bronze ingestion)
    silver_transformation_flow.from_source(
        source="/opt/fund-lens-etl",
        entrypoint="fund_lens_etl/flows/silver_transformation_flow.py:silver_transformation_flow",
    ).deploy(
        name="silver-transformation-all-states-daily",
        work_pool_name="default",
        parameters={
            "state": None,  # None = process all states
            "cycle": DEFAULT_CYCLE,
        },
        schedules=[
            CronSchedule(
                cron="0 6 * * 1-5", timezone=TIMEZONE
            )  # 6:00 AM ET, Monday-Friday (4 hours after bronze starts)
        ],
        tags=["etl", "silver", "transformation", "daily", "all-states"],
        description="Daily transformation of bronze data to silver layer for all states",
        version="2.0.0",
    )

    return 1


# ============================================================================
# Gold Transformation Deployments
# ============================================================================


def create_gold_deployments():
    """
    Create and deploy gold transformation flows.

    Returns the number of deployments created.
    """
    from fund_lens_etl.flows.gold_transformation_flow import gold_transformation_flow

    # Daily gold transformation for all states (runs after silver transformation)
    gold_transformation_flow.from_source(
        source="/opt/fund-lens-etl",
        entrypoint="fund_lens_etl/flows/gold_transformation_flow.py:gold_transformation_flow",
    ).deploy(
        name="gold-transformation-all-states-daily",
        work_pool_name="default",
        parameters={
            "state": None,  # None = process all states
            "cycle": DEFAULT_CYCLE,
        },
        schedules=[
            CronSchedule(
                cron="0 8 * * 1-5", timezone=TIMEZONE
            )  # 8:00 AM ET, Monday-Friday (2 hours after silver)
        ],
        tags=["etl", "gold", "analytics", "daily", "all-states"],
        description="Daily transformation of silver data to gold analytics layer for all states",
        version="2.0.0",
    )

    return 1


# ============================================================================
# Full Pipeline Deployment
# ============================================================================


def create_all_deployments():
    """
    Create all deployment configurations for the complete ETL pipeline.

    Returns:
        Total number of deployments created
    """
    total = 0
    total += create_bronze_deployments()
    total += create_silver_deployments()
    total += create_gold_deployments()
    return total


# ============================================================================
# Deployment Schedule Summary
# ============================================================================


def print_schedule_summary():
    """Print a human-readable summary of all deployment schedules."""
    print("=" * 80)
    print("FundLens ETL Pipeline - Deployment Schedules")
    print("=" * 80)
    print()
    print("DAILY PIPELINE (Monday-Friday) - ALL 51 STATES:")
    print("  2:00 AM ET - Bronze: Incremental ingestion (90-day lookback)")
    print("              Runtime: ~2-4 hours for all states")
    print("  6:00 AM ET - Silver: Transform bronze → silver")
    print("              Runtime: ~30-60 minutes")
    print("  8:00 AM ET - Gold: Transform silver → gold")
    print("              Runtime: ~30-60 minutes")
    print()
    print("CONFIGURATION:")
    print("  States: All 51 (50 states + DC)")
    print(f"  Cycle: {DEFAULT_CYCLE}")
    print(f"  Timezone: {TIMEZONE}")
    print("  Total Deployments: 3 (Bronze, Silver, Gold)")
    print()
    print("NOTE: No monthly full refresh. Daily incremental with 90-day lookback")
    print("      is sufficient to catch late filings and amendments.")
    print("=" * 80)
